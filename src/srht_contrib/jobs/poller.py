from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from srht_contrib.config import Settings
from srht_contrib.models import ContributionEvent, ServiceBackfillState, SyncState, TrackedActor, TrackedRepository
from srht_contrib.schemas import NormalizedEvent
from srht_contrib.services.git import GitIngestionService
from srht_contrib.services.srht_client import SourceHutClientError
from srht_contrib.services.todo import TodoIngestionService
from srht_contrib.utils.retention import RETENTION_DAYS, prune_contribution_events
from srht_contrib.utils.repositories import canonicalize_repository_name


logger = logging.getLogger(__name__)
RECENT_BACKFILL_BATCHES_PER_SERVICE = 5


@dataclass(frozen=True)
class BackfillPlan:
    scope: str
    services: list
    batches_per_service: int
    since: datetime | None


class PollerService:
    def __init__(
        self,
        todo_service: TodoIngestionService,
        git_service: GitIngestionService,
        settings: Settings,
    ) -> None:
        self.todo_service = todo_service
        self.git_service = git_service
        self.settings = settings
        self._sync_overlap = timedelta(hours=settings.sync_overlap_hours)

    def poll_all(self, db: Session, actor: str, *, run_backfill: bool = True) -> int:
        self.track_actor_request(db, actor, update_last_requested=False)
        try:
            inserted = self._poll_actor(db, actor)
        except Exception as exc:
            db.rollback()
            self._update_tracked_actor_poll_state(db, actor, status="error", error=str(exc))
            db.commit()
            raise

        self._update_tracked_actor_poll_state(db, actor, status="indexed", error=None)
        if run_backfill:
            inserted += self._run_backfill_batches(db, actor)
        db.commit()
        return inserted

    def poll_tracked_actors(self, db: Session, default_actor: str | None = None) -> dict[str, int]:
        if default_actor:
            self.track_actor_request(db, default_actor, update_last_requested=False)
            db.commit()

        results: dict[str, int] = {}
        actors = db.scalars(
            select(TrackedActor)
            .where(TrackedActor.is_active.is_(True))
            .where(
                or_(
                    TrackedActor.next_poll_after.is_(None),
                    TrackedActor.next_poll_after <= datetime.now(tz=UTC),
                )
            )
            .order_by(
                TrackedActor.priority_boosted_at.is_not(None).desc(),
                TrackedActor.priority_boosted_at.desc(),
                TrackedActor.next_poll_after.is_(None).desc(),
                TrackedActor.next_poll_after,
                TrackedActor.queued_for_discovery_at,
                TrackedActor.actor,
            )
            .limit(self.settings.discovery_batch_size)
        ).all()
        for tracked_actor in actors:
            actor = tracked_actor.actor
            run_backfill_only = (
                tracked_actor.last_polled_at is not None
                and tracked_actor.recent_backfill_status != "completed"
            )
            claimed_actor = self.track_actor_request(db, actor, update_last_requested=False)
            claimed_actor.discovery_state = "in_progress"
            claimed_actor.last_claimed_at = datetime.now(tz=UTC)
            claimed_actor.poll_attempts += 1
            db.add(claimed_actor)
            db.commit()
            try:
                if run_backfill_only:
                    results[actor] = self.poll_recent_backfill(db, actor)
                else:
                    results[actor] = self.poll_all(db, actor, run_backfill=False)
                    self._schedule_pending_backfill_or_repoll(db, actor)
                    db.commit()
            except SourceHutClientError:
                logger.exception("Scheduled poll failed for actor=%s", actor)
            except Exception:
                logger.exception("Unexpected scheduled poll failure for actor=%s", actor)
        deleted = self.prune_old_events(db)
        if deleted:
            logger.info("Pruned %s contribution events older than %s days", deleted, RETENTION_DAYS)
        return results

    def track_actor_request(
        self,
        db: Session,
        actor: str,
        *,
        update_last_requested: bool = True,
        prioritize: bool = False,
    ) -> TrackedActor:
        tracked_actor = db.scalar(select(TrackedActor).where(TrackedActor.actor == actor))
        now = datetime.now(tz=UTC)
        if tracked_actor is None:
            tracked_actor = TrackedActor(
                actor=actor,
                is_active=True,
                discovery_state="queued",
                queued_for_discovery_at=now,
                next_poll_after=now,
                recent_backfill_status="pending",
            )
            db.add(tracked_actor)

        tracked_actor.is_active = True
        if tracked_actor.queued_for_discovery_at is None:
            tracked_actor.queued_for_discovery_at = now
        if tracked_actor.next_poll_after is None:
            tracked_actor.next_poll_after = now
        if update_last_requested:
            tracked_actor.last_requested_at = now
        if prioritize:
            tracked_actor.priority_boosted_at = now
            tracked_actor.next_poll_after = now
        db.flush()
        return tracked_actor

    def _poll_actor(self, db: Session, actor: str) -> int:
        inserted = 0
        inserted += self._poll_service(db, actor, self.todo_service.service_name, self.todo_service.fetch_recent_events)
        self._sync_tracked_repositories(db, actor)
        inserted += self._poll_service(
            db,
            actor,
            self.git_service.service_name,
            lambda actor, since: self.git_service.fetch_recent_events(
                actor=actor,
                since=since,
                db=db,
            ),
        )
        return inserted

    def _poll_service(self, db: Session, actor: str, service_name: str, fetcher) -> int:
        state = db.scalar(
            select(SyncState).where(SyncState.service == service_name).where(SyncState.actor == actor)
        )
        since = datetime.now(tz=UTC) - timedelta(days=30)
        if state and state.cursor_value:
            since = datetime.fromisoformat(state.cursor_value.replace("Z", "+00:00")).astimezone(UTC) - self._sync_overlap
            logger.info(
                "Using sync cursor for %s actor=%s with overlap; since=%s",
                service_name,
                actor,
                since.isoformat(),
            )

        result = fetcher(actor=actor, since=since)
        inserted = self._insert_events(db, result.events)
        self._upsert_sync_state(db, service_name, actor, result.cursor)
        logger.info("Polled %s for %s: inserted=%s", service_name, actor, inserted)
        return inserted

    @staticmethod
    def _insert_events(db: Session, events: list[NormalizedEvent]) -> int:
        inserted = 0
        for event in events:
            try:
                with db.begin_nested():
                    model = ContributionEvent(**event.model_dump())
                    db.add(model)
                    db.flush()
                    inserted += 1
            except IntegrityError:
                logger.info("Skipping duplicate event %s for service %s", event.external_uid, event.service)
        return inserted

    @staticmethod
    def _upsert_sync_state(db: Session, service: str, actor: str, cursor_value: str) -> None:
        state = db.scalar(select(SyncState).where(SyncState.service == service).where(SyncState.actor == actor))
        now = datetime.now(tz=UTC)
        if state is None:
            db.add(SyncState(service=service, actor=actor, cursor_value=cursor_value, updated_at=now))
            db.flush()
            return

        state.cursor_value = cursor_value
        state.updated_at = now
        db.add(state)
        db.flush()

    def _sync_tracked_repositories(self, db: Session, actor: str) -> None:
        configured = self.git_service.settings.git_tracked_repositories
        for repo_name in configured:
            canonical_repo_name = canonicalize_repository_name(actor, repo_name)
            existing = db.scalar(
                select(TrackedRepository)
                .where(TrackedRepository.service == self.git_service.service_name)
                .where(TrackedRepository.actor == actor)
                .where(TrackedRepository.repo_name == canonical_repo_name)
            )
            if existing is None:
                db.add(
                    TrackedRepository(
                        service=self.git_service.service_name,
                        repo_name=canonical_repo_name,
                        actor=actor,
                    )
                )
        db.flush()

    def _update_tracked_actor_poll_state(self, db: Session, actor: str, status: str, error: str | None) -> None:
        tracked_actor = self.track_actor_request(db, actor, update_last_requested=False)
        tracked_actor.last_poll_status = status
        tracked_actor.last_poll_error = error
        now = datetime.now(tz=UTC)
        if status == "indexed":
            tracked_actor.discovery_state = "indexed"
            tracked_actor.last_polled_at = now
            tracked_actor.next_poll_after = now + timedelta(seconds=self.settings.indexed_actor_repoll_seconds)
            tracked_actor.priority_boosted_at = None
            tracked_actor.poll_attempts = 0
        elif status == "error":
            tracked_actor.discovery_state = "error"
            backoff_seconds = min(
                self.settings.discovery_error_backoff_seconds * max(tracked_actor.poll_attempts, 1),
                self.settings.discovery_error_backoff_max_seconds,
            )
            tracked_actor.next_poll_after = now + timedelta(seconds=backoff_seconds)
            tracked_actor.priority_boosted_at = None
        db.add(tracked_actor)
        db.flush()

    def poll_recent_backfill(self, db: Session, actor: str) -> int:
        try:
            inserted = self._run_backfill_batches(db, actor)
        except Exception as exc:
            db.rollback()
            self._update_tracked_actor_poll_state(db, actor, status="error", error=str(exc))
            db.commit()
            raise

        self._schedule_pending_backfill_or_repoll(db, actor)
        db.commit()
        return inserted

    def _schedule_pending_backfill_or_repoll(self, db: Session, actor: str) -> None:
        tracked_actor = self.track_actor_request(db, actor, update_last_requested=False)
        now = datetime.now(tz=UTC)
        tracked_actor.discovery_state = "indexed"
        tracked_actor.last_poll_status = "indexed"
        tracked_actor.last_poll_error = None
        tracked_actor.priority_boosted_at = None
        tracked_actor.poll_attempts = 0
        if tracked_actor.recent_backfill_status == "completed":
            tracked_actor.next_poll_after = now + timedelta(seconds=self.settings.indexed_actor_repoll_seconds)
        else:
            tracked_actor.next_poll_after = now
        db.add(tracked_actor)
        db.flush()

    def _run_backfill_batches(self, db: Session, actor: str) -> int:
        tracked_actor = self.track_actor_request(db, actor, update_last_requested=False)
        if tracked_actor.recent_backfill_status == "completed":
            return 0

        now = datetime.now(tz=UTC)
        recent_since = now - timedelta(days=RETENTION_DAYS)
        db.add(tracked_actor)
        db.flush()
        total_inserted = 0

        recent_services = [
            (self.todo_service.service_name, self.todo_service.fetch_recent_backfill_batch),
            (self.git_service.service_name, self.git_service.fetch_recent_backfill_batch),
        ]
        if tracked_actor.recent_backfill_status != "completed":
            if tracked_actor.recent_backfill_started_at is None:
                tracked_actor.recent_backfill_started_at = now
            tracked_actor.recent_backfill_status = "in_progress"
            tracked_actor.last_recent_backfill_error = None
            total_inserted += self._run_backfill_scope(
                db,
                actor,
                BackfillPlan(
                    scope="recent",
                    services=recent_services,
                    batches_per_service=RECENT_BACKFILL_BATCHES_PER_SERVICE,
                    since=recent_since,
                ),
            )
            recent_statuses = db.scalars(
                select(ServiceBackfillState.status)
                .where(ServiceBackfillState.actor == actor)
                .where(ServiceBackfillState.scope == "recent")
            ).all()
            if recent_statuses and all(status == "completed" for status in recent_statuses):
                tracked_actor.recent_backfill_status = "completed"
                tracked_actor.recent_backfill_completed_at = datetime.now(tz=UTC)
                tracked_actor.last_recent_backfill_error = None
            db.add(tracked_actor)
            db.flush()

        return total_inserted

    def _run_backfill_scope(self, db: Session, actor: str, plan: BackfillPlan) -> int:
        tracked_actor = self.track_actor_request(db, actor, update_last_requested=False)
        total_inserted = 0
        for service_name, fetcher in plan.services:
            state = self._backfill_state(db, actor, service_name, plan.scope)
            if state.status == "completed":
                continue

            if state.started_at is None:
                state.started_at = datetime.now(tz=UTC)
            state.status = "in_progress"
            state.updated_at = datetime.now(tz=UTC)

            try:
                total_inserted += self._run_service_batches(db, actor, state, fetcher, plan)
            except Exception as exc:
                state.status = "error"
                state.last_error = str(exc)
                state.updated_at = datetime.now(tz=UTC)
                tracked_actor.recent_backfill_status = "error"
                tracked_actor.last_recent_backfill_error = str(exc)
                db.add(state)
                db.add(tracked_actor)
                db.flush()
                raise

            db.add(state)
            db.flush()

        return total_inserted

    @staticmethod
    def _backfill_state(db: Session, actor: str, service_name: str, scope: str) -> ServiceBackfillState:
        state = db.scalar(
            select(ServiceBackfillState)
            .where(ServiceBackfillState.actor == actor)
            .where(ServiceBackfillState.service == service_name)
            .where(ServiceBackfillState.scope == scope)
        )
        if state is None:
            state = ServiceBackfillState(
                actor=actor,
                service=service_name,
                scope=scope,
                cursor_json=None,
                status="pending",
                started_at=None,
                completed_at=None,
                last_error=None,
                updated_at=datetime.now(tz=UTC),
            )
            db.add(state)
            db.flush()
        return state

    def _run_service_batches(
        self, db: Session, actor: str, state: ServiceBackfillState, fetcher, plan: BackfillPlan
    ) -> int:
        """Run up to plan.batches_per_service batches for one service; returns rows inserted."""
        total_inserted = 0
        for _ in range(plan.batches_per_service):
            if plan.since is None:
                result = fetcher(actor=actor, cursor_state=state.cursor_json)
            else:
                result = fetcher(actor=actor, cursor_state=state.cursor_json, since=plan.since)
            inserted = self._insert_events(db, result.events)
            total_inserted += inserted
            state.cursor_json = copy.deepcopy(result.cursor_state)
            state.last_error = None
            state.updated_at = datetime.now(tz=UTC)
            if result.complete:
                state.status = "completed"
                state.completed_at = datetime.now(tz=UTC)
                logger.info(
                    "Backfill complete for scope=%s service=%s actor=%s inserted=%s",
                    plan.scope,
                    state.service,
                    actor,
                    inserted,
                )
                break
            logger.info(
                "Backfill batch complete for scope=%s service=%s actor=%s inserted=%s",
                plan.scope,
                state.service,
                actor,
                inserted,
            )
        return total_inserted

    def prune_old_events(self, db: Session) -> int:
        deleted = prune_contribution_events(db)
        db.commit()
        return deleted
