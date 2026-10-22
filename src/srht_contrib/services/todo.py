from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
from typing import Any

from srht_contrib.config import Settings
from srht_contrib.schemas import NormalizedEvent
from srht_contrib.services.srht_client import SourceHutGraphQLClient
from srht_contrib.services.types import BackfillBatchResult
from srht_contrib.utils.dates import ensure_utc, parse_datetime


logger = logging.getLogger(__name__)


TODO_ACTIVITY_QUERY = """
query TodoActivity($cursor: Cursor) {
  me {
    canonicalName
  }
  events(cursor: $cursor) {
    results {
      id
      created
      ticket {
        id
        ref
        status
        resolution
        tracker {
          name
        }
      }
      changes {
        __typename
        eventType
        ticket {
          id
        }
        ... on Created {
          author {
            canonicalName
          }
        }
        ... on Comment {
          author {
            canonicalName
          }
        }
        ... on StatusChange {
          editor {
            canonicalName
          }
          oldStatus
          newStatus
          oldResolution
          newResolution
        }
      }
    }
    cursor
  }
}
""".strip()

TODO_TRACKERS_QUERY = """
query TodoTrackers($cursor: Cursor) {
  me {
    canonicalName
    trackers(cursor: $cursor) {
      results {
        id
        rid
        name
      }
      cursor
    }
  }
}
""".strip()

TODO_TRACKER_TICKETS_QUERY = """
query TodoTrackerTickets($trackerRid: ID!, $cursor: Cursor) {
  tracker(rid: $trackerRid) {
    id
    name
    tickets(cursor: $cursor) {
      results {
        id
        ref
        created
        updated
        status
        resolution
        submitter {
          canonicalName
        }
      }
      cursor
    }
  }
}
""".strip()

TODO_TICKET_EVENTS_QUERY = """
query TodoTicketEvents($trackerRid: ID!, $ticketId: Int!, $cursor: Cursor) {
  tracker(rid: $trackerRid) {
    id
    name
    ticket(id: $ticketId) {
      id
      ref
      status
      resolution
      events(cursor: $cursor) {
        results {
          id
          created
          changes {
            __typename
            eventType
            ticket {
              id
            }
            ... on Created {
              author {
                canonicalName
              }
            }
            ... on Comment {
              author {
                canonicalName
              }
            }
            ... on StatusChange {
              editor {
                canonicalName
              }
              oldStatus
              newStatus
              oldResolution
              newResolution
            }
          }
        }
        cursor
      }
    }
  }
}
""".strip()


TICKET_CLOSED_STATUSES = {"RESOLVED"}
TICKET_CLOSED_RESOLUTIONS = {
    "CLOSED",
    "FIXED",
    "IMPLEMENTED",
    "WONT_FIX",
    "BY_DESIGN",
    "INVALID",
    "DUPLICATE",
    "NOT_OUR_BUG",
}


class TodoSchemaError(RuntimeError):
    """Raised when SourceHut returns an unexpected todo event shape."""


def _safe_nested_name(entity: dict[str, Any] | None) -> str | None:
    if not entity:
        return None
    return entity.get("canonicalName") or entity.get("name")


def _repo_name_from_event(event: dict[str, Any]) -> str | None:
    ticket = event.get("ticket") or {}
    tracker = ticket.get("tracker") or {}
    return tracker.get("name")


def _resource_id_from_event(event: dict[str, Any]) -> str:
    ticket = event.get("ticket") or {}
    return str(ticket.get("ref") or ticket.get("id") or event["id"])


@dataclass(frozen=True)
class TrackerRef:
    id: str
    rid: str
    name: str | None


def _change_ticket_id(change: dict[str, Any], event: dict[str, Any]) -> str:
    ticket = change.get("ticket") or event.get("ticket") or {}
    return str(ticket.get("id") or event["id"])


def _change_kind(change: dict[str, Any], actor: str) -> str | None:
    """created, comment, or closed for a change the actor made that counts, else None."""
    change_type = change.get("__typename")
    if change_type == "Created" and _safe_nested_name(change.get("author")) == actor:
        return "created"
    if change_type == "Comment" and _safe_nested_name(change.get("author")) == actor:
        return "comment"
    if change_type == "StatusChange" and _safe_nested_name(change.get("editor")) == actor:
        if change.get("newStatus") in TICKET_CLOSED_STATUSES or change.get("newResolution") in TICKET_CLOSED_RESOLUTIONS:
            return "closed"
    return None


def _normalize_event_change(
    *,
    settings: Settings,
    actor: str,
    event: dict[str, Any],
    change: dict[str, Any],
) -> NormalizedEvent | None:
    kind = _change_kind(change, actor)
    if kind is None:
        return None
    event_type = f"ticket_{kind}"
    return NormalizedEvent(
        service="todo",
        event_type=event_type,
        actor=actor,
        repo_name=_repo_name_from_event(event),
        resource_id=_resource_id_from_event(event),
        external_uid=f"todo:event:{event['id']}:{kind}:{_change_ticket_id(change, event)}",
        occurred_at=parse_datetime(event["created"]),
        weight=settings.event_weights[event_type],
        raw_payload_json={"event": event, "change": change},
    )


def _log_ticket_event(ticket_ref: str, event: dict[str, Any], occurred_at: datetime) -> None:
    change_list = event.get("changes") or []
    logger.info(
        "todo ticket event ref=%s id=%s occurred_at=%s changes=%s",
        ticket_ref,
        str(event.get("id")),
        occurred_at.isoformat(),
        len(change_list) if isinstance(change_list, list) else "unknown",
    )


def _log_ticket_change(
    ticket_ref: str, event_id: str, change: dict[str, Any], normalized: NormalizedEvent | None
) -> None:
    logger.info(
        "todo ticket change ref=%s event_id=%s type=%s eventType=%s author=%s editor=%s newStatus=%s newResolution=%s",
        ticket_ref,
        event_id,
        change.get("__typename"),
        change.get("eventType"),
        _safe_nested_name(change.get("author")),
        _safe_nested_name(change.get("editor")),
        change.get("newStatus"),
        change.get("newResolution"),
    )
    if normalized is not None:
        logger.info(
            "todo ticket change accepted ref=%s event_id=%s normalized_type=%s",
            ticket_ref,
            event_id,
            normalized.event_type,
        )


def _extract_event_cursor_page(data: dict[str, Any]) -> tuple[str | None, list[dict[str, Any]], str]:
    me = data.get("me") or {}
    canonical_actor = me.get("canonicalName")
    if not canonical_actor:
        raise TodoSchemaError("todo.sr.ht response did not include me.canonicalName")

    events = data.get("events") or {}
    results = events.get("results") or []
    if not isinstance(results, list):
        raise TodoSchemaError("todo.sr.ht response did not include events.results")

    return events.get("cursor"), [event for event in results if isinstance(event, dict)], canonical_actor


@dataclass(slots=True)
class TodoPollResult:
    events: list[NormalizedEvent]
    cursor: str


class TodoIngestionService:
    """Fetches todo.sr.ht activity from the authenticated event feed and normalizes it."""

    service_name = "todo"

    def __init__(self, client: SourceHutGraphQLClient, settings: Settings) -> None:
        self.client = client
        self.settings = settings

    def fetch_recent_events(self, actor: str, since: datetime | None = None) -> TodoPollResult:
        since_dt = ensure_utc(since or (datetime.now(tz=UTC) - timedelta(days=30)))
        cursor_time = datetime.now(tz=UTC).isoformat()
        feed_result = self._fetch_from_activity_feed(actor=actor, since=since_dt)
        if feed_result.events:
            return TodoPollResult(events=feed_result.events, cursor=cursor_time)

        logger.info(
            "todo activity feed returned no normalized events for actor=%s; falling back to tracker crawl",
            actor,
        )
        tracker_events = self._fetch_from_trackers(actor=actor, since=since_dt)
        return TodoPollResult(events=tracker_events, cursor=cursor_time)

    def fetch_backfill_batch(self, actor: str, cursor_state: dict | None = None) -> BackfillBatchResult:
        return self._fetch_backfill_batch(actor, cursor_state, since=None)

    def fetch_recent_backfill_batch(
        self,
        actor: str,
        cursor_state: dict | None = None,
        *,
        since: datetime,
    ) -> BackfillBatchResult:
        return self._fetch_backfill_batch(actor, cursor_state, since=since)

    def _fetch_backfill_batch(
        self,
        actor: str,
        cursor_state: dict | None,
        *,
        since: datetime | None,
    ) -> BackfillBatchResult:
        state = {
            "trackers_cursor": None,
            "tracker_queue": [],
            "current_tracker": None,
            "current_ticket": None,
            "trackers_loaded": False,
        }
        if cursor_state:
            state.update(copy.deepcopy(cursor_state))

        if not state["trackers_loaded"]:
            return self._backfill_tracker_page(actor, state)

        if state["current_tracker"] is None:
            if not state["tracker_queue"]:
                return BackfillBatchResult(events=[], cursor_state=None, complete=True)
            state["current_tracker"] = state["tracker_queue"].pop(0)

        current_tracker = state["current_tracker"]
        if state["current_ticket"] is None and not current_tracker["pending_tickets"]:
            return self._backfill_ticket_page(state, since)

        if state["current_ticket"] is None:
            if current_tracker["pending_tickets"]:
                next_ticket = current_tracker["pending_tickets"].pop(0)
                state["current_ticket"] = {**next_ticket, "cursor": None}
            else:
                state["current_tracker"] = None
                return BackfillBatchResult(events=[], cursor_state=state, complete=False)

        return self._backfill_event_page(actor, state, since)

    def _backfill_tracker_page(self, actor: str, state: dict) -> BackfillBatchResult:
        """Read one page of the actor's trackers into the queue."""
        data = self.client.execute(TODO_TRACKERS_QUERY, {"cursor": state["trackers_cursor"]})
        me = data.get("me") or {}
        trackers_page = me.get("trackers") or {}
        results = trackers_page.get("results") or []
        state["trackers_cursor"] = trackers_page.get("cursor")
        for tracker in results:
            if not isinstance(tracker, dict):
                continue
            state["tracker_queue"].append(
                {
                    "id": str(tracker.get("id")),
                    "rid": str(tracker.get("rid")),
                    "name": tracker.get("name"),
                    "tickets_cursor": None,
                    "pending_tickets": [],
                }
            )
        state["trackers_loaded"] = not bool(state["trackers_cursor"])
        logger.info(
            "todo backfill tracker discovery actor=%s trackers=%s next_cursor=%s queue=%s",
            actor,
            len(results),
            bool(state["trackers_cursor"]),
            len(state["tracker_queue"]),
        )
        complete = state["trackers_loaded"] and not state["tracker_queue"]
        return BackfillBatchResult(events=[], cursor_state=None if complete else state, complete=complete)

    def _backfill_ticket_page(self, state: dict, since: datetime | None) -> BackfillBatchResult:
        """Queue one page of the current tracker's tickets updated since `since`."""
        current_tracker = state["current_tracker"]
        data = self.client.execute(
            TODO_TRACKER_TICKETS_QUERY,
            {"trackerRid": current_tracker["rid"], "cursor": current_tracker["tickets_cursor"]},
        )
        tracker = data.get("tracker") or {}
        tickets_page = tracker.get("tickets") or {}
        tickets = tickets_page.get("results") or []
        next_tickets_cursor = tickets_page.get("cursor")
        stop_tracker_paging = False
        for ticket in tickets:
            if not isinstance(ticket, dict):
                continue
            if since is not None and parse_datetime(ticket["updated"]) < since:
                stop_tracker_paging = True
                continue
            current_tracker["pending_tickets"].append(
                {"id": int(ticket["id"]), "ref": str(ticket.get("ref") or ticket["id"])}
            )
        current_tracker["tickets_cursor"] = None if stop_tracker_paging else next_tickets_cursor
        logger.info(
            "todo backfill tracker=%s ticket page count=%s next_cursor=%s pending_tickets=%s",
            current_tracker.get("name") or current_tracker.get("id"),
            len(tickets),
            bool(current_tracker["tickets_cursor"]),
            len(current_tracker["pending_tickets"]),
        )
        if not current_tracker["pending_tickets"] and not current_tracker["tickets_cursor"]:
            state["current_tracker"] = None
        return BackfillBatchResult(events=[], cursor_state=state, complete=False)

    def _backfill_event_page(self, actor: str, state: dict, since: datetime | None) -> BackfillBatchResult:
        """Read one page of the current ticket's events."""
        current_tracker = state["current_tracker"]
        current_ticket = state["current_ticket"]
        data = self.client.execute(
            TODO_TICKET_EVENTS_QUERY,
            {"trackerRid": current_tracker["rid"], "ticketId": current_ticket["id"], "cursor": current_ticket["cursor"]},
        )
        tracker = data.get("tracker") or {}
        ticket_payload = (tracker.get("ticket") or {}) if isinstance(tracker, dict) else {}
        event_page = ticket_payload.get("events") or {}
        page_events = event_page.get("results") or []
        next_cursor = event_page.get("cursor")
        logger.info(
            "todo backfill ticket events ref=%s tracker=%s count=%s next_cursor=%s",
            current_ticket["ref"],
            current_tracker.get("name") or current_tracker.get("id"),
            len(page_events),
            bool(next_cursor),
        )

        ticket_context = {
            "id": ticket_payload.get("id", current_ticket["id"]),
            "ref": ticket_payload.get("ref", current_ticket["ref"]),
            "status": ticket_payload.get("status"),
            "resolution": ticket_payload.get("resolution"),
            "tracker": {"name": current_tracker.get("name")},
        }
        events, stop_ticket_paging = self._page_changes(actor, page_events, ticket_context, since)

        if next_cursor and not stop_ticket_paging:
            state["current_ticket"]["cursor"] = next_cursor
        else:
            state["current_ticket"] = None
            if not current_tracker["pending_tickets"] and not current_tracker["tickets_cursor"]:
                state["current_tracker"] = None

        complete = (
            state["trackers_loaded"]
            and not state["tracker_queue"]
            and state["current_tracker"] is None
            and state["current_ticket"] is None
        )
        return BackfillBatchResult(events=events, cursor_state=None if complete else state, complete=complete)

    def _page_changes(
        self,
        actor: str,
        page_events: list,
        ticket_context: dict,
        since: datetime | None,
        log_ref: str | None = None,
    ) -> tuple[list[NormalizedEvent], bool]:
        """The actor's changes in a page of ticket events, and whether one predates
        `since`. Events and changes are logged under `log_ref` when it is given."""
        events: list[NormalizedEvent] = []
        stop = False
        for event in page_events:
            if not isinstance(event, dict):
                continue
            event["ticket"] = dict(ticket_context, tracker=dict(ticket_context["tracker"]))
            occurred_at = parse_datetime(event["created"])
            if log_ref is not None:
                _log_ticket_event(log_ref, event, occurred_at)
            if since is not None and occurred_at < since:
                stop = True
                continue
            events.extend(self._ticket_event_changes(actor, event, log_ref))
        return events, stop

    def _fetch_from_activity_feed(self, actor: str, since: datetime) -> TodoPollResult:
        events: list[NormalizedEvent] = []
        cursor: str | None = None
        effective_actor = actor

        for _ in range(10):
            data = self.client.execute(TODO_ACTIVITY_QUERY, {"cursor": cursor})
            cursor, page_events, canonical_actor = _extract_event_cursor_page(data)
            effective_actor = actor or canonical_actor
            logger.info(
                "todo page fetched for actor=%s canonical_actor=%s events=%s next_cursor=%s since=%s",
                actor,
                canonical_actor,
                len(page_events),
                bool(cursor),
                since.isoformat(),
            )

            stop_paging = False
            for event in page_events:
                event_events, before_since = self._activity_event(event, effective_actor, since)
                stop_paging = stop_paging or before_since
                events.extend(event_events)

            if stop_paging or not cursor:
                break

        logger.info("todo poll complete for actor=%s normalized_events=%s", effective_actor, len(events))
        return TodoPollResult(events=events, cursor=datetime.now(tz=UTC).isoformat())

    def _fetch_from_trackers(self, actor: str, since: datetime) -> list[NormalizedEvent]:
        data = self.client.execute(TODO_TRACKERS_QUERY, {"cursor": None})
        me = data.get("me") or {}
        canonical_actor = me.get("canonicalName") or actor
        trackers = ((me.get("trackers") or {}).get("results")) or []
        logger.info("todo tracker crawl actor=%s trackers=%s", canonical_actor, len(trackers))

        events: list[NormalizedEvent] = []
        for tracker in trackers:
            if not isinstance(tracker, dict):
                continue
            tracker_ref = TrackerRef(id=str(tracker.get("id")), rid=str(tracker.get("rid")), name=tracker.get("name"))
            tracker_events = self._fetch_tracker_tickets(actor=canonical_actor, tracker=tracker_ref, since=since)
            events.extend(tracker_events)

        logger.info("todo tracker crawl complete actor=%s normalized_events=%s", canonical_actor, len(events))
        return events

    def _fetch_tracker_tickets(
        self,
        *,
        actor: str,
        tracker: TrackerRef,
        since: datetime,
    ) -> list[NormalizedEvent]:
        tracker_ref = tracker
        events: list[NormalizedEvent] = []
        cursor: str | None = None

        for _ in range(50):
            data = self.client.execute(
                TODO_TRACKER_TICKETS_QUERY,
                {"trackerRid": tracker_ref.rid, "cursor": cursor},
            )
            tracker = data.get("tracker") or {}
            tickets_page = tracker.get("tickets") or {}
            tickets = tickets_page.get("results") or []
            cursor = tickets_page.get("cursor")
            logger.info(
                "todo tracker=%s ticket page count=%s next_cursor=%s",
                tracker_ref.name or tracker_ref.id,
                len(tickets),
                bool(cursor),
            )

            stop_paging = False
            for ticket in tickets:
                if not isinstance(ticket, dict):
                    continue
                updated_at = parse_datetime(ticket["updated"])
                if updated_at < since:
                    stop_paging = True
                    logger.info(
                        "todo ticket ref=%s skipped because updated_at=%s is before since=%s",
                        ticket.get("ref"),
                        updated_at.isoformat(),
                        since.isoformat(),
                    )
                    continue
                events.extend(self._fetch_ticket_events(actor=actor, tracker=tracker_ref, ticket=ticket, since=since))

            if stop_paging or not cursor:
                break

        return events

    def _fetch_ticket_events(
        self,
        *,
        actor: str,
        tracker: TrackerRef,
        ticket: dict[str, Any],
        since: datetime,
    ) -> list[NormalizedEvent]:
        tracker_ref = tracker
        events: list[NormalizedEvent] = []
        cursor: str | None = None
        ticket_id = int(ticket["id"])
        ticket_ref = str(ticket.get("ref") or ticket_id)

        for _ in range(50):
            data = self.client.execute(
                TODO_TICKET_EVENTS_QUERY,
                {"trackerRid": tracker_ref.rid, "ticketId": ticket_id, "cursor": cursor},
            )
            tracker = data.get("tracker") or {}
            ticket_payload = (tracker.get("ticket") or {}) if isinstance(tracker, dict) else {}
            event_page = ticket_payload.get("events") or {}
            page_events = event_page.get("results") or []
            cursor = event_page.get("cursor")
            logger.info(
                "todo ticket events ref=%s tracker=%s count=%s next_cursor=%s",
                ticket_ref,
                tracker_ref.name or tracker_ref.id,
                len(page_events),
                bool(cursor),
            )

            ticket_context = {
                "id": ticket_payload.get("id", ticket.get("id")),
                "ref": ticket_payload.get("ref", ticket_ref),
                "status": ticket_payload.get("status", ticket.get("status")),
                "resolution": ticket_payload.get("resolution", ticket.get("resolution")),
                "tracker": {"name": tracker_ref.name},
            }
            page_changes, stop_paging = self._page_changes(actor, page_events, ticket_context, since, log_ref=ticket_ref)
            events.extend(page_changes)
            if stop_paging or not cursor:
                break

        return events

    def _ticket_event_changes(
        self, actor: str, event: dict[str, Any], ticket_ref: str | None
    ) -> list[NormalizedEvent]:
        """The actor's normalized changes in one ticket event, logged when `ticket_ref` is given."""
        events: list[NormalizedEvent] = []
        event_id = str(event.get("id"))
        for change in event.get("changes") or []:
            if not isinstance(change, dict):
                continue
            normalized = _normalize_event_change(
                settings=self.settings,
                actor=actor,
                event=event,
                change=change,
            )
            if ticket_ref is not None:
                _log_ticket_change(ticket_ref, event_id, change, normalized)
            if normalized is not None:
                events.append(normalized)
        return events

    def _activity_event(
        self, event: dict[str, Any], actor: str, since: datetime
    ) -> tuple[list[NormalizedEvent], bool]:
        """The actor's changes in one activity-feed event, and whether it predates `since`."""
        occurred_at = parse_datetime(event["created"])
        event_id = str(event.get("id"))
        change_list = event.get("changes") or []
        logger.info(
            "todo event id=%s resource=%s repo=%s occurred_at=%s changes=%s",
            event_id,
            _resource_id_from_event(event),
            _repo_name_from_event(event),
            occurred_at.isoformat(),
            len(change_list) if isinstance(change_list, list) else "unknown",
        )
        if occurred_at < since:
            logger.info(
                "todo event id=%s skipped because occurred_at=%s is before since=%s",
                event_id,
                occurred_at.isoformat(),
                since.isoformat(),
            )
            return [], True

        events: list[NormalizedEvent] = []
        for change in change_list:
            if not isinstance(change, dict):
                logger.info("todo event id=%s skipped non-dict change payload", event_id)
                continue
            logger.info(
                "todo change event_id=%s type=%s eventType=%s author=%s editor=%s newStatus=%s newResolution=%s",
                event_id,
                change.get("__typename"),
                change.get("eventType"),
                _safe_nested_name(change.get("author")),
                _safe_nested_name(change.get("editor")),
                change.get("newStatus"),
                change.get("newResolution"),
            )
            normalized = _normalize_event_change(
                settings=self.settings,
                actor=actor,
                event=event,
                change=change,
            )
            if normalized is not None:
                logger.info(
                    "todo change accepted event_id=%s normalized_type=%s external_uid=%s",
                    event_id,
                    normalized.event_type,
                    normalized.external_uid,
                )
                events.append(normalized)
            else:
                logger.info(
                    "todo change skipped event_id=%s for actor=%s",
                    event_id,
                    actor,
                )
        return events, False
