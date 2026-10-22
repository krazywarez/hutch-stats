from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from srht_contrib.api.dependencies import get_actor_identity_resolver, get_db, get_poller, require_api_key
from srht_contrib.jobs.poller import PollerService
from srht_contrib.schemas import ContributionCalendarResponse, ContributionStatsResponse, PollResponse
from srht_contrib.services.aggregator import ContributionAggregator
from srht_contrib.services.srht_client import SourceHutClientError
from srht_contrib.utils.dates import parse_date, year_bounds
from srht_contrib.utils.identity import ActorIdentityResolver

router = APIRouter(prefix="/api/contributions", tags=["contributions"])


def _resolve_range(year: int | None, from_date: str | None, to_date: str | None) -> tuple[date, date]:
    if year is not None and (from_date or to_date):
        raise HTTPException(status_code=400, detail="Use either `year` or `from`/`to`, not both.")

    try:
        if year is not None:
            return year_bounds(year)
        if from_date and to_date:
            start = parse_date(from_date)
            end = parse_date(to_date)
            if start > end:
                raise HTTPException(status_code=400, detail="`from` must be on or before `to`.")
            return start, end
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.") from exc

    raise HTTPException(status_code=400, detail="Provide `year` or both `from` and `to`.")


def _contribution_range(
    year: int | None = Query(default=None, ge=1970, le=3000),
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
) -> tuple[date, date]:
    return _resolve_range(year, from_date, to_date)


def _tracked_actor(
    actor: str,
    prioritize_self: bool = Query(default=False),
    poller: PollerService = Depends(get_poller),
    db: Session = Depends(get_db),
    actor_identity_resolver: ActorIdentityResolver = Depends(get_actor_identity_resolver),
) -> str:
    """The canonical actor, recorded as requested so the poller keeps it fresh."""
    canonical_actor = actor_identity_resolver.canonicalize(actor, db=db)
    poller.track_actor_request(db, canonical_actor, prioritize=prioritize_self)
    return canonical_actor


@router.get("/{actor}", response_model=ContributionCalendarResponse)
def get_contributions(
    date_range: tuple[date, date] = Depends(_contribution_range),
    canonical_actor: str = Depends(_tracked_actor),
    db: Session = Depends(get_db),
) -> ContributionCalendarResponse:
    start, end = date_range
    response = ContributionAggregator().build_calendar(db, canonical_actor, start, end)
    db.commit()
    return response


@router.get("/{actor}/stats", response_model=ContributionStatsResponse)
def get_contribution_stats(
    date_range: tuple[date, date] = Depends(_contribution_range),
    canonical_actor: str = Depends(_tracked_actor),
    db: Session = Depends(get_db),
) -> ContributionStatsResponse:
    start, end = date_range
    response = ContributionAggregator().build_stats(db, canonical_actor, start, end)
    db.commit()
    return response


@router.post("/poll", response_model=PollResponse, dependencies=[Depends(require_api_key)])
def trigger_manual_poll(
    actor: str,
    poller: PollerService = Depends(get_poller),
    db: Session = Depends(get_db),
    actor_identity_resolver: ActorIdentityResolver = Depends(get_actor_identity_resolver),
) -> PollResponse:
    canonical_actor = actor_identity_resolver.canonicalize(actor, db=db)
    try:
        inserted = poller.poll_all(db, canonical_actor)
    except SourceHutClientError as exc:
        raise HTTPException(status_code=502, detail=f"SourceHut polling failed: {exc}") from exc
    return PollResponse(actor=canonical_actor, inserted_events=inserted, services=["todo", "git"])
