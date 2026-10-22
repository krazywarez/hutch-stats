from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from srht_contrib.api.routes_contributions import router as contributions_router
from srht_contrib.api.routes_health import router as health_router
from srht_contrib.api.routes_repositories import router as repositories_router
from srht_contrib.config import Settings, get_settings
from srht_contrib.db import make_engine, validate_db
from srht_contrib.jobs.poller import PollerService
from srht_contrib.logging import configure_logging
from srht_contrib.services.git import GitIngestionService
from srht_contrib.services.srht_client import ClientOptions, SourceHutGraphQLClient
from srht_contrib.services.todo import TodoIngestionService
from srht_contrib.utils.identity import ActorIdentityResolver


logger = logging.getLogger(__name__)


def build_poller(settings: Settings) -> PollerService:
    client_options = ClientOptions(request_delay=settings.srht_request_delay_seconds)
    todo_client = SourceHutGraphQLClient(settings.todo_srht_endpoint, settings.srht_token, options=client_options)
    git_client = SourceHutGraphQLClient(settings.git_srht_endpoint, settings.srht_token, options=client_options)
    todo_service = TodoIngestionService(todo_client, settings)
    git_service = GitIngestionService(git_client, settings)
    return PollerService(todo_service=todo_service, git_service=git_service, settings=settings)


def create_app(
    settings: Settings | None = None,
    *,
    engine: Engine | None = None,
    session_factory: sessionmaker[Session] | None = None,
    poller: PollerService | None = None,
) -> FastAPI:
    app_settings = settings or get_settings()
    configure_logging()
    app_engine = engine or make_engine(app_settings)
    app_session_factory = session_factory or sessionmaker(
        bind=app_engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )
    actor_identity_resolver = ActorIdentityResolver(app_settings.actor_aliases_json)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        validate_db(app_engine)
        app_poller = poller or build_poller(app_settings)
        app.state.poller = app_poller
        app.state.settings = app_settings
        app.state.engine = app_engine
        app.state.session_factory = app_session_factory
        app.state.actor_identity_resolver = actor_identity_resolver
        scheduler: BackgroundScheduler | None = None
        if app_settings.enable_scheduler:
            scheduler = BackgroundScheduler()
            scheduler.add_job(
                _scheduled_poll,
                "interval",
                seconds=app_settings.poll_interval_seconds,
                args=[app],
                id="srht-poller",
                replace_existing=True,
            )
            scheduler.start()
            _run_startup_poll(app)
        app.state.scheduler = scheduler
        try:
            yield
        finally:
            if scheduler is not None:
                scheduler.shutdown(wait=False)
            app_poller.todo_service.client.close()
            app_poller.git_service.client.close()

    app = FastAPI(title=app_settings.app_name, lifespan=lifespan)

    app.include_router(health_router)
    app.include_router(contributions_router)
    app.include_router(repositories_router)
    return app


def _scheduled_poll(app: FastAPI) -> None:
    settings: Settings = app.state.settings
    poller: PollerService = app.state.poller
    session_factory: sessionmaker[Session] = app.state.session_factory
    db = session_factory()
    try:
        poller.poll_tracked_actors(db, settings.default_actor)
    finally:
        db.close()


def _run_startup_poll(app: FastAPI) -> None:
    try:
        _scheduled_poll(app)
    except Exception:
        logger.exception("Initial scheduled poll failed during application startup")


app = create_app()
