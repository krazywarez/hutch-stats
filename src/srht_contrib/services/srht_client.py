from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx


logger = logging.getLogger(__name__)


class SourceHutClientError(RuntimeError):
    """Raised when a SourceHut GraphQL request fails."""


def _graphql_error_summary(errors: Any) -> str:
    if not isinstance(errors, list):
        return "unexpected error payload"
    return f"{len(errors)} GraphQL error(s)"


@dataclass(frozen=True)
class ClientOptions:
    timeout: float = 15.0
    max_retries: int = 2
    # Pause before each first attempt, to stay under SourceHut's rate limits.
    request_delay: float = 0.5


class SourceHutGraphQLClient:
    def __init__(
        self,
        endpoint: str,
        token: str,
        *,
        options: ClientOptions | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        options = options or ClientOptions()
        self.endpoint = endpoint
        self.timeout = options.timeout
        self.max_retries = options.max_retries
        self.request_delay = options.request_delay
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._client = httpx.Client(headers=headers, timeout=options.timeout, transport=transport)

    def execute(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        attempts = self.max_retries + 1

        for attempt in range(attempts):
            self._pause(attempt)
            body = self._post(payload, attempt, attempts)
            if body is None:
                continue
            self._raise_graphql_errors(body)
            return body.get("data", {})

        raise SourceHutClientError("SourceHut request exhausted retries")

    def _pause(self, attempt: int) -> None:
        """Back off exponentially between retries; pace first attempts by request_delay."""
        if attempt > 0:
            time.sleep(2 ** (attempt - 1))
        elif self.request_delay > 0:
            time.sleep(self.request_delay)

    def _post(self, payload: dict[str, Any], attempt: int, attempts: int) -> dict[str, Any] | None:
        """The response body, or None if this attempt failed and should be retried."""
        try:
            response = self._client.post(self.endpoint, json=payload)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "SourceHut HTTP failure from %s on attempt %s/%s: status=%s",
                self.endpoint,
                attempt + 1,
                attempts,
                exc.response.status_code,
            )
            if exc.response.status_code >= 500 and attempt < attempts - 1:
                return None
            raise SourceHutClientError(f"HTTP error from SourceHut: {exc.response.status_code}") from exc
        except httpx.HTTPError as exc:
            logger.warning(
                "SourceHut network failure from %s on attempt %s/%s",
                self.endpoint,
                attempt + 1,
                attempts,
            )
            if attempt < attempts - 1:
                return None
            raise SourceHutClientError("Network error while contacting SourceHut") from exc

    def _raise_graphql_errors(self, body: dict[str, Any]) -> None:
        if "errors" in body:
            logger.warning(
                "SourceHut GraphQL failure from %s: %s",
                self.endpoint,
                _graphql_error_summary(body["errors"]),
            )
            raise SourceHutClientError(
                f"GraphQL errors returned by SourceHut: {_graphql_error_summary(body['errors'])}"
            )

    def close(self) -> None:
        self._client.close()
