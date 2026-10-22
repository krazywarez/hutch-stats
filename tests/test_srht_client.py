import httpx
import pytest

from srht_contrib.services.srht_client import ClientOptions, SourceHutClientError, SourceHutGraphQLClient


def test_graphql_client_retries_http_5xx_and_succeeds() -> None:
    attempts = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        if attempts["count"] == 1:
            return httpx.Response(502, json={"error": "bad gateway"})
        return httpx.Response(200, json={"data": {"ok": True}})

    client = SourceHutGraphQLClient(
        "https://todo.sr.ht/query",
        "token",
        transport=httpx.MockTransport(handler),
    )

    data = client.execute("query Ping { ping }")

    assert data == {"ok": True}
    assert attempts["count"] == 2
    client.close()


def test_graphql_client_raises_for_graphql_errors() -> None:
    client = SourceHutGraphQLClient(
        "https://todo.sr.ht/query",
        "token",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"errors": [{"message": "nope"}]})),
    )

    with pytest.raises(SourceHutClientError):
        client.execute("query Ping { ping }")

    client.close()


def test_graphql_client_raises_for_network_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    client = SourceHutGraphQLClient(
        "https://todo.sr.ht/query",
        "token",
        options=ClientOptions(max_retries=0),
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(SourceHutClientError):
        client.execute("query Ping { ping }")

    client.close()


def test_graphql_client_applies_request_delay_and_retry_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"count": 0}
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        if attempts["count"] < 3:
            return httpx.Response(502, json={"error": "bad gateway"})
        return httpx.Response(200, json={"data": {"ok": True}})

    monkeypatch.setattr("srht_contrib.services.srht_client.time.sleep", sleeps.append)
    client = SourceHutGraphQLClient(
        "https://todo.sr.ht/query",
        "token",
        options=ClientOptions(max_retries=2, request_delay=0.5),
        transport=httpx.MockTransport(handler),
    )

    data = client.execute("query Ping { ping }")

    assert data == {"ok": True}
    assert sleeps == [0.5, 1, 2]
    client.close()
