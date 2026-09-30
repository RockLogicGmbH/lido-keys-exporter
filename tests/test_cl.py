from __future__ import annotations

import json
import logging

import httpx
import pytest

from src.clients.cl import BeaconClient, CLError, NonHeadStateError

A = "http://cl-a:5052"
B = "http://cl-b:5052/"

PK = ["0x" + f"{i:02x}" * 48 for i in range(3)]


def make_client(handler, endpoints=(A, B)):
    errors = []
    client = BeaconClient(
        list(endpoints), on_error=lambda: errors.append(1), transport=httpx.MockTransport(handler)
    )
    return client, errors


def val(index, pubkey, status="active_ongoing"):
    return {"index": str(index), "status": status, "validator": {"pubkey": pubkey}}


@pytest.mark.parametrize(
    "path",
    [
        "/eth/v1/beacon/states/finalized/validators",
        "/eth/v1/beacon/states/12345/validators",
        "/eth/v1/beacon/states/0xabc/validators",
        "/eth/v2/debug/beacon/states/1000",
        "/eth/v1/beacon/states/head",
    ],
)
def test_non_head_state_guard_makes_no_request(path):
    def handler(request):
        raise AssertionError("network must not be touched")

    client, errors = make_client(handler)
    with pytest.raises(NonHeadStateError):
        client._request("GET", path)
    assert errors == []


def test_head_slot_and_logging(caplog):
    def handler(request):
        assert request.url.path == "/eth/v1/beacon/headers/head"
        return httpx.Response(200, json={"data": {"header": {"message": {"slot": "123"}}}})

    client, _ = make_client(handler)
    with caplog.at_level(logging.INFO, logger="src.clients.cl"):
        assert client.head_slot() == 123
    assert any("beacon request GET /eth/v1/beacon/headers/head via" in r.getMessage() for r in caplog.records)


def test_failover_and_errors():
    seen = []

    def handler(request):
        seen.append(request.url.host)
        if request.url.host == "cl-a":
            raise httpx.ConnectError("down")
        return httpx.Response(200, json={"data": {"header": {"message": {"slot": "7"}}}})

    client, errors = make_client(handler)
    assert client.head_slot() == 7
    assert seen == ["cl-a", "cl-b"]
    assert len(errors) == 1


def test_4xx_counts_as_failure():
    def handler(request):
        return httpx.Response(400, json={"message": "bad"})

    client, errors = make_client(handler)
    with pytest.raises(CLError):
        client.head_slot()
    assert len(errors) == 2


def test_refresh_health():
    seen = []

    def handler(request):
        seen.append((request.url.host, request.url.path))
        if request.url.path == "/eth/v1/node/syncing":
            syncing = request.url.host == "cl-a"
            return httpx.Response(200, json={"data": {"is_syncing": syncing, "head_slot": "1"}})
        return httpx.Response(200, json={"data": {"header": {"message": {"slot": "9"}}}})

    client, _ = make_client(handler)
    assert client.refresh_health() is True
    assert client.healthy == ["http://cl-b:5052"]
    seen.clear()
    client.head_slot()
    assert seen == [("cl-b", "/eth/v1/beacon/headers/head")]


def test_refresh_health_none_healthy():
    def handler(request):
        if request.url.host == "cl-a":
            raise httpx.ConnectError("down")
        return httpx.Response(200, json={"data": {"is_syncing": True}})

    client, errors = make_client(handler)
    assert client.refresh_health() is False
    assert len(errors) == 1


def test_validators_post_batches():
    posts = []
    ids = [str(i) for i in range(1200)]

    def handler(request):
        assert request.method == "POST"
        assert request.url.path == "/eth/v1/beacon/states/head/validators"
        batch = json.loads(request.content)["ids"]
        posts.append(len(batch))
        return httpx.Response(200, json={"data": [val(i, PK[0]) for i in batch if int(i) % 100 == 0]})

    client, _ = make_client(handler)
    data = client.validators(ids)
    assert posts == [500, 500, 200]
    assert [d["index"] for d in data] == [str(i) for i in range(0, 1200, 100)]


def test_validators_unknown_ids_absent():
    def handler(request):
        return httpx.Response(200, json={"data": [val(5, PK[1], "exited_unslashed")]})

    client, _ = make_client(handler)
    assert client.validators([PK[0], PK[1]]) == [val(5, PK[1], "exited_unslashed")]
    assert client.validators([]) == []


@pytest.mark.parametrize("status", [404, 405, 415])
def test_validators_get_fallback(status):
    requests = []
    ids = [str(i) for i in range(120)]

    def handler(request):
        requests.append(request.method)
        if request.method == "POST":
            return httpx.Response(status)
        got = request.url.params["id"].split(",")
        assert request.url.path == "/eth/v1/beacon/states/head/validators"
        return httpx.Response(200, json={"data": [val(i, PK[2]) for i in got]})

    client, errors = make_client(handler)
    data = client.validators(ids)
    assert requests == ["POST", "GET", "GET", "GET"]
    assert len(data) == 120
    assert errors == []
    requests.clear()
    client.validators(["1"])
    assert requests == ["GET"]


def test_validators_all_fail():
    def handler(request):
        return httpx.Response(500)

    client, errors = make_client(handler)
    with pytest.raises(CLError):
        client.validators(["1"])
    assert len(errors) == 2
    client.close()


@pytest.mark.parametrize("status", [404, 405])
def test_404_on_one_endpoint_fails_over(status):
    def handler(request):
        if request.url.host == "cl-a":
            return httpx.Response(status)
        return httpx.Response(200, json={"data": {"header": {"message": {"slot": "5"}}}})

    client, errors = make_client(handler)
    assert client.head_slot() == 5
    assert len(errors) == 1


def test_post_unsupported_tracked_per_endpoint():
    requests = []

    def handler(request):
        requests.append((request.url.host, request.method))
        if request.method == "POST" and request.url.host == "cl-a":
            return httpx.Response(415)
        if request.url.path == "/eth/v1/node/syncing":
            return httpx.Response(200, json={"data": {"is_syncing": request.url.host == "cl-a"}})
        return httpx.Response(200, json={"data": [val(1, PK[0])]})

    client, _ = make_client(handler)
    assert len(client.validators(["1"])) == 1
    assert requests == [("cl-a", "POST"), ("cl-a", "GET")]
    client.refresh_health()  # cl-a now syncing, cl-b first
    requests.clear()
    client.validators(["1"])
    assert requests == [("cl-b", "POST")]
