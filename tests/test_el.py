from __future__ import annotations

import json

import httpx
import pytest

from src.clients.el import BlockHeader, ELError, ELRpcError, ExecutionClient, chunk_ranges

A = "http://el-a:8545"
B = "http://el-b:8545"


def make_client(handler, endpoints=(A, B)):
    errors = []
    client = ExecutionClient(
        list(endpoints), on_error=lambda: errors.append(1), transport=httpx.MockTransport(handler)
    )
    return client, errors


def rpc(request: httpx.Request) -> dict:
    return json.loads(request.content)


def result(request, value):
    return httpx.Response(200, json={"jsonrpc": "2.0", "id": rpc(request)["id"], "result": value})


def test_chunk_ranges():
    assert list(chunk_ranges(0, 9, 4)) == [(0, 3), (4, 7), (8, 9)]
    assert list(chunk_ranges(5, 5, 100)) == [(5, 5)]
    assert list(chunk_ranges(6, 5, 10)) == []
    assert list(chunk_ranges(1, 3, 0)) == [(1, 1), (2, 2), (3, 3)]


def test_block_number_and_get_block():
    def handler(request):
        body = rpc(request)
        if body["method"] == "eth_blockNumber":
            return result(request, "0x10")
        assert body["params"] == ["0x5", False]
        return result(request, {"number": "0x5", "hash": "0xABC", "timestamp": "0x64"})

    client, errors = make_client(handler)
    assert client.block_number() == 16
    assert client.get_block(5) == BlockHeader(5, "0xabc", 100)
    assert errors == []


def test_get_block_null_raises():
    client, _ = make_client(lambda r: result(r, None))
    with pytest.raises(ELError):
        client.get_block(1)


def test_failover_on_transport_error():
    seen = []

    def handler(request):
        seen.append(request.url.host)
        if request.url.host == "el-a":
            raise httpx.ConnectError("down")
        return result(request, "0x1")

    client, errors = make_client(handler)
    assert client.block_number() == 1
    assert seen == ["el-a", "el-b"]
    assert len(errors) == 1


def test_failover_on_http_error_and_bad_json():
    def handler(request):
        if request.url.host == "el-a":
            return httpx.Response(502, text="bad gateway")
        return httpx.Response(200, text="not json")

    client, errors = make_client(handler)
    with pytest.raises(ELError) as exc:
        client.block_number()
    assert not isinstance(exc.value, ELRpcError)
    assert len(errors) == 2


def test_rpc_error_raises_rpc_error():
    def handler(request):
        return httpx.Response(
            200, json={"jsonrpc": "2.0", "id": 1, "error": {"code": -32005, "message": "range too large"}}
        )

    client, errors = make_client(handler)
    with pytest.raises(ELRpcError):
        client.get_logs("0x00", [], 0, 100)
    assert len(errors) == 2


def test_rpc_error_then_success_on_next():
    def handler(request):
        if request.url.host == "el-a":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "error": {"code": -1}})
        return result(request, [])

    client, errors = make_client(handler)
    assert client.get_logs("0x00", [], 0, 1) == []
    assert len(errors) == 1


def test_refresh_health_skips_unsynced():
    calls = []

    def handler(request):
        body = rpc(request)
        calls.append((request.url.host, body["method"]))
        if body["method"] == "eth_syncing":
            if request.url.host == "el-a":
                return result(request, {"currentBlock": "0x1", "highestBlock": "0x10"})
            return result(request, False)
        return result(request, "0x2")

    client, _ = make_client(handler)
    assert client.healthy == [A, B]
    assert client.refresh_health() is True
    assert client.healthy == [B]
    calls.clear()
    assert client.block_number() == 2
    assert calls == [("el-b", "eth_blockNumber")]


def test_no_healthy_endpoints_tries_all():
    calls = []

    def handler(request):
        body = rpc(request)
        calls.append(request.url.host)
        if body["method"] == "eth_syncing":
            return result(request, {"currentBlock": "0x1"})
        return result(request, "0x3")

    client, _ = make_client(handler)
    assert client.refresh_health() is False
    calls.clear()
    assert client.block_number() == 3
    assert calls == ["el-a"]


def test_get_logs_filter_and_removed():
    captured = {}

    def handler(request):
        captured.update(rpc(request))
        return result(request, [{"logIndex": "0x0"}, {"logIndex": "0x1", "removed": True}])

    client, _ = make_client(handler)
    topics = ["0xaa", ["0x01", "0x02"]]
    logs = client.get_logs("0xdead", topics, 16, 31)
    assert logs == [{"logIndex": "0x0"}]
    assert captured["method"] == "eth_getLogs"
    assert captured["params"] == [
        {"address": "0xdead", "fromBlock": "0x10", "toBlock": "0x1f", "topics": topics}
    ]


def test_get_logs_without_topics_omits_key():
    captured = {}

    def handler(request):
        captured.update(rpc(request))
        return result(request, [])

    client, _ = make_client(handler)
    client.get_logs("0xdead", [], 1, 2)
    assert "topics" not in captured["params"][0]


def test_eth_call():
    captured = {}

    def handler(request):
        captured.update(rpc(request))
        return result(request, "0x" + "00" * 32)

    client, _ = make_client(handler)
    assert client.call("0xc1", "0x12345678") == "0x" + "00" * 32
    assert captured["method"] == "eth_call"
    assert captured["params"] == [{"to": "0xc1", "data": "0x12345678"}, "latest"]
    client.close()
