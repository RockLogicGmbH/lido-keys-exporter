from __future__ import annotations

import json

import httpx
import pytest

from src.clients.el import BlockHeader, ELError, ELRpcError, ExecutionClient, chunk_ranges, endpoint_name

A = "http://el-a:8545"
B = "http://el-b:8545"


NETHERMIND_RANGE = (
    "Block range 2000 exceeds the maximum of 1000 blocks per logs request. "
    "Use a narrower fromBlock/toBlock range or increase Receipt.MaxBlockDepth."
)


def make_client(handler, endpoints=(A, B), max_lag=5):
    errors = []
    client = ExecutionClient(
        list(endpoints), on_error=lambda: errors.append(1), transport=httpx.MockTransport(handler), max_lag=max_lag
    )
    return client, errors


def batch_handler(single):
    """Wraps a handler of (host, body) -> result value into a transport handler supporting batches."""

    def handler(request):
        body = json.loads(request.content)
        items = body if isinstance(body, list) else [body]
        out = [{"jsonrpc": "2.0", "id": item["id"], "result": single(request.url.host, item)} for item in items]
        return httpx.Response(200, json=out if isinstance(body, list) else out[0])

    return handler


def errors_of(client, host):
    return next(s.errors for s in client.endpoint_states() if s.endpoint.startswith(host))


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
            200, json={"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "internal error"}}
        )

    client, errors = make_client(handler)
    with pytest.raises(ELRpcError):
        client.get_logs("0x00", [], 0, 100)
    assert len(errors) == 2
    assert errors_of(client, "el-a") == {"rpc_error": 1}


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

    def single(host, body):
        calls.append((host, body["method"]))
        if body["method"] == "eth_syncing":
            return {"currentBlock": "0x1", "highestBlock": "0x10"} if host == "el-a" else False
        return "0x2"

    client, _ = make_client(batch_handler(single))
    assert client.healthy == [A, B]
    assert client.refresh_health() is True
    assert client.healthy == [B]
    calls.clear()
    assert client.block_number() == 2
    assert calls == [("el-b", "eth_blockNumber")]
    a = client.endpoint_states()[0]
    assert (a.up, a.syncing, a.reason, a.lag) == (False, True, "syncing", None)


def test_no_healthy_endpoints_tries_all():
    calls = []

    def single(host, body):
        calls.append(host)
        if body["method"] == "eth_syncing":
            return {"currentBlock": "0x1"}
        return "0x3"

    client, _ = make_client(batch_handler(single))
    assert client.refresh_health() is False
    calls.clear()
    assert client.block_number() == 3
    assert calls == ["el-a"]


def test_refresh_health_lagging_endpoint_unhealthy():
    heads = {"el-a": 100 - 19, "el-b": 100}

    def single(host, body):
        if body["method"] == "eth_syncing":
            return False
        return hex(heads[host])

    client, errors = make_client(batch_handler(single))
    assert client.refresh_health() is True
    assert client.healthy == [B]
    a, b = client.endpoint_states()
    assert (a.kind, a.endpoint, a.url) == ("el", "el-a:8545", A)
    assert (a.up, a.syncing, a.head, a.lag, a.reason) == (False, False, 81, 19, "lagging")
    assert (b.up, b.head, b.lag, b.reason) == (True, 100, 0, "")
    assert a.errors == {"lagging": 1}
    assert errors == []

    heads["el-a"] = 96
    client.refresh_health()
    a = client.endpoint_states()[0]
    assert (a.up, a.lag, a.reason) == (True, 4, "")
    assert a.errors == {"lagging": 1}
    assert client.healthy == [A, B]


def test_refresh_health_batch_fallback_and_unreachable():
    methods = []

    def handler(request):
        body = json.loads(request.content)
        if request.url.host == "el-b":
            raise httpx.ConnectError("down")
        if isinstance(body, list):
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": None, "error": {"code": -32600}})
        methods.append(body["method"])
        return result(request, False if body["method"] == "eth_syncing" else "0x10")

    client, errors = make_client(handler)
    assert client.refresh_health() is True
    assert methods == ["eth_syncing", "eth_blockNumber"]
    a, b = client.endpoint_states()
    assert (a.up, a.head, a.lag) == (True, 16, 0)
    assert (b.up, b.reason, b.head, b.errors) == (False, "unreachable", None, {"unreachable": 1})
    assert len(errors) == 1


def test_refresh_health_http_error():
    def handler(request):
        if request.url.host == "el-a":
            return httpx.Response(503)
        return batch_handler(lambda h, b: False if b["method"] == "eth_syncing" else "0x1")(request)

    client, errors = make_client(handler)
    assert client.refresh_health() is True
    a = client.endpoint_states()[0]
    assert (a.up, a.reason, a.errors) == (False, "http_error", {"http_error": 1})
    assert len(errors) == 1


def test_get_block_null_falls_back_to_next():
    seen = []

    def single(host, body):
        seen.append(host)
        if host == "el-a":
            return None
        return {"number": "0x7", "hash": "0xAA", "timestamp": "0x1"}

    client, errors = make_client(batch_handler(single))
    assert client.get_block(7) == BlockHeader(7, "0xaa", 1)
    assert seen == ["el-a", "el-b"]
    assert errors_of(client, "el-a") == {"not_found": 1}
    assert errors == []


def test_range_limit_split_on_same_endpoint():
    requests = []

    def handler(request):
        body = rpc(request)
        flt = body["params"][0]
        a, b = int(flt["fromBlock"], 16), int(flt["toBlock"], 16)
        requests.append((request.url.host, a, b))
        if b - a + 1 > 1000:
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": body["id"], "error": {"code": -32602, "message": NETHERMIND_RANGE}},
            )
        return result(request, [{"blockNumber": hex(a)}, {"blockNumber": hex(b)}])

    client, errors = make_client(handler)
    logs = client.get_logs("0xdead", [], 0, 1999)
    assert requests == [("el-a", 0, 1999), ("el-a", 0, 999), ("el-a", 1000, 1999)]
    assert [int(e["blockNumber"], 16) for e in logs] == [0, 999, 1000, 1999]
    assert errors == []
    assert errors_of(client, "el-a") == {"range_limit": 1}
    assert errors_of(client, "el-b") == {}

    requests.clear()
    client.get_logs("0xdead", [], 2000, 4499)
    assert requests == [("el-a", 2000, 2999), ("el-a", 3000, 3999), ("el-a", 4000, 4499)]
    assert errors_of(client, "el-a") == {"range_limit": 1}


def test_range_limit_halves_recursively():
    requests = []

    def handler(request):
        body = rpc(request)
        flt = body["params"][0]
        a, b = int(flt["fromBlock"], 16), int(flt["toBlock"], 16)
        requests.append((a, b))
        if b - a + 1 > 300:
            return httpx.Response(
                200, json={"jsonrpc": "2.0", "id": body["id"], "error": {"code": -32005, "message": "Query returned more than 10000 results"}}
            )
        return result(request, [{"blockNumber": hex(a)}])

    client, errors = make_client(handler, endpoints=(A,))
    logs = client.get_logs("0xdead", [], 0, 999)
    assert [int(e["blockNumber"], 16) for e in logs] == [0, 250, 500, 750]
    assert requests == [(0, 999), (0, 499), (0, 249), (250, 499), (500, 749), (750, 999)]
    assert errors_of(client, "el-a") == {"range_limit": 2}
    assert errors == []


def test_range_limit_on_single_block_fails_over():
    def handler(request):
        body = rpc(request)
        if request.url.host == "el-a":
            return httpx.Response(
                200, json={"jsonrpc": "2.0", "id": body["id"], "error": {"code": -32602, "message": "block range too wide"}}
            )
        return result(request, [{"logIndex": "0x0"}])

    client, errors = make_client(handler)
    assert client.get_logs("0xdead", [], 5, 6) == [{"logIndex": "0x0"}]
    assert errors_of(client, "el-a") == {"range_limit": 2}
    assert errors == []


def test_endpoint_states_error_counts():
    def handler(request):
        if request.url.host == "el-a":
            raise httpx.ConnectError("down")
        if request.url.host == "el-b":
            return httpx.Response(500)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "error": {"code": -1, "message": "boom"}})

    client, errors = make_client(handler, endpoints=(A, B, "https://user:pw@el-c.example/key?x=1"))
    for _ in range(2):
        with pytest.raises(ELRpcError):
            client.block_number()
    states = client.endpoint_states()
    assert [s.endpoint for s in states] == ["el-a:8545", "el-b:8545", "el-c.example"]
    assert [s.errors for s in states] == [{"unreachable": 2}, {"http_error": 2}, {"rpc_error": 2}]
    assert len(errors) == 6
    states[0].errors["unreachable"] = 99
    assert client.endpoint_states()[0].errors == {"unreachable": 2}


def test_endpoint_name():
    assert endpoint_name("http://user:secret@node:8545/path?key=1") == "node:8545"
    assert endpoint_name("https://rpc.example.org/v3/abc") == "rpc.example.org"
    assert endpoint_name("http://[::1]:8545") == "[::1]:8545"


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


def test_get_logs_skips_endpoint_missing_to_block():
    calls = []

    def single(host, body):
        calls.append((host, body["method"]))
        if body["method"] == "eth_getBlockByNumber":
            if host == "el-a" and int(body["params"][0], 16) > 100:
                return None
            return {"number": body["params"][0], "hash": "0xaa", "timestamp": "0x1"}
        return [{"blockNumber": "0x65", "host": host}]

    client, errors = make_client(batch_handler(single))
    client.get_block(101)  # el-a lacks block 101, el-b answers
    calls.clear()
    logs = client.get_logs("0xdead", [], 90, 101)
    assert logs == [{"blockNumber": "0x65", "host": "el-b"}]
    assert calls == [("el-a", "eth_getBlockByNumber"), ("el-b", "eth_getLogs")]
    assert errors_of(client, "el-a") == {"not_found": 2}
    assert errors == []

    calls.clear()
    assert client.get_logs("0xdead", [], 90, 100)[0]["host"] == "el-a"
    assert calls == [("el-a", "eth_getLogs")]


def test_get_logs_skips_endpoint_behind_by_health_head():
    calls = []

    def single(host, body):
        calls.append((host, body["method"]))
        method = body["method"]
        if method == "eth_syncing":
            return False
        if method == "eth_blockNumber":
            return hex(98 if host == "el-a" else 100)
        if method == "eth_getBlockByNumber":
            return None if host == "el-a" else {"number": body["params"][0], "hash": "0xaa", "timestamp": "0x1"}
        return [{"host": host}]

    client, _ = make_client(batch_handler(single))
    assert client.refresh_health()
    calls.clear()
    assert client.get_logs("0xdead", [], 90, 100) == [{"host": "el-b"}]
    assert calls == [("el-a", "eth_getBlockByNumber"), ("el-b", "eth_getLogs")]


def test_get_logs_all_endpoints_missing_block_raises():
    def single(host, body):
        return None if body["method"] == "eth_getBlockByNumber" else []

    client, _ = make_client(batch_handler(single))
    with pytest.raises(ELError):
        client.get_block(50)
    with pytest.raises(ELError) as exc:
        client.get_logs("0xdead", [], 40, 50)
    assert not isinstance(exc.value, ELRpcError)


def test_rate_limit_is_not_a_range_limit():
    requests = []

    def handler(request):
        body = rpc(request)
        requests.append(request.url.host)
        if request.url.host == "el-a":
            return httpx.Response(
                200, json={"jsonrpc": "2.0", "id": body["id"], "error": {"code": -32005, "message": "rate limit exceeded"}}
            )
        return result(request, [])

    client, errors = make_client(handler)
    assert client.get_logs("0xdead", [], 0, 999) == []
    assert requests == ["el-a", "el-b"]
    assert errors_of(client, "el-a") == {"rpc_error": 1}
    assert errors == [1]


def test_result_count_limit_is_not_remembered():
    requests = []

    def handler(request):
        flt = rpc(request)["params"][0]
        a, b = int(flt["fromBlock"], 16), int(flt["toBlock"], 16)
        requests.append((a, b))
        if a == 0 and b - a + 1 > 500:
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": rpc(request)["id"], "error": {"message": "query returned more than 10000 results"}},
            )
        return result(request, [])

    client, _ = make_client(handler, endpoints=(A,))
    client.get_logs("0xdead", [], 0, 999)
    assert requests == [(0, 999), (0, 499), (500, 999)]
    requests.clear()
    client.get_logs("0xdead", [], 1000, 1999)
    assert requests == [(1000, 1999)]
