from __future__ import annotations


import httpx
import pytest

from src.clients.keys_api import KeysApiClient, KeysApiError
from src.config import KeysApiSource

SOURCE = KeysApiSource(url="http://keys-api:3000", module_id=2, operator_id=7)


def pk(n: int) -> str:
    return "0x" + f"{n:02x}" * 48


def client_for(handler) -> KeysApiClient:
    return KeysApiClient(timeout=5, transport=httpx.MockTransport(handler))


def key(n, **kw):
    entry = {"key": pk(n).upper().replace("0X", "0x"), "depositSignature": "0x", "operatorIndex": 7,
             "used": True, "moduleAddress": "0x" + "00" * 20, "index": n, "vetted": True}
    entry.update(kw)
    return entry


def test_fetch_nested_keys():
    seen = {}

    def handler(request: httpx.Request):
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        body = {"data": {"keys": [key(1), key(2, used=False), key(3, operatorIndex=8), key(4)], "module": {}}, "meta": {}}
        return httpx.Response(200, json=body)

    c = client_for(handler)
    assert c.fetch(SOURCE) == [pk(1), pk(4)]
    assert seen["path"] == "/v1/modules/2/keys"
    assert seen["params"] == {"used": "true", "operatorIndex": "7"}
    c.close()


def test_fetch_flat_list_and_missing_fields():
    def handler(request):
        entry = {"key": "01" * 48}
        return httpx.Response(200, json={"data": [entry, {"key": "0x12"}, key(5)]})

    assert client_for(handler).fetch(SOURCE) == [pk(1), pk(5)]


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(500, text="boom"),
        httpx.Response(404, json={"error": "nope"}),
        httpx.Response(200, text="not json"),
        httpx.Response(200, json={"data": {"module": {}}}),
        httpx.Response(200, json=[1, 2]),
    ],
)
def test_fetch_errors(response):
    with pytest.raises(KeysApiError):
        client_for(lambda request: response).fetch(SOURCE)


def test_transport_error():
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(KeysApiError):
        client_for(handler).fetch(SOURCE)
