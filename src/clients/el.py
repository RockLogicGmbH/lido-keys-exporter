"""Execution layer JSON-RPC client with endpoint failover."""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional

import httpx

log = logging.getLogger(__name__)


class ELError(Exception):
    """All execution endpoints failed."""


class ELRpcError(ELError):
    """A JSON-RPC error object was returned (e.g. log range too large)."""


@dataclass(frozen=True)
class BlockHeader:
    number: int
    hash: str
    timestamp: int


def chunk_ranges(start: int, end: int, size: int) -> Iterator[tuple[int, int]]:
    """Inclusive (a, b) ranges covering start..end."""
    size = max(1, size)
    a = start
    while a <= end:
        b = min(end, a + size - 1)
        yield a, b
        a = b + 1


def endpoint_name(url: str) -> str:
    """Endpoint without credentials, path or query, for logs."""
    try:
        parsed = httpx.URL(url)
        return f"{parsed.scheme}://{parsed.host}" + (f":{parsed.port}" if parsed.port else "")
    except Exception:
        return "<invalid url>"


class ExecutionClient:
    def __init__(
        self,
        endpoints: list[str],
        timeout: float = 30,
        on_error: Optional[Callable[[], None]] = None,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        if not endpoints:
            raise ValueError("at least one execution endpoint is required")
        self.endpoints = list(endpoints)
        self.healthy = list(endpoints)
        self._on_error = on_error
        self._ids = itertools.count(1)
        self._http = httpx.Client(timeout=timeout, transport=transport)

    def _error(self) -> None:
        if self._on_error is not None:
            self._on_error()

    def _post(self, endpoint: str, method: str, params: list) -> Any:
        """Single request; returns the JSON body or raises httpx/ValueError."""
        payload = {"jsonrpc": "2.0", "id": next(self._ids), "method": method, "params": params}
        resp = self._http.post(endpoint, json=payload)
        resp.raise_for_status()
        body = resp.json()
        if not isinstance(body, dict):
            raise ValueError("JSON-RPC response is not an object")
        return body

    def refresh_health(self) -> bool:
        healthy = []
        for endpoint in self.endpoints:
            try:
                body = self._post(endpoint, "eth_syncing", [])
            except (httpx.HTTPError, ValueError) as exc:
                log.warning("execution endpoint %s unreachable: %s", endpoint_name(endpoint), exc)
                self._error()
                continue
            if body.get("result") is False:
                healthy.append(endpoint)
            else:
                log.warning("execution endpoint %s not synced", endpoint_name(endpoint))
        self.healthy = healthy
        return bool(healthy)

    def _call(self, method: str, params: list) -> Any:
        last_rpc: Optional[Any] = None
        last_exc: Optional[Exception] = None
        for endpoint in self.healthy or self.endpoints:
            try:
                body = self._post(endpoint, method, params)
            except (httpx.HTTPError, ValueError) as exc:
                log.warning("%s via %s failed: %s", method, endpoint_name(endpoint), exc)
                self._error()
                last_exc, last_rpc = exc, None
                continue
            if body.get("error") is not None:
                log.warning("%s via %s returned error: %s", method, endpoint_name(endpoint), body["error"])
                self._error()
                last_rpc, last_exc = body["error"], None
                continue
            if "result" not in body:
                log.warning("%s via %s: response without result", method, endpoint_name(endpoint))
                self._error()
                last_exc, last_rpc = ValueError("missing result"), None
                continue
            return body["result"]
        if last_rpc is not None:
            raise ELRpcError(f"{method}: {last_rpc}")
        raise ELError(f"{method}: all execution endpoints failed: {last_exc}")

    def block_number(self) -> int:
        return int(self._call("eth_blockNumber", []), 16)

    def get_block(self, number: int) -> BlockHeader:
        block = self._call("eth_getBlockByNumber", [hex(number), False])
        if not block:
            raise ELError(f"block {number} not found")
        return BlockHeader(
            number=int(block["number"], 16),
            hash=block["hash"].lower(),
            timestamp=int(block["timestamp"], 16),
        )

    def get_logs(self, address: str, topics: list, from_block: int, to_block: int) -> list[dict]:
        flt: dict[str, Any] = {"address": address, "fromBlock": hex(from_block), "toBlock": hex(to_block)}
        if topics:
            flt["topics"] = topics
        logs = self._call("eth_getLogs", [flt])
        if not isinstance(logs, list):
            raise ELError("eth_getLogs returned a non-list result")
        return [entry for entry in logs if not entry.get("removed")]

    def call(self, to: str, data: str, block: str = "latest") -> str:
        return self._call("eth_call", [{"to": to, "data": data}, block])

    def close(self) -> None:
        self._http.close()
