"""Execution layer JSON-RPC client with endpoint failover."""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterator, NoReturn, Optional

import httpx

from .health import EndpointState, EndpointTracker, HealthResult, endpoint_name

__all__ = ["BlockHeader", "ELError", "ELRpcError", "ExecutionClient", "chunk_ranges", "endpoint_name"]

log = logging.getLogger(__name__)

RANGE_LIMIT_MARKERS = (
    "exceeds the maximum",
    "block range",
    "range too large",
    "query returned more than",
    "too many blocks",
    "maximum block range",
    "limit exceeded",
)
# Limits on the number of results, not blocks: split the request but do not shrink later ones.
RESULT_LIMIT_MARKERS = ("query returned more than",)
# Rate limits sometimes read like range limits ("rate limit exceeded"); they are ordinary errors.
RATE_LIMIT_MARKERS = ("rate limit", "rate-limit", "ratelimit", "request limit", "too many requests")


class ELError(Exception):
    """All execution endpoints failed."""


class ELRpcError(ELError):
    """A JSON-RPC error object was returned (e.g. log range too large)."""


@dataclass(frozen=True)
class BlockHeader:
    number: int
    hash: str
    timestamp: int


class _Failure(Exception):
    """One endpoint failed one request."""

    def __init__(self, reason: str, detail: Any) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


def chunk_ranges(start: int, end: int, size: int) -> Iterator[tuple[int, int]]:
    """Inclusive (a, b) ranges covering start..end."""
    size = max(1, size)
    a = start
    while a <= end:
        b = min(end, a + size - 1)
        yield a, b
        a = b + 1


def _message(error: Any) -> str:
    message = error.get("message", "") if isinstance(error, dict) else str(error)
    return str(message).lower()


def is_range_limit_error(error: Any) -> bool:
    message = _message(error)
    if any(marker in message for marker in RATE_LIMIT_MARKERS):
        return False
    return any(marker in message for marker in RANGE_LIMIT_MARKERS)


def is_result_limit_error(error: Any) -> bool:
    return any(marker in _message(error) for marker in RESULT_LIMIT_MARKERS)


class ExecutionClient:
    def __init__(
        self,
        endpoints: list[str],
        timeout: float = 30,
        on_error: Optional[Callable[[], None]] = None,
        transport: Optional[httpx.BaseTransport] = None,
        max_lag: int = 5,
    ) -> None:
        if not endpoints:
            raise ValueError("at least one execution endpoint is required")
        self.endpoints = list(endpoints)
        self.tracker = EndpointTracker("el", self.endpoints, max_lag)
        self._max_range: dict[str, int] = {}
        # Highest block each endpoint is known to have, and the lowest it was seen without.
        self._heads: dict[str, int] = {}
        self._missing: dict[str, int] = {}
        self._on_error = on_error
        self._ids = itertools.count(1)
        self._http = httpx.Client(timeout=timeout, transport=transport)

    @property
    def healthy(self) -> list[str]:
        return self.tracker.up_urls()

    def endpoint_states(self) -> list[EndpointState]:
        return self.tracker.states()

    def _error(self) -> None:
        if self._on_error is not None:
            self._on_error()

    def _send(self, endpoint: str, payload: Any) -> Any:
        """POST a payload; raises _Failure for transport, HTTP and decoding problems."""
        try:
            resp = self._http.post(endpoint, json=payload)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            raise _Failure("http_error", exc) from exc
        except httpx.HTTPError as exc:
            raise _Failure("unreachable", exc) from exc
        except ValueError as exc:
            raise _Failure("rpc_error", exc) from exc

    def _payload(self, method: str, params: list) -> dict:
        return {"jsonrpc": "2.0", "id": next(self._ids), "method": method, "params": params}

    @staticmethod
    def _result(body: Any) -> Any:
        if not isinstance(body, dict):
            raise _Failure("rpc_error", "JSON-RPC response is not an object")
        if body.get("error") is not None:
            raise _Failure("rpc_error", body["error"])
        if "result" not in body:
            raise _Failure("rpc_error", "missing result")
        return body["result"]

    def _attempt(self, endpoint: str, method: str, params: list) -> Any:
        return self._result(self._send(endpoint, self._payload(method, params)))

    def _failed(self, endpoint: str, method: str, failure: _Failure) -> None:
        log.warning("%s via %s failed: %s", method, endpoint_name(endpoint), failure)
        self.tracker.record_error(endpoint, failure.reason)
        if failure.reason in ("unreachable", "http_error", "rpc_error"):
            self._error()

    @staticmethod
    def _raise(method: str, last: Optional[_Failure]) -> NoReturn:
        if last is not None and last.reason in ("rpc_error", "range_limit") and isinstance(last.detail, dict):
            raise ELRpcError(f"{method}: {last.detail}")
        raise ELError(f"{method}: all execution endpoints failed: {last}")

    def _health_of(self, endpoint: str) -> HealthResult:
        sync_req, head_req = self._payload("eth_syncing", []), self._payload("eth_blockNumber", [])
        try:
            body = self._send(endpoint, [sync_req, head_req])
            if isinstance(body, list):
                by_id = {b.get("id"): b for b in body if isinstance(b, dict)}
                sync_body, head_body = by_id.get(sync_req["id"]), by_id.get(head_req["id"])
            else:
                sync_body = self._send(endpoint, self._payload("eth_syncing", []))
                head_body = self._send(endpoint, self._payload("eth_blockNumber", []))
        except _Failure as exc:
            reason = exc.reason if exc.reason in ("unreachable", "http_error") else "unreachable"
            log.warning("execution endpoint %s unreachable: %s", endpoint_name(endpoint), exc)
            self._error()
            return False, None, None, reason
        try:
            syncing = self._result(sync_body) is not False
        except _Failure:
            syncing = True
        try:
            head: Optional[int] = int(self._result(head_body), 16)
        except (_Failure, TypeError, ValueError):
            head = None
        if syncing:
            log.warning("execution endpoint %s not synced", endpoint_name(endpoint))
        return True, syncing, head, ""

    def _note_head(self, endpoint: str, number: int) -> None:
        self._heads[endpoint] = max(self._heads.get(endpoint, number), number)
        if self._missing.get(endpoint, number + 1) <= number:
            del self._missing[endpoint]

    def refresh_health(self) -> bool:
        results = {endpoint: self._health_of(endpoint) for endpoint in self.endpoints}
        for endpoint, (_reachable, _syncing, head, _reason) in results.items():
            if head is not None:
                self._heads[endpoint] = head
                if self._missing.get(endpoint, head + 1) <= head:
                    del self._missing[endpoint]
        self.tracker.update_health(results)
        for state in self.tracker.states():
            if state.reason == "lagging":
                log.warning("execution endpoint %s is %s blocks behind", state.endpoint, state.lag)
        return bool(self.tracker.up_urls())

    def _call(self, method: str, params: list) -> Any:
        return self._call_on(method, params)[1]

    def _call_on(self, method: str, params: list) -> tuple[str, Any]:
        last: Optional[_Failure] = None
        for endpoint in self.tracker.healthy_urls():
            try:
                return endpoint, self._attempt(endpoint, method, params)
            except _Failure as exc:
                self._failed(endpoint, method, exc)
                last = exc
        self._raise(method, last)

    def block_number(self) -> int:
        endpoint, result = self._call_on("eth_blockNumber", [])
        number = int(result, 16)
        self._note_head(endpoint, number)
        return number

    def get_block(self, number: int) -> BlockHeader:
        last: Optional[_Failure] = None
        for endpoint in self.tracker.healthy_urls():
            try:
                block = self._attempt(endpoint, "eth_getBlockByNumber", [hex(number), False])
            except _Failure as exc:
                self._failed(endpoint, "eth_getBlockByNumber", exc)
                last = exc
                continue
            if not block:
                self._not_found(endpoint, number)
                continue
            self._note_head(endpoint, number)
            return BlockHeader(
                number=int(block["number"], 16),
                hash=block["hash"].lower(),
                timestamp=int(block["timestamp"], 16),
            )
        if last is None:
            raise ELError(f"block {number} not found")
        self._raise("eth_getBlockByNumber", last)

    def _not_found(self, endpoint: str, number: int) -> None:
        log.warning("block %s not found via %s", number, endpoint_name(endpoint))
        self.tracker.record_error(endpoint, "not_found")
        self._missing[endpoint] = min(number, self._missing.get(endpoint, number))

    def _serves(self, endpoint: str, number: int) -> bool:
        """False if the endpoint is known to lack block number (checked with eth_getBlockByNumber)."""
        head, missing = self._heads.get(endpoint), self._missing.get(endpoint)
        if (head is None or head >= number) and (missing is None or missing > number):
            return True
        if self._attempt(endpoint, "eth_getBlockByNumber", [hex(number), False]):
            self._note_head(endpoint, number)
            return True
        self._not_found(endpoint, number)
        return False

    def _logs_on(
        self, endpoint: str, address: str, topics: list, from_block: int, to_block: int, local: dict[str, int]
    ) -> list[dict]:
        """Logs from one endpoint, split into ranges it accepts."""
        limits = [x for x in (self._max_range.get(endpoint), local.get("max")) if x is not None]
        limit = min(limits) if limits else None
        if limit is not None and to_block - from_block + 1 > limit:
            out: list[dict] = []
            for a, b in chunk_ranges(from_block, to_block, limit):
                out.extend(self._logs_on(endpoint, address, topics, a, b, local))
            return out
        flt: dict[str, Any] = {"address": address, "fromBlock": hex(from_block), "toBlock": hex(to_block)}
        if topics:
            flt["topics"] = topics
        try:
            logs = self._attempt(endpoint, "eth_getLogs", [flt])
        except _Failure as exc:
            if exc.reason != "rpc_error" or not is_range_limit_error(exc.detail):
                raise
            size = to_block - from_block + 1
            if size <= 1:
                raise _Failure("range_limit", exc.detail) from exc
            learned = max(1, size // 2)
            self.tracker.record_error(endpoint, "range_limit")
            log.info("eth_getLogs via %s: range limit, using at most %s blocks", endpoint_name(endpoint), learned)
            if is_result_limit_error(exc.detail):
                local["max"] = min(learned, local.get("max", learned))
            else:
                self._max_range[endpoint] = min(learned, self._max_range.get(endpoint, learned))
            return self._logs_on(endpoint, address, topics, from_block, to_block, local)
        if not isinstance(logs, list):
            raise _Failure("rpc_error", "eth_getLogs returned a non-list result")
        return logs

    def get_logs(self, address: str, topics: list, from_block: int, to_block: int) -> list[dict]:
        last: Optional[_Failure] = None
        for endpoint in self.tracker.healthy_urls():
            try:
                if not self._serves(endpoint, to_block):
                    last = _Failure("not_found", f"block {to_block} not available")
                    continue
                logs = self._logs_on(endpoint, address, topics, from_block, to_block, {})
            except _Failure as exc:
                self._failed(endpoint, "eth_getLogs", exc)
                last = exc
                continue
            return [entry for entry in logs if not entry.get("removed")]
        self._raise("eth_getLogs", last)

    def call(self, to: str, data: str, block: str = "latest") -> str:
        return self._call("eth_call", [{"to": to, "data": data}, block])

    def close(self) -> None:
        self._http.close()
