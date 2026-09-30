"""Beacon node HTTP client restricted to head state queries."""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

import httpx

from .el import endpoint_name

log = logging.getLogger(__name__)

HEAD_STATE_PREFIX = "/eth/v1/beacon/states/head/"
POST_BATCH = 500
GET_BATCH = 50
POST_UNSUPPORTED_STATUSES = frozenset({404, 405, 415})


class CLError(Exception):
    """All beacon endpoints failed."""


class NonHeadStateError(CLError):
    """A state query other than the head state was attempted."""


class BeaconClient:
    def __init__(
        self,
        endpoints: list[str],
        timeout: float = 30,
        on_error: Optional[Callable[[], None]] = None,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        if not endpoints:
            raise ValueError("at least one beacon endpoint is required")
        self.endpoints = [e.rstrip("/") for e in endpoints]
        self.healthy = list(self.endpoints)
        self._on_error = on_error
        self._post_unsupported: set[str] = set()
        self._last_endpoint: Optional[str] = None
        self._http = httpx.Client(timeout=timeout, transport=transport)

    def _error(self) -> None:
        if self._on_error is not None:
            self._on_error()

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict] = None,
        json: Any = None,
        passthrough: frozenset = frozenset(),
    ) -> tuple[int, Any]:
        """Returns (status, body); statuses in passthrough are returned with body None, any other
        non-2xx status counts as a failure of that endpoint."""
        if "/states/" in path and not path.startswith(HEAD_STATE_PREFIX):
            raise NonHeadStateError(f"refusing non-head state query: {path}")
        last_exc: Optional[Exception] = None
        for endpoint in self.healthy or self.endpoints:
            log.info("beacon request %s %s via %s", method, path, endpoint_name(endpoint))
            try:
                resp = self._http.request(method, endpoint + path, params=params, json=json)
                self._last_endpoint = endpoint
                if resp.status_code in passthrough:
                    return resp.status_code, None
                resp.raise_for_status()
                return resp.status_code, resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                log.warning("beacon %s %s via %s failed: %s", method, path, endpoint_name(endpoint), exc)
                self._error()
                last_exc = exc
        raise CLError(f"{method} {path}: all beacon endpoints failed: {last_exc}")

    def refresh_health(self) -> bool:
        healthy = []
        path = "/eth/v1/node/syncing"
        for endpoint in self.endpoints:
            log.info("beacon request GET %s via %s", path, endpoint_name(endpoint))
            try:
                resp = self._http.get(endpoint + path)
                resp.raise_for_status()
                syncing = resp.json()["data"]["is_syncing"]
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                log.warning("beacon endpoint %s unreachable: %s", endpoint_name(endpoint), exc)
                self._error()
                continue
            if syncing is False:
                healthy.append(endpoint)
            else:
                log.warning("beacon endpoint %s not synced", endpoint_name(endpoint))
        self.healthy = healthy
        return bool(healthy)

    def head_slot(self) -> int:
        status, body = self._request("GET", "/eth/v1/beacon/headers/head")
        if body is None:
            raise CLError(f"head header unavailable (HTTP {status})")
        try:
            return int(body["data"]["header"]["message"]["slot"])
        except (KeyError, TypeError, ValueError) as exc:
            raise CLError(f"malformed header response: {exc}") from exc

    def validators(self, ids: list[str]) -> list[dict]:
        """Head state validator lookup by index or pubkey; unknown ids are absent."""
        ids = list(dict.fromkeys(str(i) for i in ids))
        path = HEAD_STATE_PREFIX + "validators"
        out: list[dict] = []
        pos = 0
        use_get = False
        while pos < len(ids):
            body = None
            if not use_get and (self.healthy or self.endpoints)[0] not in self._post_unsupported:
                batch = ids[pos : pos + POST_BATCH]
                status, body = self._request(
                    "POST", path, json={"ids": batch}, passthrough=POST_UNSUPPORTED_STATUSES
                )
                if body is None:
                    endpoint = self._last_endpoint or self.endpoints[0]
                    log.info(
                        "beacon POST validators unsupported by %s (HTTP %s), using GET", endpoint_name(endpoint), status
                    )
                    self._post_unsupported.add(endpoint)
                    use_get = True
            if body is None:
                batch = ids[pos : pos + GET_BATCH]
                _, body = self._request("GET", path, params={"id": ",".join(batch)})
            data = body.get("data") if isinstance(body, dict) else None
            if not isinstance(data, list):
                raise CLError("malformed validators response")
            out.extend(data)
            pos += len(batch)
        return out

    def close(self) -> None:
        self._http.close()
