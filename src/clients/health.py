"""Per-endpoint health and error bookkeeping shared by the EL and CL clients."""

from __future__ import annotations

import copy
import threading
from dataclasses import dataclass, field
from typing import Optional

import httpx

REASONS = ("unreachable", "http_error", "rpc_error", "range_limit", "not_found", "syncing", "lagging")


def endpoint_name(url: str) -> str:
    """host:port of an endpoint without scheme, credentials, path or query."""
    try:
        parsed = httpx.URL(url)
        host = parsed.host
        if not host:
            return "<invalid url>"
        if ":" in host:
            host = f"[{host}]"
        return host + (f":{parsed.port}" if parsed.port else "")
    except Exception:
        return "<invalid url>"


@dataclass
class EndpointState:
    kind: str
    endpoint: str
    url: str
    up: bool = True
    syncing: bool = False
    head: Optional[int] = None
    lag: Optional[int] = None
    reason: str = ""
    errors: dict[str, int] = field(default_factory=dict)


# url -> (reachable, syncing, head, reason)
HealthResult = tuple[bool, Optional[bool], Optional[int], str]


class EndpointTracker:
    """Thread safe endpoint states in config order."""

    def __init__(self, kind: str, urls: list[str], max_lag: int = 5) -> None:
        self.kind = kind
        self.max_lag = max_lag
        self._lock = threading.Lock()
        self._states = {url: EndpointState(kind=kind, endpoint=endpoint_name(url), url=url) for url in urls}

    @property
    def urls(self) -> list[str]:
        return list(self._states)

    def record_error(self, url: str, reason: str) -> None:
        with self._lock:
            state = self._states.get(url)
            if state is not None:
                state.errors[reason] = state.errors.get(reason, 0) + 1

    def states(self) -> list[EndpointState]:
        with self._lock:
            return [copy.deepcopy(s) for s in self._states.values()]

    def up_urls(self) -> list[str]:
        with self._lock:
            return [url for url, s in self._states.items() if s.up]

    def healthy_urls(self) -> list[str]:
        """Up endpoints in config order; all endpoints when none is up."""
        return self.up_urls() or self.urls

    def update_health(self, results: dict[str, HealthResult]) -> None:
        with self._lock:
            candidates = {}
            for url, (reachable, syncing, head, reason) in results.items():
                state = self._states.get(url)
                if state is None:
                    continue
                state.lag = None
                if not reachable:
                    reason = reason or "unreachable"
                    state.up, state.syncing, state.head, state.reason = False, False, None, reason
                elif syncing:
                    reason = "syncing"
                    state.up, state.syncing, state.head, state.reason = False, True, head, reason
                else:
                    state.syncing, state.head = False, head
                    candidates[url] = state
                    continue
                state.errors[reason] = state.errors.get(reason, 0) + 1
            heads = [s.head for s in candidates.values() if s.head is not None]
            best = max(heads) if heads else None
            for state in candidates.values():
                if best is not None and state.head is not None:
                    state.lag = best - state.head
                if state.lag is not None and state.lag > self.max_lag:
                    state.up, state.reason = False, "lagging"
                    state.errors["lagging"] = state.errors.get("lagging", 0) + 1
                else:
                    state.up, state.reason = True, ""
