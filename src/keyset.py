"""The monitored key set: Keys API sources plus static pubkeys."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from .config import Config, KeysApiSource

log = logging.getLogger(__name__)

GROUP_LABELS = ("set", "origin", "module_id", "operator_id")


@dataclass(frozen=True)
class KeyInfo:
    pubkey: str
    set_name: str
    origin: str
    module_id: Optional[int]
    operator_id: Optional[int]
    label: str = ""

    def labels(self) -> dict[str, str]:
        return {
            "set": self.set_name,
            "origin": self.origin,
            "module_id": "" if self.module_id is None else str(self.module_id),
            "operator_id": "" if self.operator_id is None else str(self.operator_id),
        }


@dataclass
class RefreshResult:
    ok: bool
    added: set[str] = field(default_factory=set)
    removed: set[str] = field(default_factory=set)
    errors: int = 0


class KeySet:
    """Thread-safe view of all monitored keys, refreshed from the Keys API."""

    def __init__(
        self,
        cfg: Config,
        fetch: Callable[[KeysApiSource], list[str]],
        on_error: Optional[Callable[[], None]] = None,
    ):
        self._cfg = cfg
        self._fetch = fetch
        self._on_error = on_error
        self._lock = threading.Lock()
        self._keys: dict[str, KeyInfo] = {}
        self._last_good: dict[tuple[int, int], list[str]] = {}
        self._warned_duplicates: set[str] = set()
        self.last_success_timestamp: Optional[float] = None

    def sources(self) -> list[tuple[str, KeysApiSource]]:
        return [(s.name, src) for s in self._cfg.sets for src in s.keys_api]

    def has_static(self) -> bool:
        return any(s.static_pubkeys for s in self._cfg.sets)

    def refresh(self) -> RefreshResult:
        errors = 0
        loaded: dict[tuple[int, int], list[str]] = {}
        for set_idx, key_set in enumerate(self._cfg.sets):
            for src_idx, source in enumerate(key_set.keys_api):
                slot = (set_idx, src_idx)
                try:
                    loaded[slot] = list(self._fetch(source))
                except Exception as exc:
                    errors += 1
                    log.warning(
                        "keys api fetch failed for set %s (%s module %s operator %s): %s",
                        key_set.name, source.url, source.module_id, source.operator_id, exc,
                    )
                    if self._on_error is not None:
                        self._on_error()

        keys: dict[str, KeyInfo] = {}
        with self._lock:
            self._last_good.update(loaded)
            for set_idx, key_set in enumerate(self._cfg.sets):
                entries: list[KeyInfo] = []
                for src_idx, source in enumerate(key_set.keys_api):
                    for pubkey in self._last_good.get((set_idx, src_idx), []):
                        entries.append(KeyInfo(pubkey, key_set.name, "keys_api", source.module_id, source.operator_id))
                for static in key_set.static_pubkeys:
                    entries.append(KeyInfo(static.pubkey, key_set.name, "static", None, None, static.label))
                for info in entries:
                    if info.pubkey in keys:
                        if info.pubkey not in self._warned_duplicates:
                            self._warned_duplicates.add(info.pubkey)
                            first = keys[info.pubkey]
                            log.warning(
                                "duplicate pubkey %s in set %s (%s), keeping set %s (%s)",
                                info.pubkey, info.set_name, info.origin, first.set_name, first.origin,
                            )
                        continue
                    keys[info.pubkey] = info
            added = set(keys) - set(self._keys)
            removed = set(self._keys) - set(keys)
            self._keys = keys
            ok = errors == 0
            if ok:
                self.last_success_timestamp = time.time()
        log.info("key set refreshed: %d keys (+%d -%d), %d errors", len(keys), len(added), len(removed), errors)
        return RefreshResult(ok=ok, added=added, removed=removed, errors=errors)

    def all_sources_loaded(self) -> bool:
        """True once every Keys API source has been loaded successfully at least once."""
        with self._lock:
            return all(
                (set_idx, src_idx) in self._last_good
                for set_idx, key_set in enumerate(self._cfg.sets)
                for src_idx in range(len(key_set.keys_api))
            )

    def get(self, pubkey: str) -> Optional[KeyInfo]:
        with self._lock:
            return self._keys.get(pubkey)

    def all(self) -> dict[str, KeyInfo]:
        with self._lock:
            return dict(self._keys)

    def __len__(self) -> int:
        with self._lock:
            return len(self._keys)

    def match_exit(self, module_id: int, operator_id: int, pubkey: str) -> Optional[KeyInfo]:
        """Match an exit request by pubkey, else by a configured (module, operator) source."""
        info = self.get(pubkey)
        if info is not None:
            return info
        for set_name, source in self.sources():
            if source.module_id == module_id and source.operator_id == operator_id:
                return KeyInfo(pubkey, set_name, "keys_api", module_id, operator_id)
        return None

    def exit_topic_filter(self) -> Optional[tuple[list[int], list[int]]]:
        """Topic filter for ValidatorExitRequest logs; None means fetch all and match locally."""
        if self.has_static():
            return None
        sources = [src for _, src in self.sources()]
        return (
            sorted({s.module_id for s in sources}),
            sorted({s.operator_id for s in sources}),
        )
