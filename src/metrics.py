"""Prometheus metrics: event counters plus gauges rebuilt from the store on every scrape."""

from __future__ import annotations

import logging
from collections import Counter as TallyCounter
from typing import TYPE_CHECKING, Iterator, Optional

from prometheus_client import CollectorRegistry, Counter, Gauge
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from .contracts import slot_at

if TYPE_CHECKING:
    from .keyset import KeySet
    from .store import Store

log = logging.getLogger(__name__)

GROUP = ("set", "origin", "module_id", "operator_id")
PER_KEY = GROUP + ("pubkey", "validator_index")
ERROR_COMPONENTS = ("keys_api", "el", "cl", "store")


class Metrics:
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry if registry is not None else CollectorRegistry()
        r = self.registry
        self.deposits = Counter(
            "lido_keys_deposits", "Initial deposits to monitored keys", GROUP + ("credentials",), registry=r
        )
        self.topups = Counter("lido_keys_topups", "Top-up deposits to monitored keys", GROUP, registry=r)
        self.exit_requests = Counter(
            "lido_keys_exit_requests", "Lido exit requests for monitored keys", GROUP, registry=r
        )
        self.triggered = Counter(
            "lido_keys_triggered_withdrawals",
            "EIP-7002 triggered withdrawal requests for monitored keys",
            GROUP + ("kind", "source"),
            registry=r,
        )
        self.triggered_gwei = Counter(
            "lido_keys_triggered_withdrawal_gwei",
            "Amount requested by EIP-7002 partial withdrawals (gwei)",
            GROUP + ("source",),
            registry=r,
        )
        self.errors = Counter("lido_keys_errors", "Errors by component", ("component",), registry=r)
        for component in ERROR_COMPONENTS:
            self.errors.labels(component=component)
        self.up = Gauge("lido_keys_up", "1 if the last iteration fully succeeded", registry=r)
        self.keyset_last_refresh = Gauge(
            "lido_keys_keyset_last_refresh_timestamp_seconds",
            "Time of the last fully successful key set refresh",
            registry=r,
        )
        self.build_info = Gauge("lido_keys_build_info", "Build information", ("version",), registry=r)
        self._state: Optional[StateCollector] = None

    def error(self, component: str) -> None:
        self.errors.labels(component=component).inc()

    def register_state(self, store: "Store", keyset: "KeySet") -> "StateCollector":
        if self._state is not None:
            self.registry.unregister(self._state)
        self._state = StateCollector(store, keyset)
        self.registry.register(self._state)
        return self._state


def _group_values(labels: dict[str, str]) -> list[str]:
    return [labels[k] for k in GROUP]


class StateCollector(Collector):
    """Builds the key and store derived gauges at scrape time."""

    def __init__(self, store: "Store", keyset: "KeySet"):
        self.store = store
        self.keyset = keyset

    def describe(self) -> list:
        return []

    def collect(self) -> Iterator[GaugeMetricFamily]:
        try:
            families = self._build()
        except Exception:
            log.exception("building state metrics failed")
            return
        yield from families

    def _build(self) -> list[GaugeMetricFamily]:
        keys = self.keyset.all()
        summary = self.store.deposit_summary()
        validators = self.store.validators()
        open_requests = self.store.open_exit_requests()
        triggered = self.store.triggered_last()
        cursors = self.store.all_cursors()

        def vindex(pubkey: str) -> str:
            info = validators.get(pubkey)
            return str(info.index) if info is not None and info.index is not None else ""

        monitored = GaugeMetricFamily("lido_keys_monitored", "Monitored keys", labels=GROUP)
        deposited = GaugeMetricFamily("lido_keys_deposited", "Monitored keys with a known deposit", labels=GROUP)
        deposit_ts = GaugeMetricFamily(
            "lido_keys_deposit_timestamp_seconds", "Block time of the initial deposit", labels=PER_KEY
        )
        mismatch = GaugeMetricFamily(
            "lido_keys_deposit_credentials_mismatch",
            "1 if a deposit used unexpected withdrawal credentials",
            labels=PER_KEY,
        )
        n_monitored: TallyCounter = TallyCounter()
        n_deposited: TallyCounter = TallyCounter()
        for pubkey, key in keys.items():
            group = tuple(_group_values(key.labels()))
            n_monitored[group] += 1
            info = validators.get(pubkey)
            dep = summary.get(pubkey)
            if dep is not None or (info is not None and info.prior_deposit):
                n_deposited[group] += 1
            if dep is not None:
                values = list(group) + [pubkey, vindex(pubkey)]
                deposit_ts.add_metric(values, dep.initial_timestamp)
                mismatch.add_metric(values, 1 if dep.mismatch else 0)
        for group, n in n_monitored.items():
            monitored.add_metric(list(group), n)
            deposited.add_metric(list(group), n_deposited.get(group, 0))

        exit_open = GaugeMetricFamily(
            "lido_keys_exit_request_open", "Oldest open exit request timestamp per key", labels=PER_KEY
        )
        exits_open = GaugeMetricFamily(
            "lido_keys_exit_requests_open", "Keys with an open exit request", labels=GROUP
        )
        oldest: dict[str, tuple[int, tuple[str, ...]]] = {}
        for rec in open_requests:
            group = (
                rec.set_name,
                rec.origin,
                "" if rec.module_id is None else str(rec.module_id),
                "" if rec.operator_id is None else str(rec.operator_id),
            )
            prev = oldest.get(rec.pubkey)
            if prev is None or rec.request_timestamp < prev[0]:
                oldest[rec.pubkey] = (rec.request_timestamp, group)
        n_open: TallyCounter = TallyCounter()
        for pubkey, (ts, group) in oldest.items():
            exit_open.add_metric(list(group) + [pubkey, vindex(pubkey)], ts)
            n_open[group] += 1
        for group, n in n_open.items():
            exits_open.add_metric(list(group), n)

        trig = GaugeMetricFamily(
            "lido_keys_triggered_withdrawal_last_timestamp_seconds",
            "Block time of the last EIP-7002 request per key and kind",
            labels=PER_KEY + ("kind",),
        )
        for (pubkey, kind), ts in triggered.items():
            key = keys.get(pubkey)
            if key is None:
                continue
            trig.add_metric(_group_values(key.labels()) + [pubkey, vindex(pubkey), kind], ts)

        last_block = GaugeMetricFamily("lido_keys_last_processed_block", "Lowest processed block over all streams")
        last_ts = GaugeMetricFamily(
            "lido_keys_last_processed_block_timestamp", "Block time of the lowest processed block"
        )
        last_slot = GaugeMetricFamily("lido_keys_last_processed_slot", "Slot of the lowest processed block")
        stream_block = GaugeMetricFamily(
            "lido_keys_stream_last_processed_block", "Last processed block per stream", labels=("stream",)
        )
        for c in cursors:
            stream_block.add_metric([c.stream], c.block_number)
        if cursors:
            lowest = min(cursors, key=lambda c: c.block_number)
            last_block.add_metric([], lowest.block_number)
            if lowest.block_timestamp is not None:
                last_ts.add_metric([], lowest.block_timestamp)
                last_slot.add_metric([], slot_at(lowest.block_timestamp))

        return [
            monitored,
            deposited,
            deposit_ts,
            mismatch,
            exit_open,
            exits_open,
            trig,
            last_block,
            last_ts,
            last_slot,
            stream_block,
        ]
