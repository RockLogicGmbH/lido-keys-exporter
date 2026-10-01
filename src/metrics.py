"""Prometheus metrics: process counters plus store, key set and endpoint metrics built on every scrape."""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter as TallyCounter
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, Optional

from prometheus_client import CollectorRegistry, Counter, Gauge
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from .clients.health import REASONS
from .contracts import slot_at

if TYPE_CHECKING:
    from .keyset import KeySet
    from .store import Store

log = logging.getLogger(__name__)

GROUP = ("set", "origin", "module_id", "operator_id")
PER_KEY = GROUP + ("pubkey", "validator_index")
ERROR_COMPONENTS = ("keys_api", "el", "cl", "store")
DEPOSIT_KINDS = ("initial", "topup")
GWEI = 1e9
DAY = 86400


class Metrics:
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry if registry is not None else CollectorRegistry()
        r = self.registry
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
        self._endpoints: Optional[EndpointCollector] = None

    def error(self, component: str) -> None:
        self.errors.labels(component=component).inc()

    def register_state(
        self, store: "Store", keyset: "KeySet", clock: Callable[[], float] = time.time
    ) -> "StateCollector":
        if self._state is not None:
            self.registry.unregister(self._state)
        self._state = StateCollector(store, keyset, clock)
        self.registry.register(self._state)
        return self._state

    def register_endpoints(self, *clients: Any) -> "EndpointCollector":
        """Export the health of every endpoint of the given clients (anything with endpoint_states())."""
        if self._endpoints is not None:
            self.registry.unregister(self._endpoints)
        self._endpoints = EndpointCollector(clients)
        self.registry.register(self._endpoints)
        return self._endpoints


def _group_values(labels: dict[str, str]) -> list[str]:
    return [labels[k] for k in GROUP]


def _opt(value: Optional[int]) -> str:
    return "" if value is None else str(value)


class _MonotonicTotals:
    """Turns store totals, which drop on reclassification, reorgs or key set changes, into counters that never
    decrease: every drop is added to a persisted offset."""

    def __init__(self, store: "Store"):
        self.store = store
        self._offsets: Optional[dict[str, tuple[float, float]]] = None
        self._changed: dict[str, tuple[float, float]] = {}

    def _load(self) -> dict[str, tuple[float, float]]:
        if self._offsets is None:
            self._offsets = self.store.counter_offsets()
        return self._offsets

    def known(self, name: str) -> list[list[str]]:
        """Label values of every series of the counter seen before, also those no longer in the store."""
        prefix = name + "\x1f"
        return [k.split("\x1f")[1:] for k in sorted(self._load()) if k.startswith(prefix)]

    def value(self, name: str, labels: list[str], raw: float) -> float:
        self._offsets = self._load()
        series = "\x1f".join([name, *labels])
        last, offset = self._offsets.get(series, (raw, 0.0))
        if raw < last - 1e-9:
            offset += last - raw
        if (raw, offset) != self._offsets.get(series):
            self._offsets[series] = self._changed[series] = (raw, offset)
        return raw + offset

    def save(self) -> None:
        if self._changed:
            self.store.save_counter_offsets(self._changed)
            self._changed = {}

    def discard(self) -> None:
        self._offsets, self._changed = None, {}


class StateCollector(Collector):
    """Builds the key and store derived metrics at scrape time; totals come from the store and survive restarts."""

    def __init__(self, store: "Store", keyset: "KeySet", clock: Callable[[], float] = time.time):
        self.store = store
        self.keyset = keyset
        self.clock = clock
        self._lock = threading.Lock()
        self._totals = _MonotonicTotals(store)
        self._emitted: set[tuple[str, tuple[str, ...]]] = set()

    def describe(self) -> list:
        return []

    def collect(self) -> Iterator[Metric]:
        with self._lock:
            try:
                families = self._build()
                self._totals.save()
            except Exception:
                self._totals.discard()
                log.exception("building state metrics failed")
                return
        yield from families

    def _counter(self, family: CounterMetricFamily, labels: list[str], raw: float) -> None:
        self._emitted.add((family.name, tuple(labels)))
        family.add_metric(labels, self._totals.value(family.name, labels, raw))

    def _build(self) -> list[Metric]:
        self._emitted = set()
        since = int(self.clock()) - DAY
        keys = self.keyset.all()
        validators = self.store.validators()

        def vindex(pubkey: str) -> str:
            info = validators.get(pubkey)
            return str(info.index) if info is not None and info.index is not None else ""

        def group_of(pubkey: str) -> Optional[tuple[str, ...]]:
            key = keys.get(pubkey)
            return tuple(_group_values(key.labels())) if key is not None else None

        families: list[Metric] = []
        groups: set[tuple[str, ...]] = set()
        families += self._keys_and_deposits(keys, validators, vindex, groups, since)
        families += self._exit_requests(vindex, groups, since)
        families += self._triggered(group_of, vindex, since)
        families += self._cursors()
        # A series whose rows are gone (reorg, key set change) keeps its last value instead of vanishing.
        for family in families:
            if isinstance(family, CounterMetricFamily):
                for labels in self._totals.known(family.name):
                    if (family.name, tuple(labels)) not in self._emitted:
                        self._counter(family, labels, 0)
        return families

    def _keys_and_deposits(self, keys, validators, vindex, groups: set, since: int) -> list[Metric]:
        summary = self.store.deposit_summary()
        totals = self.store.deposit_event_totals()
        recent = self.store.deposit_event_totals(since)

        monitored = GaugeMetricFamily("lido_keys_monitored", "Monitored keys", labels=GROUP)
        deposited = GaugeMetricFamily("lido_keys_deposited", "Monitored keys with a known deposit", labels=GROUP)
        kind_labels = GROUP + ("kind",)
        events_total = CounterMetricFamily(
            "lido_keys_deposit_events", "Deposit events seen for monitored keys", labels=kind_labels
        )
        eth_total = CounterMetricFamily(
            "lido_keys_deposit_eth", "ETH deposited to monitored keys", labels=kind_labels
        )
        events_24h = GaugeMetricFamily(
            "lido_keys_deposit_events_24h", "Deposit events in the last 24h by block time", labels=kind_labels
        )
        eth_24h = GaugeMetricFamily(
            "lido_keys_deposit_eth_24h", "ETH deposited in the last 24h by block time", labels=kind_labels
        )
        deposit_ts = GaugeMetricFamily(
            "lido_keys_deposit_timestamp_seconds", "Block time of the initial deposit", labels=PER_KEY
        )
        deposit_eth = GaugeMetricFamily(
            "lido_keys_deposit_initial_eth", "Amount of the initial deposit in ETH", labels=PER_KEY + ("credentials",)
        )
        mismatch = GaugeMetricFamily(
            "lido_keys_deposit_credentials_mismatch",
            "1 if the initial deposit used unexpected withdrawal credentials",
            labels=PER_KEY,
        )
        topups = GaugeMetricFamily("lido_keys_topups", "Top-up deposits per key", labels=PER_KEY)
        topup_eth = GaugeMetricFamily("lido_keys_topup_eth", "ETH added by top-ups per key", labels=PER_KEY)
        topup_last = GaugeMetricFamily(
            "lido_keys_topup_last_timestamp_seconds", "Block time of the latest top-up per key", labels=PER_KEY
        )

        n_monitored: TallyCounter = TallyCounter()
        n_deposited: TallyCounter = TallyCounter()
        all_time: dict[tuple, list[int]] = {}
        window: dict[tuple, list[int]] = {}
        for pubkey, key in keys.items():
            group = tuple(_group_values(key.labels()))
            groups.add(group)
            n_monitored[group] += 1
            info = validators.get(pubkey)
            dep = summary.get(pubkey)
            if dep is not None or (info is not None and info.prior_deposit):
                n_deposited[group] += 1
            for source, target in ((totals, all_time), (recent, window)):
                for kind, (count, gwei) in source.get(pubkey, {}).items():
                    acc = target.setdefault(group + (kind,), [0, 0])
                    acc[0] += count
                    acc[1] += gwei
            if dep is None:
                continue
            values = list(group) + [pubkey, vindex(pubkey)]
            if dep.initial_timestamp is not None:
                deposit_ts.add_metric(values, dep.initial_timestamp)
                deposit_eth.add_metric(values + [dep.credentials_type or ""], (dep.initial_gwei or 0) / GWEI)
                mismatch.add_metric(values, 1 if dep.mismatch else 0)
            if dep.topups:
                topups.add_metric(values, dep.topups)
                topup_eth.add_metric(values, dep.topup_gwei / GWEI)
                if dep.last_topup_timestamp is not None:
                    topup_last.add_metric(values, dep.last_topup_timestamp)
        for group in sorted(n_monitored):
            monitored.add_metric(list(group), n_monitored[group])
            deposited.add_metric(list(group), n_deposited.get(group, 0))
            for kind in DEPOSIT_KINDS:
                count, gwei = all_time.get(group + (kind,), (0, 0))
                self._counter(events_total, list(group) + [kind], count)
                self._counter(eth_total, list(group) + [kind], gwei / GWEI)
                count, gwei = window.get(group + (kind,), (0, 0))
                events_24h.add_metric(list(group) + [kind], count)
                eth_24h.add_metric(list(group) + [kind], gwei / GWEI)
        return [
            monitored,
            deposited,
            events_total,
            eth_total,
            events_24h,
            eth_24h,
            deposit_ts,
            deposit_eth,
            mismatch,
            topups,
            topup_eth,
            topup_last,
        ]

    def _exit_requests(self, vindex, groups: set, since: int) -> list[Metric]:
        def group_key(set_name: str, origin: str, module_id: Optional[int], operator_id: Optional[int]) -> tuple:
            return (set_name, origin, _opt(module_id), _opt(operator_id))

        totals = {group_key(*k): n for k, n in self.store.exit_request_totals().items()}
        recent = {group_key(*k): n for k, n in self.store.exit_request_totals(since).items()}
        requests_total = CounterMetricFamily(
            "lido_keys_exit_requests", "Lido exit requests for monitored keys", labels=GROUP
        )
        requests_24h = GaugeMetricFamily(
            "lido_keys_exit_requests_24h", "Lido exit requests in the last 24h by block time", labels=GROUP
        )
        for group in sorted(groups | set(totals)):
            self._counter(requests_total, list(group), totals.get(group, 0))
            requests_24h.add_metric(list(group), recent.get(group, 0))

        exit_open = GaugeMetricFamily(
            "lido_keys_exit_request_open", "Oldest open exit request timestamp per key", labels=PER_KEY
        )
        exits_open = GaugeMetricFamily(
            "lido_keys_exit_requests_open", "Keys with an open exit request", labels=GROUP
        )
        oldest: dict[str, tuple[int, tuple[str, ...]]] = {}
        for rec in self.store.open_exit_requests(checked_only=True):
            group = group_key(rec.set_name, rec.origin, rec.module_id, rec.operator_id)
            prev = oldest.get(rec.pubkey)
            if prev is None or rec.request_timestamp < prev[0]:
                oldest[rec.pubkey] = (rec.request_timestamp, group)
        n_open: TallyCounter = TallyCounter()
        for pubkey, (ts, group) in oldest.items():
            exit_open.add_metric(list(group) + [pubkey, vindex(pubkey)], ts)
            n_open[group] += 1
        for group, n in n_open.items():
            exits_open.add_metric(list(group), n)
        return [requests_total, requests_24h, exit_open, exits_open]

    def _triggered(self, group_of, vindex, since: int) -> list[Metric]:
        totals: dict[tuple, list[int]] = {}
        for (pubkey, kind, source), (count, gwei) in self.store.triggered_totals().items():
            group = group_of(pubkey)
            if group is not None:
                acc = totals.setdefault(group + (kind, source), [0, 0])
                acc[0] += count
                acc[1] += gwei
        recent: TallyCounter = TallyCounter()
        for (pubkey, kind, source), (count, _gwei) in self.store.triggered_totals(since).items():
            group = group_of(pubkey)
            if group is not None:
                recent[group + (kind, source)] += count

        kind_source = GROUP + ("kind", "source")
        requests_total = CounterMetricFamily(
            "lido_keys_triggered_withdrawals",
            "EIP-7002 triggered withdrawal requests for monitored keys",
            labels=kind_source,
        )
        gwei_total = CounterMetricFamily(
            "lido_keys_triggered_withdrawal_gwei",
            "Amount requested by EIP-7002 partial withdrawals (gwei)",
            labels=GROUP + ("source",),
        )
        requests_24h = GaugeMetricFamily(
            "lido_keys_triggered_withdrawals_24h",
            "EIP-7002 triggered withdrawal requests in the last 24h by block time",
            labels=kind_source,
        )
        gwei: TallyCounter = TallyCounter()
        for labels in sorted(totals):
            count, amount = totals[labels]
            self._counter(requests_total, list(labels), count)
            requests_24h.add_metric(list(labels), recent.get(labels, 0))
            if labels[len(GROUP)] == "partial":
                gwei[labels[: len(GROUP)] + labels[-1:]] += amount
        for labels in sorted(gwei):
            self._counter(gwei_total, list(labels), gwei[labels])

        last = GaugeMetricFamily(
            "lido_keys_triggered_withdrawal_last_timestamp_seconds",
            "Block time of the last EIP-7002 request per key, kind and source",
            labels=PER_KEY + ("kind", "source"),
        )
        for (pubkey, kind, source), ts in sorted(self.store.triggered_last().items()):
            group = group_of(pubkey)
            if group is not None:
                last.add_metric(list(group) + [pubkey, vindex(pubkey), kind, source], ts)
        return [requests_total, gwei_total, requests_24h, last]

    def _cursors(self) -> list[Metric]:
        cursors = self.store.all_cursors()
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
        return [last_block, last_ts, last_slot, stream_block]


class EndpointCollector(Collector):
    """Health of every EL and beacon endpoint, read from the clients at scrape time."""

    def __init__(self, clients: Iterable[Any]):
        self.clients = list(clients)

    def describe(self) -> list:
        return []

    def collect(self) -> Iterator[Metric]:
        try:
            families = self._build()
        except Exception:
            log.exception("building endpoint metrics failed")
            return
        yield from families

    def _build(self) -> list[Metric]:
        labels = ("kind", "endpoint")
        up = GaugeMetricFamily(
            "lido_keys_endpoint_up", "1 if the endpoint is reachable, synced and not lagging", labels=labels
        )
        syncing = GaugeMetricFamily("lido_keys_endpoint_syncing", "1 if the endpoint reports syncing", labels=labels)
        head = GaugeMetricFamily(
            "lido_keys_endpoint_head", "Head block (EL) or slot (CL) of the endpoint", labels=labels
        )
        lag = GaugeMetricFamily(
            "lido_keys_endpoint_lag", "Blocks (EL) or slots (CL) behind the best endpoint of its kind", labels=labels
        )
        errors = CounterMetricFamily(
            "lido_keys_endpoint_errors", "Endpoint errors by reason", labels=labels + ("reason",)
        )
        for client in self.clients:
            for state in client.endpoint_states():
                values = [state.kind, state.endpoint]
                up.add_metric(values, 1 if state.up else 0)
                syncing.add_metric(values, 1 if state.syncing else 0)
                if state.head is not None:
                    head.add_metric(values, state.head)
                if state.lag is not None:
                    lag.add_metric(values, state.lag)
                reasons = list(REASONS) + sorted(set(state.errors) - set(REASONS))
                for reason in reasons:
                    errors.add_metric(values + [reason], state.errors.get(reason, 0))
        return [up, syncing, head, lag, errors]
