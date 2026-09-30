"""Main loop: follows the EL log streams, refreshes the key set and does per-epoch beacon work."""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from typing import Any, Callable, Optional

from .clients.cl import BeaconClient, CLError
from .clients.el import ELError, ELRpcError, ExecutionClient
from .clients.keys_api import KeysApiError
from .config import Config
from .contracts import (
    BLOCKS_PER_DAY,
    DEPOSIT_CONTRACT,
    DEPOSIT_EVENT_TOPIC,
    EXIT_REQUEST_TOPIC,
    EXITING_STATUSES,
    GENESIS_TIME,
    LIDO_LOCATOR,
    SELECTOR_VALIDATORS_EXIT_BUS_ORACLE,
    SELECTOR_WITHDRAWAL_VAULT,
    SECONDS_PER_SLOT,
    SLOTS_PER_EPOCH,
    WITHDRAWAL_REQUEST_CONTRACT,
    DepositLog,
    credentials_match,
    credentials_type,
    decode_address_word,
    decode_deposit,
    decode_exit_request,
    decode_withdrawal_request,
    slot_at,
    uint_topic,
)
from .keyset import KeySet
from .metrics import Metrics
from .store import DepositRecord, ExitRequestRecord, Store, TriggeredRecord, ValidatorInfo

log = logging.getLogger(__name__)

STREAM_DEPOSITS = "deposits"
STREAM_EXITS = "exits"
STREAM_TRIGGERED = "triggered"

Pending = list[Callable[[], None]]
FAR_FUTURE_EPOCH = 2**64 - 1


def _component(exc: BaseException, default: str) -> str:
    if isinstance(exc, ELError):
        return "el"
    if isinstance(exc, CLError):
        return "cl"
    if isinstance(exc, sqlite3.Error):
        return "store"
    if isinstance(exc, KeysApiError):
        return "keys_api"
    return default


class Exporter:
    def __init__(
        self,
        cfg: Config,
        el: ExecutionClient,
        cl: BeaconClient,
        keyset: KeySet,
        store: Store,
        metrics: Metrics,
        clock: Callable[[], float] = time.time,
    ):
        self.cfg = cfg
        self.el = el
        self.cl = cl
        self.keyset = keyset
        self.store = store
        self.metrics = metrics
        self.clock = clock
        self.vebo: Optional[str] = None
        self.vault: Optional[str] = None
        self.safe_head: Optional[int] = None
        self._next_refresh: Optional[float] = None
        self._exits_epoch: Optional[int] = None
        self._seed_epoch: Optional[int] = None
        self._seed_pending = False
        self._unmatched: set[str] = set()

    @property
    def addresses_resolved(self) -> bool:
        return self.vebo is not None and self.vault is not None

    def resolve_addresses(self) -> bool:
        try:
            vebo = decode_address_word(self.el.call(LIDO_LOCATOR, SELECTOR_VALIDATORS_EXIT_BUS_ORACLE))
            vault = self.cfg.expected_vault_address or decode_address_word(
                self.el.call(LIDO_LOCATOR, SELECTOR_WITHDRAWAL_VAULT)
            )
        except Exception:
            log.exception("resolving Lido addresses failed")
            self.metrics.error("el")
            return False
        for name, old, new in (("ValidatorsExitBusOracle", self.vebo, vebo), ("withdrawal vault", self.vault, vault)):
            if old is not None and old != new:
                log.warning("%s address changed from %s to %s", name, old, new)
        if self.vebo is None:
            log.info("ValidatorsExitBusOracle %s, withdrawal vault %s", vebo, vault)
        self.vebo, self.vault = vebo, vault
        return True

    # -- main loop ---------------------------------------------------------

    def tick(self) -> None:
        ok = True

        def step(component: str, fn: Callable[[], Any]) -> Any:
            nonlocal ok
            try:
                return fn()
            except Exception as exc:
                ok = False
                log.exception("%s failed", getattr(fn, "__name__", "step"))
                self.metrics.error(_component(exc, component))
                return None

        el_ok = bool(step("el", self.el.refresh_health))
        cl_ok = bool(step("cl", self.cl.refresh_health))

        now = self.clock()
        if self._next_refresh is None or now >= self._next_refresh:
            self._next_refresh = now + self.cfg.keyset_refresh_minutes * 60
            if step("keys_api", self.refresh_keyset) is False:
                ok = False
        elif not self.addresses_resolved:
            self.resolve_addresses()
        if self._unmatched and self.vault is not None:
            step("store", self.match_unmatched)

        step("el", self.process_el)
        step("cl", self.epoch_work)

        self.metrics.up.set(1 if ok and el_ok and cl_ok and self.addresses_resolved else 0)

    def run(self, stop: threading.Event) -> None:
        self.resolve_addresses()
        while not stop.is_set():
            self.tick()
            stop.wait(self.cfg.poll_interval_seconds)

    # -- key set -----------------------------------------------------------

    def refresh_keyset(self) -> bool:
        result = self.keyset.refresh()
        if self.keyset.last_success_timestamp is not None:
            self.metrics.keyset_last_refresh.set(self.keyset.last_success_timestamp)
        log.info(
            "key set refreshed: %d keys, %d added, %d removed, %d errors",
            len(self.keyset),
            len(result.added),
            len(result.removed),
            result.errors,
        )
        self._unmatched |= set(result.added)
        self._unmatched -= set(result.removed)
        self._seed_pending = True
        self.resolve_addresses()
        return result.ok

    def match_unmatched(self) -> None:
        """Match stored raw deposits against keys that recently joined the key set."""
        pubkeys = sorted(self._unmatched)
        pending: Pending = []
        with self.store.transaction():
            validators = self.store.validators()
            for dep, ts in self.store.raw_deposits_for(pubkeys):
                self._match_deposit(dep, ts, validators, pending)
        self._unmatched.clear()
        for fn in pending:
            fn()

    # -- execution layer ---------------------------------------------------

    def process_el(self) -> None:
        head = self.el.block_number()
        safe = head - self.cfg.confirmations
        if safe < 0:
            return
        self.safe_head = safe
        self._check_reorg()

        if self.vault is not None:
            self._process_stream(
                STREAM_DEPOSITS,
                DEPOSIT_CONTRACT,
                [DEPOSIT_EVENT_TOPIC],
                self.cfg.deposit_lookback_days,
                safe,
                self._handle_deposit,
            )
        if self.vebo is not None:
            flt = self.keyset.exit_topic_filter()
            if flt == ([], []):
                self._advance_cursor(STREAM_EXITS, safe)
            else:
                topics: list = [EXIT_REQUEST_TOPIC]
                if flt is not None:
                    topics += [[uint_topic(m) for m in flt[0]], [uint_topic(o) for o in flt[1]]]
                self._process_stream(
                    STREAM_EXITS, self.vebo, topics, self.cfg.exit_lookback_days, safe, self._handle_exit
                )
        if self.vault is not None and not self.keyset.all_sources_loaded():
            # Triggered requests are not kept for later matching, so wait for a complete key set.
            log.info("triggered: waiting for every Keys API source to load before scanning")
        elif self.vault is not None:
            self._process_stream(
                STREAM_TRIGGERED,
                WITHDRAWAL_REQUEST_CONTRACT,
                [],
                self.cfg.triggered_lookback_days,
                safe,
                self._handle_triggered,
            )

    def _check_reorg(self) -> None:
        checked: dict[tuple[int, str], bool] = {}
        rewind_to: Optional[int] = None
        for cursor in self.store.all_cursors():
            if cursor.block_hash is None:
                continue
            key = (cursor.block_number, cursor.block_hash)
            if key not in checked:
                checked[key] = self.el.get_block(cursor.block_number).hash == cursor.block_hash
            if not checked[key]:
                target = max(0, cursor.block_number - self.cfg.reorg_rewind_blocks + 1)
                rewind_to = target if rewind_to is None else min(rewind_to, target)
        if rewind_to is not None:
            # Keep events whose block is still canonical so they are neither dropped nor counted twice.
            keep = {h for n, h in self.store.event_blocks(rewind_to) if self.el.get_block(n).hash == h}
            log.warning("reorg detected, rewinding to block %d (%d event blocks still canonical)", rewind_to, len(keep))
            self.store.rewind(rewind_to, keep_hashes=keep)

    def _advance_cursor(self, stream: str, safe: int) -> None:
        cursor = self.store.get_cursor(stream)
        if cursor is not None and cursor.block_number >= safe:
            return
        header = self.el.get_block(safe)
        self.store.set_cursor(stream, header.number, header.hash, header.timestamp)

    def _process_stream(
        self,
        stream: str,
        address: str,
        topics: list,
        days: float,
        safe: int,
        handler: Callable[[dict, int, dict[str, ValidatorInfo], Pending], None],
    ) -> None:
        floor = max(0, safe - int(days * BLOCKS_PER_DAY))
        cursor = self.store.get_cursor(stream)
        if cursor is None:
            start = floor
        else:
            start = cursor.block_number + 1
            if start < floor:
                log.warning(
                    "%s: gap since block %d exceeds the lookback, resuming at %d", stream, cursor.block_number, floor
                )
                self.metrics.error("el")
                start = floor

        size = self.cfg.log_chunk_blocks
        a = start
        while a <= safe:
            b = min(safe, a + size - 1)
            header = self.el.get_block(b)
            try:
                logs = self.el.get_logs(address, topics, a, b)
            except ELRpcError:
                if size == 1:
                    raise
                size = max(1, size // 2)
                log.info("%s: getLogs %d-%d rejected, chunk size now %d", stream, a, b, size)
                continue
            headers = {header.number: header}
            prepared: list[tuple[dict, int]] = []
            for entry in logs:
                number = int(entry["blockNumber"], 16)
                ts_hex = entry.get("blockTimestamp")
                if number not in headers and not ts_hex:
                    headers[number] = self.el.get_block(number)
                known = headers.get(number)
                if known is not None and (entry.get("blockHash") or "").lower() != known.hash:
                    raise ELError(f"{stream}: log in block {number} is not on the canonical chain, retrying")
                prepared.append((entry, int(ts_hex, 16) if ts_hex else headers[number].timestamp))
            if self.el.get_block(b).hash != header.hash:
                raise ELError(f"{stream}: block {b} changed while fetching logs, retrying")

            pending: Pending = []
            with self.store.transaction():
                validators = self.store.validators()
                for entry, ts in prepared:
                    handler(entry, ts, validators, pending)
                self.store.set_cursor(stream, b, header.hash, header.timestamp)
            for fn in pending:
                fn()
            a = b + 1

        if stream == STREAM_DEPOSITS:
            self.store.prune_raw_deposits(floor)

    def _decode(self, decoder: Callable[[dict], Any], entry: dict) -> Any:
        try:
            return decoder(entry)
        except Exception:
            log.exception("cannot decode log %s/%s", entry.get("transactionHash"), entry.get("logIndex"))
            self.metrics.error("el")
            return None

    def _handle_deposit(self, entry: dict, ts: int, validators: dict[str, ValidatorInfo], pending: Pending) -> None:
        dep = self._decode(decode_deposit, entry)
        if dep is None:
            return
        self.store.insert_raw_deposit(dep, ts)
        self._match_deposit(dep, ts, validators, pending)

    def _match_deposit(
        self, dep: DepositLog, ts: int, validators: dict[str, ValidatorInfo], pending: Pending
    ) -> None:
        key = self.keyset.get(dep.pubkey)
        if key is None:
            return
        info = validators.get(dep.pubkey)
        ref = dep.ref
        is_topup = self.store.has_earlier_deposit(dep.pubkey, ref.block_number, ref.log_index) or bool(
            info is not None and info.prior_deposit
        )
        ctype = credentials_type(dep.withdrawal_credentials)
        mismatch = self.vault is not None and not credentials_match(dep.withdrawal_credentials, self.vault)
        rec = DepositRecord(
            pubkey=dep.pubkey,
            amount_gwei=dep.amount_gwei,
            withdrawal_credentials=dep.withdrawal_credentials,
            credentials_type=ctype,
            mismatch=mismatch,
            is_topup=is_topup,
            block_number=ref.block_number,
            block_hash=ref.block_hash,
            block_timestamp=ts,
            tx_hash=ref.tx_hash,
            log_index=ref.log_index,
        )
        if not self.store.insert_deposit(rec):
            return
        labels = key.labels()
        if mismatch:
            log.error(
                "deposit for %s uses unexpected withdrawal credentials %s (tx %s)",
                dep.pubkey,
                dep.withdrawal_credentials,
                ref.tx_hash,
            )
        if is_topup:
            log.info("top-up of %d gwei for %s in block %d", dep.amount_gwei, dep.pubkey, ref.block_number)
            pending.append(lambda: self.metrics.topups.labels(**labels).inc())
        else:
            log.info("deposit for %s in block %d", dep.pubkey, ref.block_number)
            pending.append(lambda: self.metrics.deposits.labels(**labels, credentials=ctype).inc())

    def _handle_exit(self, entry: dict, ts: int, validators: dict[str, ValidatorInfo], pending: Pending) -> None:
        ev = self._decode(decode_exit_request, entry)
        if ev is None:
            return
        key = self.keyset.match_exit(ev.module_id, ev.operator_id, ev.pubkey)
        if key is None:
            return
        ref = ev.ref
        rec = ExitRequestRecord(
            pubkey=ev.pubkey,
            set_name=key.set_name,
            origin=key.origin,
            module_id=key.module_id,
            operator_id=key.operator_id,
            validator_index=ev.validator_index,
            request_timestamp=ev.request_timestamp,
            block_number=ref.block_number,
            block_hash=ref.block_hash,
            block_timestamp=ts,
            tx_hash=ref.tx_hash,
            log_index=ref.log_index,
        )
        inserted = self.store.insert_exit_request(rec)
        self.store.upsert_validator(ev.pubkey, index=ev.validator_index)
        if inserted:
            log.warning("exit request for %s (validator %d) in block %d", ev.pubkey, ev.validator_index, ref.block_number)
            labels = key.labels()
            pending.append(lambda: self.metrics.exit_requests.labels(**labels).inc())

    def _handle_triggered(
        self, entry: dict, ts: int, validators: dict[str, ValidatorInfo], pending: Pending
    ) -> None:
        req = self._decode(decode_withdrawal_request, entry)
        if req is None:
            return
        key = self.keyset.get(req.pubkey)
        if key is None:
            return
        addr = req.source_address
        source = self.cfg.known_sources.get(addr) or ("lido-withdrawal-vault" if addr == self.vault else addr)
        ref = req.ref
        kind = req.kind
        rec = TriggeredRecord(
            pubkey=req.pubkey,
            kind=kind,
            amount_gwei=req.amount_gwei,
            source_address=addr,
            source_name=source,
            block_number=ref.block_number,
            block_hash=ref.block_hash,
            block_timestamp=ts,
            slot=slot_at(ts),
            tx_hash=ref.tx_hash,
            log_index=ref.log_index,
        )
        if not self.store.insert_triggered(rec):
            return
        log.warning("triggered %s withdrawal for %s from %s in block %d", kind, req.pubkey, source, ref.block_number)
        labels = key.labels()
        amount = req.amount_gwei

        def count() -> None:
            self.metrics.triggered.labels(**labels, kind=kind, source=source).inc()
            if kind == "partial":
                self.metrics.triggered_gwei.labels(**labels, source=source).inc(amount)

        pending.append(count)

    # -- consensus layer ---------------------------------------------------

    def epoch_work(self) -> None:
        """Beacon lookups; each succeeds at most once per epoch and is retried next tick on failure."""
        epoch = self.cl.head_slot() // SLOTS_PER_EPOCH
        if epoch != self._exits_epoch:
            self.check_open_exits()
            self._exits_epoch = epoch
        if epoch != self._seed_epoch:
            if self._seed_pending and not self._unmatched and self._deposits_caught_up():
                self.seed_deposits()
            self._seed_epoch = epoch

    def _deposits_caught_up(self) -> bool:
        cursor = self.store.get_cursor(STREAM_DEPOSITS)
        return cursor is not None and self.safe_head is not None and cursor.block_number >= self.safe_head

    def check_open_exits(self) -> None:
        open_requests = self.store.open_exit_requests()
        if not open_requests:
            return
        by_index: dict[int, set[str]] = {}
        for rec in open_requests:
            by_index.setdefault(rec.validator_index, set()).add(rec.pubkey)
        data = self.cl.validators([str(i) for i in sorted(by_index)])
        now = int(self.clock())
        with self.store.transaction():
            for entry in data:
                status = entry.get("status")
                if status not in EXITING_STATUSES:
                    continue
                pubkeys = set(by_index.get(int(entry["index"]), set()))
                pubkey = (entry.get("validator") or {}).get("pubkey")
                if pubkey:
                    pubkeys.add(pubkey.lower())
                for pk in pubkeys:
                    if self.store.close_exit_requests(pk, status, now):
                        log.info("exit request for %s closed, validator status %s", pk, status)

    def seed_deposits(self) -> None:
        """Mark keys deposited before the lookback and reclassify deposits of pre-existing validators as top-ups."""
        if not self.cfg.seed_deposited_from_beacon:
            self._seed_pending = False
            return
        deposited = self.store.deposited_pubkeys()
        validators = self.store.validators()
        cursor = self.store.get_cursor(STREAM_DEPOSITS)
        head_ts = cursor.block_timestamp if cursor is not None and cursor.block_timestamp else self.clock()
        since = int(head_ts - self.cfg.deposit_lookback_days * 86400)
        recent = self.store.initial_deposits_since(since)
        keys = self.keyset.all()
        candidates = []
        for pubkey in keys:
            info = validators.get(pubkey)
            if (
                info is None
                or info.index is None
                or (pubkey not in deposited and not info.prior_deposit)
                or pubkey in recent
            ):
                candidates.append(pubkey)
        data = self.cl.validators(candidates) if candidates else []
        seeded = reclassified = 0
        with self.store.transaction():
            for entry in data:
                pubkey = entry["validator"]["pubkey"].lower()
                prior = True if pubkey not in deposited else None
                eligible = int(entry["validator"].get("activation_eligibility_epoch", FAR_FUTURE_EPOCH))
                if pubkey in recent and eligible != FAR_FUTURE_EPOCH:
                    eligible_ts = GENESIS_TIME + eligible * SLOTS_PER_EPOCH * SECONDS_PER_SLOT
                    if eligible_ts < recent[pubkey]:
                        # The validator existed before its earliest deposit seen in the lookback.
                        changed = self.store.mark_topups_after(pubkey, eligible_ts)
                        if changed:
                            reclassified += changed
                            prior = True
                            log.warning("%d deposit(s) of %s reclassified as top-ups", changed, pubkey)
                seeded += pubkey not in deposited
                self.store.upsert_validator(pubkey, index=int(entry["index"]), prior_deposit=prior)
        self._seed_pending = False
        log.info(
            "seeding: %d candidates, %d on the beacon chain, %d marked as deposited before the lookback, "
            "%d deposits reclassified",
            len(candidates),
            len(data),
            seeded,
            reclassified,
        )
