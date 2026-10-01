from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import pytest

from src.app import STREAM_DEPOSITS, STREAM_EXITS, Exporter
from src.config import Config, KeysApiSource
from src.contracts import (
    DEPOSIT_CONTRACT,
    EXIT_REQUEST_TOPIC,
    GENESIS_TIME,
    LIDO_LOCATOR,
    SELECTOR_VALIDATORS_EXIT_BUS_ORACLE,
    SELECTOR_WITHDRAWAL_VAULT,
    WITHDRAWAL_REQUEST_CONTRACT,
)
from src.keyset import KeySet
from src.metrics import Metrics
from src.store import Store
from tests.fakes import (
    FakeBeaconClient,
    FakeChain,
    FakeExecutionClient,
    address,
    address_word,
    credentials,
    deposit_log,
    exit_request_log,
    pubkey,
    withdrawal_request_log,
)

VEBO = address(0xE0)
VAULT = address(0xB9)
OPS = address(0x0A)
STRANGER = address(0x5E)
WC = credentials(VAULT)
GROUP = {"set": "main", "origin": "keys_api", "module_id": "1", "operator_id": "7"}
LOOKBACK_BLOCKS = 7200  # 1 day


class Env:
    def __init__(self, tmp_path: Path, max_range: Optional[int] = None, **overrides: Any):
        cfg: dict[str, Any] = dict(
            execution_endpoints=["http://el"],
            beacon_endpoints=["http://cl"],
            data_dir=tmp_path,
            deposit_lookback_days=1,
            exit_lookback_days=1,
            triggered_lookback_days=1,
            log_chunk_blocks=1000,
            known_sources={OPS: "ops-wallet"},
            sets=[{"name": "main", "keys_api": [{"url": "http://keys-api", "module_id": 1, "operator_id": 7}]}],
        )
        cfg.update(overrides)
        self.cfg = Config.model_validate(cfg)
        self.chain = FakeChain(head=20_000)
        self.chain.call_results[(LIDO_LOCATOR, SELECTOR_VALIDATORS_EXIT_BUS_ORACLE)] = address_word(VEBO)
        self.chain.call_results[(LIDO_LOCATOR, SELECTOR_WITHDRAWAL_VAULT)] = address_word(VAULT)
        self.el = FakeExecutionClient(self.chain, max_range=max_range)
        self.cl = FakeBeaconClient()
        self.keys: dict[tuple[int, int], list[str]] = {(1, 7): []}
        self.now = 1_700_000_000.0
        self.store = Store(tmp_path / "state.sqlite3")
        self.restart()

    def fetch(self, source: KeysApiSource) -> list[str]:
        value = self.keys[(source.module_id, source.operator_id)]
        if isinstance(value, Exception):
            raise value
        return list(value)

    def restart(self) -> None:
        self.metrics = Metrics()
        self.keyset = KeySet(self.cfg, fetch=self.fetch, on_error=lambda: self.metrics.error("keys_api"))
        self.metrics.register_state(self.store, self.keyset)
        self.exporter = Exporter(
            self.cfg, self.el, self.cl, self.keyset, self.store, self.metrics, clock=lambda: self.now
        )

    def tick(self, advance: float = 12.0) -> None:
        self.now += advance
        self.exporter.tick()

    def refresh_tick(self) -> None:
        self.tick(advance=self.cfg.keyset_refresh_minutes * 60 + 1)

    def value(self, name: str, **labels: str) -> Optional[float]:
        return self.metrics.registry.get_sample_value(name, labels)

    def safe(self) -> int:
        return self.chain.head - self.cfg.confirmations

    def deposits(self, kind: str = "initial", **group: str) -> Optional[float]:
        return self.value("lido_keys_deposit_events_total", **(group or GROUP), kind=kind)


@pytest.fixture
def env(tmp_path: Path) -> Env:
    return Env(tmp_path)


def assert_head_only(cl: FakeBeaconClient) -> None:
    for path in cl.paths:
        if "/states/" in path:
            assert path.startswith("/eth/v1/beacon/states/head/"), path


def test_initial_deposit(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.chain.add_log(19_000, deposit_log(pk, WC))
    env.chain.add_log(19_001, deposit_log(pubkey(99), WC))  # not ours
    env.tick()

    assert env.deposits("initial") == 1
    assert env.deposits("topup") == 0
    assert env.value("lido_keys_monitored", **GROUP) == 1
    assert env.value("lido_keys_deposited", **GROUP) == 1
    per_key = dict(GROUP, pubkey=pk, validator_index="")
    assert env.value("lido_keys_deposit_timestamp_seconds", **per_key) == env.chain.timestamp(19_000)
    assert env.value("lido_keys_deposit_credentials_mismatch", **per_key) == 0
    assert env.value("lido_keys_deposit_initial_eth", **per_key, credentials="0x01") == 32
    assert env.value("lido_keys_topups", **per_key) is None
    summary = env.store.deposit_summary()[pk]
    assert summary.initial_block == 19_000
    assert summary.credentials_type == "0x01"
    assert env.value("lido_keys_up") == 1
    assert env.value("lido_keys_last_processed_block") == env.safe()
    assert env.value("lido_keys_stream_last_processed_block", stream="deposits") == env.safe()
    assert env.value("lido_keys_errors_total", component="el") == 0
    assert_head_only(env.cl)


def test_deposit_logs_without_block_timestamp_and_with(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.chain.include_log_timestamps = True
    env.chain.add_log(19_500, deposit_log(pk, WC))
    env.tick()
    assert env.store.deposit_summary()[pk].initial_timestamp == env.chain.timestamp(19_500)


def test_topup_counted_separately(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.chain.add_log(19_000, deposit_log(pk, WC))
    env.chain.add_log(19_100, deposit_log(pk, WC, amount_gwei=1_000_000_000))
    env.tick()
    assert env.deposits("initial") == 1
    assert env.deposits("topup") == 1
    assert env.store.deposit_summary()[pk].deposits == 2
    assert env.store.deposit_summary()[pk].initial_block == 19_000


def test_credentials_mismatch(env: Env) -> None:
    good, bad, zero, compounding = pubkey(1), pubkey(2), pubkey(3), pubkey(4)
    env.keys[(1, 7)] = [good, bad, zero, compounding]
    env.chain.add_log(19_000, deposit_log(good, WC))
    env.chain.add_log(19_000, deposit_log(bad, credentials(STRANGER)))
    env.chain.add_log(19_001, deposit_log(zero, "0x00" + "ab" * 31))
    env.chain.add_log(19_002, deposit_log(compounding, credentials(VAULT, prefix=2)))
    env.tick()

    def mismatch(pk: str) -> Optional[float]:
        return env.value("lido_keys_deposit_credentials_mismatch", **GROUP, pubkey=pk, validator_index="")

    assert mismatch(good) == 0
    assert mismatch(bad) == 1
    assert mismatch(zero) == 1
    assert mismatch(compounding) == 0
    assert env.deposits("initial") == 4

    def ctype(pk: str, credentials: str) -> Optional[float]:
        return env.value("lido_keys_deposit_initial_eth", **GROUP, pubkey=pk, validator_index="", credentials=credentials)

    assert ctype(bad, "0x01") == 32
    assert ctype(zero, "0x00") == 32
    assert ctype(compounding, "0x02") == 32


def test_topup_with_other_credentials_is_no_mismatch(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.chain.add_log(19_000, deposit_log(pk, WC))
    env.chain.add_log(19_100, deposit_log(pk, credentials(STRANGER), amount_gwei=1_000_000_000))
    env.tick()
    per_key = dict(GROUP, pubkey=pk, validator_index="")
    assert env.value("lido_keys_deposit_credentials_mismatch", **per_key) == 0
    assert env.store.deposit_summary()[pk].mismatch is False
    assert env.value("lido_keys_topups", **per_key) == 1
    assert env.value("lido_keys_topup_eth", **per_key) == 1
    assert env.value("lido_keys_topup_last_timestamp_seconds", **per_key) == env.chain.timestamp(19_100)


def test_expected_credentials_from_config(tmp_path: Path) -> None:
    other_vault = address(0xCC)
    env = Env(tmp_path, expected_withdrawal_credentials=other_vault)
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.chain.add_log(19_000, deposit_log(pk, WC))
    env.tick()
    assert env.exporter.vault == other_vault
    assert env.store.deposit_summary()[pk].mismatch is True
    assert (LIDO_LOCATOR, SELECTOR_WITHDRAWAL_VAULT) not in env.el.calls


def test_deposit_before_key_known_matched_on_refresh(env: Env) -> None:
    pk = pubkey(5)
    env.chain.add_log(19_000, deposit_log(pk, WC))
    env.tick()
    assert env.store.deposited_pubkeys() == set()
    assert env.deposits("initial") is None  # no monitored keys in the group yet

    env.keys[(1, 7)] = [pk]
    env.tick()  # refresh not due yet
    assert env.store.deposited_pubkeys() == set()
    env.refresh_tick()
    assert env.store.deposited_pubkeys() == {pk}
    assert env.store.deposit_summary()[pk].initial_block == 19_000
    assert env.deposits("initial") == 1


def test_seeding_marks_prior_deposit_so_later_deposit_is_topup(env: Env) -> None:
    old, fresh = pubkey(1), pubkey(2)
    env.keys[(1, 7)] = [old, fresh]
    env.cl.add_validator(old, 4242)
    env.tick()

    info = env.store.validators()[old]
    assert info.prior_deposit is True and info.index == 4242
    assert fresh not in env.store.validators()
    assert env.value("lido_keys_deposited", **GROUP) == 1
    assert [sorted(c) for c in env.cl.validator_calls] == [sorted([old, fresh])]

    env.chain.mine(10)
    env.chain.add_log(env.chain.head - 5, deposit_log(old, WC, amount_gwei=5_000_000_000))
    env.chain.add_log(env.chain.head - 5, deposit_log(fresh, WC))
    env.tick()
    assert env.deposits("topup") == 1
    assert env.deposits("initial") == 1
    assert env.value("lido_keys_deposited", **GROUP) == 2
    assert_head_only(env.cl)


def test_seeding_waits_for_deposits_stream_and_runs_once_per_refresh(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.el.fail = True
    env.tick()
    assert env.cl.validator_calls == []
    env.el.fail = False
    env.tick()  # same epoch: no beacon work yet
    assert env.cl.validator_calls == []
    env.cl.next_epoch()
    env.tick()
    assert env.cl.validator_calls == [[pk]]
    env.cl.next_epoch()
    env.tick()
    assert env.cl.validator_calls == [[pk]]


def test_seeding_disabled(tmp_path: Path) -> None:
    env = Env(tmp_path, seed_deposited_from_beacon=False)
    env.keys[(1, 7)] = [pubkey(1)]
    env.cl.add_validator(pubkey(1), 1)
    env.tick()
    assert env.cl.validator_calls == []
    assert env.store.validators() == {}


def test_exit_request_open_until_exiting(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.cl.add_validator(pk, 777)
    request_ts = int(env.chain.timestamp(19_500))
    env.chain.add_log(19_500, exit_request_log(VEBO, 1, 7, 777, pk, request_ts))
    env.chain.add_log(19_501, exit_request_log(VEBO, 1, 8, 778, pubkey(9), request_ts))  # other operator
    env.tick()

    assert env.value("lido_keys_exit_requests_total", **GROUP) == 1
    per_key = dict(GROUP, pubkey=pk, validator_index="777")
    assert env.value("lido_keys_exit_request_open", **per_key) == request_ts
    assert env.value("lido_keys_exit_requests_open", **GROUP) == 1
    exit_calls = env.el.calls_for(VEBO)
    assert exit_calls and all(c[1][0] == EXIT_REQUEST_TOPIC and len(c[1]) == 3 for c in exit_calls)

    env.cl.next_epoch()
    env.tick()
    assert env.value("lido_keys_exit_request_open", **per_key) == request_ts

    env.cl.set_status(pk, "active_exiting")
    env.tick()  # same epoch: not checked yet
    assert env.value("lido_keys_exit_request_open", **per_key) == request_ts
    env.cl.next_epoch()
    env.tick()
    assert env.value("lido_keys_exit_request_open", **per_key) is None
    assert env.value("lido_keys_exit_requests_open", **GROUP) is None
    assert env.store.open_exit_requests() == []
    assert env.store.exit_request_count() == 1

    calls = len(env.cl.validator_calls)
    env.cl.next_epoch()
    env.tick()
    assert len(env.cl.validator_calls) == calls  # nothing open: no beacon lookup
    assert_head_only(env.cl)


def test_exit_request_for_static_pubkey(tmp_path: Path) -> None:
    static = pubkey(50)
    env = Env(
        tmp_path,
        sets=[
            {
                "name": "main",
                "keys_api": [{"url": "http://keys-api", "module_id": 1, "operator_id": 7}],
                "static_pubkeys": [{"pubkey": static, "label": "solo"}],
            }
        ],
    )
    env.chain.add_log(19_500, exit_request_log(VEBO, 3, 99, 555, static, 1234))
    env.chain.add_log(19_501, exit_request_log(VEBO, 3, 99, 556, pubkey(51), 1234))
    env.tick()

    calls = env.el.calls_for(VEBO)
    assert calls and all(c[1] == [EXIT_REQUEST_TOPIC] for c in calls)
    static_group = {"set": "main", "origin": "static", "module_id": "", "operator_id": ""}
    assert env.value("lido_keys_exit_requests_total", **static_group) == 1
    assert env.value("lido_keys_exit_request_open", **static_group, pubkey=static, validator_index="555") == 1234
    assert env.store.exit_request_count() == 1


def test_no_exit_sources_only_advances_cursor(tmp_path: Path) -> None:
    env = Env(tmp_path, sets=[{"name": "empty"}])
    env.chain.add_log(19_500, exit_request_log(VEBO, 1, 7, 1, pubkey(1), 1))
    env.tick()
    assert env.el.calls_for(VEBO) == []
    assert env.store.get_cursor(STREAM_EXITS).block_number == env.safe()


def test_triggered_withdrawals_and_source_names(env: Env) -> None:
    pk1, pk2 = pubkey(1), pubkey(2)
    env.keys[(1, 7)] = [pk1, pk2]
    env.chain.add_log(19_600, withdrawal_request_log(OPS, pk1, 0))
    env.chain.add_log(19_601, withdrawal_request_log(VAULT, pk2, 0))
    env.chain.add_log(19_602, withdrawal_request_log(STRANGER, pk2, 2_000_000_000))
    env.chain.add_log(19_603, withdrawal_request_log(OPS, pk1, 1_000_000_000))
    env.chain.add_log(19_604, withdrawal_request_log(OPS, pubkey(77), 0))  # not ours
    env.tick()

    def trig(kind: str, source: str) -> Optional[float]:
        return env.value("lido_keys_triggered_withdrawals_total", **GROUP, kind=kind, source=source)

    assert trig("exit", "ops-wallet") == 1
    assert trig("exit", "lido-withdrawal-vault") == 1
    assert trig("partial", STRANGER) == 1
    assert trig("partial", "ops-wallet") == 1
    assert env.value("lido_keys_triggered_withdrawal_gwei_total", **GROUP, source=STRANGER) == 2_000_000_000
    assert env.value("lido_keys_triggered_withdrawal_gwei_total", **GROUP, source="ops-wallet") == 1_000_000_000
    assert env.value("lido_keys_triggered_withdrawal_gwei_total", **GROUP, source="lido-withdrawal-vault") is None
    last = env.value(
        "lido_keys_triggered_withdrawal_last_timestamp_seconds",
        **GROUP,
        pubkey=pk2,
        validator_index="",
        kind="partial",
        source=STRANGER,
    )
    assert last == env.chain.timestamp(19_602)
    calls = env.el.calls_for(WITHDRAWAL_REQUEST_CONTRACT)
    assert calls and all(c[1] == [] for c in calls)


def test_restart_continues_from_cursor(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.chain.add_log(19_000, deposit_log(pk, WC))
    env.chain.add_log(19_010, exit_request_log(VEBO, 1, 7, 10, pk, 111))
    env.chain.add_log(19_020, withdrawal_request_log(OPS, pk, 0))
    env.tick()
    first_safe = env.safe()

    env.chain.mine(500)
    env.chain.add_log(env.chain.head - 100, deposit_log(pk, WC, amount_gwei=1))
    env.chain.add_log(env.chain.head - 100, withdrawal_request_log(OPS, pk, 5))
    env.el.get_logs_calls.clear()
    env.restart()
    env.tick()

    # Totals come from the store: they survive the restart and include the new events once.
    assert env.deposits("initial") == 1
    assert env.deposits("topup") == 1
    assert env.value("lido_keys_exit_requests_total", **GROUP) == 1
    assert env.value("lido_keys_triggered_withdrawals_total", **GROUP, kind="exit", source="ops-wallet") == 1
    assert env.value("lido_keys_triggered_withdrawals_total", **GROUP, kind="partial", source="ops-wallet") == 1
    assert env.value("lido_keys_triggered_withdrawal_gwei_total", **GROUP, source="ops-wallet") == 5
    # Gauges are rebuilt from the store.
    assert env.value("lido_keys_deposited", **GROUP) == 1
    assert env.value("lido_keys_exit_requests_open", **GROUP) == 1
    # No gap and no rescan: every stream resumes at the stored cursor.
    for addr in (DEPOSIT_CONTRACT, VEBO, WITHDRAWAL_REQUEST_CONTRACT):
        calls = env.el.calls_for(addr)
        assert calls[0][2] == first_safe + 1
        assert calls[-1][3] == env.safe()
        assert all(b[2] == a[3] + 1 for a, b in zip(calls, calls[1:]))
    assert env.value("lido_keys_errors_total", component="el") == 0


def test_gap_larger_than_lookback(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.tick()
    env.chain.mine(LOOKBACK_BLOCKS + 1000)
    env.chain.add_log(env.chain.head - LOOKBACK_BLOCKS - 500, deposit_log(pk, WC))  # lost in the gap
    env.chain.add_log(env.chain.head - 100, deposit_log(pubkey(2), WC))
    env.keys[(1, 7)] = [pk, pubkey(2)]
    env.el.get_logs_calls.clear()
    env.restart()
    env.tick()

    floor = env.safe() - LOOKBACK_BLOCKS
    assert env.el.calls_for(DEPOSIT_CONTRACT)[0][2] == floor
    assert env.value("lido_keys_errors_total", component="el") == 3  # one per stream
    assert env.store.deposited_pubkeys() == {pubkey(2)}


def test_reorg_drops_non_canonical_events(env: Env) -> None:
    pk1, pk2 = pubkey(1), pubkey(2)
    env.keys[(1, 7)] = [pk1, pk2]
    head = env.chain.head
    env.chain.add_log(head - 3, deposit_log(pk1, WC))
    env.chain.add_log(head - 3, withdrawal_request_log(OPS, pk1, 0))
    env.tick()
    assert env.store.deposited_pubkeys() == {pk1}
    assert env.store.triggered_last()

    env.chain.reorg(head - 5)
    env.chain.mine(3)
    env.chain.add_log(head - 4, deposit_log(pk2, WC))
    env.tick()

    assert env.store.deposited_pubkeys() == {pk2}
    assert env.store.triggered_last() == {}
    assert env.value("lido_keys_deposit_timestamp_seconds", **GROUP, pubkey=pk1, validator_index="") is None
    assert env.value("lido_keys_deposit_timestamp_seconds", **GROUP, pubkey=pk2, validator_index="") is not None
    cursor = env.store.get_cursor(STREAM_DEPOSITS)
    assert cursor.block_number == env.safe()
    assert cursor.block_hash == env.chain.block_hash(env.safe())


def test_confirmations_hold_back_recent_blocks(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.chain.add_log(env.chain.head, deposit_log(pk, WC))
    env.tick()
    assert env.store.deposited_pubkeys() == set()
    env.chain.mine(2)
    env.tick()
    assert env.store.deposited_pubkeys() == {pk}


def test_get_logs_chunk_halving(tmp_path: Path) -> None:
    env = Env(tmp_path, max_range=300)
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.chain.add_log(15_000, deposit_log(pk, WC))
    env.chain.add_log(19_900, deposit_log(pk, WC, amount_gwei=1))
    env.tick()

    assert env.el.rejected_ranges
    calls = env.el.calls_for(DEPOSIT_CONTRACT)
    assert all(b - a + 1 <= 300 for _, _, a, b in calls)
    assert calls[0][2] == env.safe() - LOOKBACK_BLOCKS
    assert calls[-1][3] == env.safe()
    assert all(b[2] == a[3] + 1 for a, b in zip(calls, calls[1:]))
    assert env.deposits("initial") == 1
    assert env.deposits("topup") == 1


def test_keys_api_failure_keeps_last_keys(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.tick()
    assert env.value("lido_keys_up") == 1
    refreshed = env.value("lido_keys_keyset_last_refresh_timestamp_seconds")
    assert refreshed

    env.keys[(1, 7)] = RuntimeError("keys api down")  # type: ignore[assignment]
    env.refresh_tick()
    assert env.value("lido_keys_errors_total", component="keys_api") == 1
    assert env.value("lido_keys_monitored", **GROUP) == 1
    assert env.value("lido_keys_keyset_last_refresh_timestamp_seconds") == refreshed
    assert env.value("lido_keys_up") == 0


def test_unresolved_addresses_retry_and_up(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.chain.add_log(19_000, deposit_log(pk, WC))
    saved = dict(env.chain.call_results)
    env.chain.call_results.clear()
    env.tick()
    assert env.value("lido_keys_up") == 0
    assert env.store.deposited_pubkeys() == set()
    assert env.value("lido_keys_errors_total", component="el") >= 1

    env.chain.call_results.update(saved)
    env.tick()
    assert env.exporter.vebo == VEBO and env.exporter.vault == VAULT
    assert env.store.deposited_pubkeys() == {pk}
    assert env.value("lido_keys_up") == 1


def test_el_outage_does_not_raise(env: Env) -> None:
    env.keys[(1, 7)] = [pubkey(1)]
    env.tick()
    env.el.fail = True
    env.tick()
    assert env.value("lido_keys_up") == 0
    env.cl.fail = True
    env.cl.next_epoch()
    env.tick()
    assert env.value("lido_keys_errors_total", component="cl") >= 1


def test_beacon_never_non_head_state(env: Env) -> None:
    pks = [pubkey(i) for i in range(1, 6)]
    env.keys[(1, 7)] = pks
    for i, pk in enumerate(pks):
        env.cl.add_validator(pk, 100 + i)
        env.chain.add_log(19_000 + i, exit_request_log(VEBO, 1, 7, 100 + i, pk, 1))
    for _ in range(5):
        env.cl.next_epoch()
        env.refresh_tick()
    assert any("/states/" in p for p in env.cl.paths)
    assert_head_only(env.cl)


def test_triggered_waits_for_keys_api(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = RuntimeError("keys api down")  # type: ignore[assignment]
    env.chain.add_log(19_600, withdrawal_request_log(STRANGER, pk, 0))
    env.tick()
    assert env.store.get_cursor("triggered") is None
    assert env.store.triggered_last() == {}

    env.keys[(1, 7)] = [pk]
    env.refresh_tick()
    assert env.value("lido_keys_triggered_withdrawals_total", **GROUP, kind="exit", source=STRANGER) == 1


def test_reorg_of_cursor_block_does_not_recount(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.cl.add_validator(pk, 777)
    head = env.chain.head
    env.chain.add_log(head - 30, deposit_log(pk, WC))
    env.chain.add_log(head - 30, withdrawal_request_log(STRANGER, pk, 0))
    env.chain.add_log(head - 30, exit_request_log(VEBO, 1, 7, 777, pk, 1))
    env.tick()
    env.cl.set_status(pk, "active_exiting")
    env.cl.next_epoch()
    env.tick()
    assert env.store.open_exit_requests() == []

    env.chain.reorg(env.safe())
    env.chain.mine(1)
    env.tick()
    assert env.deposits("initial") == 1
    assert env.value("lido_keys_triggered_withdrawals_total", **GROUP, kind="exit", source=STRANGER) == 1
    assert env.value("lido_keys_exit_requests_total", **GROUP) == 1
    assert env.store.open_exit_requests() == []
    assert env.store.get_cursor(STREAM_DEPOSITS).block_hash == env.chain.block_hash(env.safe())


def test_log_from_other_fork_is_not_committed(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.chain.add_log(env.safe(), deposit_log(pk, WC))
    real = env.chain.rpc_log
    env.chain.rpc_log = lambda e: dict(real(e), blockHash="0x" + "ff" * 32)  # type: ignore[method-assign]
    env.tick()
    assert env.store.get_cursor(STREAM_DEPOSITS).block_number < env.safe()
    assert env.store.deposited_pubkeys() == set()
    assert env.value("lido_keys_up") == 0
    env.chain.rpc_log = real  # type: ignore[method-assign]
    env.tick()
    assert env.store.deposited_pubkeys() == {pk}


def test_no_seeding_while_added_keys_unmatched(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    pk = pubkey(5)
    env.cl.add_validator(pk, 5)
    env.chain.add_log(19_000, deposit_log(pk, WC))
    env.tick()
    env.keys[(1, 7)] = [pk]
    real = env.store.raw_deposits_for
    monkeypatch.setattr(env.store, "raw_deposits_for", lambda pks: (_ for _ in ()).throw(RuntimeError("boom")))
    env.cl.next_epoch()
    env.refresh_tick()
    assert env.store.validators().get(pk) is None
    monkeypatch.setattr(env.store, "raw_deposits_for", real)
    env.cl.next_epoch()
    env.tick()
    assert env.deposits("initial") == 1
    assert env.deposits("topup") == 0


def test_seeding_reclassifies_topup_of_existing_validator(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.cl.add_validator(pk, 11, eligibility_epoch=0)
    env.chain.add_log(19_000, deposit_log(pk, WC, amount_gwei=1_000_000_000))
    env.tick()
    assert env.store.initial_deposits_since(0) == {}
    info = env.store.validators()[pk]
    assert info.prior_deposit is True and info.index == 11

    assert env.deposits("initial") == 0
    assert env.deposits("topup") == 1

    env.chain.mine(10)
    env.chain.add_log(env.chain.head - 5, deposit_log(pk, WC))
    env.tick()
    assert env.deposits("initial") == 0
    assert env.deposits("topup") == 2
    per_key = dict(GROUP, pubkey=pk, validator_index="11")
    assert env.value("lido_keys_deposit_timestamp_seconds", **per_key) is None
    assert env.value("lido_keys_topups", **per_key) == 2


def test_reclassification_moves_event_to_topup_and_clears_mismatch(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.cl.add_validator(pk, 11, eligibility_epoch=0)
    env.chain.add_log(19_000, deposit_log(pk, credentials(STRANGER), amount_gwei=2_000_000_000))
    env.exporter.epoch_work = lambda: None  # type: ignore[method-assign]
    env.tick()
    per_key = dict(GROUP, pubkey=pk, validator_index="")
    assert env.value("lido_keys_deposit_credentials_mismatch", **per_key) == 1
    assert env.deposits("initial") == 1

    del env.exporter.epoch_work
    env.tick()
    per_key["validator_index"] = "11"
    assert env.value("lido_keys_deposit_credentials_mismatch", **per_key) is None
    assert env.store.deposit_summary()[pk].mismatch is False
    # Counters never decrease (a drop would read as a reset): the event stays counted as initial too.
    assert env.deposits("initial") == 1
    assert env.deposits("topup") == 1
    assert env.value("lido_keys_deposit_eth_total", **GROUP, kind="topup") == 2
    assert env.value("lido_keys_topups", **per_key) == 1
    assert env.value("lido_keys_deposit_timestamp_seconds", **per_key) is None

    env.chain.mine(10)
    env.chain.add_log(env.chain.head - 5, deposit_log(pubkey(2), WC))
    env.keys[(1, 7)] = [pk, pubkey(2)]
    env.restart()
    env.tick()
    env.tick()
    assert env.deposits("initial") == 2
    assert env.deposits("topup") == 1


def test_totals_survive_restart(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.chain.add_log(19_000, deposit_log(pk, WC))
    env.chain.add_log(19_001, deposit_log(pk, WC, amount_gwei=1_000_000_000))
    env.chain.add_log(19_010, exit_request_log(VEBO, 1, 7, 10, pk, 111))
    env.chain.add_log(19_020, withdrawal_request_log(OPS, pk, 3))
    env.tick()
    names = [
        ("lido_keys_deposit_events_total", dict(GROUP, kind="initial")),
        ("lido_keys_deposit_events_total", dict(GROUP, kind="topup")),
        ("lido_keys_deposit_eth_total", dict(GROUP, kind="topup")),
        ("lido_keys_exit_requests_total", GROUP),
        ("lido_keys_triggered_withdrawals_total", dict(GROUP, kind="partial", source="ops-wallet")),
        ("lido_keys_triggered_withdrawal_gwei_total", dict(GROUP, source="ops-wallet")),
    ]
    before = [env.value(n, **labels) for n, labels in names]
    assert before == [1, 1, 1, 1, 1, 3]
    env.restart()
    env.tick()
    assert [env.value(n, **labels) for n, labels in names] == before


def test_seeding_keeps_initial_deposit_of_new_validator(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    far = (env.chain.timestamp(19_500) - GENESIS_TIME) // (32 * 12)
    env.cl.add_validator(pk, 11, eligibility_epoch=far)
    env.chain.add_log(19_000, deposit_log(pk, WC))
    env.tick()
    assert env.store.initial_deposits_since(0) == {pk: env.chain.timestamp(19_000)}
    assert env.store.validators()[pk].prior_deposit is False


def test_failed_beacon_lookups_retry_next_tick(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.cl.add_validator(pk, 3)
    env.cl.fail = True
    env.tick()
    env.cl.fail = False
    env.tick()  # same epoch, but the failed seeding pass is retried
    assert env.store.validators()[pk].prior_deposit is True


def test_exit_request_exported_only_after_beacon_check(env: Env) -> None:
    pk = pubkey(1)
    env.keys[(1, 7)] = [pk]
    env.cl.add_validator(pk, 777)
    env.cl.set_status(pk, "exited_unslashed")
    env.chain.add_log(19_000, exit_request_log(VEBO, 1, 7, 777, pk, int(env.chain.timestamp(19_000))))
    env.exporter.epoch_work = lambda: None  # type: ignore[method-assign]
    env.tick()
    per_key = dict(GROUP, pubkey=pk, validator_index="777")
    assert len(env.store.open_exit_requests()) == 1
    assert env.value("lido_keys_exit_request_open", **per_key) is None
    assert env.value("lido_keys_exit_requests_total", **GROUP) == 1

    del env.exporter.epoch_work
    env.tick()
    assert env.store.open_exit_requests() == []
    assert env.value("lido_keys_exit_request_open", **per_key) is None
