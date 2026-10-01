from __future__ import annotations

from pathlib import Path

from prometheus_client import generate_latest

from src.clients.health import REASONS, EndpointState
from src.config import Config
from src.contracts import slot_at
from src.keyset import KeySet
from src.metrics import Metrics
from src.store import DepositRecord, ExitRequestRecord, Store, TriggeredRecord
from tests.fakes import pubkey

PK1, PK2, PK3, GONE = pubkey(1), pubkey(2), pubkey(3), pubkey(4)
STATIC = pubkey(10)
GROUP = {"set": "main", "origin": "keys_api", "module_id": "1", "operator_id": "7"}
STATIC_GROUP = {"set": "main", "origin": "static", "module_id": "", "operator_id": ""}


def make_keyset() -> KeySet:
    cfg = Config.model_validate(
        {
            "execution_endpoints": ["http://el"],
            "beacon_endpoints": ["http://cl"],
            "sets": [
                {
                    "name": "main",
                    "keys_api": [{"url": "http://keys-api", "module_id": 1, "operator_id": 7}],
                    "static_pubkeys": [{"pubkey": STATIC}],
                }
            ],
        }
    )
    keyset = KeySet(cfg, fetch=lambda source: [PK1, PK2, PK3])
    keyset.refresh()
    return keyset


def deposit(
    pk: str,
    block: int,
    ts: int,
    mismatch: bool = False,
    topup: bool = False,
    log_index: int = 0,
    amount_gwei: int = 32_000_000_000,
    ctype: str = "0x01",
):
    return DepositRecord(
        pubkey=pk,
        amount_gwei=amount_gwei,
        withdrawal_credentials=ctype + "00" * 31,
        credentials_type=ctype,
        mismatch=mismatch,
        is_topup=topup,
        block_number=block,
        block_hash="0x" + "aa" * 32,
        block_timestamp=ts,
        tx_hash="0x" + f"{block:064x}",
        log_index=log_index,
    )


def triggered(pk: str, block: int, ts: int, kind: str = "exit", source: str = "ops", amount: int = 0) -> TriggeredRecord:
    return TriggeredRecord(
        pubkey=pk,
        kind=kind,
        amount_gwei=amount,
        source_address="0x" + "cc" * 20,
        source_name=source,
        block_number=block,
        block_hash="0x" + "dd" * 32,
        block_timestamp=ts,
        slot=slot_at(ts),
        tx_hash="0x" + f"{block:064x}",
        log_index=0,
    )


def exit_request(pk: str, index: int, ts: int, block: int, set_name: str = "main") -> ExitRequestRecord:
    return ExitRequestRecord(
        pubkey=pk,
        set_name=set_name,
        origin="keys_api",
        module_id=1,
        operator_id=7,
        validator_index=index,
        request_timestamp=ts,
        block_number=block,
        block_hash="0x" + "bb" * 32,
        block_timestamp=ts,
        tx_hash="0x" + f"{block:064x}",
        log_index=0,
    )


def test_counters_and_static_gauges() -> None:
    m = Metrics()
    text = generate_latest(m.registry).decode()
    for component in ("keys_api", "el", "cl", "store"):
        assert f'lido_keys_errors_total{{component="{component}"}} 0.0' in text
    m.error("el")
    assert m.registry.get_sample_value("lido_keys_errors_total", {"component": "el"}) == 1
    m.up.set(1)
    m.keyset_last_refresh.set(123)
    m.build_info.labels(version="1.2.3").set(1)
    get = m.registry.get_sample_value
    for removed in ("lido_keys_deposits", "lido_keys_topups_total", "lido_keys_exit_requests_total"):
        assert removed not in text
    assert get("lido_keys_up") == 1
    assert get("lido_keys_keyset_last_refresh_timestamp_seconds") == 123
    assert get("lido_keys_build_info", {"version": "1.2.3"}) == 1


def test_state_collector(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite3")
    keyset = make_keyset()
    m = Metrics()
    m.register_state(store, keyset, clock=lambda: 10_000)
    get = m.registry.get_sample_value

    # Nothing processed yet: key counts and zero totals only, no cursor gauges.
    assert get("lido_keys_monitored", GROUP) == 3
    assert get("lido_keys_monitored", STATIC_GROUP) == 1
    assert get("lido_keys_deposited", GROUP) == 0
    for kind in ("initial", "topup"):
        assert get("lido_keys_deposit_events_total", dict(STATIC_GROUP, kind=kind)) == 0
        assert get("lido_keys_deposit_eth_24h", dict(GROUP, kind=kind)) == 0
    assert get("lido_keys_exit_requests_total", GROUP) == 0
    assert get("lido_keys_last_processed_block") is None

    store.insert_deposit(deposit(PK1, 100, 1000))
    store.insert_deposit(deposit(PK1, 110, 1100, mismatch=True, topup=True))
    store.insert_deposit(deposit(GONE, 120, 1200))
    store.upsert_validator(PK1, index=11)
    store.upsert_validator(PK2, index=12, prior_deposit=True)
    store.insert_exit_request(exit_request(PK2, 12, 5000, 200))
    store.insert_exit_request(exit_request(PK2, 12, 4000, 201))
    store.insert_exit_request(exit_request(GONE, 99, 6000, 202, set_name="old"))
    store.insert_triggered(triggered(PK3, 300, 7000, kind="partial", amount=1))
    store.set_cursor("deposits", 500, "0x" + "01" * 32, 1_700_000_000)
    store.set_cursor("exits", 400, "0x" + "02" * 32, 1_699_998_800)
    store.set_cursor("triggered", 600, None, None)

    assert get("lido_keys_deposited", GROUP) == 2
    assert get("lido_keys_deposit_timestamp_seconds", dict(GROUP, pubkey=PK1, validator_index="11")) == 1000
    # Top-ups never flag a credentials mismatch.
    assert get("lido_keys_deposit_credentials_mismatch", dict(GROUP, pubkey=PK1, validator_index="11")) == 0
    assert get("lido_keys_deposit_initial_eth", dict(GROUP, pubkey=PK1, validator_index="11", credentials="0x01")) == 32
    assert get("lido_keys_topups", dict(GROUP, pubkey=PK1, validator_index="11")) == 1
    assert get("lido_keys_topup_last_timestamp_seconds", dict(GROUP, pubkey=PK1, validator_index="11")) == 1100
    assert get("lido_keys_deposit_events_total", dict(GROUP, kind="initial")) == 1
    assert get("lido_keys_deposit_events_total", dict(GROUP, kind="topup")) == 1
    # Prior deposits count as deposited but have no per-key deposit series.
    assert get("lido_keys_deposit_timestamp_seconds", dict(GROUP, pubkey=PK2, validator_index="12")) is None
    # Keys that left the key set drop out of per-key deposit series.
    text = generate_latest(m.registry).decode()
    assert f'lido_keys_deposit_timestamp_seconds{{module_id="1",operator_id="7",origin="keys_api",pubkey="{GONE}"' not in text

    # Open requests are exported only once compared with the beacon chain.
    assert get("lido_keys_exit_request_open", dict(GROUP, pubkey=PK2, validator_index="12")) is None
    assert get("lido_keys_exit_requests_total", GROUP) == 2
    store.mark_exit_requests_checked([PK2, GONE], 8000)
    assert get("lido_keys_exit_request_open", dict(GROUP, pubkey=PK2, validator_index="12")) == 4000
    old_group = dict(GROUP, set="old")
    assert get("lido_keys_exit_request_open", dict(old_group, pubkey=GONE, validator_index="")) == 6000
    assert get("lido_keys_exit_requests_open", GROUP) == 1
    assert get("lido_keys_exit_requests_open", old_group) == 1
    assert get("lido_keys_exit_requests_total", GROUP) == 2
    assert get("lido_keys_exit_requests_total", old_group) == 1
    assert get("lido_keys_exit_requests_total", STATIC_GROUP) == 0

    assert (
        get(
            "lido_keys_triggered_withdrawal_last_timestamp_seconds",
            dict(GROUP, pubkey=PK3, validator_index="", kind="partial", source="ops"),
        )
        == 7000
    )
    assert get("lido_keys_triggered_withdrawals_total", dict(GROUP, kind="partial", source="ops")) == 1
    assert get("lido_keys_triggered_withdrawal_gwei_total", dict(GROUP, source="ops")) == 1

    assert get("lido_keys_last_processed_block") == 400
    assert get("lido_keys_last_processed_block_timestamp") == 1_699_998_800
    assert get("lido_keys_last_processed_slot") == slot_at(1_699_998_800)
    assert get("lido_keys_stream_last_processed_block", {"stream": "triggered"}) == 600

    store.close_exit_requests(PK2, "active_exiting", 9000)
    assert get("lido_keys_exit_request_open", dict(GROUP, pubkey=PK2, validator_index="12")) is None
    assert get("lido_keys_exit_requests_open", GROUP) is None
    assert get("lido_keys_exit_requests_total", GROUP) == 2
    store.close()


def test_state_collector_failure_does_not_break_scrape(tmp_path: Path) -> None:
    class Broken:
        def __getattr__(self, name: str):
            raise RuntimeError("boom")

    m = Metrics()
    m.register_state(Broken(), make_keyset())  # type: ignore[arg-type]
    m.up.set(1)
    text = generate_latest(m.registry).decode()
    assert "lido_keys_up 1.0" in text
    assert "lido_keys_monitored" not in text


def test_topup_only_key_has_no_deposit_series(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite3")
    m = Metrics()
    m.register_state(store, make_keyset(), clock=lambda: 10_000)
    get = m.registry.get_sample_value
    store.insert_deposit(deposit(PK1, 100, 1000, topup=True, amount_gwei=1_000_000_000, ctype="0x02"))
    store.insert_deposit(deposit(PK1, 101, 1200, topup=True, amount_gwei=500_000_000, ctype="0x02", log_index=1))
    per_key = dict(GROUP, pubkey=PK1, validator_index="")
    assert get("lido_keys_deposit_timestamp_seconds", per_key) is None
    assert get("lido_keys_deposit_credentials_mismatch", per_key) is None
    assert get("lido_keys_deposit_initial_eth", dict(per_key, credentials="0x02")) is None
    assert get("lido_keys_topups", per_key) == 2
    assert get("lido_keys_topup_eth", per_key) == 1.5
    assert get("lido_keys_topup_last_timestamp_seconds", per_key) == 1200
    assert get("lido_keys_deposited", GROUP) == 1
    assert get("lido_keys_deposit_events_total", dict(GROUP, kind="initial")) == 0
    assert get("lido_keys_deposit_events_total", dict(GROUP, kind="topup")) == 2
    assert get("lido_keys_deposit_eth_total", dict(GROUP, kind="topup")) == 1.5
    store.close()


def test_24h_windows_use_block_time(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite3")
    now = [1_700_000_000.0]
    m = Metrics()
    m.register_state(store, make_keyset(), clock=lambda: now[0])
    get = m.registry.get_sample_value
    day = 86400
    old, fresh = int(now[0]) - day - 1, int(now[0]) - 60
    store.insert_deposit(deposit(PK1, 100, old))
    store.insert_deposit(deposit(PK2, 101, fresh))
    store.insert_deposit(deposit(PK2, 102, fresh, topup=True, amount_gwei=2_000_000_000))
    store.insert_exit_request(exit_request(PK1, 1, old, 103))
    store.insert_exit_request(exit_request(PK2, 2, fresh, 104))
    store.insert_triggered(triggered(PK1, 105, old, source="ops"))
    store.insert_triggered(triggered(PK2, 106, fresh, source="ops"))
    store.insert_triggered(triggered(PK3, 107, old, kind="partial", source="stranger", amount=7))

    assert get("lido_keys_deposit_events_total", dict(GROUP, kind="initial")) == 2
    assert get("lido_keys_deposit_events_24h", dict(GROUP, kind="initial")) == 1
    assert get("lido_keys_deposit_events_24h", dict(GROUP, kind="topup")) == 1
    assert get("lido_keys_deposit_eth_24h", dict(GROUP, kind="initial")) == 32
    assert get("lido_keys_deposit_eth_24h", dict(GROUP, kind="topup")) == 2
    assert get("lido_keys_exit_requests_total", GROUP) == 2
    assert get("lido_keys_exit_requests_24h", GROUP) == 1
    assert get("lido_keys_triggered_withdrawals_total", dict(GROUP, kind="exit", source="ops")) == 2
    assert get("lido_keys_triggered_withdrawals_24h", dict(GROUP, kind="exit", source="ops")) == 1
    # Series stay present with 0 once the window has passed.
    assert get("lido_keys_triggered_withdrawals_24h", dict(GROUP, kind="partial", source="stranger")) == 0
    assert get("lido_keys_triggered_withdrawal_gwei_total", dict(GROUP, source="stranger")) == 7

    now[0] += day
    assert get("lido_keys_deposit_events_24h", dict(GROUP, kind="initial")) == 0
    assert get("lido_keys_exit_requests_24h", GROUP) == 0
    assert get("lido_keys_deposit_events_total", dict(GROUP, kind="initial")) == 2
    store.close()


def test_triggered_last_timestamp_per_source(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite3")
    m = Metrics()
    m.register_state(store, make_keyset())
    get = m.registry.get_sample_value
    store.upsert_validator(PK1, index=5)
    store.insert_triggered(triggered(PK1, 100, 1000, source="lido-withdrawal-vault"))
    store.insert_triggered(triggered(PK1, 101, 2000, source="0x" + "5e" * 20))
    store.insert_triggered(triggered(GONE, 102, 3000, source="ops"))
    per_key = dict(GROUP, pubkey=PK1, validator_index="5", kind="exit")
    name = "lido_keys_triggered_withdrawal_last_timestamp_seconds"
    assert get(name, dict(per_key, source="lido-withdrawal-vault")) == 1000
    assert get(name, dict(per_key, source="0x" + "5e" * 20)) == 2000
    assert GONE not in generate_latest(m.registry).decode()
    store.close()


class FakeClient:
    def __init__(self, states: list[EndpointState]):
        self.states = states

    def endpoint_states(self) -> list[EndpointState]:
        return self.states


def test_endpoint_collector() -> None:
    el = FakeClient(
        [
            EndpointState(
                "el",
                "nethermind:8545",
                "http://nethermind:8545",
                up=False,
                head=100,
                lag=19,
                reason="lagging",
                errors={"lagging": 2, "range_limit": 3},
            ),
            EndpointState("el", "geth:8545", "http://user:pw@geth:8545", head=119, lag=0),
        ]
    )
    cl = FakeClient([EndpointState("cl", "teku:5051", "http://teku:5051", up=False, syncing=True, reason="syncing")])
    m = Metrics()
    m.register_endpoints(el, cl)
    get = m.registry.get_sample_value
    neth = {"kind": "el", "endpoint": "nethermind:8545"}
    teku = {"kind": "cl", "endpoint": "teku:5051"}
    assert get("lido_keys_endpoint_up", neth) == 0
    assert get("lido_keys_endpoint_up", {"kind": "el", "endpoint": "geth:8545"}) == 1
    assert get("lido_keys_endpoint_lag", neth) == 19
    assert get("lido_keys_endpoint_head", neth) == 100
    assert get("lido_keys_endpoint_syncing", teku) == 1
    assert get("lido_keys_endpoint_head", teku) is None
    assert get("lido_keys_endpoint_lag", teku) is None
    assert get("lido_keys_endpoint_errors_total", dict(neth, reason="range_limit")) == 3
    for reason in REASONS:
        assert get("lido_keys_endpoint_errors_total", dict(teku, reason=reason)) == 0
    text = generate_latest(m.registry).decode()
    assert "user:pw" not in text and "http://" not in text


def test_endpoint_collector_failure_does_not_break_scrape() -> None:
    class Broken:
        def endpoint_states(self):
            raise RuntimeError("boom")

    m = Metrics()
    m.register_endpoints(Broken())
    m.up.set(1)
    text = generate_latest(m.registry).decode()
    assert "lido_keys_up 1.0" in text
    assert "lido_keys_endpoint_up" not in text


def test_store_totals_never_decrease(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite3")
    keyset = make_keyset()
    m = Metrics()
    m.register_state(store, keyset, clock=lambda: 10_000)
    get = m.registry.get_sample_value
    store.insert_deposit(deposit(PK1, 100, 1000))
    store.insert_deposit(deposit(PK2, 101, 1000, amount_gwei=1_000_000_000))
    store.insert_triggered(triggered(PK1, 102, 1000))
    store.insert_exit_request(exit_request(PK1, 11, 1000, 103))
    initial = dict(GROUP, kind="initial")
    assert get("lido_keys_deposit_events_total", initial) == 2
    assert get("lido_keys_deposit_eth_total", initial) == 33

    store.mark_topups_after(PK2, 0)
    store.rewind(102)
    assert get("lido_keys_deposit_events_total", initial) == 2
    assert get("lido_keys_deposit_eth_total", initial) == 33
    assert get("lido_keys_deposit_events_total", dict(GROUP, kind="topup")) == 1
    assert get("lido_keys_exit_requests_total", GROUP) == 1

    store.insert_deposit(deposit(PK3, 200, 2000))
    store.insert_triggered(triggered(PK1, 201, 2000))
    assert get("lido_keys_deposit_events_total", initial) == 3
    assert get("lido_keys_triggered_withdrawals_total", dict(GROUP, kind="exit", source="ops")) == 2

    # The offsets are persisted, so a restart does not drop the counters either.
    m2 = Metrics()
    m2.register_state(store, keyset, clock=lambda: 10_000)
    assert m2.registry.get_sample_value("lido_keys_deposit_events_total", initial) == 3
    assert m2.registry.get_sample_value("lido_keys_exit_requests_total", GROUP) == 1
    store.close()


def test_openmetrics_family_names_unique(tmp_path: Path) -> None:
    from prometheus_client.openmetrics.exposition import generate_latest as om_latest
    from prometheus_client.openmetrics.parser import text_string_to_metric_families

    store = Store(tmp_path / "s.sqlite3")
    m = Metrics()
    m.register_state(store, make_keyset(), clock=lambda: 10_000)
    store.insert_deposit(deposit(PK1, 100, 1000))
    store.insert_deposit(deposit(PK1, 110, 1100, topup=True))
    names = [f.name for f in text_string_to_metric_families(om_latest(m.registry).decode())]
    assert len(names) == len(set(names))
    store.close()
