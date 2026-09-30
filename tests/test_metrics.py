from __future__ import annotations

from pathlib import Path

from prometheus_client import generate_latest

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


def deposit(pk: str, block: int, ts: int, mismatch: bool = False, topup: bool = False, log_index: int = 0):
    return DepositRecord(
        pubkey=pk,
        amount_gwei=32_000_000_000,
        withdrawal_credentials="0x01" + "00" * 31,
        credentials_type="0x01",
        mismatch=mismatch,
        is_topup=topup,
        block_number=block,
        block_hash="0x" + "aa" * 32,
        block_timestamp=ts,
        tx_hash="0x" + f"{block:064x}",
        log_index=log_index,
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
    m.deposits.labels(**GROUP, credentials="0x01").inc()
    m.topups.labels(**GROUP).inc()
    m.exit_requests.labels(**GROUP).inc()
    m.triggered.labels(**GROUP, kind="exit", source="x").inc()
    m.triggered_gwei.labels(**GROUP, source="x").inc(5)
    m.up.set(1)
    m.keyset_last_refresh.set(123)
    m.build_info.labels(version="1.2.3").set(1)
    get = m.registry.get_sample_value
    assert get("lido_keys_deposits_total", dict(GROUP, credentials="0x01")) == 1
    assert get("lido_keys_topups_total", GROUP) == 1
    assert get("lido_keys_exit_requests_total", GROUP) == 1
    assert get("lido_keys_triggered_withdrawals_total", dict(GROUP, kind="exit", source="x")) == 1
    assert get("lido_keys_triggered_withdrawal_gwei_total", dict(GROUP, source="x")) == 5
    assert get("lido_keys_up") == 1
    assert get("lido_keys_keyset_last_refresh_timestamp_seconds") == 123
    assert get("lido_keys_build_info", {"version": "1.2.3"}) == 1


def test_state_collector(tmp_path: Path) -> None:
    store = Store(tmp_path / "s.sqlite3")
    keyset = make_keyset()
    m = Metrics()
    m.register_state(store, keyset)
    get = m.registry.get_sample_value

    # Nothing processed yet: key counts only, no cursor gauges.
    assert get("lido_keys_monitored", GROUP) == 3
    assert get("lido_keys_monitored", STATIC_GROUP) == 1
    assert get("lido_keys_deposited", GROUP) == 0
    assert get("lido_keys_last_processed_block") is None

    store.insert_deposit(deposit(PK1, 100, 1000))
    store.insert_deposit(deposit(PK1, 110, 1100, mismatch=True, topup=True))
    store.insert_deposit(deposit(GONE, 120, 1200))
    store.upsert_validator(PK1, index=11)
    store.upsert_validator(PK2, index=12, prior_deposit=True)
    store.insert_exit_request(exit_request(PK2, 12, 5000, 200))
    store.insert_exit_request(exit_request(PK2, 12, 4000, 201))
    store.insert_exit_request(exit_request(GONE, 99, 6000, 202, set_name="old"))
    store.insert_triggered(
        TriggeredRecord(
            pubkey=PK3,
            kind="partial",
            amount_gwei=1,
            source_address="0x" + "cc" * 20,
            source_name="ops",
            block_number=300,
            block_hash="0x" + "dd" * 32,
            block_timestamp=7000,
            slot=slot_at(7000),
            tx_hash="0x" + "ee" * 32,
            log_index=0,
        )
    )
    store.set_cursor("deposits", 500, "0x" + "01" * 32, 1_700_000_000)
    store.set_cursor("exits", 400, "0x" + "02" * 32, 1_699_998_800)
    store.set_cursor("triggered", 600, None, None)

    assert get("lido_keys_deposited", GROUP) == 2
    assert get("lido_keys_deposit_timestamp_seconds", dict(GROUP, pubkey=PK1, validator_index="11")) == 1000
    assert get("lido_keys_deposit_credentials_mismatch", dict(GROUP, pubkey=PK1, validator_index="11")) == 1
    # Prior deposits count as deposited but have no per-key deposit series.
    assert get("lido_keys_deposit_timestamp_seconds", dict(GROUP, pubkey=PK2, validator_index="12")) is None
    # Keys that left the key set drop out of per-key deposit series.
    text = generate_latest(m.registry).decode()
    assert f'lido_keys_deposit_timestamp_seconds{{module_id="1",operator_id="7",origin="keys_api",pubkey="{GONE}"' not in text

    assert get("lido_keys_exit_request_open", dict(GROUP, pubkey=PK2, validator_index="12")) == 4000
    old_group = dict(GROUP, set="old")
    assert get("lido_keys_exit_request_open", dict(old_group, pubkey=GONE, validator_index="")) == 6000
    assert get("lido_keys_exit_requests_open", GROUP) == 1
    assert get("lido_keys_exit_requests_open", old_group) == 1

    assert (
        get(
            "lido_keys_triggered_withdrawal_last_timestamp_seconds",
            dict(GROUP, pubkey=PK3, validator_index="", kind="partial"),
        )
        == 7000
    )

    assert get("lido_keys_last_processed_block") == 400
    assert get("lido_keys_last_processed_block_timestamp") == 1_699_998_800
    assert get("lido_keys_last_processed_slot") == slot_at(1_699_998_800)
    assert get("lido_keys_stream_last_processed_block", {"stream": "triggered"}) == 600

    store.close_exit_requests(PK2, "active_exiting", 9000)
    assert get("lido_keys_exit_request_open", dict(GROUP, pubkey=PK2, validator_index="12")) is None
    assert get("lido_keys_exit_requests_open", GROUP) is None
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
