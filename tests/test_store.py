from __future__ import annotations

import sqlite3
import threading

import pytest

from src.contracts import DepositLog, LogRef
from src.store import DepositDetails, DepositRecord, ExitRequestRecord, Store, TriggeredRecord


def pk(n: int) -> str:
    return "0x" + f"{n:02x}" * 48


def tx(n: int) -> str:
    return "0x" + f"{n:064x}"


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "sub" / "state.sqlite3")
    yield s
    s.close()


def deposit(pubkey, block, log_index=0, topup=False, mismatch=False, ctype="0x01", txn=None, amount=32_000_000_000):
    return DepositRecord(
        pubkey=pubkey, amount_gwei=amount, withdrawal_credentials=ctype + "00" * 31,
        credentials_type=ctype, mismatch=mismatch, is_topup=topup, block_number=block,
        block_hash=f"0x{block:064x}", block_timestamp=1000 + block, tx_hash=txn or tx(block * 10 + log_index),
        log_index=log_index,
    )


def exit_req(pubkey, block, log_index=0, ts=500, set_name="main", module_id=2):
    return ExitRequestRecord(
        pubkey=pubkey, set_name=set_name, origin="keys_api", module_id=module_id, operator_id=0, validator_index=block,
        request_timestamp=ts, block_number=block, block_hash=f"0x{block:064x}", block_timestamp=1000 + block,
        tx_hash=tx(block * 10 + log_index), log_index=log_index,
    )


def triggered(pubkey, block, kind="exit", log_index=0, source="x"):
    return TriggeredRecord(
        pubkey=pubkey, kind=kind, amount_gwei=0 if kind == "exit" else 5, source_address="0x" + "11" * 20,
        source_name=source, block_number=block, block_hash=f"0x{block:064x}", block_timestamp=1000 + block,
        slot=block, tx_hash=tx(block * 10 + log_index), log_index=log_index,
    )


def raw(pubkey, block, log_index=0):
    ref = LogRef(block, f"0x{block:064x}", tx(block * 10 + log_index), log_index, None)
    return DepositLog(ref, pubkey, "0x01" + "00" * 31, 32_000_000_000, block)


def test_schema_and_wal(tmp_path):
    path = tmp_path / "a" / "b.sqlite3"
    s = Store(path)
    s.close()
    conn = sqlite3.connect(path)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("SELECT version FROM schema_version").fetchall() == [(1,)]
    conn.close()
    Store(path).close()  # reopen is idempotent


def test_cursors(store):
    assert store.get_cursor("deposits") is None
    store.set_cursor("deposits", 10, "0xaa", 100)
    store.set_cursor("deposits", 11, "0xbb", 112)
    store.set_cursor("exits", 5, None, None)
    c = store.get_cursor("deposits")
    assert (c.block_number, c.block_hash, c.block_timestamp) == (11, "0xbb", 112)
    assert [c.stream for c in store.all_cursors()] == ["deposits", "exits"]


def test_transaction_rollback(store):
    with pytest.raises(RuntimeError):
        with store.transaction():
            store.set_cursor("deposits", 1, None, None)
            assert store.insert_deposit(deposit(pk(1), 1))
            raise RuntimeError
    assert store.get_cursor("deposits") is None
    assert store.deposited_pubkeys() == set()
    with store.transaction():
        store.set_cursor("deposits", 2, None, None)
    assert store.get_cursor("deposits").block_number == 2


def test_raw_deposits(store):
    assert store.insert_raw_deposit(raw(pk(1), 20, 1), 1020)
    assert not store.insert_raw_deposit(raw(pk(1), 20, 1), 1020)
    store.insert_raw_deposit(raw(pk(1), 10), 1010)
    store.insert_raw_deposit(raw(pk(2), 15), 1015)
    store.insert_raw_deposit(raw(pk(3), 12), 1012)
    got = store.raw_deposits_for([pk(1), pk(2)])
    assert [(d.pubkey, d.ref.block_number, ts) for d, ts in got] == [
        (pk(1), 10, 1010), (pk(2), 15, 1015), (pk(1), 20, 1020),
    ]
    assert got[0][0] == DepositLog(
        LogRef(10, f"0x{10:064x}", tx(100), 0, 1010), pk(1), "0x01" + "00" * 31, 32_000_000_000, 10
    )
    many = [pk(1)] + ["0x" + f"{i:096x}" for i in range(1200)]
    assert len(store.raw_deposits_for(many)) == 2
    store.prune_raw_deposits(15)
    assert [d.ref.block_number for d, _ in store.raw_deposits_for([pk(1), pk(2), pk(3)])] == [15, 20]


def test_deposits_and_summary(store):
    assert store.insert_deposit(deposit(pk(1), 10))
    assert not store.insert_deposit(deposit(pk(1), 10))
    assert store.insert_deposit(deposit(pk(1), 12, topup=True, mismatch=True, amount=1_000_000_000))
    assert store.insert_deposit(deposit(pk(1), 13, topup=True, amount=2_000_000_000))
    assert store.insert_deposit(deposit(pk(2), 11, topup=True, ctype="0x02"))
    assert store.insert_deposit(deposit(pk(2), 14, topup=True))
    assert store.has_earlier_deposit(pk(1), 10, 1)
    assert not store.has_earlier_deposit(pk(1), 10, 0)
    assert store.has_earlier_deposit(pk(1), 11, 0)
    assert not store.has_earlier_deposit(pk(3), 100, 0)
    s = store.deposit_summary()
    assert s[pk(1)] == DepositDetails(
        pk(1), 10, 1010, 32_000_000_000, "0x01", False, topups=2, topup_gwei=3_000_000_000,
        last_topup_timestamp=1013, deposits=3,
    )
    # Only top-ups seen: no initial deposit, top-ups never fall back to being the deposit.
    assert s[pk(2)] == DepositDetails(
        pk(2), None, None, None, None, False, topups=2, topup_gwei=64_000_000_000,
        last_topup_timestamp=1014, deposits=2,
    )
    assert store.deposited_pubkeys() == {pk(1), pk(2)}


def test_mismatch_only_on_initial_deposits(store):
    store.insert_deposit(deposit(pk(1), 10, mismatch=True))
    store.insert_deposit(deposit(pk(2), 11, topup=True, mismatch=True))
    s = store.deposit_summary()
    assert s[pk(1)].mismatch is True
    assert s[pk(2)].mismatch is False
    assert store._query("SELECT pubkey, mismatch FROM deposits ORDER BY pubkey") == [(pk(1), 1), (pk(2), 0)]


def test_old_topup_mismatch_cleared_on_open(tmp_path):
    path = tmp_path / "s.sqlite3"
    s = Store(path)
    s.insert_deposit(deposit(pk(1), 10))
    s.insert_deposit(deposit(pk(1), 11))
    s._execute("UPDATE deposits SET mismatch = 1")
    s._execute("UPDATE deposits SET is_topup = 1 WHERE block_number = 11")
    s.close()
    s = Store(path)
    assert s._query("SELECT block_number, mismatch FROM deposits ORDER BY block_number") == [(10, 1), (11, 0)]
    s.close()


def test_deposit_event_totals(store):
    store.insert_deposit(deposit(pk(1), 10))
    store.insert_deposit(deposit(pk(1), 20, topup=True, amount=1_000_000_000))
    store.insert_deposit(deposit(pk(1), 30, topup=True, amount=2_000_000_000))
    store.insert_deposit(deposit(pk(2), 25))
    assert store.deposit_event_totals() == {
        pk(1): {"initial": (1, 32_000_000_000), "topup": (2, 3_000_000_000)},
        pk(2): {"initial": (1, 32_000_000_000)},
    }
    assert store.deposit_event_totals(since_timestamp=1025) == {
        pk(1): {"topup": (1, 2_000_000_000)},
        pk(2): {"initial": (1, 32_000_000_000)},
    }
    assert store.deposit_event_totals(since_timestamp=2000) == {}


def test_summary_prefers_non_topup(store):
    store.insert_deposit(deposit(pk(1), 5, topup=True))
    store.insert_deposit(deposit(pk(1), 9))
    assert store.deposit_summary()[pk(1)].initial_block == 9


def test_validators_upsert(store):
    store.upsert_validator(pk(1), index=7)
    assert store.validators()[pk(1)].index == 7
    assert store.validators()[pk(1)].prior_deposit is False
    store.upsert_validator(pk(1), prior_deposit=True)
    v = store.validators()[pk(1)]
    assert (v.index, v.prior_deposit) == (7, True)
    store.upsert_validator(pk(1), index=8)
    v = store.validators()[pk(1)]
    assert (v.index, v.prior_deposit) == (8, True)
    store.upsert_validator(pk(1), prior_deposit=False)
    assert store.validators()[pk(1)].prior_deposit is False
    store.upsert_validator(pk(2))
    assert store.validators()[pk(2)].index is None


def test_exit_requests(store):
    assert store.insert_exit_request(exit_req(pk(1), 10))
    assert not store.insert_exit_request(exit_req(pk(1), 10))
    assert store.insert_exit_request(exit_req(pk(1), 11))
    assert store.insert_exit_request(exit_req(pk(2), 12))
    assert store.exit_request_count() == 3
    assert store.exit_request_totals() == {("main", "keys_api", 2, 0): 3}
    opened = store.open_exit_requests()
    assert [r.pubkey for r in opened] == [pk(1), pk(1), pk(2)]
    assert opened[0] == exit_req(pk(1), 10)
    assert store.close_exit_requests(pk(1), "active_exiting", 999) == 2
    assert store.close_exit_requests(pk(1), "active_exiting", 999) == 0
    assert [r.pubkey for r in store.open_exit_requests()] == [pk(2)]
    assert store.exit_request_count() == 3


def test_exit_request_totals(store):
    store.insert_exit_request(exit_req(pk(1), 10))
    store.insert_exit_request(exit_req(pk(2), 20))
    store.insert_exit_request(exit_req(pk(3), 30, set_name="other", module_id=None))
    store.close_exit_requests(pk(1), "active_exiting", 999)
    assert store.exit_request_totals() == {("main", "keys_api", 2, 0): 2, ("other", "keys_api", None, 0): 1}
    assert store.exit_request_totals(since_timestamp=1020) == {
        ("main", "keys_api", 2, 0): 1,
        ("other", "keys_api", None, 0): 1,
    }


def test_triggered(store):
    assert store.insert_triggered(triggered(pk(1), 10))
    assert not store.insert_triggered(triggered(pk(1), 10))
    store.insert_triggered(triggered(pk(1), 20))
    store.insert_triggered(triggered(pk(1), 15, kind="partial"))
    store.insert_triggered(triggered(pk(1), 16, kind="partial", source="y"))
    store.insert_triggered(triggered(pk(1), 17, kind="partial", source="y"))
    assert store.triggered_last() == {
        (pk(1), "exit", "x"): 1020,
        (pk(1), "partial", "x"): 1015,
        (pk(1), "partial", "y"): 1017,
    }
    assert store.triggered_totals() == {
        (pk(1), "exit", "x"): (2, 0),
        (pk(1), "partial", "x"): (1, 5),
        (pk(1), "partial", "y"): (2, 10),
    }
    assert store.triggered_totals(since_timestamp=1017) == {(pk(1), "exit", "x"): (1, 0), (pk(1), "partial", "y"): (1, 5)}


def test_rewind(store):
    store.insert_raw_deposit(raw(pk(1), 10), 1010)
    store.insert_raw_deposit(raw(pk(1), 20), 1020)
    store.insert_deposit(deposit(pk(1), 10))
    store.insert_deposit(deposit(pk(1), 20))
    store.insert_exit_request(exit_req(pk(1), 19))
    store.insert_exit_request(exit_req(pk(1), 21))
    store.insert_triggered(triggered(pk(1), 25))
    store.upsert_validator(pk(1), index=3, prior_deposit=True)
    store.set_cursor("deposits", 30, "0xaa", 1)
    store.set_cursor("exits", 18, "0xbb", 2)
    store.set_cursor("triggered", 20, "0xcc", 3)
    store.rewind(20)
    assert [d.ref.block_number for d, _ in store.raw_deposits_for([pk(1)])] == [10]
    assert store.deposited_pubkeys() == {pk(1)}
    assert store.deposit_summary()[pk(1)].deposits == 1
    assert [r.block_number for r in store.open_exit_requests()] == [19]
    assert store.triggered_last() == {}
    assert store.validators()[pk(1)].index == 3
    cursors = {c.stream: c for c in store.all_cursors()}
    assert (cursors["deposits"].block_number, cursors["deposits"].block_hash, cursors["deposits"].block_timestamp) == (19, None, None)
    assert (cursors["exits"].block_number, cursors["exits"].block_hash) == (18, "0xbb")
    assert cursors["triggered"].block_number == 19


def test_concurrent_access(store):
    errors = []

    def writer():
        try:
            for i in range(200):
                with store.transaction():
                    store.insert_triggered(triggered(pk(1), i))
                    store.set_cursor("triggered", i, None, None)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    def reader():
        try:
            for _ in range(200):
                store.triggered_last()
                store.all_cursors()
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=writer), threading.Thread(target=reader), threading.Thread(target=reader)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert store.get_cursor("triggered").block_number == 199


def test_rewind_keeps_canonical_blocks(store):
    store.insert_deposit(deposit(pk(1), 20))
    store.insert_deposit(deposit(pk(2), 21))
    ex = exit_req(pk(1), 22)
    store.insert_exit_request(ex)
    store.close_exit_requests(pk(1), "exited_unslashed", 99)
    store.set_cursor("deposits", 30, "0xaa", 1)
    assert store.event_blocks(21) == {(21, f"0x{21:064x}"), (22, f"0x{22:064x}")}
    store.rewind(20, keep_hashes=[f"0x{20:064x}", f"0x{22:064x}"])
    assert store.deposited_pubkeys() == {pk(1)}
    assert store.insert_deposit(deposit(pk(1), 20)) is False
    assert store.insert_exit_request(ex) is False
    assert store.open_exit_requests() == []
    assert store.get_cursor("deposits").block_number == 19


def test_reclassify_topups(store):
    store.insert_deposit(deposit(pk(1), 10))
    store.insert_deposit(deposit(pk(1), 20, topup=True))
    store.insert_deposit(deposit(pk(2), 5))
    assert store.initial_deposits_since(1008) == {pk(1): 1010}
    assert store.mark_topups_after(pk(1), 1009) == 1
    assert store.initial_deposits_since(0) == {pk(2): 1005}


def test_reclassification_moves_totals_and_clears_mismatch(store):
    store.insert_deposit(deposit(pk(1), 10, mismatch=True))
    assert store.deposit_summary()[pk(1)].mismatch is True
    assert store.mark_topups_after(pk(1), 1000) == 1
    assert store.deposit_event_totals() == {pk(1): {"topup": (1, 32_000_000_000)}}
    s = store.deposit_summary()[pk(1)]
    assert (s.initial_block, s.mismatch, s.topups) == (None, False, 1)
