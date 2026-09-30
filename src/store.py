"""SQLite state: cursors, deposits, validators, exit requests and triggered withdrawals."""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional

from .contracts import DepositLog, LogRef

SCHEMA_VERSION = 1
IN_CHUNK = 500

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS cursors (
    stream TEXT PRIMARY KEY,
    block_number INTEGER NOT NULL,
    block_hash TEXT,
    block_timestamp INTEGER
);
CREATE TABLE IF NOT EXISTS raw_deposits (
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    block_timestamp INTEGER NOT NULL,
    pubkey TEXT NOT NULL,
    withdrawal_credentials TEXT NOT NULL,
    amount_gwei INTEGER NOT NULL,
    deposit_index INTEGER NOT NULL,
    PRIMARY KEY (tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS raw_deposits_pubkey ON raw_deposits (pubkey);
CREATE INDEX IF NOT EXISTS raw_deposits_block ON raw_deposits (block_number);
CREATE TABLE IF NOT EXISTS deposits (
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    pubkey TEXT NOT NULL,
    amount_gwei INTEGER NOT NULL,
    withdrawal_credentials TEXT NOT NULL,
    credentials_type TEXT NOT NULL,
    mismatch INTEGER NOT NULL,
    is_topup INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    block_timestamp INTEGER NOT NULL,
    PRIMARY KEY (tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS deposits_pubkey ON deposits (pubkey);
CREATE TABLE IF NOT EXISTS validators (
    pubkey TEXT PRIMARY KEY,
    validator_index INTEGER,
    prior_deposit INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS exit_requests (
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    pubkey TEXT NOT NULL,
    set_name TEXT NOT NULL,
    origin TEXT NOT NULL,
    module_id INTEGER,
    operator_id INTEGER,
    validator_index INTEGER NOT NULL,
    request_timestamp INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    block_timestamp INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    closed_status TEXT,
    closed_at INTEGER,
    PRIMARY KEY (tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS exit_requests_status ON exit_requests (status, pubkey);
CREATE TABLE IF NOT EXISTS triggered (
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    pubkey TEXT NOT NULL,
    kind TEXT NOT NULL,
    amount_gwei INTEGER NOT NULL,
    source_address TEXT NOT NULL,
    source_name TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    block_timestamp INTEGER NOT NULL,
    slot INTEGER NOT NULL,
    PRIMARY KEY (tx_hash, log_index)
);
"""

EVENT_TABLES = ("raw_deposits", "deposits", "exit_requests", "triggered")


@dataclass(frozen=True)
class Cursor:
    stream: str
    block_number: int
    block_hash: Optional[str]
    block_timestamp: Optional[int]


@dataclass(frozen=True)
class DepositRecord:
    pubkey: str
    amount_gwei: int
    withdrawal_credentials: str
    credentials_type: str
    mismatch: bool
    is_topup: bool
    block_number: int
    block_hash: str
    block_timestamp: int
    tx_hash: str
    log_index: int


@dataclass(frozen=True)
class ExitRequestRecord:
    pubkey: str
    set_name: str
    origin: str
    module_id: Optional[int]
    operator_id: Optional[int]
    validator_index: int
    request_timestamp: int
    block_number: int
    block_hash: str
    block_timestamp: int
    tx_hash: str
    log_index: int
    status: str = "open"
    closed_status: Optional[str] = None
    closed_at: Optional[int] = None


@dataclass(frozen=True)
class TriggeredRecord:
    pubkey: str
    kind: str
    amount_gwei: int
    source_address: str
    source_name: str
    block_number: int
    block_hash: str
    block_timestamp: int
    slot: int
    tx_hash: str
    log_index: int


@dataclass(frozen=True)
class ValidatorInfo:
    pubkey: str
    index: Optional[int]
    prior_deposit: bool


@dataclass(frozen=True)
class DepositSummary:
    pubkey: str
    initial_block: int
    initial_timestamp: int
    credentials_type: str
    mismatch: bool
    deposits: int


def _chunks(items: list[str], size: int = IN_CHUNK) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


class Store:
    """Thread-safe SQLite store; every method holds the same re-entrant lock."""

    def __init__(self, path: Path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        with self.transaction():
            self._create_schema()

    def _create_schema(self) -> None:
        for statement in SCHEMA.split(";"):
            if statement.strip():
                self._conn.execute(statement)
        if self._conn.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0] == 0:
            self._conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
        self._conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        with self._lock:
            if self._conn.in_transaction:
                yield
                return
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            self._conn.execute("COMMIT")

    def _execute(self, sql: str, params: Iterable = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._conn.execute(sql, tuple(params))

    def _query(self, sql: str, params: Iterable = ()) -> list[tuple]:
        with self._lock:
            return self._conn.execute(sql, tuple(params)).fetchall()

    # Cursors

    def get_cursor(self, stream: str) -> Optional[Cursor]:
        rows = self._query(
            "SELECT stream, block_number, block_hash, block_timestamp FROM cursors WHERE stream = ?", (stream,)
        )
        return Cursor(*rows[0]) if rows else None

    def set_cursor(
        self, stream: str, block_number: int, block_hash: Optional[str], block_timestamp: Optional[int]
    ) -> None:
        self._execute(
            "INSERT INTO cursors (stream, block_number, block_hash, block_timestamp) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(stream) DO UPDATE SET block_number = excluded.block_number, "
            "block_hash = excluded.block_hash, block_timestamp = excluded.block_timestamp",
            (stream, block_number, block_hash, block_timestamp),
        )

    def all_cursors(self) -> list[Cursor]:
        rows = self._query("SELECT stream, block_number, block_hash, block_timestamp FROM cursors ORDER BY stream")
        return [Cursor(*r) for r in rows]

    # Raw deposits

    def insert_raw_deposit(self, dep: DepositLog, block_timestamp: int) -> bool:
        cur = self._execute(
            "INSERT OR IGNORE INTO raw_deposits (tx_hash, log_index, block_number, block_hash, block_timestamp, "
            "pubkey, withdrawal_credentials, amount_gwei, deposit_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                dep.ref.tx_hash,
                dep.ref.log_index,
                dep.ref.block_number,
                dep.ref.block_hash,
                block_timestamp,
                dep.pubkey,
                dep.withdrawal_credentials,
                dep.amount_gwei,
                dep.deposit_index,
            ),
        )
        return cur.rowcount == 1

    def raw_deposits_for(self, pubkeys: Iterable[str]) -> list[tuple[DepositLog, int]]:
        keys = sorted(set(pubkeys))
        rows: list[tuple] = []
        with self._lock:
            for chunk in _chunks(keys):
                marks = ",".join("?" * len(chunk))
                rows.extend(
                    self._query(
                        "SELECT tx_hash, log_index, block_number, block_hash, block_timestamp, pubkey, "
                        f"withdrawal_credentials, amount_gwei, deposit_index FROM raw_deposits WHERE pubkey IN ({marks})",
                        chunk,
                    )
                )
        rows.sort(key=lambda r: (r[2], r[1]))
        result = []
        for tx_hash, log_index, number, block_hash, ts, pubkey, wc, amount, index in rows:
            ref = LogRef(block_number=number, block_hash=block_hash, tx_hash=tx_hash, log_index=log_index, block_timestamp=ts)
            result.append((DepositLog(ref, pubkey, wc, amount, index), ts))
        return result

    def prune_raw_deposits(self, before_block: int) -> None:
        self._execute("DELETE FROM raw_deposits WHERE block_number < ?", (before_block,))

    # Deposits

    def insert_deposit(self, rec: DepositRecord) -> bool:
        cur = self._execute(
            "INSERT OR IGNORE INTO deposits (tx_hash, log_index, pubkey, amount_gwei, withdrawal_credentials, "
            "credentials_type, mismatch, is_topup, block_number, block_hash, block_timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                rec.tx_hash,
                rec.log_index,
                rec.pubkey,
                rec.amount_gwei,
                rec.withdrawal_credentials,
                rec.credentials_type,
                int(rec.mismatch),
                int(rec.is_topup),
                rec.block_number,
                rec.block_hash,
                rec.block_timestamp,
            ),
        )
        return cur.rowcount == 1

    def has_earlier_deposit(self, pubkey: str, block_number: int, log_index: int) -> bool:
        rows = self._query(
            "SELECT 1 FROM deposits WHERE pubkey = ? AND (block_number < ? OR (block_number = ? AND log_index < ?)) "
            "LIMIT 1",
            (pubkey, block_number, block_number, log_index),
        )
        return bool(rows)

    def deposit_summary(self) -> dict[str, DepositSummary]:
        rows = self._query(
            "SELECT pubkey, block_number, block_timestamp, credentials_type, mismatch, is_topup FROM deposits "
            "ORDER BY pubkey, is_topup, block_number, log_index"
        )
        result: dict[str, DepositSummary] = {}
        for pubkey, number, ts, ctype, mismatch, _topup in rows:
            prev = result.get(pubkey)
            if prev is None:
                result[pubkey] = DepositSummary(pubkey, number, ts, ctype, bool(mismatch), 1)
            else:
                result[pubkey] = DepositSummary(
                    pubkey,
                    prev.initial_block,
                    prev.initial_timestamp,
                    prev.credentials_type,
                    prev.mismatch or bool(mismatch),
                    prev.deposits + 1,
                )
        return result

    def deposited_pubkeys(self) -> set[str]:
        return {r[0] for r in self._query("SELECT DISTINCT pubkey FROM deposits")}

    def initial_deposits_since(self, since_timestamp: int) -> dict[str, int]:
        """Pubkey -> earliest block time of non-topup deposits at or after since_timestamp."""
        rows = self._query(
            "SELECT pubkey, MIN(block_timestamp) FROM deposits WHERE is_topup = 0 AND block_timestamp >= ? "
            "GROUP BY pubkey",
            (since_timestamp,),
        )
        return {p: ts for p, ts in rows}

    def mark_topups_after(self, pubkey: str, timestamp: int) -> int:
        """Reclassify deposits of pubkey made after timestamp as top-ups."""
        cur = self._execute(
            "UPDATE deposits SET is_topup = 1 WHERE pubkey = ? AND is_topup = 0 AND block_timestamp > ?",
            (pubkey, timestamp),
        )
        return cur.rowcount

    # Validators

    def upsert_validator(
        self, pubkey: str, index: Optional[int] = None, prior_deposit: Optional[bool] = None
    ) -> None:
        self._execute(
            "INSERT INTO validators (pubkey, validator_index, prior_deposit) VALUES (?, ?, ?) "
            "ON CONFLICT(pubkey) DO UPDATE SET "
            "validator_index = COALESCE(?, validator_index), prior_deposit = COALESCE(?, prior_deposit)",
            (
                pubkey,
                index,
                int(bool(prior_deposit)),
                index,
                None if prior_deposit is None else int(prior_deposit),
            ),
        )

    def validators(self) -> dict[str, ValidatorInfo]:
        rows = self._query("SELECT pubkey, validator_index, prior_deposit FROM validators")
        return {p: ValidatorInfo(p, i, bool(d)) for p, i, d in rows}

    # Exit requests

    def insert_exit_request(self, rec: ExitRequestRecord) -> bool:
        cur = self._execute(
            "INSERT OR IGNORE INTO exit_requests (tx_hash, log_index, pubkey, set_name, origin, module_id, "
            "operator_id, validator_index, request_timestamp, block_number, block_hash, block_timestamp, status, "
            "closed_status, closed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                rec.tx_hash,
                rec.log_index,
                rec.pubkey,
                rec.set_name,
                rec.origin,
                rec.module_id,
                rec.operator_id,
                rec.validator_index,
                rec.request_timestamp,
                rec.block_number,
                rec.block_hash,
                rec.block_timestamp,
                rec.status,
                rec.closed_status,
                rec.closed_at,
            ),
        )
        return cur.rowcount == 1

    def open_exit_requests(self) -> list[ExitRequestRecord]:
        rows = self._query(
            "SELECT pubkey, set_name, origin, module_id, operator_id, validator_index, request_timestamp, "
            "block_number, block_hash, block_timestamp, tx_hash, log_index, status, closed_status, closed_at "
            "FROM exit_requests WHERE status = 'open' ORDER BY block_number, log_index"
        )
        return [ExitRequestRecord(*r) for r in rows]

    def close_exit_requests(self, pubkey: str, closed_status: str, closed_at: int) -> int:
        cur = self._execute(
            "UPDATE exit_requests SET status = 'closed', closed_status = ?, closed_at = ? "
            "WHERE pubkey = ? AND status = 'open'",
            (closed_status, closed_at, pubkey),
        )
        return cur.rowcount

    def exit_request_count(self) -> int:
        return self._query("SELECT COUNT(*) FROM exit_requests")[0][0]

    # Triggered withdrawals

    def insert_triggered(self, rec: TriggeredRecord) -> bool:
        cur = self._execute(
            "INSERT OR IGNORE INTO triggered (tx_hash, log_index, pubkey, kind, amount_gwei, source_address, "
            "source_name, block_number, block_hash, block_timestamp, slot) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                rec.tx_hash,
                rec.log_index,
                rec.pubkey,
                rec.kind,
                rec.amount_gwei,
                rec.source_address,
                rec.source_name,
                rec.block_number,
                rec.block_hash,
                rec.block_timestamp,
                rec.slot,
            ),
        )
        return cur.rowcount == 1

    def triggered_last(self) -> dict[tuple[str, str], int]:
        rows = self._query("SELECT pubkey, kind, MAX(block_timestamp) FROM triggered GROUP BY pubkey, kind")
        return {(p, k): ts for p, k, ts in rows}

    # Reorgs

    def event_blocks(self, from_block: int) -> set[tuple[int, str]]:
        """Distinct (block_number, block_hash) of stored events at or above from_block."""
        out: set[tuple[int, str]] = set()
        with self._lock:
            for table in EVENT_TABLES:
                out.update(
                    self._query(f"SELECT DISTINCT block_number, block_hash FROM {table} WHERE block_number >= ?", (from_block,))
                )
        return out

    def rewind(self, from_block: int, keep_hashes: Iterable[str] = ()) -> None:
        """Drop events at or above from_block (except those in keep_hashes blocks) and move cursors back."""
        keep = sorted(set(keep_hashes))
        with self.transaction():
            for table in EVENT_TABLES:
                sql = f"DELETE FROM {table} WHERE block_number >= ?"
                params: list = [from_block]
                for chunk in _chunks(keep):
                    sql += f" AND block_hash NOT IN ({','.join('?' * len(chunk))})"
                    params.extend(chunk)
                self._conn.execute(sql, params)
            self._conn.execute(
                "UPDATE cursors SET block_number = ?, block_hash = NULL, block_timestamp = NULL "
                "WHERE block_number >= ?",
                (from_block - 1, from_block),
            )
