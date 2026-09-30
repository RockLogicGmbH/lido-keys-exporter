"""In-memory stand-ins for the EL and beacon clients."""

from __future__ import annotations

import hashlib
from typing import Any, Optional

from eth_abi import encode as abi_encode

from src.clients.cl import CLError, NonHeadStateError
from src.clients.el import BlockHeader, ELError, ELRpcError
from src.contracts import (
    DEPOSIT_CONTRACT,
    DEPOSIT_EVENT_TOPIC,
    EXIT_REQUEST_TOPIC,
    GENESIS_TIME,
    SECONDS_PER_SLOT,
    WITHDRAWAL_REQUEST_CONTRACT,
    hex_to_bytes,
    uint_topic,
)


def pubkey(i: int) -> str:
    return "0x" + i.to_bytes(48, "big").hex()


def address(i: int) -> str:
    return "0x" + i.to_bytes(20, "big").hex()


def credentials(addr: str, prefix: int = 1) -> str:
    return "0x" + bytes([prefix]).hex() + "00" * 11 + addr[2:]


def address_word(addr: str) -> str:
    return "0x" + "00" * 12 + addr[2:]


def deposit_log(pk: str, wc: str, amount_gwei: int = 32_000_000_000, index: int = 0) -> tuple[str, list[str], str]:
    data = abi_encode(
        ["bytes"] * 5,
        [
            hex_to_bytes(pk),
            hex_to_bytes(wc),
            amount_gwei.to_bytes(8, "little"),
            b"\x11" * 96,
            index.to_bytes(8, "little"),
        ],
    )
    return DEPOSIT_CONTRACT, [DEPOSIT_EVENT_TOPIC], "0x" + data.hex()


def exit_request_log(
    vebo: str, module_id: int, operator_id: int, validator_index: int, pk: str, timestamp: int
) -> tuple[str, list[str], str]:
    topics = [EXIT_REQUEST_TOPIC, uint_topic(module_id), uint_topic(operator_id), uint_topic(validator_index)]
    data = abi_encode(["bytes", "uint256"], [hex_to_bytes(pk), timestamp])
    return vebo, topics, "0x" + data.hex()


def withdrawal_request_log(source: str, pk: str, amount_gwei: int = 0) -> tuple[str, list[str], str]:
    data = hex_to_bytes(source) + hex_to_bytes(pk) + amount_gwei.to_bytes(8, "big")
    return WITHDRAWAL_REQUEST_CONTRACT, [], "0x" + data.hex()


class FakeChain:
    """Lazily generated chain: block n has a hash derived from n and its fork generation."""

    def __init__(self, head: int = 20_000, first_timestamp: int = GENESIS_TIME + 10_000_000):
        self.head = head
        self.first_timestamp = first_timestamp
        self._generation: dict[int, int] = {}
        self._logs: list[dict[str, Any]] = []
        self._log_counter: dict[int, int] = {}
        self.call_results: dict[tuple[str, str], str] = {}
        self.include_log_timestamps = False

    def block_hash(self, number: int) -> str:
        gen = self._generation.get(number, 0)
        return "0x" + hashlib.sha256(f"block:{number}:{gen}".encode()).hexdigest()

    def timestamp(self, number: int) -> int:
        return self.first_timestamp + number * SECONDS_PER_SLOT

    def header(self, number: int) -> BlockHeader:
        return BlockHeader(number=number, hash=self.block_hash(number), timestamp=self.timestamp(number))

    def mine(self, n: int = 1) -> int:
        self.head += n
        return self.head

    def add_log(self, block: int, spec: tuple[str, list[str], str]) -> dict[str, Any]:
        """Add a log (address, topics, data) to a block; returns its identity."""
        addr, topics, data = spec
        if block > self.head:
            self.head = block
        index = self._log_counter.get(block, 0)
        self._log_counter[block] = index + 1
        gen = self._generation.get(block, 0)
        tx = "0x" + hashlib.sha256(f"tx:{block}:{gen}:{index}".encode()).hexdigest()
        entry = {
            "address": addr.lower(),
            "topics": [t.lower() for t in topics],
            "data": data,
            "block": block,
            "tx": tx,
            "log_index": index,
        }
        self._logs.append(entry)
        return entry

    def reorg(self, from_block: int) -> None:
        """Replace every block from from_block with a new fork; their logs disappear."""
        for n in range(from_block, self.head + 1):
            self._generation[n] = self._generation.get(n, 0) + 1
            self._log_counter.pop(n, None)
        self._logs = [e for e in self._logs if e["block"] < from_block]

    def rpc_log(self, entry: dict[str, Any]) -> dict[str, Any]:
        out = {
            "address": entry["address"],
            "topics": list(entry["topics"]),
            "data": entry["data"],
            "blockNumber": hex(entry["block"]),
            "blockHash": self.block_hash(entry["block"]),
            "transactionHash": entry["tx"],
            "logIndex": hex(entry["log_index"]),
            "removed": False,
        }
        if self.include_log_timestamps:
            out["blockTimestamp"] = hex(self.timestamp(entry["block"]))
        return out


def _topic_match(log_topics: list[str], topics: list) -> bool:
    for i, want in enumerate(topics):
        if want is None:
            continue
        if i >= len(log_topics):
            return False
        options = want if isinstance(want, list) else [want]
        if log_topics[i] not in [o.lower() for o in options]:
            return False
    return True


class FakeExecutionClient:
    def __init__(self, chain: FakeChain, max_range: Optional[int] = None, healthy: bool = True):
        self.chain = chain
        self.max_range = max_range
        self.healthy = healthy
        self.fail = False
        self.get_logs_calls: list[tuple[str, list, int, int]] = []
        self.rejected_ranges: list[tuple[int, int]] = []
        self.calls: list[tuple[str, str]] = []

    def _check(self) -> None:
        if self.fail:
            raise ELError("fake EL unavailable")

    def refresh_health(self) -> bool:
        return self.healthy

    def block_number(self) -> int:
        self._check()
        return self.chain.head

    def get_block(self, number: int) -> BlockHeader:
        self._check()
        if number < 0 or number > self.chain.head:
            raise ELError(f"block {number} not found")
        return self.chain.header(number)

    def get_logs(self, address: str, topics: list, from_block: int, to_block: int) -> list[dict]:
        self._check()
        if self.max_range is not None and to_block - from_block + 1 > self.max_range:
            self.rejected_ranges.append((from_block, to_block))
            raise ELRpcError("query returned more than 10000 results")
        self.get_logs_calls.append((address.lower(), topics, from_block, to_block))
        out = [
            self.chain.rpc_log(e)
            for e in self.chain._logs
            if e["address"] == address.lower()
            and from_block <= e["block"] <= to_block
            and _topic_match(e["topics"], topics)
        ]
        out.sort(key=lambda e: (int(e["blockNumber"], 16), int(e["logIndex"], 16)))
        return out

    def call(self, to: str, data: str, block: str = "latest") -> str:
        self._check()
        self.calls.append((to, data))
        try:
            return self.chain.call_results[(to.lower(), data.lower())]
        except KeyError:
            raise ELRpcError(f"execution reverted: {to} {data}") from None

    def calls_for(self, address: str) -> list[tuple[str, list, int, int]]:
        return [c for c in self.get_logs_calls if c[0] == address.lower()]

    def close(self) -> None:
        pass


class FakeBeaconClient:
    """Head state only beacon node; records every path it was asked for."""

    def __init__(self, slot: int = 1000, healthy: bool = True):
        self.slot = slot
        self.healthy = healthy
        self.fail = False
        self.paths: list[str] = []
        self.validator_calls: list[list[str]] = []
        self._validators: dict[str, dict[str, Any]] = {}

    def _request(self, path: str) -> None:
        if "/states/" in path and not path.startswith("/eth/v1/beacon/states/head/"):
            raise NonHeadStateError(path)
        self.paths.append(path)
        if self.fail:
            raise CLError("fake beacon unavailable")

    def add_validator(
        self, pk: str, index: int, status: str = "active_ongoing", eligibility_epoch: Optional[int] = None
    ) -> None:
        validator: dict[str, Any] = {"pubkey": pk.lower()}
        if eligibility_epoch is not None:
            validator["activation_eligibility_epoch"] = str(eligibility_epoch)
        self._validators[pk.lower()] = {
            "index": str(index),
            "balance": "32000000000",
            "status": status,
            "validator": validator,
        }

    def set_status(self, pk: str, status: str) -> None:
        self._validators[pk.lower()]["status"] = status

    def next_epoch(self) -> None:
        self.slot += 32

    def refresh_health(self) -> bool:
        self._request("/eth/v1/node/syncing")
        return self.healthy

    def head_slot(self) -> int:
        self._request("/eth/v1/beacon/headers/head")
        return self.slot

    def validators(self, ids: list[str]) -> list[dict]:
        self._request("/eth/v1/beacon/states/head/validators")
        self.validator_calls.append(list(ids))
        by_index = {v["index"]: v for v in self._validators.values()}
        out = []
        for i in ids:
            entry = self._validators.get(i.lower()) or by_index.get(i)
            if entry is not None:
                out.append(dict(entry, validator=dict(entry["validator"])))
        return out

    def close(self) -> None:
        pass
