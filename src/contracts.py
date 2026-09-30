"""Mainnet constants, topic hashes and log decoders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from eth_abi import decode as abi_decode
from eth_utils import keccak

DEPOSIT_CONTRACT = "0x00000000219ab540356cbb839cbe05303d7705fa"
WITHDRAWAL_REQUEST_CONTRACT = "0x00000961ef480eb55e80d19ad83579a64c007002"
LIDO_LOCATOR = "0xc1d0b3de6792bf6b4b37eccdcc24e45978cfd2eb"

GENESIS_TIME = 1606824023
SECONDS_PER_SLOT = 12
SLOTS_PER_EPOCH = 32
BLOCKS_PER_DAY = 86400 // SECONDS_PER_SLOT

DEPOSIT_EVENT_SIGNATURE = "DepositEvent(bytes,bytes,bytes,bytes,bytes)"
EXIT_REQUEST_SIGNATURE = "ValidatorExitRequest(uint256,uint256,uint256,bytes,uint256)"

# Beacon chain statuses that close an open exit request.
EXITING_STATUSES = frozenset(
    {
        "active_exiting",
        "active_slashed",
        "exited_unslashed",
        "exited_slashed",
        "withdrawal_possible",
        "withdrawal_done",
    }
)


def event_topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


def function_selector(signature: str) -> str:
    return "0x" + keccak(text=signature)[:4].hex()


DEPOSIT_EVENT_TOPIC = event_topic(DEPOSIT_EVENT_SIGNATURE)
EXIT_REQUEST_TOPIC = event_topic(EXIT_REQUEST_SIGNATURE)
SELECTOR_VALIDATORS_EXIT_BUS_ORACLE = function_selector("validatorsExitBusOracle()")
SELECTOR_WITHDRAWAL_VAULT = function_selector("withdrawalVault()")


def uint_topic(value: int) -> str:
    return "0x" + value.to_bytes(32, "big").hex()


def hex_to_bytes(value: str) -> bytes:
    return bytes.fromhex(value[2:] if value.startswith("0x") else value)


def to_hex(value: bytes) -> str:
    return "0x" + value.hex()


def decode_address_word(value: str) -> str:
    """Decode an ABI encoded address return value from eth_call."""
    raw = hex_to_bytes(value)
    if len(raw) != 32:
        raise ValueError(f"unexpected eth_call result length {len(raw)}")
    return to_hex(raw[12:])


def slot_at(timestamp: int) -> int:
    return max(0, (timestamp - GENESIS_TIME) // SECONDS_PER_SLOT)


@dataclass(frozen=True)
class LogRef:
    block_number: int
    block_hash: str
    tx_hash: str
    log_index: int
    block_timestamp: Optional[int]


def log_ref(log: dict[str, Any]) -> LogRef:
    ts = log.get("blockTimestamp")
    return LogRef(
        block_number=int(log["blockNumber"], 16),
        block_hash=log["blockHash"].lower(),
        tx_hash=log["transactionHash"].lower(),
        log_index=int(log["logIndex"], 16),
        block_timestamp=int(ts, 16) if ts else None,
    )


@dataclass(frozen=True)
class DepositLog:
    ref: LogRef
    pubkey: str
    withdrawal_credentials: str
    amount_gwei: int
    deposit_index: int


def decode_deposit(log: dict[str, Any]) -> DepositLog:
    pubkey, credentials, amount, _signature, index = abi_decode(["bytes"] * 5, hex_to_bytes(log["data"]))
    if len(pubkey) != 48 or len(credentials) != 32 or len(amount) != 8 or len(index) != 8:
        raise ValueError("malformed DepositEvent")
    return DepositLog(
        ref=log_ref(log),
        pubkey=to_hex(pubkey),
        withdrawal_credentials=to_hex(credentials),
        # The deposit contract encodes amount and index as little endian uint64.
        amount_gwei=int.from_bytes(amount, "little"),
        deposit_index=int.from_bytes(index, "little"),
    )


@dataclass(frozen=True)
class ExitRequestLog:
    ref: LogRef
    module_id: int
    operator_id: int
    validator_index: int
    pubkey: str
    request_timestamp: int


def decode_exit_request(log: dict[str, Any]) -> ExitRequestLog:
    topics = log["topics"]
    if len(topics) != 4 or topics[0].lower() != EXIT_REQUEST_TOPIC:
        raise ValueError("not a ValidatorExitRequest log")
    pubkey, timestamp = abi_decode(["bytes", "uint256"], hex_to_bytes(log["data"]))
    if len(pubkey) != 48:
        raise ValueError("malformed ValidatorExitRequest pubkey")
    return ExitRequestLog(
        ref=log_ref(log),
        module_id=int(topics[1], 16),
        operator_id=int(topics[2], 16),
        validator_index=int(topics[3], 16),
        pubkey=to_hex(pubkey),
        request_timestamp=timestamp,
    )


@dataclass(frozen=True)
class WithdrawalRequestLog:
    ref: LogRef
    source_address: str
    pubkey: str
    amount_gwei: int

    @property
    def kind(self) -> str:
        return "exit" if self.amount_gwei == 0 else "partial"


def decode_withdrawal_request(log: dict[str, Any]) -> WithdrawalRequestLog:
    """Decode the anonymous log the EIP-7002 predeploy writes for every request.

    Data layout (76 bytes): source address (20) ++ validator pubkey (48) ++
    amount in gwei (8, big endian). This is the same encoding as the request
    in the beacon block's execution_requests.withdrawals.
    """
    data = hex_to_bytes(log["data"])
    if len(data) != 76:
        raise ValueError(f"unexpected withdrawal request log length {len(data)}")
    return WithdrawalRequestLog(
        ref=log_ref(log),
        source_address=to_hex(data[:20]),
        pubkey=to_hex(data[20:68]),
        amount_gwei=int.from_bytes(data[68:76], "big"),
    )


def credentials_type(credentials: str) -> str:
    return credentials[:4].lower()


def credentials_match(credentials: str, vault_address: str) -> bool:
    """True for 0x01 or 0x02 credentials that point at the given address."""
    raw = hex_to_bytes(credentials)
    return (
        len(raw) == 32
        and raw[0] in (1, 2)
        and raw[1:12] == b"\x00" * 11
        and raw[12:] == hex_to_bytes(vault_address)
    )


def credentials_address(credentials: str) -> Optional[str]:
    raw = hex_to_bytes(credentials)
    if len(raw) == 32 and raw[0] in (1, 2):
        return to_hex(raw[12:])
    return None
