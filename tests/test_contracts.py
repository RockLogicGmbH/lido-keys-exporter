from __future__ import annotations

import pytest
from eth_abi import encode

from src.contracts import (
    DEPOSIT_EVENT_TOPIC,
    EXIT_REQUEST_TOPIC,
    SELECTOR_VALIDATORS_EXIT_BUS_ORACLE,
    SELECTOR_WITHDRAWAL_VAULT,
    credentials_address,
    credentials_match,
    credentials_type,
    decode_address_word,
    decode_deposit,
    decode_exit_request,
    decode_withdrawal_request,
    slot_at,
    uint_topic,
    GENESIS_TIME,
)

PUBKEY = bytes(range(48))
VAULT = "0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f"


def _base_log(data: bytes, topics: list[str]) -> dict:
    return {
        "address": "0x00",
        "blockNumber": hex(100),
        "blockHash": "0x" + "AB" * 32,
        "transactionHash": "0x" + "CD" * 32,
        "logIndex": hex(3),
        "data": "0x" + data.hex(),
        "topics": topics,
    }


def test_topics():
    assert DEPOSIT_EVENT_TOPIC == "0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5"
    assert EXIT_REQUEST_TOPIC == "0x96395f55c4997466e5035d777f0e1ba82b8cae217aaad05cf07839eb7c75bcf2"


def test_selectors():
    assert len(SELECTOR_VALIDATORS_EXIT_BUS_ORACLE) == 10
    assert len(SELECTOR_WITHDRAWAL_VAULT) == 10


def test_uint_topic():
    assert uint_topic(2) == "0x" + "00" * 31 + "02"


def test_decode_deposit_roundtrip():
    wc = b"\x01" + b"\x00" * 11 + bytes.fromhex(VAULT[2:])
    amount = (32_000_000_000).to_bytes(8, "little")
    index = (12345).to_bytes(8, "little")
    data = encode(["bytes"] * 5, [PUBKEY, wc, amount, b"\x11" * 96, index])
    log = _base_log(data, [DEPOSIT_EVENT_TOPIC])
    log["blockTimestamp"] = hex(1_700_000_000)
    dep = decode_deposit(log)
    assert dep.pubkey == "0x" + PUBKEY.hex()
    assert dep.withdrawal_credentials == "0x" + wc.hex()
    assert dep.amount_gwei == 32_000_000_000
    assert dep.deposit_index == 12345
    assert dep.ref.block_number == 100
    assert dep.ref.log_index == 3
    assert dep.ref.block_hash == "0x" + "ab" * 32
    assert dep.ref.tx_hash == "0x" + "cd" * 32
    assert dep.ref.block_timestamp == 1_700_000_000


def test_decode_deposit_malformed():
    data = encode(["bytes"] * 5, [b"\x00" * 47, b"\x00" * 32, b"\x00" * 8, b"", b"\x00" * 8])
    with pytest.raises(ValueError):
        decode_deposit(_base_log(data, [DEPOSIT_EVENT_TOPIC]))


def test_decode_exit_request_roundtrip():
    data = encode(["bytes", "uint256"], [PUBKEY, 1_750_000_000])
    topics = [EXIT_REQUEST_TOPIC, uint_topic(2), uint_topic(7), uint_topic(987654)]
    ex = decode_exit_request(_base_log(data, topics))
    assert (ex.module_id, ex.operator_id, ex.validator_index) == (2, 7, 987654)
    assert ex.pubkey == "0x" + PUBKEY.hex()
    assert ex.request_timestamp == 1_750_000_000
    assert ex.ref.block_timestamp is None


def test_decode_exit_request_wrong_topic():
    data = encode(["bytes", "uint256"], [PUBKEY, 1])
    with pytest.raises(ValueError):
        decode_exit_request(_base_log(data, [DEPOSIT_EVENT_TOPIC, uint_topic(1), uint_topic(1), uint_topic(1)]))


@pytest.mark.parametrize("amount,kind", [(0, "exit"), (1_000_000_000, "partial")])
def test_decode_withdrawal_request(amount, kind):
    source = bytes.fromhex("11" * 20)
    data = source + PUBKEY + amount.to_bytes(8, "big")
    assert len(data) == 76
    req = decode_withdrawal_request(_base_log(data, []))
    assert req.source_address == "0x" + "11" * 20
    assert req.pubkey == "0x" + PUBKEY.hex()
    assert req.amount_gwei == amount
    assert req.kind == kind


def test_decode_withdrawal_request_bad_length():
    with pytest.raises(ValueError):
        decode_withdrawal_request(_base_log(b"\x00" * 75, []))


def test_credentials_match():
    addr = bytes.fromhex(VAULT[2:])
    wc01 = "0x" + (b"\x01" + b"\x00" * 11 + addr).hex()
    wc02 = "0x" + (b"\x02" + b"\x00" * 11 + addr).hex()
    wc_other = "0x" + (b"\x01" + b"\x00" * 11 + b"\x22" * 20).hex()
    wc00 = "0x" + (b"\x00" + b"\x00" * 11 + addr).hex()
    assert credentials_match(wc01, VAULT)
    assert credentials_match(wc02, VAULT)
    assert not credentials_match(wc_other, VAULT)
    assert not credentials_match(wc00, VAULT)
    assert credentials_type(wc02) == "0x02"
    assert credentials_address(wc01) == VAULT
    assert credentials_address(wc00) is None


def test_decode_address_word_and_slot():
    assert decode_address_word("0x" + "00" * 12 + VAULT[2:]) == VAULT
    with pytest.raises(ValueError):
        decode_address_word("0x1234")
    assert slot_at(GENESIS_TIME + 25) == 2
    assert slot_at(0) == 0
