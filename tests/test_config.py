from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from src.config import Config, load_config, normalize_address, normalize_pubkey

PK = "0x" + "ab" * 48


def _write(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def _base() -> dict:
    return {
        "execution_endpoints": ["http://el:8545/"],
        "beacon_endpoints": ["http://cl:5052"],
        "sets": [
            {
                "name": "main",
                "keys_api": [{"url": "http://keys-api:3000/", "module_id": 2, "operator_id": 0}],
                "static_pubkeys": [{"pubkey": "AB" * 48, "label": "x"}],
            }
        ],
    }


def test_defaults(tmp_path):
    cfg = load_config(_write(tmp_path, _base()), env={})
    assert cfg.network == "mainnet"
    assert cfg.execution_endpoints == ["http://el:8545"]
    assert cfg.sets[0].keys_api[0].url == "http://keys-api:3000"
    assert cfg.sets[0].static_pubkeys[0].pubkey == PK
    assert cfg.confirmations == 2
    assert cfg.log_chunk_blocks == 2000
    assert cfg.exit_lookback_days == 14
    assert cfg.deposit_lookback_days == 7
    assert cfg.triggered_lookback_days == 7
    assert cfg.poll_interval_seconds == 12
    assert cfg.seed_deposited_from_beacon is True
    assert cfg.listen_host == "0.0.0.0"
    assert cfg.listen_port == 9800
    assert cfg.expected_vault_address is None


def test_env_overrides(tmp_path):
    env = {
        "LKE_EXECUTION_ENDPOINTS": "http://a:8545, http://b:8545",
        "LKE_BEACON_ENDPOINTS": "http://c:5052",
        "LKE_LISTEN": "127.0.0.1:9999",
        "LKE_DATA_DIR": "/tmp/x",
        "LKE_LOG_LEVEL": "DEBUG",
    }
    cfg = load_config(_write(tmp_path, _base()), env=env)
    assert cfg.execution_endpoints == ["http://a:8545", "http://b:8545"]
    assert cfg.beacon_endpoints == ["http://c:5052"]
    assert (cfg.listen_host, cfg.listen_port) == ("127.0.0.1", 9999)
    assert cfg.data_dir == Path("/tmp/x")
    assert cfg.log_level == "DEBUG"


def test_expected_credentials_and_sources(tmp_path):
    data = _base()
    data["expected_withdrawal_credentials"] = "0x010000000000000000000000B9D7934878B5FB9610B3FE8A5E441E8FAD7E293F"
    data["known_sources"] = {"0xB9D7934878B5FB9610B3FE8A5E441E8FAD7E293F": "lido-vault"}
    cfg = load_config(_write(tmp_path, data), env={})
    assert cfg.expected_vault_address == "0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f"
    assert cfg.known_sources == {"0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f": "lido-vault"}

    data["expected_withdrawal_credentials"] = "0xB9D7934878B5FB9610B3FE8A5E441E8FAD7E293F"
    cfg = load_config(_write(tmp_path, data), env={})
    assert cfg.expected_vault_address == "0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(expected_withdrawal_credentials="0x1234"),
        lambda d: d.update(network="holesky"),
        lambda d: d.update(sets=[]),
        lambda d: d.update(sets=[{"name": "a"}, {"name": "a"}]),
        lambda d: d.update(execution_endpoints=[" "]),
        lambda d: d["sets"][0]["static_pubkeys"].append({"pubkey": "0x12"}),
    ],
)
def test_invalid(tmp_path, mutate):
    data = _base()
    mutate(data)
    with pytest.raises(ValidationError):
        Config.model_validate(data)


def test_normalize():
    assert normalize_pubkey("AB" * 48) == PK
    assert normalize_address(" 0xABCDEF0123456789ABCDEF0123456789ABCDEF01 ") == "0xabcdef0123456789abcdef0123456789abcdef01"
    with pytest.raises(ValueError):
        normalize_pubkey("0x1234")
    with pytest.raises(ValueError):
        normalize_address("0xzz")
