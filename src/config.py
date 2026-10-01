"""Configuration: one YAML file, endpoint and runtime overrides from the environment."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

PUBKEY_RE = re.compile(r"^0x[0-9a-f]{96}$")
ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")

ENV_PREFIX = "LKE_"
DEFAULT_CONFIG_PATH = "/opt/app/config.yaml"


def normalize_pubkey(value: str) -> str:
    value = value.strip().lower()
    if not value.startswith("0x"):
        value = "0x" + value
    if not PUBKEY_RE.match(value):
        raise ValueError(f"invalid validator pubkey: {value}")
    return value


def normalize_address(value: str) -> str:
    value = value.strip().lower()
    if not value.startswith("0x"):
        value = "0x" + value
    if not ADDRESS_RE.match(value):
        raise ValueError(f"invalid address: {value}")
    return value


class KeysApiSource(BaseModel):
    url: str
    module_id: int = Field(ge=0)
    operator_id: int = Field(ge=0)

    @field_validator("url")
    @classmethod
    def _strip_slash(cls, v: str) -> str:
        return v.rstrip("/")


class StaticKey(BaseModel):
    pubkey: str
    label: str = ""

    @field_validator("pubkey")
    @classmethod
    def _pubkey(cls, v: str) -> str:
        return normalize_pubkey(v)


class KeySetConfig(BaseModel):
    name: str
    keys_api: list[KeysApiSource] = []
    static_pubkeys: list[StaticKey] = []


class Config(BaseModel):
    network: Literal["mainnet"] = "mainnet"
    listen: str = "0.0.0.0:9800"
    data_dir: Path = Path("/opt/app/data")
    execution_endpoints: list[str]
    beacon_endpoints: list[str]
    confirmations: int = Field(default=2, ge=0)
    keyset_refresh_minutes: float = Field(default=10, gt=0)
    exit_lookback_days: float = Field(default=14, ge=0)
    deposit_lookback_days: float = Field(default=7, ge=0)
    triggered_lookback_days: float = Field(default=7, ge=0)
    log_chunk_blocks: int = Field(default=2000, ge=1)
    poll_interval_seconds: float = Field(default=12, gt=0)
    reorg_rewind_blocks: int = Field(default=64, ge=1)
    http_timeout_seconds: float = Field(default=30, gt=0)
    # Blocks (EL) / slots (CL) an endpoint may trail the best endpoint of its kind.
    max_endpoint_lag: int = Field(default=5, ge=0)
    # Full 32 byte credentials or the 20 byte vault address. Resolved from the
    # LidoLocator (withdrawalVault()) when not set.
    expected_withdrawal_credentials: Optional[str] = None
    # Look up keys without a deposit in the lookback window on the beacon chain
    # (head state) so a later deposit to them is counted as a top-up.
    seed_deposited_from_beacon: bool = True
    sets: list[KeySetConfig] = Field(min_length=1)
    known_sources: dict[str, str] = {}
    log_level: str = "INFO"

    @field_validator("execution_endpoints", "beacon_endpoints")
    @classmethod
    def _endpoints(cls, v: list[str]) -> list[str]:
        v = [e.strip().rstrip("/") for e in v if e.strip()]
        if not v:
            raise ValueError("at least one endpoint is required")
        return v

    @field_validator("known_sources")
    @classmethod
    def _sources(cls, v: dict[str, str]) -> dict[str, str]:
        return {normalize_address(k): name for k, name in v.items()}

    @field_validator("expected_withdrawal_credentials")
    @classmethod
    def _credentials(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.strip().lower()
        if not re.match(r"^0x([0-9a-f]{40}|[0-9a-f]{64})$", v):
            raise ValueError("expected_withdrawal_credentials must be a 20 byte address or 32 byte credentials")
        return v

    @model_validator(mode="after")
    def _unique_sets(self) -> "Config":
        names = [s.name for s in self.sets]
        if len(names) != len(set(names)):
            raise ValueError("set names must be unique")
        return self

    @property
    def listen_host(self) -> str:
        return self.listen.rsplit(":", 1)[0] or "0.0.0.0"

    @property
    def listen_port(self) -> int:
        return int(self.listen.rsplit(":", 1)[1])

    @property
    def expected_vault_address(self) -> Optional[str]:
        """The withdrawal vault address part of expected_withdrawal_credentials."""
        if self.expected_withdrawal_credentials is None:
            return None
        return "0x" + self.expected_withdrawal_credentials[-40:]


def _split(value: str) -> list[str]:
    return [p.strip() for p in value.split(",") if p.strip()]


def load_config(path: str | Path, env: Optional[dict[str, str]] = None) -> Config:
    env = os.environ if env is None else env
    with open(path) as fh:
        raw = yaml.safe_load(fh) or {}

    overrides = {
        "EXECUTION_ENDPOINTS": ("execution_endpoints", _split),
        "BEACON_ENDPOINTS": ("beacon_endpoints", _split),
        "LISTEN": ("listen", str),
        "DATA_DIR": ("data_dir", str),
        "LOG_LEVEL": ("log_level", str),
    }
    for name, (key, parse) in overrides.items():
        value = env.get(ENV_PREFIX + name)
        if value:
            raw[key] = parse(value)
    return Config.model_validate(raw)
