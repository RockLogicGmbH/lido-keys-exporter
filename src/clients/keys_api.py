"""Lido Keys API client."""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from ..config import KeysApiSource, normalize_pubkey

log = logging.getLogger(__name__)


class KeysApiError(Exception):
    pass


class KeysApiClient:
    def __init__(self, timeout: float = 30, transport: Optional[httpx.BaseTransport] = None):
        self._http = httpx.Client(timeout=timeout, transport=transport)

    def fetch(self, source: KeysApiSource) -> list[str]:
        """Used pubkeys of one operator in one staking module."""
        url = f"{source.url}/v1/modules/{source.module_id}/keys"
        params = {"used": "true", "operatorIndex": str(source.operator_id)}
        try:
            resp = self._http.get(url, params=params)
            resp.raise_for_status()
            body = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise KeysApiError(f"{url}: {exc}") from exc

        entries = self._entries(body)
        keys: list[str] = []
        for entry in entries:
            if not isinstance(entry, dict) or "key" not in entry:
                continue
            if entry.get("used", True) is not True:
                continue
            operator = entry.get("operatorIndex")
            if operator is not None and int(operator) != source.operator_id:
                continue
            try:
                keys.append(normalize_pubkey(entry["key"]))
            except ValueError as exc:
                log.warning("skipping invalid key from %s: %s", url, exc)
        return keys

    @staticmethod
    def _entries(body: Any) -> list[Any]:
        data = body.get("data") if isinstance(body, dict) else None
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get("keys"), list):
            return data["keys"]
        raise KeysApiError("unexpected Keys API response shape")

    def close(self) -> None:
        self._http.close()
