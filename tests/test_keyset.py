from __future__ import annotations

import pytest

from src.config import Config, KeysApiSource
from src.keyset import GROUP_LABELS, KeyInfo, KeySet


def pk(n: int) -> str:
    return "0x" + f"{n:02x}" * 48


def make_cfg(sets) -> Config:
    return Config.model_validate(
        {"execution_endpoints": ["http://el"], "beacon_endpoints": ["http://cl"], "sets": sets}
    )


SRC_A = {"url": "http://k", "module_id": 1, "operator_id": 5}
SRC_B = {"url": "http://k", "module_id": 2, "operator_id": 0}


class Fetcher:
    def __init__(self, data):
        self.data = data
        self.calls = []

    def __call__(self, source: KeysApiSource):
        self.calls.append((source.module_id, source.operator_id))
        value = self.data[(source.module_id, source.operator_id)]
        if isinstance(value, Exception):
            raise value
        return value


def test_labels():
    info = KeyInfo(pk(1), "main", "static", None, None, "x")
    assert info.labels() == {"set": "main", "origin": "static", "module_id": "", "operator_id": ""}
    assert tuple(KeyInfo(pk(1), "a", "keys_api", 2, 0).labels()) == GROUP_LABELS
    assert KeyInfo(pk(1), "a", "keys_api", 2, 0).labels()["module_id"] == "2"


def test_refresh_and_lookup():
    cfg = make_cfg([
        {"name": "a", "keys_api": [SRC_A], "static_pubkeys": [{"pubkey": pk(9), "label": "lbl"}]},
        {"name": "b", "keys_api": [SRC_B]},
    ])
    fetch = Fetcher({(1, 5): [pk(1), pk(2)], (2, 0): [pk(3)]})
    errors = []
    ks = KeySet(cfg, fetch, on_error=lambda: errors.append(1))
    assert ks.last_success_timestamp is None
    res = ks.refresh()
    assert res.ok and res.errors == 0
    assert res.added == {pk(1), pk(2), pk(3), pk(9)} and res.removed == set()
    assert ks.last_success_timestamp is not None
    assert len(ks) == 4
    assert ks.get(pk(1)) == KeyInfo(pk(1), "a", "keys_api", 1, 5)
    assert ks.get(pk(9)) == KeyInfo(pk(9), "a", "static", None, None, "lbl")
    assert ks.get(pk(3)).set_name == "b"
    assert ks.get(pk(4)) is None
    assert ks.has_static()
    assert [(n, s.module_id) for n, s in ks.sources()] == [("a", 1), ("b", 2)]
    all_keys = ks.all()
    all_keys.clear()
    assert len(ks) == 4

    fetch.data[(1, 5)] = [pk(1)]
    res = ks.refresh()
    assert res.removed == {pk(2)} and res.added == set()
    assert not errors


def test_failed_source_keeps_last_good():
    cfg = make_cfg([{"name": "a", "keys_api": [SRC_A, SRC_B]}])
    fetch = Fetcher({(1, 5): [pk(1)], (2, 0): RuntimeError("down")})
    errors = []
    ks = KeySet(cfg, fetch, on_error=lambda: errors.append(1))
    res = ks.refresh()
    assert not res.ok and res.errors == 1
    assert errors == [1]
    assert ks.last_success_timestamp is None
    assert set(ks.all()) == {pk(1)}

    fetch.data[(2, 0)] = [pk(2)]
    assert ks.refresh().ok
    ts = ks.last_success_timestamp
    assert set(ks.all()) == {pk(1), pk(2)}

    fetch.data[(1, 5)] = RuntimeError("down")
    res = ks.refresh()
    assert not res.ok and res.removed == set()
    assert set(ks.all()) == {pk(1), pk(2)}
    assert ks.last_success_timestamp == ts
    assert len(errors) == 2


def test_duplicates_first_wins(caplog):
    cfg = make_cfg([
        {"name": "a", "keys_api": [SRC_A], "static_pubkeys": [{"pubkey": pk(1)}]},
        {"name": "b", "static_pubkeys": [{"pubkey": pk(1)}]},
    ])
    ks = KeySet(cfg, Fetcher({(1, 5): [pk(1)]}))
    ks.refresh()
    ks.refresh()
    assert ks.get(pk(1)).origin == "keys_api"
    assert len([r for r in caplog.records if "duplicate" in r.getMessage()]) == 1


def test_match_exit():
    cfg = make_cfg([{"name": "a", "keys_api": [SRC_A]}, {"name": "s", "static_pubkeys": [{"pubkey": pk(9)}]}])
    ks = KeySet(cfg, Fetcher({(1, 5): [pk(1)]}))
    ks.refresh()
    assert ks.match_exit(7, 7, pk(9)).origin == "static"
    assert ks.match_exit(1, 5, pk(1)) == ks.get(pk(1))
    assert ks.match_exit(1, 5, pk(2)) == KeyInfo(pk(2), "a", "keys_api", 1, 5)
    assert ks.match_exit(1, 6, pk(2)) is None


def test_exit_topic_filter():
    ks = KeySet(make_cfg([{"name": "a", "keys_api": [SRC_B, SRC_A, SRC_A]}]), Fetcher({}))
    assert ks.exit_topic_filter() == ([1, 2], [0, 5])
    ks = KeySet(make_cfg([{"name": "a", "keys_api": [SRC_A]}, {"name": "b", "static_pubkeys": [{"pubkey": pk(1)}]}]), Fetcher({}))
    assert ks.exit_topic_filter() is None
    ks = KeySet(make_cfg([{"name": "a"}]), Fetcher({}))
    assert ks.exit_topic_filter() == ([], [])
    assert ks.refresh().ok and len(ks) == 0


def test_all_sources_loaded():
    cfg = make_cfg([{"name": "a", "keys_api": [SRC_A, SRC_B]}])
    fetch = Fetcher({(1, 5): [pk(1)], (2, 0): RuntimeError("down")})
    ks = KeySet(cfg, fetch)
    assert not ks.all_sources_loaded()
    ks.refresh()
    assert not ks.all_sources_loaded()
    fetch.data[(2, 0)] = []
    ks.refresh()
    assert ks.all_sources_loaded()
    fetch.data[(1, 5)] = RuntimeError("down")
    ks.refresh()
    assert ks.all_sources_loaded()
    assert KeySet(make_cfg([{"name": "s", "static_pubkeys": [{"pubkey": pk(3)}]}]), fetch).all_sources_loaded()
