"""Entry point: python -m src --config config.yaml"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
from typing import Optional

from prometheus_client import start_http_server

from . import __version__
from .app import Exporter
from .clients.cl import BeaconClient
from .clients.el import ExecutionClient
from .clients.keys_api import KeysApiClient
from .config import DEFAULT_CONFIG_PATH, load_config
from .keyset import KeySet
from .metrics import Metrics
from .store import Store

log = logging.getLogger("src")


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(prog="lido-keys-exporter", description=__doc__)
    parser.add_argument("--config", default=os.environ.get("LKE_CONFIG") or DEFAULT_CONFIG_PATH)
    parser.add_argument("--version", action="version", version=__version__)
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    logging.basicConfig(level=cfg.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log.info("lido-keys-exporter %s starting", __version__)

    store = Store(cfg.data_dir / "state.sqlite3")
    metrics = Metrics()
    metrics.build_info.labels(version=__version__).set(1)
    keys_api = KeysApiClient(timeout=cfg.http_timeout_seconds)
    el = ExecutionClient(cfg.execution_endpoints, timeout=cfg.http_timeout_seconds, on_error=lambda: metrics.error("el"))
    cl = BeaconClient(cfg.beacon_endpoints, timeout=cfg.http_timeout_seconds, on_error=lambda: metrics.error("cl"))
    keyset = KeySet(cfg, fetch=keys_api.fetch, on_error=lambda: metrics.error("keys_api"))
    metrics.register_state(store, keyset)
    start_http_server(cfg.listen_port, addr=cfg.listen_host, registry=metrics.registry)
    log.info("metrics listening on %s", cfg.listen)

    stop = threading.Event()

    def _stop(signum: int, _frame: object) -> None:
        log.info("received signal %d, stopping", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    try:
        Exporter(cfg, el, cl, keyset, store, metrics).run(stop)
    finally:
        for closable in (el, cl, keys_api, store):
            try:
                closable.close()
            except Exception:
                log.exception("close failed")
    log.info("stopped")


if __name__ == "__main__":
    main()
