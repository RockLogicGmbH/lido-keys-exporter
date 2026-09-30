# lido-keys-exporter

Prometheus exporter that watches a set of Lido validator keys on Ethereum mainnet and exposes metrics for:

1. **Deposits** of the monitored keys to the beacon deposit contract (`DepositEvent` logs), including a withdrawal credentials check and separate counting of top-ups.
2. **Lido exit requests** for the monitored keys (`ValidatorExitRequest` logs of the ValidatorsExitBusOracle). A request stays open until the validator is exiting on the beacon chain.
3. **EIP-7002 triggered withdrawals** for the monitored keys (full exits with amount 0 and partial withdrawals), read from the execution layer logs of the withdrawal request predeploy `0x00000961Ef480Eb55e80D19ad83579A64c007002`.

The exporter only produces metrics. Alerting is done by Grafana (rules in this repo) or any other Prometheus consumer. It only talks to your own nodes: execution JSON-RPC endpoints, beacon node HTTP API endpoints and the Lido Keys API.

## Metrics

Group labels on key metrics: `set`, `origin` (`keys_api` or `static`), `module_id`, `operator_id`. Per-key series also carry `pubkey` and `validator_index` (empty if the index is not known yet).

| Metric | Type | Extra labels | Description |
| --- | --- | --- | --- |
| `lido_keys_monitored` | gauge | group | Keys in the key set |
| `lido_keys_deposited` | gauge | group | Keys with a seen deposit or found on the beacon chain |
| `lido_keys_deposits_total` | counter | group, `credentials` | Initial deposits of the monitored keys (`credentials` = `0x01`, `0x02`, ...) |
| `lido_keys_topups_total` | counter | group | Deposits to an already deposited key |
| `lido_keys_deposit_timestamp_seconds` | gauge | group, pubkey, validator_index | Block time of the initial deposit |
| `lido_keys_deposit_credentials_mismatch` | gauge | group, pubkey, validator_index | 1 if a deposit did not point to the expected withdrawal vault |
| `lido_keys_exit_requests_total` | counter | group | Lido exit requests for the monitored keys |
| `lido_keys_exit_request_open` | gauge | group, pubkey, validator_index | Request time of the oldest open exit request; the series disappears once the validator is exiting |
| `lido_keys_exit_requests_open` | gauge | group | Keys with an open exit request |
| `lido_keys_triggered_withdrawals_total` | counter | group, `kind`, `source` | EIP-7002 requests (`kind` = `exit` or `partial`) |
| `lido_keys_triggered_withdrawal_gwei_total` | counter | group, `source` | Requested amount of partial withdrawals in gwei |
| `lido_keys_triggered_withdrawal_last_timestamp_seconds` | gauge | group, pubkey, validator_index, `kind` | Block time of the last EIP-7002 request per key |
| `lido_keys_last_processed_block` | gauge | | Lowest processed block over all log streams |
| `lido_keys_last_processed_block_timestamp` | gauge | | Timestamp of that block |
| `lido_keys_last_processed_slot` | gauge | | Slot of that block |
| `lido_keys_stream_last_processed_block` | gauge | `stream` | Processed block per stream (`deposits`, `exits`, `triggered`) |
| `lido_keys_keyset_last_refresh_timestamp_seconds` | gauge | | Last fully successful key set refresh |
| `lido_keys_errors_total` | counter | `component` | Errors by `keys_api`, `el`, `cl`, `store` |
| `lido_keys_up` | gauge | | 1 if endpoints are healthy, contract addresses are resolved and the last iteration succeeded |
| `lido_keys_build_info` | gauge | `version` | Always 1 |

The `source` label is the name from `known_sources`, `lido-withdrawal-vault` for the Lido withdrawal vault, or the raw address. Counters start from zero when the process restarts (normal Prometheus behaviour, use `increase()`); gauges are rebuilt from the persisted state. The shipped dashboard and alert queries use `increase(x[w]) > 0 or (x unless x offset w)` so the first event of a new series is not missed.

## Configuration

Copy `config.example.yaml` to `config.yaml`; every option is documented there. Main points:

- `execution_endpoints` / `beacon_endpoints`: several endpoints per kind, tried in order; endpoints that report syncing are skipped.
- `sets`: named key sets. Each set combines Keys API sources (`url`, `module_id`, `operator_id`; read via `GET {url}/v1/modules/{module_id}/keys?used=true&operatorIndex={operator_id}`) and optional static pubkeys with a label. The key set is refreshed every `keyset_refresh_minutes`; if a source fails, its last loaded keys stay in use.
- `expected_withdrawal_credentials`: vault address or full credentials. When unset, the Lido withdrawal vault is resolved from the LidoLocator.
- `known_sources`: names for EIP-7002 request source addresses. Names starting with `lido` are treated as expected by the alert rules.
- Lookbacks on the first start: `exit_lookback_days` (14), `deposit_lookback_days` (7), `triggered_lookback_days` (7).

Environment overrides: `LKE_CONFIG` (config path, default `/opt/app/config.yaml`), `LKE_EXECUTION_ENDPOINTS`, `LKE_BEACON_ENDPOINTS` (comma separated), `LKE_LISTEN`, `LKE_DATA_DIR`, `LKE_LOG_LEVEL`.

State is kept in SQLite at `{data_dir}/state.sqlite3`.

## Running locally

```sh
poetry install
cp config.example.yaml config.yaml   # adjust endpoints and key sets
# data_dir defaults to /opt/app/data; use a local directory instead
LKE_DATA_DIR=./data poetry run python -m src --config config.yaml
curl -s localhost:9800/metrics | grep lido_keys_
```

Run the tests with `poetry run pytest -q`.

## Docker compose

Copy `.env.example` to `.env` and `config.example.yaml` to `config.yaml` first. The `.env` file is optional for the app (Compose 2.24 or newer is needed for the optional `env_file`).

- Exporter only (to be scraped by an existing Prometheus):

  ```sh
  docker compose -f docker-compose.app.yml up -d
  ```

- Full stack with Prometheus and Grafana (includes `docker-compose.app.yml`):

  ```sh
  docker compose up -d
  ```

  Grafana listens on `${GRAFANA_PORT:-3000}` (login from `GF_SECURITY_ADMIN_USER` / `GF_SECURITY_ADMIN_PASSWORD`), Prometheus on `${PROMETHEUS_PORT:-9090}`, the exporter on `${LKE_PORT:-9800}`.

The image is `${LKE_IMAGE:-lido-keys-exporter}:${LKE_IMAGE_TAG:-latest}`; set `LKE_IMAGE` to a published image (the release workflow pushes to `ghcr.io/<owner>/<repo>`) or let `docker compose build` build it locally. The container runs as user `2000:2000` and stores its state in the `lke-data` volume (`/opt/app/data`).

## Grafana

Provisioned automatically in the full stack, in the folder **Lido Keys Exporter**:

- Dashboard `lido-keys-exporter` with rows for health, key set, deposits, exit requests and triggered withdrawals, filterable by set, module and operator.
- Alert rules (group `lido-keys-exporter`, evaluated every minute, label `severity`):

  | Rule | Severity |
  | --- | --- |
  | Deposit withdrawal credentials mismatch | critical |
  | Exit request open longer than `LKE_EXIT_WARN_HOURS` (48) | warning |
  | Exit request open longer than `LKE_EXIT_CRIT_HOURS` (84) | critical |
  | Triggered withdrawal from a source not named `lido*` | critical |
  | Triggered exit for one of the monitored keys | warning |
  | Exporter stalled (last processed block older than 5 minutes, for 5m) | critical |
  | Exporter down (`lido_keys_up == 0` or no data, for 5m) | critical |
  | Key set not refreshed for 30 minutes | warning |
  | More than 10 data source errors in 15 minutes per component | warning |

  Grafana does not expand environment variables in provisioned alert rules, so the Grafana entrypoint in `docker-compose.yml` renders `LKE_EXIT_WARN_HOURS` / `LKE_EXIT_CRIT_HOURS` into a copy of the provisioning directory. No contact points or notification policies are provisioned; configure them in Grafana. The colour thresholds of the open exit requests table are fixed at 48h and 84h.

## Release process

Images are built by `.github/workflows/docker.yml` and pushed to GHCR. Pushes to branches do not build images.

1. Bump `version` in `pyproject.toml` and merge to `main`.
2. Tag and push: `git tag vX.Y.Z && git push origin vX.Y.Z`. The workflow fails if the tag does not match the version in `pyproject.toml`. It publishes `X.Y.Z`, `X.Y`, `X` (not for `0.x`) and `latest`.
3. For an edge build from `main`, run the workflow manually (workflow_dispatch) on `main`. It publishes `edge` and `sha-<commit>`. Manual runs on other branches are rejected.

## Design notes

- **Beacon: head state only.** The exporter never requests a historical or non-head state (historical state requests make the beacon node replay state, which can stall it). The beacon client refuses any `/states/` path other than `head` before sending a request, and every beacon request is logged. Validator lookups run at most once per epoch and only for open exit requests or seeding.
- **EIP-7002 from EL logs.** Triggered withdrawals are decoded from the anonymous logs of the withdrawal request predeploy (source address 20 bytes, pubkey 48 bytes, amount 8 bytes big-endian). Amount 0 is a full exit, anything else a partial withdrawal. No beacon state is needed.
- **Deposits and top-ups.** Every deposit contract log inside the deposit lookback is stored, so a key that appears in the Keys API later can still be matched. A second deposit to a key is a top-up. Keys without a deposit in the lookback are looked up once in the beacon head state (`seed_deposited_from_beacon`); if they exist there, a later deposit counts as a top-up. The same lookup also checks keys whose first deposit falls inside the lookback: if the validator became activation eligible before that deposit, the deposit is reclassified as a top-up in the stored state (counters already emitted are not changed). The credentials check compares the address part and accepts type `0x01` and `0x02`.
- **Contract addresses.** The ValidatorsExitBusOracle and, unless configured, the withdrawal vault are resolved from the LidoLocator `0xC1d0b3DE6792Bf6b4b37EccdcC24e45978Cfd2Eb` at start and on each key set refresh.
- **Triggered withdrawals and the key set.** EIP-7002 logs are not stored for later matching, so the triggered stream is not scanned until every Keys API source has loaded successfully at least once since the process started.
- **Restarts.** Log processing continues from the stored cursor per stream. If the gap is larger than the lookback, processing resumes at the lookback limit and `lido_keys_errors_total{component="el"}` increases.
- **Reorgs.** Only blocks up to `head - confirmations` are processed. When a stored cursor block is no longer canonical, the cursors move back `reorg_rewind_blocks` blocks and that range is scanned again. Only events whose block is no longer canonical are dropped, so canonical events are not counted twice and closed exit requests stay closed. A log chunk is committed only if its logs and the chunk end block agree with the canonical headers.
- **Log ranges** are requested in chunks of `log_chunk_blocks`; the chunk size is halved when a node rejects a range.

## License

MIT, see [LICENSE](LICENSE).
