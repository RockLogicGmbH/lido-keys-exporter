# lido-keys-exporter

Prometheus exporter that watches a set of Lido validator keys on Ethereum mainnet and exposes metrics for:

1. **Deposits** of the monitored keys to the beacon deposit contract (`DepositEvent` logs), including a withdrawal credentials check and separate counting of top-ups.
2. **Lido exit requests** for the monitored keys (`ValidatorExitRequest` logs of the ValidatorsExitBusOracle). A request stays open until the validator is exiting on the beacon chain.
3. **EIP-7002 triggered withdrawals** for the monitored keys (full exits with amount 0 and partial withdrawals), read from the execution layer logs of the withdrawal request predeploy `0x00000961Ef480Eb55e80D19ad83579A64c007002`.

The exporter only produces metrics. Alerting is done by Grafana (rules in this repo) or any other Prometheus consumer. It only talks to your own nodes: execution JSON-RPC endpoints, beacon node HTTP API endpoints and the Lido Keys API.

## Metrics

Group labels on key metrics: `set`, `origin` (`keys_api` or `static`), `module_id`, `operator_id`. Per-key series also carry `pubkey` and `validator_index` (empty if the index is not known yet). `kind` on deposit metrics is `initial` or `topup`.

| Metric | Type | Extra labels | Description |
| --- | --- | --- | --- |
| `lido_keys_monitored` | gauge | group | Used keys in the key set |
| `lido_keys_deposited` | gauge | group | Keys with a stored deposit or found on the beacon chain |
| `lido_keys_deposit_events_total` | counter | group, `kind` | Stored deposit events (both kinds always present) |
| `lido_keys_deposit_eth_total` | counter | group, `kind` | Deposited ETH of the stored deposit events |
| `lido_keys_deposit_events_24h` | gauge | group, `kind` | Deposit events with a block time in the last 24 hours |
| `lido_keys_deposit_eth_24h` | gauge | group, `kind` | Deposited ETH in the last 24 hours |
| `lido_keys_deposit_timestamp_seconds` | gauge | group, pubkey, validator_index | Block time of the initial deposit (only keys with a stored initial deposit) |
| `lido_keys_deposit_initial_eth` | gauge | group, pubkey, validator_index, `credentials` | Amount of the initial deposit (`credentials` = `0x01`, `0x02`, ...) |
| `lido_keys_deposit_credentials_mismatch` | gauge | group, pubkey, validator_index | 1 if the initial deposit did not point to the expected withdrawal vault |
| `lido_keys_topups` | gauge | group, pubkey, validator_index | Stored top-ups per key (only keys with top-ups) |
| `lido_keys_topup_eth` | gauge | group, pubkey, validator_index | ETH of those top-ups |
| `lido_keys_topup_last_timestamp_seconds` | gauge | group, pubkey, validator_index | Block time of the last top-up |
| `lido_keys_exit_requests_total` | counter | group | Stored Lido exit requests for the monitored keys |
| `lido_keys_exit_requests_24h` | gauge | group | Exit requests with a block time in the last 24 hours |
| `lido_keys_exit_request_open` | gauge | group, pubkey, validator_index | Request time of the oldest open exit request, exported once the validator has been looked up on the beacon chain (at most one epoch after the request is seen); the series disappears once the validator is exiting |
| `lido_keys_exit_requests_open` | gauge | group | Keys with an open exit request |
| `lido_keys_triggered_withdrawals_total` | counter | group, `kind`, `source` | Stored EIP-7002 requests (`kind` = `exit` or `partial`) |
| `lido_keys_triggered_withdrawal_gwei_total` | counter | group, `source` | Requested amount of partial withdrawals in gwei |
| `lido_keys_triggered_withdrawals_24h` | gauge | group, `kind`, `source` | EIP-7002 requests with a block time in the last 24 hours |
| `lido_keys_triggered_withdrawal_last_timestamp_seconds` | gauge | group, pubkey, validator_index, `kind`, `source` | Block time of the last EIP-7002 request per key |
| `lido_keys_last_processed_block` | gauge | | Lowest processed block over all log streams |
| `lido_keys_last_processed_block_timestamp` | gauge | | Timestamp of that block |
| `lido_keys_last_processed_slot` | gauge | | Slot of that block |
| `lido_keys_stream_last_processed_block` | gauge | `stream` | Processed block per stream (`deposits`, `exits`, `triggered`) |
| `lido_keys_keyset_last_refresh_timestamp_seconds` | gauge | | Last fully successful key set refresh |
| `lido_keys_endpoint_up` | gauge | `kind`, `endpoint` | 1 if the endpoint is used (reachable, not syncing, not lagging) |
| `lido_keys_endpoint_syncing` | gauge | `kind`, `endpoint` | 1 if the endpoint reports syncing |
| `lido_keys_endpoint_head` | gauge | `kind`, `endpoint` | Head block (`el`) or head slot (`cl`), when known |
| `lido_keys_endpoint_lag` | gauge | `kind`, `endpoint` | Blocks / slots behind the best endpoint of the same kind, when known |
| `lido_keys_endpoint_errors_total` | counter | `kind`, `endpoint`, `reason` | Errors per endpoint; `reason` = `unreachable`, `http_error`, `rpc_error`, `range_limit`, `not_found`, `syncing`, `lagging` (all present from the start) |
| `lido_keys_errors_total` | counter | `component` | Errors by `keys_api`, `el`, `cl`, `store` |
| `lido_keys_up` | gauge | | 1 if at least one endpoint of each kind is healthy, contract addresses are resolved and the last iteration succeeded |
| `lido_keys_build_info` | gauge | `version` | Always 1 |

The `source` label is the name from `known_sources`, `lido-withdrawal-vault` for the Lido withdrawal vault, or the raw address. `kind` on endpoint metrics is `el` (execution) or `cl` (beacon); `endpoint` is `host:port` of the configured URL (scheme, credentials and path are never exported).

Event totals and per-key series are computed from the SQLite store at every scrape, so they survive restarts. The `_total` counters never decrease: when stored totals drop (a deposit reclassified as a top-up, events dropped by a reorg, a key leaving the key set), the drop is added to an offset persisted in the store, because Prometheus would read any decrease as a counter reset and `increase()` would spike. A reclassified deposit therefore stays counted under `initial` and is also counted under `topup`; the gauges (`_24h`, per-key series) show the corrected state. They cover the events seen since the lookback window of the first start; use `increase()` for activity over time (the history found on the first start appears as the initial value, not as an increase). The `_24h` gauges count events whose block time is within the last 24 hours of the exporter clock.

## Configuration

Copy `config.example.yaml` to `config.yaml`; every option is documented there. Main points:

- `execution_endpoints` / `beacon_endpoints`: several endpoints per kind, tried in order; endpoints that are unreachable, report syncing or lag behind are skipped.
- `max_endpoint_lag` (5): blocks (execution) or slots (beacon) an endpoint may be behind the best endpoint of its kind before it is skipped.
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

- Dashboard `lido-keys-exporter` with rows for health, endpoints, key set, deposit events, exit requests and triggered withdrawals, filterable by set, module and operator.
- Alert rules (group `lido-keys-exporter`, evaluated every minute, label `severity`):

  | Rule | Severity |
  | --- | --- |
  | Deposit withdrawal credentials mismatch (initial deposits) | critical |
  | Exit request not handled: open longer than `LKE_EXIT_RESPONSE_HOURS` (2) and up to `LKE_EXIT_WARN_HOURS` | critical |
  | Exit request open longer than `LKE_EXIT_WARN_HOURS` (48) | warning |
  | Exit request open longer than `LKE_EXIT_CRIT_HOURS` (84) | critical |
  | Triggered withdrawal from a source not named `lido*` in the last hour (by block time) | critical |
  | Triggered exit for one of the monitored keys in the last hour (by block time) | warning |
  | Exporter stalled (last processed block older than 5 minutes, for 5m) | critical |
  | Exporter down (`lido_keys_up == 0` or no data, for 5m) | critical |
  | Key set not refreshed for 30 minutes | warning |
  | Data source endpoint unhealthy (`lido_keys_endpoint_up == 0` for 5m, names the beacon or execution node) | warning |
  | Data source endpoint lagging more than `LKE_ENDPOINT_LAG` (5) blocks / slots for 5m | warning |
  | All data source endpoints of one kind down for 2m | critical |
  | More than 10 request errors in 15 minutes per endpoint and reason (`rpc_error`; `range_limit` and `not_found` are handled, and `unreachable`, `http_error`, `syncing`, `lagging` are counted at every health check and covered by the endpoint rules) | warning |

  Grafana does not expand environment variables in provisioned alert rules, so the Grafana entrypoint in `docker-compose.yml` renders `LKE_EXIT_RESPONSE_HOURS`, `LKE_EXIT_WARN_HOURS`, `LKE_EXIT_CRIT_HOURS` and `LKE_ENDPOINT_LAG` into a copy of the provisioning directory. No contact points or notification policies are provisioned; configure them in Grafana. The colour thresholds of the open exit requests table are fixed at 2h, 48h and 84h.

## Upgrading from 0.1.0

- The in-memory counters `lido_keys_deposits_total` and `lido_keys_topups_total` are replaced by `lido_keys_deposit_events_total{kind}` and `lido_keys_deposit_eth_total{kind}`; `lido_keys_exit_requests_total`, `lido_keys_triggered_withdrawals_total` and `lido_keys_triggered_withdrawal_gwei_total` keep their names but are now computed from the store. Queries using the old deposit counters must be updated.
- The alert rules changed (triggered withdrawal rules are based on the block time of the last request, data source errors are reported per endpoint, new exit response and endpoint rules). Update provisioned rules and the dashboard together with the image, and set `LKE_EXIT_RESPONSE_HOURS` / `LKE_ENDPOINT_LAG` if the defaults do not fit.
- No state migration is needed; the existing SQLite store is used as is (new indexes, the `counter_offsets` table and the `exit_requests.checked_at` column are created on start). Open exit requests from 0.1.0 are exported again after the first beacon lookup.
- `lido_keys_deposit_eth` (per key, initial deposit) is renamed to `lido_keys_deposit_initial_eth`, so its name does not clash with the `lido_keys_deposit_eth_total` counter family in OpenMetrics.

## Release process

Images are built by `.github/workflows/docker.yml` and pushed to GHCR. Pushes to branches do not build images.

1. Bump `version` in `pyproject.toml` and merge to `main`.
2. Tag and push: `git tag vX.Y.Z && git push origin vX.Y.Z`. The workflow fails if the tag does not match the version in `pyproject.toml`. It publishes `X.Y.Z`, `X.Y`, `X` (not for `0.x`) and `latest`.
3. For an edge build from `main`, run the workflow manually (workflow_dispatch) on `main`. It publishes `edge` and `sha-<commit>`. Manual runs on other branches are rejected.

## Design notes

- **Beacon: head state only.** The exporter never requests a historical or non-head state (historical state requests make the beacon node replay state, which can stall it). The beacon client refuses any `/states/` path other than `head` before sending a request, and every beacon request is logged. Validator lookups run at most once per epoch and only for open exit requests or seeding.
- **EIP-7002 from EL logs.** Triggered withdrawals are decoded from the anonymous logs of the withdrawal request predeploy (source address 20 bytes, pubkey 48 bytes, amount 8 bytes big-endian). Amount 0 is a full exit, anything else a partial withdrawal. No beacon state is needed.
- **Deposits and top-ups.** Every deposit contract log inside the deposit lookback is stored, so a key that appears in the Keys API later can still be matched. A second deposit to a key is a top-up. Keys without a deposit in the lookback are looked up once in the beacon head state (`seed_deposited_from_beacon`); if they exist there, a later deposit counts as a top-up. The same lookup also checks keys whose first deposit falls inside the lookback: if the validator became activation eligible before that deposit, the deposit is reclassified as a top-up in the stored state (the store-based totals follow). The credentials check compares the address part, accepts type `0x01` and `0x02` and is only done for initial deposits (the beacon chain ignores the credentials of top-ups); reclassified deposits lose their mismatch flag.
- **Contract addresses.** The ValidatorsExitBusOracle and, unless configured, the withdrawal vault are resolved from the LidoLocator `0xC1d0b3DE6792Bf6b4b37EccdcC24e45978Cfd2Eb` at start and on each key set refresh.
- **Triggered withdrawals and the key set.** EIP-7002 logs are not stored for later matching, so the triggered stream is not scanned until every Keys API source has loaded successfully at least once since the process started.
- **Restarts.** Log processing continues from the stored cursor per stream. If the gap is larger than the lookback, processing resumes at the lookback limit and `lido_keys_errors_total{component="el"}` increases.
- **Reorgs.** Only blocks up to `head - confirmations` are processed. When a stored cursor block is no longer canonical, the cursors move back `reorg_rewind_blocks` blocks and that range is scanned again. Only events whose block is no longer canonical are dropped, so canonical events are not counted twice and closed exit requests stay closed. A log chunk is committed only if its logs and the chunk end block agree with the canonical headers.
- **Endpoint health.** Before each iteration every endpoint is checked (`eth_syncing` + `eth_blockNumber` as one batch, `/eth/v1/node/syncing` on beacon nodes). An endpoint is skipped while it is unreachable, reports syncing or is more than `max_endpoint_lag` blocks / slots behind the best endpoint of its kind; a node that claims to be synced but is behind is therefore not used first. If no endpoint of a kind is healthy, all are tried. A block that one execution node does not know yet (`not_found`) is requested from the next one.
- **Log ranges** are requested in chunks of `log_chunk_blocks`. When a node rejects a range as too large (e.g. Nethermind's default 1000 block limit), the request is split on the same endpoint instead of failing over, and the learned maximum range is used for later requests to that endpoint. Result count limits (`query returned more than ...`) only split the request at hand, and rate limit messages (`rate limit exceeded`) are ordinary errors that fail over. The chunk size is still halved as a fallback for other range errors.
- **Log completeness.** Nodes may return the logs up to their own head for a range that goes beyond it. Before `eth_getLogs` is sent to an execution node that is known to be behind the end of the range (its last reported head, or a block it answered with null), the end block is requested from that node; if it does not have it, the next node is used.
- **Exit requests from history.** Requests found in the lookback are stored as open and exported only after the validator status was checked on the beacon chain, so requests handled long ago do not raise exit alerts on a first start.

## License

MIT, see [LICENSE](LICENSE).
