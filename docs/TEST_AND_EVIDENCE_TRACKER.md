# Market Surveillance Pipeline — Test & Evidence Tracker

> **Screenshot Instructions:**  
> Paste screenshots directly into this doc (drag and drop) right under the matching test.  
> Name raw files as `T<number>_<what>.png` (example: `T6_kafka_ordering.png`) so the slides team can find them.

---

## Latest Updates (Pushed to `main` at `617aae5`)

Five commits landed: 3-broker Kafka, price-shock tuning, Cassandra `(ticker, day)` partitions, the live Kafka order proof, and working notifications. **Everyone must reset and rebuild before running anything.**

| Commit | What Changed |
| :--- | :--- |
| `193d78c` | Optional 3-broker Kafka stack, tested with a broker stopped mid-run |
| `ef18f06` | Price-shock tuning |
| `8887bd1` | Cassandra tables partitioned by `(ticker, day)` |
| `b97b791` | `verify_consumer.py --watch`, the live order proof |
| `617aae5` | Alerting service, checked webhooks, dashboard notification line |

---

## Results for the Slide

> **Note:** No data was lost and wash-trade detection is strong; price-shock detection is weak and should be stated honestly.

| Metric | Result | What It Means |
| :--- | :--- | :--- |
| **Ticks stored** | 140,751 / 140,751 | No data lost |
| **Wash trade precision / recall** | 0.94 / 0.99 | Almost every alert is real, almost none missed |
| **Price shock precision / recall** | 0.34 / 0.43 | Most alerts false, more than half missed |
| **Latency p50 / p95** | 1.8 s / 3.0 s | Half of ticks processed in 1.8 s, 95% in 3 s |
| **Throughput into Kafka** | 10.5k msgs/s | Ingestion is not the bottleneck |
| **Throughput through detection + storage** | 1.26k ticks/s | Spark and Cassandra set the real speed |
| **Failed writes with 1 node down, RF=1** | 41% | One copy of data is not enough |
| **Failed writes with 1 node down, RF=3** | 0% | Three nodes plus QUORUM survives a failure |

*These numbers are from before the latest changes. Price-shock precision/recall will change after the tuning (commit `ef18f06`), so re-run test 2 and update this table. Kafka is no longer a single point of failure when running the 3-broker stack.*

---

## Action Items After Pulling

> [!IMPORTANT]
> **Everyone, after pulling:**
> 1. Run schema reset once inside the pyspark container (recreates tables with the new key; wipes local test data):
>    ```bash
>    python -m persistence.schema_tools --reset
>    ```
> 2. Rebuild containers:
>    ```bash
>    docker compose up -d --build
>    ```
> 3. Use both compose files from now on:
>    ```bash
>    docker compose -f stream-processing/docker/docker-compose.yml -f stream-processing/docker/docker-compose.kafka3.yml ...
>    ```

### Per Person Responsibilities:
- **Frank:** New detector defaults (`--cusum-h 2.5`, `--zscore-threshold 8`, `--ewma-threshold 4`); see *"Price-shock tuning"* in his README.
- **Kyle:** `persistence_client.py` now reads by day partition; `app.py` has the fixed wash-trade banner and the notification-status line.
- **Presenter:** Set up the Discord webhook (see [Getting notifications working](#getting-notifications-working)).
- **Must re-run because of these changes:** 
  - **Test 1** (schema changed)
  - **Test 2** (new detector thresholds, new precision/recall numbers)

---

## Status Overview

Four of ten tests have passed; the six gaps are the priority, led by the alerting daemon (test 5) because Obj 3 promises alerting.

| # | Test | Proves | Owner | Status |
| :-: | :--- | :--- | :--- | :-: |
| 1 | Persistence test suite | Sink, schema checks, idempotent writes (re-run: schema changed) | Hector / Kyle | 🟡 In progress |
| 2 | Batch evaluation | No data loss, accuracy, throughput (re-run: new thresholds) | Elijah | 🟡 In progress |
| 3 | Failover drill | Why 3 nodes | Hector / Kyle | 🟢 Passed |
| 4 | Dashboard, live mode | Obj 3 end to end | Hector / Kyle | 🟢 Passed |
| 5 | Alerting daemon + notifications | Obj 3 alerting works (real Discord not yet tested) | Hector / Kyle | 🟡 In progress |
| 6 | Kafka ordering check | Obj 1: per-ticker order kept (passed in dev run) | Agatha | 🟢 Passed |
| 7 | Parity check | Streaming = offline result | Frank | 🟢 Passed |
| 8 | Fresh-clone test | Runs on presenting laptop | Presenter | ⚪ Not started |
| 9 | Mock mode | Backup if Docker fails | Kyle | ⚪ Not started |
| 10 | 15-minute continuous run | Stability, no memory creep | Elijah | ⚪ Not started |

*Note: Owners for tests 1–4 are guesses based on objective areas; correct them if needed.*

---

## Completed Tests (1–4)

*These passed already, but each still needs a clean screenshot for the slides. Re-run on the presenting laptop if the old output is lost.*

### Test 1 — Persistence Test Suite
- **Proves:** Database writer saves correctly, checks data shape, and never duplicates a tick.
- **Command:**
  ```bash
  docker compose -f stream-processing/docker/docker-compose.yml exec pyspark python3 -m pytest persistence -q
  ```
- **Pass condition:** Output ends with `11 passed`.
- **Evidence:** Screenshot of the terminal showing `11 passed`.
- **Screenshot Placeholder:**  
  *(Paste `T1_persistence_tests.png` here)*

---

### Test 2 — Batch Evaluation (~4 min)
- **Proves:** No data loss, detection accuracy, and throughput for Obj 4.
- **Command:**
  ```bash
  docker compose -f stream-processing/docker/docker-compose.yml exec pyspark python3 evaluation/run_evaluation.py batch
  ```
- **Pass condition:** Summary shows `140,751 / 140,751` ticks stored, plus precision, recall, latency, and throughput figures.
- **Evidence:** Screenshot of the full summary block. These numbers feed the results slide.
- **Screenshot Placeholder:**  
  *(Paste `T2_batch_evaluation.png` here)*

---

### Test 3 — Failover Drill (~9 min)
- **Proves:** System keeps working when one Cassandra node dies, justifying 3 nodes.
- **Command:**
  ```bash
  python evaluation/failover_drill.py
  ```
- **Pass condition:** RF=3 shows `0%` failed writes; RF=1 shows about `41%` failed.
- **Evidence:**
  - `nodetool status` with all 3 nodes UN (up) before the drill
  - `nodetool status` with one node DN (down) during the drill
  - Drill output comparing RF=1 vs RF=3
- **Screenshot Placeholders:**  
  *(Paste `T3_nodetool_3_un.png` here)*  
  *(Paste `T3_nodetool_1_dn.png` here)*  
  *(Paste `T3_failover_drill_output.png` here)*

---

### Test 4 — Dashboard, Live Mode
- **Proves:** Obj 3 end-to-end: ticks flow from CSV to the dashboard.
- **Pass condition:** Dashboard updates live, KPI tiles change, and alerts appear.
- **Evidence:** KPI tiles, a critical alert, the 3-pane Plotly view, and the Live Streaming Graph.
- **Screenshot Placeholders:**  
  *(Paste `T4_kpi_tiles.png` here)*  
  *(Paste `T4_critical_alert.png` here)*  
  *(Paste `T4_3pane_plotly.png` here)*  
  *(Paste `T4_live_streaming_graph.png` here)*

---

## Gap Tests (5–10)

*These have never been run. Each one closes a claim the proposal makes, so all six should pass before the presentation.*

### Test 5 — Alerting Daemon (Hector / Kyle)
The alerting daemon now runs as its own service in the Docker stack and alerts on live pipeline data. Remaining work is confirming it on the 3-node cluster and a real Discord channel. Full checklist is in [Getting notifications working](#getting-notifications-working).
- **Command:**
  ```bash
  docker compose -f stream-processing/docker/docker-compose.yml -f stream-processing/docker/docker-compose.kafka3.yml exec alerting python3 persistence/run_alerting.py --test-webhook
  ```
- **Pass condition:** Reads from the cluster, writes to `alerts.log`, and groups a burst into one alert.
- **Evidence:** Terminal output of the daemon, plus `alerts.log` contents.
- **Screenshot Placeholders:**  
  *(Paste `T5_alerting_daemon.png` here)*  
  *(Paste `T5_alerts_log.png` here)*

---

### Test 6 — Kafka Ordering Check (Agatha)
- **Proves:** Obj 1's core claim: keying by ticker keeps each stock's ticks in order.
- **Run this in a terminal next to the producer during the demo:**
  ```bash
  docker compose -f stream-processing/docker/docker-compose.yml -f stream-processing/docker/docker-compose.kafka3.yml exec pyspark python3 data-ingestion/verify_consumer.py --bootstrap-servers kafka:9092 --watch
  ```
  *Refreshes every second. The top line is a green `ORDER HOLDS: 0 violations in N messages` or a red `ORDER VIOLATED`. Below it, one row per partition shows its tickers, leading broker, messages checked, newest event time, and violations.*
- **Pass condition:** Status stays `ORDER HOLDS` and each ticker stays on one partition.  
  *Dev run result: AAPL→4, GOOGL and MSFT→3, NVDA and TSLA→7, leaders spread across b1, b2, b3.*
- **Demo moment:** Stop `kafka3` while it runs. The leader column changes and the status stays `ORDER HOLDS`.
- **Evidence:** Screenshot before stopping `kafka3`, and one after (leader changed, still 0 violations).
  - [ ] Re-run on the presenting laptop after the schema reset
  - [ ] Practice the stop-kafka3 moment once
  - [ ] Ctrl+C now exits cleanly (hang fixed); confirm it does
- **Screenshot Placeholders:**  
  *(Paste `T6_kafka_ordering_before_stop.png` here)*  
  *(Paste `T6_kafka_ordering_after_stop.png` here)*

---

### Test 7 — Parity Check (Frank)
- **Proves:** Streaming output matches an offline computation of the same day, including what lands in Cassandra.
- **Command:**
  ```bash
  python stream-processing/parity_check.py run --day 2026-09-16 --sink cassandra
  ```
- **Pass condition:** Streaming and offline results match (or differ only within a stated tolerance).
- **Evidence:** Parity summary output.
- **Screenshot Placeholder:**  
  *(Paste `T7_parity_summary.png` here)*

---

### Test 8 — Fresh-Clone Test (Presenter, ~5 min)
- **Proves:** Catches anything that only works on one person's machine, and measures cold-start time.
- **Commands:**
  ```bash
  git clone <repo-url> fresh-test
  cd fresh-test
  docker compose up -d --build
  ```
- **Pass condition:** Every container starts and the dashboard loads with no manual fixes.
- **Evidence:** `docker compose ps` showing all services up, and the measured start time.
- **Screenshot Placeholder:**  
  *(Paste `T8_fresh_clone_ps.png` here)*

---

### Test 9 — Mock Mode (Kyle)
- **Proves:** Backup if Docker fails on stage.
- **How:** Set `USE_MOCK=True`, stop Docker, and start the dashboard.
- **Pass condition:** Dashboard loads and shows data with Docker fully off.
- **Evidence:** Screenshot of the mock dashboard.
- **Screenshot Placeholder:**  
  *(Paste `T9_mock_mode_dashboard.png` here)*

---

### Test 10 — 15-Minute Continuous Run (Elijah)
- **Proves:** The proposal promises 10–15 minutes of stable running; only 6 minutes was tested.
- **How:** Run the full live pipeline with the dashboard open for 15 minutes. Record memory every 5 minutes with `docker stats --no-stream`.
- **Pass condition:** No crashes, dashboard keeps updating, and memory stays roughly flat.
- **Evidence:** `docker stats` at 0, 5, 10, and 15 minutes, plus a dashboard screenshot at the 15-minute mark.
- **Screenshot Placeholders:**  
  *(Paste `T10_docker_stats_0m.png` here)*  
  *(Paste `T10_docker_stats_5m.png` here)*  
  *(Paste `T10_docker_stats_10m.png` here)*  
  *(Paste `T10_docker_stats_15m.png` here)*  
  *(Paste `T10_dashboard_15m.png` here)*

---

## Getting Notifications Working

Notifications now work in the stack: critical alerts show a dashboard banner and go to a webhook, and every delivery is checked. The only thing left is connecting a real Discord channel and capturing the evidence.

### What's Built:
- Alerting service in Docker, alerting on live data.
- Each webhook delivery is checked; a bad URL is reported (e.g., `failed: HTTP 404`) instead of failing silently.
- Results go to the daemon output and the audit log.
- Dashboard line under the banner, e.g. `"📣 Last alert dispatched 02:11:49 UTC: … → discord ✓"`.
- Banner fix: critical wash-trade alerts say *"volume far above normal"* instead of the wrong *"price moved Nx"*.
- Tested with a mock webhook receiver: real alerts delivered (204), bad URL reported as failed. Nothing posted to a real Discord yet.

### Step 1 — Prove the Daemon Works (Test 5)
1. Start the 3-node cluster and confirm `nodetool status` shows 3 `UN`.
2. Start a live replay so new anomalies are being written.
3. Run:
   ```bash
   python persistence/run_alerting.py --live
   ```
4. Confirm new lines appear in `alerts.log` within a few seconds of an anomaly.
5. Confirm debouncing: one burst of anomalies on the same ticker produces one grouped alert, not dozens.
6. Pick one alert and match it to its row in `surveillance.anomalies` with `cqlsh`.
7. Stop `cassandra3` and confirm alerts keep coming (`LOCAL_QUORUM` should still work).

### Step 2 — Connect Discord (Presenter, ~2 min)
1. In Discord, open the channel → **Settings** → **Integrations** → **Webhooks** → **New Webhook** → **Copy URL**.
2. Copy `stream-processing/docker/.env.example` to `.env` in the same folder, and paste the URL after `DISCORD_WEBHOOK_URL=`. Git ignores `.env`; never commit the URL.
3. Restart the service and send a test:
   ```bash
   docker compose -f stream-processing/docker/docker-compose.yml -f stream-processing/docker/docker-compose.kafka3.yml up -d alerting
   docker compose -f stream-processing/docker/docker-compose.yml -f stream-processing/docker/docker-compose.kafka3.yml exec alerting python3 persistence/run_alerting.py --test-webhook
   ```
4. **Pass condition:** A `[TEST]` message appears in the channel and the command prints `discord webhook: ok 204`.
   - [ ] Test message received in Discord
   - [ ] A real critical alert arrives during a live run
   - [ ] Dashboard shows the `"Last alert dispatched … → discord ✓"` line
   - [ ] Pull the Wi-Fi once and confirm the daemon keeps running and reports the failure
   - **Evidence:** A critical alert in Discord side-by-side with the same alert on the dashboard banner.
   - *(Paste `T5_discord_side_by_side.png` here)*

---

## Evidence Checklist

> **Tip:** The strongest single visual is the dashboard still updating while `cassandra3` is stopped; get that one first. Paste each screenshot under its item.

### Cluster Health
- [ ] `nodetool status` showing 3 UN nodes
- [ ] `nodetool status` showing one DN node during the failover drill
- [ ] Dashboard still updating while `cassandra3` is stopped

### Dashboard
- [ ] KPI tiles
- [ ] A critical alert on screen
- [ ] 3-pane Plotly view
- [ ] Live Streaming Graph

### Data Proof
- [ ] `cqlsh` query of `surveillance.anomalies` matching an alert on screen
- [ ] Batch evaluation summary in the terminal
- [ ] Kafka ordering check output (test 6)
- [ ] Parity check output (test 7)
- [ ] `docker stats` at 0 and 15 minutes (test 10)

### Notifications
- [ ] Lines in `alerts.log` from the 3-node run
- [ ] Critical alert arriving in the chosen notification channel

### Backup
- [ ] 3–5 minute screen recording of the full live demo
- [ ] Mock-mode dashboard screenshot
