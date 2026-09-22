# Pipeline Integration & Evaluation (Objective 4)

**Status: not started.** Depends on Objectives 1–3 all being wired
together and runnable end-to-end.

## What this needs to do

Per the proposal's Specific Objective 4 ("Pipeline Integration &
Evaluation"):

- Run the full pipeline end-to-end (`data-ingestion` → `stream-processing`
  → `persistence`) for a continuous stretch (10–15 min) as the demo run.
- **Latency**: timestamp at producer send (`event_time_ms` in the Kafka
  payload) vs. timestamp at Cassandra write. Report min/median/p95.
- **Throughput**: run `data-ingestion/producer.py --max-rate` and watch
  Kafka consumer lag (`kafka-consumer-groups.sh --describe`) to find the
  practical throughput ceiling before lag starts climbing.
- **Detection accuracy**: join Objective 2's output against the `label`
  column in `data-ingestion`'s labeled CSV — report precision/recall for
  `price_shock` and `wash_trade` separately.
- Document explicitly-out-of-scope future work (per the proposal):
  multi-asset correlated ML models, NLP sentiment analysis, automated
  closed-loop order cancellation. Just document why, don't build these.

## Results

TBD — fill in once the pipeline is running end-to-end. This is usually
the section that matters most for grading: actual numbers + screenshots
beat a claim that it "works."
