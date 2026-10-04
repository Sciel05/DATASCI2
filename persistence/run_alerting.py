import argparse
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Set

# Add project root to sys.path so `persistence` package is importable
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from persistence.alerting.config import CONFIG, AlertConfig
from persistence.alerting.alert_engine import Alert, AlertEngine
from persistence.alerting.notifier import Notifier
from persistence.persistence_client import get_persistence_client

running = True

def handle_shutdown(signum, frame):
    global running
    print("\n[Alert Daemon] Initiating graceful shutdown...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def main():
    parser = argparse.ArgumentParser(description="Real-Time Market Surveillance Alert Daemon")
    parser.add_argument("--live", action="store_true", help="Connect to live Cassandra instead of mock")
    parser.add_argument("--interval", type=float, default=CONFIG.poll_interval_sec, help="Poll interval in seconds")
    parser.add_argument("--lookback", type=int, default=5, help="Lookback window in minutes for polling")
    parser.add_argument("--test-webhook", action="store_true",
                        help="Send one test alert to every configured webhook (DISCORD_WEBHOOK_URL, "
                        "SLACK_WEBHOOK_URL), print the outcome and exit (1 if none is set or any failed)")
    args = parser.parse_args()
    if args.test_webhook:
        sys.exit(test_webhook())

    config = AlertConfig(
        use_mock=not args.live,
        poll_interval_sec=args.interval,
    )

    print("=================================================================")
    print("      REAL-TIME MARKET SURVEILLANCE — ALERTING DAEMON            ")
    print(f"  Mode: {'LIVE CASSANDRA' if not config.use_mock else 'MOCK SIMULATOR'}")
    print(f"  Poll Cadence: {config.poll_interval_sec}s | Lookback: {args.lookback}m")
    print(f"  Monitored Assets: {', '.join(config.monitored_tickers)}")
    print(f"  Audit Log: {config.alerts_log_path}")
    print("=================================================================")

    client = get_persistence_client(config=config)
    health = client.check_health()
    print(f"Client Health: {health['status']} | Backend: {health['backend']}")

    engine = AlertEngine(config=config)
    notifier = Notifier(config=config)

    seen_events: Set[str] = set()
    total_processed = 0
    total_alerts = 0
    last_heartbeat = time.time()

    while running:
        t_loop_start = time.time()
        try:
            df = client.fetch_recent_anomalies(lookback_minutes=args.lookback, limit=100)
            new_events = []

            for _, row in df.iterrows():
                # Refined Objective 2 can emit more than one tick for the same
                # ticker/millisecond. Its deterministic tick_id prevents both
                # accidental drops here and duplicate alerts after retries.
                event_key = row.get("tick_id")
                if not event_key or str(event_key) == "nan":
                    event_key = f"{row['ticker']}:{row['event_time']}"
                event_key = str(event_key)
                if event_key not in seen_events:
                    seen_events.add(event_key)
                    new_events.append(row.to_dict())

            # Maintain seen_events set bound
            if len(seen_events) > 5000:
                seen_events.clear()

            # Process new anomalies in chronological order
            new_events.reverse()
            for evt in new_events:
                total_processed += 1
                alert = engine.process_event(evt)
                if alert:
                    total_alerts += 1
                    notifier.notify(alert)

            # Flush expired bursts
            expired = engine.flush_expired_bursts(datetime.now(timezone.utc))

            # Periodic status heartbeat
            if time.time() - last_heartbeat >= 30.0:
                print(f"[Heartbeat] Active. Cumulative anomalies: {total_processed}, Incidents alerted: {total_alerts}")
                last_heartbeat = time.time()

        except Exception as e:
            print(f"[Error] Polling failed: {e}", file=sys.stderr)

        elapsed = time.time() - t_loop_start
        sleep_time = max(0.1, config.poll_interval_sec - elapsed)
        time.sleep(sleep_time)

    print("[Alert Daemon] Stopped. Total incidents alerted:", total_alerts)

def test_webhook() -> int:
    """One clearly labeled test alert through the real Notifier, so a webhook
    URL can be checked before a demo without waiting for a real anomaly."""
    notifier = Notifier(config=CONFIG)
    now = datetime.now(timezone.utc)
    alert = Alert(
        alert_id="webhook-test", ticker="TEST", severity="WARNING", anomaly_type="webhook_test",
        start_time=now, end_time=now, peak_price=0.0, peak_zscore=0.0, peak_vwap_div=0.0,
        total_volume=0.0, tick_count=0, signals="test",
        message="[TEST] Market surveillance webhook check: if you can read this, alerts will arrive here.",
    )
    deliveries = notifier.notify(alert)
    if not deliveries:
        print("No webhook configured: set DISCORD_WEBHOOK_URL and/or SLACK_WEBHOOK_URL "
              "(Docker stack: stream-processing/docker/.env, see .env.example).")
        return 1
    return 0 if all(v.startswith("ok") for v in deliveries.values()) else 1


if __name__ == "__main__":
    main()
