import argparse
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Set, Tuple

# Add project root to sys.path so `persistence` package is importable
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from persistence.alerting.config import CONFIG, AlertConfig
from persistence.alerting.alert_engine import AlertEngine
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
    args = parser.parse_args()

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

    seen_events: Set[Tuple[str, str]] = set()
    total_processed = 0
    total_alerts = 0
    last_heartbeat = time.time()

    while running:
        t_loop_start = time.time()
        try:
            df = client.fetch_recent_anomalies(lookback_minutes=args.lookback, limit=100)
            new_events = []

            for _, row in df.iterrows():
                event_key = (row["ticker"], str(row["event_time"]))
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

if __name__ == "__main__":
    main()
