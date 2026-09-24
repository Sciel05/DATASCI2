import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

# Add project root to sys.path so `persistence` package is importable
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from persistence.alerting.config import AlertConfig
from persistence.alerting.alert_engine import Alert, AlertEngine
from persistence.alerting.notifier import Notifier
from persistence.persistence_client import MockReplayClient

class TestAlertingPipeline(unittest.TestCase):
    def setUp(self):
        self.config = AlertConfig(
            use_mock=True,
            zscore_critical=4.5,
            zscore_warning=3.0,
            vwap_critical=0.05,
            vwap_warning=0.025,
            burst_window_sec=5.0,
            cooldown_sec=15.0,
            alerts_log_path=os.path.join(os.path.dirname(__file__), "test_alerts.log"),
        )
        self.engine = AlertEngine(config=self.config)
        self.notifier = Notifier(config=self.config)

    def tearDown(self):
        if os.path.exists(self.config.alerts_log_path):
            try:
                os.remove(self.config.alerts_log_path)
            except OSError:
                pass

    def test_severity_classification(self):
        self.assertEqual(self.engine.classify_severity(5.2, 0.01, "price_shock"), "CRITICAL")
        self.assertEqual(self.engine.classify_severity(2.0, 0.06, "price_shock"), "CRITICAL")
        self.assertEqual(self.engine.classify_severity(3.5, 0.01, "price_shock"), "WARNING")
        self.assertEqual(self.engine.classify_severity(1.0, 0.03, "price_shock"), "WARNING")
        self.assertEqual(self.engine.classify_severity(1.0, 0.01, "wash_trade"), "WARNING")
        self.assertEqual(self.engine.classify_severity(1.0, 0.01, "normal"), "INFO")

    def test_burst_debouncing_and_escalation(self):
        now = datetime.now(timezone.utc)

        # 1st tick: Warning level price shock
        evt1 = {
            "ticker": "AAPL",
            "anomaly_type": "price_shock",
            "price": 180.0,
            "volume": 5000.0,
            "zscore": 3.2,
            "vwap_divergence": 0.02,
            "event_time": now,
        }
        alert1 = self.engine.process_event(evt1)
        self.assertIsNotNone(alert1)
        self.assertEqual(alert1.severity, "WARNING")
        self.assertEqual(alert1.tick_count, 1)

        # 2nd tick: 2 seconds later (within 5s burst window), same warning severity -> debounced (None)
        evt2 = {
            "ticker": "AAPL",
            "anomaly_type": "price_shock",
            "price": 180.5,
            "volume": 6000.0,
            "zscore": 3.4,
            "vwap_divergence": 0.021,
            "event_time": now + timedelta(seconds=2),
        }
        alert2 = self.engine.process_event(evt2)
        self.assertIsNone(alert2, "Consecutive tick within burst window must be debounced")

        # 3rd tick: 4 seconds later (still within burst window), escalates to CRITICAL
        evt3 = {
            "ticker": "AAPL",
            "anomaly_type": "price_shock",
            "price": 182.0,
            "volume": 12000.0,
            "zscore": 4.8,
            "vwap_divergence": 0.055,
            "event_time": now + timedelta(seconds=4),
        }
        alert3 = self.engine.process_event(evt3)
        self.assertIsNotNone(alert3, "Severity escalation must trigger an updated alert")
        self.assertEqual(alert3.severity, "CRITICAL")
        self.assertEqual(alert3.tick_count, 3)
        self.assertEqual(alert3.peak_zscore, 4.8)

    def test_cooldown_enforcement(self):
        now = datetime.now(timezone.utc)

        # Initial alert
        evt1 = {
            "ticker": "TSLA",
            "anomaly_type": "wash_trade",
            "price": 240.0,
            "volume": 50000.0,
            "zscore": 1.5,
            "vwap_divergence": 0.005,
            "event_time": now,
        }
        alert1 = self.engine.process_event(evt1)
        self.assertIsNotNone(alert1)

        # Flush burst after 6 seconds (exceeding 5s burst window)
        self.engine.flush_expired_bursts(current_time=now + timedelta(seconds=6))

        # Event at 10s: outside burst window, but inside 15s cooldown -> suppressed
        evt2 = {
            "ticker": "TSLA",
            "anomaly_type": "wash_trade",
            "price": 240.2,
            "volume": 30000.0,
            "zscore": 1.6,
            "vwap_divergence": 0.006,
            "event_time": now + timedelta(seconds=10),
        }
        alert2 = self.engine.process_event(evt2)
        self.assertIsNone(alert2, "Events within cooldown period must be suppressed")

        # Event at 25s: cooldown (15s) has expired -> new alert permitted
        evt3 = {
            "ticker": "TSLA",
            "anomaly_type": "wash_trade",
            "price": 241.0,
            "volume": 45000.0,
            "zscore": 1.7,
            "vwap_divergence": 0.007,
            "event_time": now + timedelta(seconds=25),
        }
        alert3 = self.engine.process_event(evt3)
        self.assertIsNotNone(alert3, "Events after cooldown expiry must trigger a new alert incident")

    def test_mock_client_math_grounding(self):
        client = MockReplayClient(config=self.config)
        health = client.check_health()
        self.assertEqual(health["status"], "healthy")
        self.assertEqual(health["dataset_anomalies_loaded"], 1176)

        df = client.fetch_recent_anomalies(lookback_minutes=15, limit=50)
        self.assertFalse(df.empty)
        self.assertTrue(set(["ticker", "event_time", "price", "volume", "zscore", "vwap_divergence", "anomaly_type"]).issubset(df.columns))

    def test_notifier_audit_logging(self):
        alert = Alert(
            alert_id="test1234",
            ticker="NVDA",
            severity="CRITICAL",
            anomaly_type="price_shock",
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            peak_price=125.50,
            peak_zscore=5.10,
            peak_vwap_div=0.065,
            total_volume=45000.0,
            tick_count=4,
            message="[CRITICAL] NVDA test alert",
        )
        self.notifier.notify(alert)
        self.assertTrue(os.path.exists(self.config.alerts_log_path))
        with open(self.config.alerts_log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 1)
        self.assertIn("NVDA", lines[0])
        self.assertIn("CRITICAL", lines[0])

if __name__ == "__main__":
    unittest.main()
