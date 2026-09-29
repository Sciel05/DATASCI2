import os
import sys
import unittest
from datetime import datetime, timezone


_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from persistence.cassandra_sink import ANOMALY_COLUMNS, prepare_detector_output


class TestObjective2PersistenceContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("Objective2PersistenceContractTest")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_refined_fields_survive_sink_normalization(self):
        now = datetime.now(timezone.utc)
        fields = [
            "ticker", "timestamp", "price", "volume", "zscore", "vwap_divergence",
            "is_anomaly", "anomaly_type", "ewma_divergence", "cusum_price",
            "cusum_volume", "risk_score", "signals", "incident_id", "tick_id",
        ]
        row = (
            "NVDA", now, 220.5, 9000.0, 5.5, 0.012, True, "price_shock",
            3.1, 6.2, 0.0, 1.1, "zscore,vwap", "NVDA-1000", "NVDA-1000-0",
        )
        normalized = prepare_detector_output(self.spark.createDataFrame([row], fields))
        result = normalized.select(*ANOMALY_COLUMNS).first().asDict()

        self.assertEqual(result["tick_id"], "NVDA-1000-0")
        self.assertEqual(result["incident_id"], "NVDA-1000")
        self.assertEqual(result["signals"], "zscore,vwap")
        self.assertAlmostEqual(result["risk_score"], 1.1)

    def test_original_eight_field_contract_remains_accepted(self):
        now = datetime.now(timezone.utc)
        fields = [
            "ticker", "timestamp", "price", "volume", "zscore",
            "vwap_divergence", "is_anomaly", "anomaly_type",
        ]
        row = ("AAPL", now, 180.0, 500.0, 4.2, 0.03, True, "price_shock")
        result = prepare_detector_output(
            self.spark.createDataFrame([row], fields)
        ).select(*ANOMALY_COLUMNS).first().asDict()

        self.assertTrue(result["tick_id"].startswith("AAPL-"))
        self.assertIsNone(result["risk_score"])
        self.assertIsNone(result["signals"])
        self.assertIsNone(result["incident_id"])


if __name__ == "__main__":
    unittest.main()
