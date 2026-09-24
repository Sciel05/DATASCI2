import os
from dataclasses import dataclass

@dataclass(frozen=True)
class AlertConfig:
    # Statistical thresholds calibrated against labeled anomalies in the project dataset.
    # Frank's detection flags zscore > 3 as price shock; >= 4.5 represents extreme deviation.
    zscore_critical: float = 4.5
    zscore_warning: float = 3.0
    vwap_critical: float = 0.05
    vwap_warning: float = 0.025

    # Burst aggregation and cooldown intervals.
    # burst_window_sec groups rapid consecutive anomaly ticks on the same ticker into one incident.
    # cooldown_sec prevents repeated alerting for the same ticker after the incident subsides.
    burst_window_sec: float = 10.0
    cooldown_sec: float = 60.0

    # Cassandra connection parameters.
    cassandra_host: str = os.getenv("CASSANDRA_HOST", "127.0.0.1")
    cassandra_port: int = int(os.getenv("CASSANDRA_PORT", "9042"))
    cassandra_keyspace: str = "surveillance"

    # Runtime mode toggle. True uses mathematical replay from CSV; False connects to live Cassandra.
    use_mock: bool = os.getenv("USE_MOCK", "True").lower() in ("true", "1")
    mock_csv_path: str = os.path.join(
        os.path.dirname(__file__),
        "../../data-ingestion/aapl_msft_googl_tsla_nvda_ticks_labeled.csv"
    )

    # Polling cadence and UI history window.
    poll_interval_sec: float = 3.0
    dashboard_lookback_minutes: int = 15

    # Monitored ticker universe.
    monitored_tickers: tuple = ("AAPL", "MSFT", "GOOGL", "TSLA", "NVDA")

    # Notification endpoints and destinations.
    discord_webhook_url: str = os.getenv("DISCORD_WEBHOOK_URL", "")
    slack_webhook_url: str = os.getenv("SLACK_WEBHOOK_URL", "")
    alerts_log_path: str = os.path.join(os.path.dirname(__file__), "../alerts.log")

CONFIG = AlertConfig()
