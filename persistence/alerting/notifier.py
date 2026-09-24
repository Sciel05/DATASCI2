import json
import logging
import os
import threading
from datetime import datetime
from typing import Optional

import requests

from .alert_engine import Alert
from .config import CONFIG, AlertConfig

logger = logging.getLogger("market_surveillance.notifier")

class Notifier:
    def __init__(self, config: AlertConfig = CONFIG):
        self.config = config
        self._ensure_log_dir()

    def _ensure_log_dir(self):
        log_dir = os.path.dirname(self.config.alerts_log_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

    def notify(self, alert: Alert):
        """Dispatch alert across all active notification channels."""
        self._notify_console(alert)
        self._notify_log_file(alert)

        if self.config.discord_webhook_url:
            threading.Thread(target=self._notify_discord, args=(alert,), daemon=True).start()

        if self.config.slack_webhook_url:
            threading.Thread(target=self._notify_slack, args=(alert,), daemon=True).start()

    def _notify_console(self, alert: Alert):
        colors = {
            "CRITICAL": "\033[91m",  # Red
            "WARNING": "\033[93m",   # Yellow
            "INFO": "\033[96m",      # Cyan
        }
        reset = "\033[0m"
        color = colors.get(alert.severity, "")

        ts_str = alert.start_time.strftime("%Y-%m-%d %H:%M:%S")
        dur_sec = max(0.0, (alert.end_time - alert.start_time).total_seconds())

        print(
            f"{color}[{alert.severity:<8}]{reset} "
            f"{alert.ticker:<5} | {alert.anomaly_type:<11} | "
            f"Z: {alert.peak_zscore:+5.2f} | "
            f"${alert.peak_price:7.2f} | "
            f"VWAP Div: {alert.peak_vwap_div * 100:4.2f}% | "
            f"Ticks: {alert.tick_count:<3} | "
            f"Dur: {dur_sec:4.1f}s | "
            f"{ts_str}"
        )

    def _notify_log_file(self, alert: Alert):
        try:
            with open(self.config.alerts_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(alert.to_dict()) + "\n")
        except Exception as e:
            logger.error(f"Failed to append alert to log file {self.config.alerts_log_path}: {e}")

    def _notify_discord(self, alert: Alert):
        color_hex = 15548997 if alert.severity == "CRITICAL" else 16766720
        payload = {
            "embeds": [
                {
                    "title": f"[{alert.severity}] Market Surveillance Alert: {alert.ticker}",
                    "description": alert.message,
                    "color": color_hex,
                    "fields": [
                        {"name": "Ticker", "value": alert.ticker, "inline": True},
                        {"name": "Severity", "value": alert.severity, "inline": True},
                        {"name": "Type", "value": alert.anomaly_type, "inline": True},
                        {"name": "Price", "value": f"${alert.peak_price:.2f}", "inline": True},
                        {"name": "Peak Z-Score", "value": f"{alert.peak_zscore:+.2f}", "inline": True},
                        {"name": "VWAP Divergence", "value": f"{alert.peak_vwap_div * 100:.2f}%", "inline": True},
                        {"name": "Tick Count", "value": str(alert.tick_count), "inline": True},
                        {"name": "Start Time (UTC)", "value": alert.start_time.strftime("%H:%M:%S"), "inline": True},
                    ],
                    "timestamp": alert.created_at.isoformat(),
                }
            ]
        }
        try:
            requests.post(self.config.discord_webhook_url, json=payload, timeout=3.0)
        except Exception as e:
            logger.warning(f"Discord webhook failed: {e}")

    def _notify_slack(self, alert: Alert):
        payload = {
            "text": f"*{alert.severity}*: {alert.message}",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{alert.severity} Market Alert: {alert.ticker}*\n{alert.message}",
                    },
                }
            ],
        }
        try:
            requests.post(self.config.slack_webhook_url, json=payload, timeout=3.0)
        except Exception as e:
            logger.warning(f"Slack webhook failed: {e}")
