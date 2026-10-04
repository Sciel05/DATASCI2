import json
import logging
import os
from datetime import datetime
from typing import Dict, Optional

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

    def notify(self, alert: Alert) -> Dict[str, str]:
        """Dispatch alert across all active notification channels.

        Webhooks are posted one after the other (at most 3 s each; alerts
        are a few per minute) so their outcome can be checked and recorded:
        it is printed, written into the alert's line in the audit log as
        "deliveries" (the dashboard shows the latest), and returned as
        {channel: "ok 204" | "failed: ..."}.
        """
        self._notify_console(alert)
        deliveries = {}
        if self.config.discord_webhook_url:
            deliveries["discord"] = self._post("discord", self.config.discord_webhook_url, self._discord_payload(alert))
        if self.config.slack_webhook_url:
            deliveries["slack"] = self._post("slack", self.config.slack_webhook_url, self._slack_payload(alert))
        for channel, outcome in deliveries.items():
            print(f"           -> {channel} webhook: {outcome}")
        self._notify_log_file(alert, deliveries)
        return deliveries

    @staticmethod
    def _post(channel: str, url: str, payload: dict) -> str:
        """POST a webhook and report the outcome. requests doesn't raise on
        an HTTP error status, so a rejected webhook (wrong or revoked URL,
        rate limit) has to be caught from the status code."""
        try:
            response = requests.post(url, json=payload, timeout=3.0)
        except Exception as e:
            logger.warning(f"{channel} webhook failed: {e}")
            return f"failed: {type(e).__name__}"
        if response.ok:
            return f"ok {response.status_code}"
        logger.warning(f"{channel} webhook rejected: HTTP {response.status_code} {response.text[:200]}")
        return f"failed: HTTP {response.status_code}"

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
            f"Risk: {alert.peak_risk_score:5.2f} | "
            f"Z: {alert.peak_zscore:+5.2f} | "
            f"${alert.peak_price:7.2f} | "
            f"VWAP Div: {alert.peak_vwap_div * 100:4.2f}% | "
            f"Ticks: {alert.tick_count:<3} | "
            f"Dur: {dur_sec:4.1f}s | "
            f"{ts_str}"
        )

    def _notify_log_file(self, alert: Alert, deliveries: Optional[Dict[str, str]] = None):
        try:
            with open(self.config.alerts_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(dict(alert.to_dict(), deliveries=deliveries or {})) + "\n")
        except Exception as e:
            logger.error(f"Failed to append alert to log file {self.config.alerts_log_path}: {e}")

    @staticmethod
    def _discord_payload(alert: Alert) -> dict:
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
                        {"name": "Peak Risk", "value": f"{alert.peak_risk_score:.2f}", "inline": True},
                        {"name": "Signals", "value": alert.signals or "legacy", "inline": True},
                        {"name": "VWAP Divergence", "value": f"{alert.peak_vwap_div * 100:.2f}%", "inline": True},
                        {"name": "Tick Count", "value": str(alert.tick_count), "inline": True},
                        {"name": "Start Time (UTC)", "value": alert.start_time.strftime("%H:%M:%S"), "inline": True},
                    ],
                    "timestamp": alert.created_at.isoformat(),
                }
            ]
        }
        return payload

    @staticmethod
    def _slack_payload(alert: Alert) -> dict:
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
        return payload
