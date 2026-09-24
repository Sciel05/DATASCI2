import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from .config import CONFIG, AlertConfig

@dataclass
class Alert:
    alert_id: str
    ticker: str
    severity: str
    anomaly_type: str
    start_time: datetime
    end_time: datetime
    peak_price: float
    peak_zscore: float
    peak_vwap_div: float
    total_volume: float
    tick_count: int
    message: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "alert_id": self.alert_id,
            "ticker": self.ticker,
            "severity": self.severity,
            "anomaly_type": self.anomaly_type,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "peak_price": round(self.peak_price, 4),
            "peak_zscore": round(self.peak_zscore, 2),
            "peak_vwap_div": round(self.peak_vwap_div, 4),
            "total_volume": round(self.total_volume, 2),
            "tick_count": self.tick_count,
            "message": self.message,
            "created_at": self.created_at.isoformat(),
            # Latency from last anomalous event to alert generation (ms).
            # Used by Objective 4 evaluation for min/median/p95 latency reporting.
            "detection_latency_ms": round(
                (self.created_at - self.end_time).total_seconds() * 1000, 2
            ),
        }


class AlertEngine:
    def __init__(self, config: AlertConfig = CONFIG):
        self.config = config
        self._active_bursts: Dict[Tuple[str, str], Alert] = {}
        self._last_alert_time: Dict[Tuple[str, str], datetime] = {}
        self._alert_history: List[Alert] = []

    def classify_severity(self, zscore: float, vwap_div: float, anomaly_type: str) -> str:
        abs_z = abs(zscore)
        abs_v = abs(vwap_div)

        if abs_z >= self.config.zscore_critical or abs_v >= self.config.vwap_critical:
            return "CRITICAL"
        if abs_z >= self.config.zscore_warning or abs_v >= self.config.vwap_warning or anomaly_type == "wash_trade":
            return "WARNING"
        return "INFO"

    def _compose_message(
        self,
        ticker: str,
        severity: str,
        anomaly_type: str,
        price: float,
        zscore: float,
        vwap_div: float,
        ticks: int,
    ) -> str:
        if anomaly_type == "price_shock":
            direction = "spike" if zscore > 0 else "drop"
            return (
                f"[{severity}] {ticker} price {direction} to ${price:.2f} "
                f"(Z-score: {zscore:+.2f}, VWAP divergence: {vwap_div * 100:.2f}%, {ticks} ticks)"
            )
        elif anomaly_type == "wash_trade":
            return (
                f"[{severity}] {ticker} suspected wash trade at ${price:.2f} "
                f"(near-zero net price shift, VWAP div: {vwap_div * 100:.2f}%, {ticks} ticks)"
            )
        return f"[{severity}] {ticker} anomalous behavior detected (Z-score: {zscore:+.2f})"

    def process_event(self, event: dict) -> Optional[Alert]:
        ticker = event.get("ticker", "UNKNOWN")
        anomaly_type = event.get("anomaly_type") or "price_shock"
        price = float(event.get("price", 0.0))
        volume = float(event.get("volume", 0.0))
        zscore = float(event.get("zscore", 0.0))
        vwap_div = float(event.get("vwap_divergence", 0.0))

        raw_time = event.get("event_time")
        if isinstance(raw_time, str):
            event_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        elif isinstance(raw_time, datetime):
            event_time = raw_time if raw_time.tzinfo else raw_time.replace(tzinfo=timezone.utc)
        else:
            event_time = datetime.now(timezone.utc)

        severity = self.classify_severity(zscore, vwap_div, anomaly_type)
        key = (ticker, anomaly_type)

        # Check existing burst window
        if key in self._active_bursts:
            burst = self._active_bursts[key]
            delta_sec = (event_time - burst.end_time).total_seconds()

            if 0.0 <= delta_sec <= self.config.burst_window_sec:
                # Absorb tick into existing burst
                burst.end_time = event_time
                burst.tick_count += 1
                burst.total_volume += volume
                burst.peak_price = price

                if abs(zscore) > abs(burst.peak_zscore):
                    burst.peak_zscore = zscore
                if abs(vwap_div) > abs(burst.peak_vwap_div):
                    burst.peak_vwap_div = vwap_div

                # Escalate if new tick pushed severity from WARNING to CRITICAL
                if burst.severity != "CRITICAL" and severity == "CRITICAL":
                    burst.severity = "CRITICAL"
                    burst.message = self._compose_message(
                        ticker, burst.severity, anomaly_type, burst.peak_price,
                        burst.peak_zscore, burst.peak_vwap_div, burst.tick_count
                    )
                    return burst

                # Absorbed without escalation: suppress redundant notification
                return None

        # Expired burst or new incident
        if key in self._last_alert_time:
            cooldown_delta = (event_time - self._last_alert_time[key]).total_seconds()
            if 0.0 <= cooldown_delta < self.config.cooldown_sec:
                return None

        # Initialize new alert incident
        alert_id = str(uuid.uuid4())[:8]
        msg = self._compose_message(ticker, severity, anomaly_type, price, zscore, vwap_div, ticks=1)
        alert = Alert(
            alert_id=alert_id,
            ticker=ticker,
            severity=severity,
            anomaly_type=anomaly_type,
            start_time=event_time,
            end_time=event_time,
            peak_price=price,
            peak_zscore=zscore,
            peak_vwap_div=vwap_div,
            total_volume=volume,
            tick_count=1,
            message=msg,
        )

        self._active_bursts[key] = alert
        self._last_alert_time[key] = event_time
        self._alert_history.append(alert)
        if len(self._alert_history) > 500:
            self._alert_history = self._alert_history[-500:]

        return alert

    def flush_expired_bursts(self, current_time: Optional[datetime] = None) -> List[Alert]:
        now = current_time or datetime.now(timezone.utc)
        expired = []
        to_delete = []

        for key, burst in self._active_bursts.items():
            if (now - burst.end_time).total_seconds() > self.config.burst_window_sec:
                expired.append(burst)
                to_delete.append(key)

        for key in to_delete:
            del self._active_bursts[key]

        return expired

    def get_recent_alerts(self, limit: int = 50) -> List[Alert]:
        return list(reversed(self._alert_history[-limit:]))
