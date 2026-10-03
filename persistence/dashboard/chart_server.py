"""
chart_server.py - Ultra-fast in-process HTTP micro-API for 60 FPS HTML5 Canvas charts.
Serves normalized OHLC, volume, rolling Z-score, and anomaly markers directly from Cassandra.
"""

import http.server
import json
import logging
import math
import socketserver
import threading
import os
import sys
import time
from urllib.parse import parse_qs, urlparse

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from persistence.persistence_client import get_persistence_client

logger = logging.getLogger("chart_server")
_SERVER_INSTANCE = None
_SERVER_LOCK = threading.Lock()
PORT = 8506


def get_chart_data_dict(ticker: str = "AAPL", lookback: int = 15, is_live: bool = True) -> dict:
    from persistence.alerting.config import AlertConfig
    cfg = AlertConfig(use_mock=not is_live)
    client = get_persistence_client(config=cfg)
    df_ohlc = client.fetch_recent_ohlc(ticker=ticker, lookback_minutes=lookback)
    df_anom = client.fetch_recent_anomalies(lookback_minutes=lookback, ticker=ticker, limit=100)

    candles = []
    volumes = []
    zscores = []
    vwap_line = []
    vwap_divs = []

    if not df_ohlc.empty:
        clean_ohlc = df_ohlc.drop_duplicates(subset=["time"]).sort_values("time").copy()
        for _, row in clean_ohlc.iterrows():
            ts = int(row["time"].timestamp())
            op = float(row["open"])
            hi = float(row["high"])
            lo = float(row["low"])
            cl = float(row["close"])
            vol = float(row["volume"])
            zs = float(row["zscore"]) if not math.isnan(float(row["zscore"])) else 0.0
            raw_vw = float(row.get("vwap", cl))
            vw = raw_vw if not math.isnan(raw_vw) else cl
            raw_vw_div = float(row.get("vwap_divergence", 0.0))
            vw_div = raw_vw_div if not math.isnan(raw_vw_div) else 0.0

            candles.append({
                "time": ts,
                "open": round(op, 4),
                "high": round(hi, 4),
                "low": round(lo, 4),
                "close": round(cl, 4),
            })

            is_green = cl >= op
            volumes.append({
                "time": ts,
                "value": round(vol, 2),
                "color": "rgba(16, 185, 129, 0.45)" if is_green else "rgba(239, 68, 68, 0.45)",
            })

            zscores.append({
                "time": ts,
                "value": round(zs, 3),
            })

            vwap_line.append({
                "time": ts,
                "value": round(vw, 4),
            })

            vwap_divs.append({
                "time": ts,
                "value": round(vw_div, 4),
            })

    markers = []
    if not df_anom.empty and candles:
        candle_times = [c["time"] for c in candles]
        candle_anomalies = {}

        for _, row in df_anom.iterrows():
            a_ts = int(row["event_time"].timestamp())
            snapped_ts = min(candle_times, key=lambda t: abs(t - a_ts)) if candle_times else a_ts

            zs = float(row.get("zscore", 0.0))
            risk = float(row.get("risk_score", 1.0))

            if abs(zs) < 3.0 and risk < 1.0:
                continue

            if snapped_ts not in candle_anomalies:
                candle_anomalies[snapped_ts] = []
            candle_anomalies[snapped_ts].append(row)

        for ts, group in candle_anomalies.items():
            count = len(group)
            peak = max(
                group,
                key=lambda r: (
                    float(r.get("risk_score", 0.0)) >= 2.0 or abs(float(r.get("zscore", 0.0))) >= 4.5,
                    float(r.get("risk_score", 0.0)),
                    abs(float(r.get("zscore", 0.0))),
                ),
            )
            p_zs = float(peak.get("zscore", 0.0))
            p_risk = float(peak.get("risk_score", 1.0))
            p_type = str(peak.get("anomaly_type", "")).lower()
            is_crit = p_risk >= 2.0 or abs(p_zs) >= 4.5
            marker_color = "#ef4444" if is_crit else "#f59e0b"

            suffix = f" [x{count}]" if count > 1 else ""

            if "wash" in p_type:
                shape = "circle"
                position = "aboveBar"
                text = f"WASH SALE{suffix}"
            elif p_zs < 0:
                shape = "arrowDown"
                position = "aboveBar"
                text = f"DROP ({p_zs:+.1f}σ){suffix}"
            else:
                shape = "arrowUp"
                position = "belowBar"
                text = f"SPIKE ({p_zs:+.1f}σ){suffix}"

            markers.append({
                "time": ts,
                "position": position,
                "color": marker_color,
                "shape": shape,
                "text": text,
            })

        markers.sort(key=lambda m: m["time"])

    first_close = candles[0]["close"] if candles else 0.0
    last_close = candles[-1]["close"] if candles else 0.0
    pct_chg = ((last_close - first_close) / first_close) * 100 if first_close > 0 else 0.0

    return {
        "ticker": ticker,
        "candles": candles,
        "volumes": volumes,
        "zscores": zscores,
        "vwap_line": vwap_line,
        "vwap_divs": vwap_divs,
        "markers": markers,
        "last_price": last_close,
        "pct_change": round(pct_chg, 2),
        "anomalies_count": len(df_anom),
        "server_time": int(time.time()),
    }


class ChartDataHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Connection", "close")

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/chart_data":
            qs = parse_qs(parsed.query)
            ticker = qs.get("ticker", ["AAPL"])[0].upper()
            try:
                lookback = int(qs.get("lookback", ["15"])[0])
            except ValueError:
                lookback = 15

            is_live_val = qs.get("live", ["1"])[0].lower() in ("1", "true")
            try:
                payload = get_chart_data_dict(ticker=ticker, lookback=lookback, is_live=is_live_val)
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self._send_cors_headers()
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                return
            except Exception as e:
                try:
                    err_body = json.dumps({"error": str(e)}).encode("utf-8")
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self._send_cors_headers()
                    self.send_header("Content-Length", str(len(err_body)))
                    self.end_headers()
                    self.wfile.write(err_body)
                except Exception:
                    pass
                return

        # 404 for unknown endpoints
        self.send_response(404)
        self.end_headers()


class ReusableTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def start_chart_server(port: int = PORT):
    global _SERVER_INSTANCE
    with _SERVER_LOCK:
        if _SERVER_INSTANCE is not None:
            return _SERVER_INSTANCE

        try:
            server = ReusableTCPServer(("127.0.0.1", port), ChartDataHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True, name="ChartDataServer")
            thread.start()
            _SERVER_INSTANCE = server
            print(f"[ChartServer] Background streaming micro-API started on http://127.0.0.1:{port}/api/chart_data")
            return _SERVER_INSTANCE
        except OSError as e:
            # Port might already be bound by a previous instance
            print(f"[ChartServer] Port {port} already bound ({e}). Reusing existing server.")
            return None


if __name__ == "__main__":
    start_chart_server()
    print("Serving chart data... Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
