"""Live surveillance dashboard: a small JSON API over Cassandra plus one page.

The page (index.html) loads each chart once and then polls for only what is
new, appending candles and anomaly markers in place -- no page reruns, so
nothing fades or redraws between updates.

    python persistence/live_dashboard/server.py            # http://localhost:8502

Reads CASSANDRA_HOST / CASSANDRA_PORT / CASSANDRA_KEYSPACE (defaults
127.0.0.1 / 9042 / surveillance) and DASHBOARD_PORT (8502). Standard
library only, apart from cassandra-driver.

Endpoints (all times are epoch seconds, UTC):
    GET /api/candles?ticker=NVDA&bucket=5&since=<t>   OHLCV candles from the bucket containing t
    GET /api/anomalies?since=<t>&limit=100            flagged ticks across all tickers, newest first
    GET /api/summary?minutes=15                       last price, window change, anomaly counts
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from cassandra.cluster import Cluster

TICKERS = tuple(os.getenv("DASHBOARD_TICKERS", "AAPL,MSFT,GOOGL,TSLA,NVDA").split(","))
PAGE = Path(__file__).resolve().parent / "index.html"
MAX_LOOKBACK_S = 3 * 3600


def epoch_s(dt):
    """The driver returns naive UTC datetimes."""
    return dt.replace(tzinfo=timezone.utc).timestamp()


def utc(ts):
    return datetime.fromtimestamp(ts, timezone.utc)


class Store:
    def __init__(self):
        host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
        port = int(os.getenv("CASSANDRA_PORT", "9042"))
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "surveillance")
        self.cluster = Cluster([host], port=port)
        self.session = self.cluster.connect(self.keyspace)
        self.session.default_timeout = 10
        prep = self.session.prepare
        self.q_ticks = prep(
            "SELECT event_time, price, volume FROM raw_events "
            "WHERE ticker = ? AND event_time >= ? ORDER BY event_time ASC"
        )
        self.q_latest = prep("SELECT event_time, price FROM raw_events WHERE ticker = ? LIMIT 1")
        self.q_first = prep(
            "SELECT price FROM raw_events WHERE ticker = ? AND event_time >= ? "
            "ORDER BY event_time ASC LIMIT 1"
        )
        self.q_anomalies = prep(
            "SELECT ticker, event_time, price, volume, anomaly_type, risk_score, signals, "
            "zscore, vwap_divergence, incident_id FROM anomalies "
            "WHERE ticker = ? AND event_time >= ? LIMIT ?"
        )
        self.q_anomaly_types = prep(
            "SELECT anomaly_type FROM anomalies WHERE ticker = ? AND event_time >= ?"
        )

    def candles(self, ticker, bucket, since):
        start = since - since % bucket  # re-send the bucket `since` falls in: it may have grown
        out = []
        for r in self.session.execute(self.q_ticks, (ticker, utc(start))):
            t = int(epoch_s(r.event_time) // bucket * bucket)
            if out and out[-1]["time"] == t:
                c = out[-1]
                c["high"] = max(c["high"], r.price)
                c["low"] = min(c["low"], r.price)
                c["close"] = r.price
                c["volume"] += r.volume
            else:
                out.append({"time": t, "open": r.price, "high": r.price, "low": r.price,
                            "close": r.price, "volume": r.volume})
        return out

    def anomalies(self, since, limit):
        rows = []
        for ticker in TICKERS:
            for r in self.session.execute(self.q_anomalies, (ticker, utc(since), limit)):
                rows.append({
                    "ticker": r.ticker,
                    "time": epoch_s(r.event_time),
                    "price": r.price,
                    "volume": r.volume,
                    "type": r.anomaly_type,
                    "risk": r.risk_score,
                    "signals": r.signals,
                    "zscore": r.zscore,
                    "vwap_div": r.vwap_divergence,
                    "incident": r.incident_id,
                })
        rows.sort(key=lambda a: a["time"], reverse=True)
        return rows[:limit]

    def summary(self, minutes):
        since = utc(time.time() - minutes * 60)
        out = []
        for ticker in TICKERS:
            latest = self.session.execute(self.q_latest, (ticker,)).one()
            first = self.session.execute(self.q_first, (ticker, since)).one()
            counts = {"price_shock": 0, "wash_trade": 0}
            for r in self.session.execute(self.q_anomaly_types, (ticker, since)):
                counts[r.anomaly_type] = counts.get(r.anomaly_type, 0) + 1
            out.append({
                "ticker": ticker,
                "price": latest.price if latest else None,
                "last_tick": epoch_s(latest.event_time) if latest else None,
                "window_open": first.price if first else None,
                "anomalies": counts,
            })
        return out


class Handler(BaseHTTPRequestHandler):
    store = None

    def log_message(self, *_):  # one line per poll would drown the log
        pass

    def send(self, status, body, content_type="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        now = time.time()
        try:
            if url.path in ("/", "/index.html"):
                self.send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
            elif url.path == "/api/candles":
                ticker = q.get("ticker", "")
                if ticker not in TICKERS:
                    return self.send(400, {"error": f"unknown ticker {ticker!r}"})
                bucket = max(1, min(300, int(q.get("bucket", 5))))
                since = max(now - MAX_LOOKBACK_S, float(q.get("since", now - 900)))
                self.send(200, {"candles": self.store.candles(ticker, bucket, since), "now": now})
            elif url.path == "/api/anomalies":
                since = max(now - MAX_LOOKBACK_S, float(q.get("since", now - 900)))
                limit = max(1, min(5000, int(q.get("limit", 100))))
                self.send(200, {"anomalies": self.store.anomalies(since, limit), "now": now})
            elif url.path == "/api/summary":
                minutes = max(1, min(180, int(q.get("minutes", 15))))
                self.send(200, {"tickers": self.store.summary(minutes), "now": now})
            else:
                self.send(404, {"error": "not found"})
        except (ValueError, TypeError) as e:
            self.send(400, {"error": str(e)})
        except Exception as e:  # Cassandra down or timing out: the page shows it and keeps polling
            self.send(503, {"error": f"{type(e).__name__}: {e}"})


def main():
    port = int(os.getenv("DASHBOARD_PORT", "8502"))
    Handler.store = Store()
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"live dashboard on http://localhost:{port} (keyspace {Handler.store.keyspace})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        Handler.store.cluster.shutdown()
        sys.exit(0)


if __name__ == "__main__":
    main()
