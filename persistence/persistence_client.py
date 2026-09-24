import os
import time
import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

from .alerting.config import CONFIG, AlertConfig

class PersistenceClient(ABC):
    @abstractmethod
    def fetch_recent_anomalies(
        self,
        lookback_minutes: int = 15,
        ticker: Optional[str] = None,
        limit: int = 200,
    ) -> pd.DataFrame:
        """Fetch anomalies within the lookback window, ordered by event_time descending."""
        pass

    @abstractmethod
    def fetch_ticker_metrics(self, lookback_minutes: int = 15) -> Dict[str, dict]:
        """Fetch summary metrics per ticker across the lookback window."""
        pass

    @abstractmethod
    def check_health(self) -> dict:
        """Return connectivity status, latency, and active backend mode."""
        pass


class CassandraClient(PersistenceClient):
    def __init__(self, config: AlertConfig = CONFIG):
        self.config = config
        self._cluster = None
        self._session = None
        self._prepared_stmt = None
        self._connect()

    def _connect(self):
        try:
            from cassandra.cluster import Cluster
            from cassandra.query import PreparedStatement

            self._cluster = Cluster(
                contact_points=[self.config.cassandra_host],
                port=self.config.cassandra_port,
                connect_timeout=5.0,
            )
            self._session = self._cluster.connect(self.config.cassandra_keyspace)

            # Check whether price and volume columns exist in the anomalies table
            has_price_volume = False
            try:
                table_meta = self._cluster.metadata.keyspaces.get(self.config.cassandra_keyspace)
                if table_meta and "anomalies" in table_meta.tables:
                    cols = table_meta.tables["anomalies"].columns
                    has_price_volume = "price" in cols and "volume" in cols
            except Exception:
                has_price_volume = False

            # Query is scoped to partition key (ticker) to avoid cluster-wide table scans
            # and avoid requiring ALLOW FILTERING in Cassandra.
            if has_price_volume:
                query = """
                    SELECT ticker, event_time, price, volume, zscore, vwap_divergence, anomaly_type
                    FROM anomalies
                    WHERE ticker = ? AND event_time >= ?
                    ORDER BY event_time DESC
                    LIMIT ?
                """
            else:
                query = """
                    SELECT ticker, event_time, zscore, vwap_divergence, anomaly_type
                    FROM anomalies
                    WHERE ticker = ? AND event_time >= ?
                    ORDER BY event_time DESC
                    LIMIT ?
                """
            self._has_price_volume = has_price_volume
            self._prepared_stmt = self._session.prepare(query)
        except Exception as e:
            self._session = None
            raise ConnectionError(f"Failed to connect to Cassandra at {self.config.cassandra_host}:{self.config.cassandra_port}: {e}")

    def fetch_recent_anomalies(
        self,
        lookback_minutes: int = 15,
        ticker: Optional[str] = None,
        limit: int = 200,
    ) -> pd.DataFrame:
        if not self._session:
            self._connect()

        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
        tickers_to_query = [ticker] if ticker else list(self.config.monitored_tickers)

        records = []
        for sym in tickers_to_query:
            rows = self._session.execute(self._prepared_stmt, (sym, cutoff_time, limit))
            for row in rows:
                records.append({
                    "ticker": row.ticker,
                    "event_time": row.event_time,
                    "price": float(getattr(row, "price", 0.0) or 0.0),
                    "volume": float(getattr(row, "volume", 0.0) or 0.0),
                    "zscore": float(row.zscore),
                    "vwap_divergence": float(row.vwap_divergence),
                    "anomaly_type": str(row.anomaly_type),
                })

        if not records:
            return pd.DataFrame(columns=[
                "ticker", "event_time", "price", "volume", "zscore", "vwap_divergence", "anomaly_type"
            ])

        df = pd.DataFrame(records)
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
        return df.sort_values("event_time", ascending=False).head(limit).reset_index(drop=True)

    def fetch_ticker_metrics(self, lookback_minutes: int = 15) -> Dict[str, dict]:
        df = self.fetch_recent_anomalies(lookback_minutes=lookback_minutes, limit=1000)
        metrics = {}
        for sym in self.config.monitored_tickers:
            sym_df = df[df["ticker"] == sym]
            metrics[sym] = {
                "anomaly_count": len(sym_df),
                "price_shocks": len(sym_df[sym_df["anomaly_type"] == "price_shock"]),
                "wash_trades": len(sym_df[sym_df["anomaly_type"] == "wash_trade"]),
                "max_zscore": float(sym_df["zscore"].abs().max()) if not sym_df.empty else 0.0,
                "latest_event_time": sym_df["event_time"].max() if not sym_df.empty else None,
            }
        return metrics

    def check_health(self) -> dict:
        t0 = time.perf_counter()
        try:
            if not self._session:
                self._connect()
            self._session.execute("SELECT now() FROM system.local")
            latency_ms = (time.perf_counter() - t0) * 1000.0
            return {
                "status": "healthy",
                "backend": "Cassandra",
                "host": f"{self.config.cassandra_host}:{self.config.cassandra_port}",
                "keyspace": self.config.cassandra_keyspace,
                "latency_ms": round(latency_ms, 2),
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "backend": "Cassandra",
                "error": str(e),
                "latency_ms": -1.0,
            }


class MockReplayClient(PersistenceClient):
    """
    Antislop mock simulator: computes real rolling statistics from the labeled CSV dataset
    and replays anomalies along an advancing clock window.
    """
    def __init__(self, config: AlertConfig = CONFIG):
        self.config = config
        self._dataset_anomalies = self._precompute_dataset_anomalies()
        self._sim_window_seconds = 15 * 60

        # Pre-offset by 3 simulated minutes so the first poll immediately returns
        # a populated anomaly window instead of starting cold at offset=0.
        # At 5x replay speed, 3 simulated minutes = 36 real seconds of pre-advance.
        _warm_offset_real_seconds = (3 * 60) / 5.0
        self._start_wall_time = time.time() - _warm_offset_real_seconds

    def _precompute_dataset_anomalies(self) -> pd.DataFrame:
        csv_path = self.config.mock_csv_path
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Mock dataset not found at {csv_path}")

        raw = pd.read_csv(csv_path)
        raw["dt"] = pd.to_datetime(raw["event_time_ms"], unit="ms", utc=True)
        raw = raw.sort_values(["ticker", "dt"]).reset_index(drop=True)

        processed = []
        for sym, group in raw.groupby("ticker"):
            g = group.set_index("dt")
            rolling_2m = g.rolling("2min", closed="left")
            mean_p = rolling_2m["price"].mean()
            std_p = rolling_2m["price"].std().replace(0, np.nan)
            zscore = ((g["price"] - mean_p) / std_p).fillna(0.0)

            cum_pv = (g["price"] * g["volume"]).cumsum()
            cum_v = g["volume"].cumsum()
            vwap = (cum_pv / cum_v).fillna(g["price"])
            vwap_div = (np.abs(g["price"] - vwap) / vwap).fillna(0.0)

            anomalies = g[g["label"] != "normal"].copy()
            anomalies["zscore"] = zscore.loc[anomalies.index]
            anomalies["vwap_divergence"] = vwap_div.loc[anomalies.index]
            anomalies["anomaly_type"] = anomalies["label"]
            processed.append(anomalies)

        df = pd.concat(processed).reset_index()
        df = df.sort_values("dt").reset_index(drop=True)

        # Baseline time offsets for simulated stream progression
        min_ts = df["dt"].min().timestamp()
        df["relative_offset"] = df["dt"].apply(lambda t: t.timestamp() - min_ts)
        self._max_relative_offset = df["relative_offset"].max()
        return df

    def _get_active_simulated_df(self) -> pd.DataFrame:
        now_utc = datetime.now(timezone.utc)
        elapsed = time.time() - self._start_wall_time
        
        # Advance replay stream at 5x simulated speed
        effective_offset = (elapsed * 5.0) % (self._max_relative_offset or 1.0)
        
        window_start_offset = max(0.0, effective_offset - self._sim_window_seconds)
        sub = self._dataset_anomalies[
            (self._dataset_anomalies["relative_offset"] >= window_start_offset) &
            (self._dataset_anomalies["relative_offset"] <= effective_offset)
        ].copy()

        # Re-anchor event timestamps to current UTC time
        sub["event_time"] = sub["relative_offset"].apply(
            lambda off: now_utc - timedelta(seconds=(effective_offset - off))
        )
        return sub

    def fetch_recent_anomalies(
        self,
        lookback_minutes: int = 15,
        ticker: Optional[str] = None,
        limit: int = 200,
    ) -> pd.DataFrame:
        sub = self._get_active_simulated_df()
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
        filtered = sub[sub["event_time"] >= cutoff]

        if ticker:
            filtered = filtered[filtered["ticker"] == ticker]

        cols = ["ticker", "event_time", "price", "volume", "zscore", "vwap_divergence", "anomaly_type"]
        res = filtered[cols].sort_values("event_time", ascending=False).head(limit).reset_index(drop=True)
        return res

    def fetch_ticker_metrics(self, lookback_minutes: int = 15) -> Dict[str, dict]:
        df = self.fetch_recent_anomalies(lookback_minutes=lookback_minutes, limit=1000)
        metrics = {}
        for sym in self.config.monitored_tickers:
            sym_df = df[df["ticker"] == sym]
            metrics[sym] = {
                "anomaly_count": len(sym_df),
                "price_shocks": len(sym_df[sym_df["anomaly_type"] == "price_shock"]),
                "wash_trades": len(sym_df[sym_df["anomaly_type"] == "wash_trade"]),
                "max_zscore": float(sym_df["zscore"].abs().max()) if not sym_df.empty else 0.0,
                "latest_event_time": sym_df["event_time"].max() if not sym_df.empty else None,
            }
        return metrics

    def check_health(self) -> dict:
        return {
            "status": "healthy",
            "backend": "MockReplay (CSV-anchored)",
            "host": "localhost (in-process)",
            "dataset_anomalies_loaded": len(self._dataset_anomalies),
            "latency_ms": 0.45,
        }


def get_persistence_client(config: AlertConfig = CONFIG) -> PersistenceClient:
    """Factory function returning the configured persistence client."""
    if config.use_mock:
        return MockReplayClient(config=config)
    try:
        return CassandraClient(config=config)
    except Exception as err:
        # Fall back to mock client if Cassandra connection fails during development
        print(f"Warning: Cassandra unavailable ({err}). Falling back to MockReplayClient.")
        return MockReplayClient(config=config)
