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

    @abstractmethod
    def fetch_recent_ohlc(
        self,
        ticker: str,
        lookback_minutes: int = 15,
    ) -> pd.DataFrame:
        """Return 30-second OHLC bars with rolling z-score for the given ticker.

        Columns: time, open, high, low, close, volume, zscore
        """
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
            try:
                ohlc = self.fetch_recent_ohlc(ticker=sym, lookback_minutes=lookback_minutes)
                last_price = float(ohlc["close"].iloc[-1]) if not ohlc.empty else 0.0
                first_price = float(ohlc["close"].iloc[0]) if not ohlc.empty else last_price
                pct_chg = ((last_price - first_price) / first_price) * 100 if first_price > 0 else 0.0
            except Exception:
                last_price = 0.0
                pct_chg = 0.0
            metrics[sym] = {
                "anomaly_count": len(sym_df),
                "price_shocks": len(sym_df[sym_df["anomaly_type"] == "price_shock"]),
                "wash_trades": len(sym_df[sym_df["anomaly_type"] == "wash_trade"]),
                "max_zscore": float(sym_df["zscore"].abs().max()) if not sym_df.empty else 0.0,
                "latest_event_time": sym_df["event_time"].max() if not sym_df.empty else None,
                "latest_price": last_price,
                "pct_change": pct_chg,
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

    def fetch_recent_ohlc(
        self,
        ticker: str,
        lookback_minutes: int = 15,
    ) -> pd.DataFrame:
        """Query raw_events from Cassandra and resample into 30s OHLC bars."""
        if not self._session:
            self._connect()

        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
        query = self._session.prepare(
            "SELECT event_time, price, volume FROM raw_events "
            "WHERE ticker = ? AND event_time >= ? ORDER BY event_time ASC"
        )
        rows = self._session.execute(query, (ticker, cutoff_time))
        records = [
            {"event_time": row.event_time, "price": float(row.price), "volume": float(row.volume)}
            for row in rows
        ]

        empty = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "zscore"])
        if not records:
            return empty

        df = pd.DataFrame(records)
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
        df = df.set_index("event_time").sort_index()

        ohlc = df["price"].resample("30s").ohlc().dropna()
        vol = df["volume"].resample("30s").sum().reindex(ohlc.index).fillna(0)

        # Rolling z-score on 30s closes (window=15 candles, min_periods=5)
        roll_mean = ohlc["close"].rolling(window=15, min_periods=5).mean()
        roll_std = ohlc["close"].rolling(window=15, min_periods=5).std()
        # Guard against near-zero std causing z-score blowup:
        # suppress where std is below epsilon (price barely moving in window)
        eps = 1e-4
        std_safe = roll_std.where((roll_std >= eps) & (roll_std > 0), other=np.nan)
        zscore = ((ohlc["close"] - roll_mean) / std_safe).fillna(0.0).clip(-6.0, 6.0)
        zscore = zscore.where(roll_std >= eps, other=0.0).clip(-6.0, 6.0)

        result = pd.DataFrame({
            "time": ohlc.index,
            "open": ohlc["open"].values,
            "high": ohlc["high"].values,
            "low": ohlc["low"].values,
            "close": ohlc["close"].values,
            "volume": vol.values,
            "zscore": zscore.values,
        })
        return result.reset_index(drop=True)


class MockReplayClient(PersistenceClient):
    """
    Antislop mock simulator: computes real rolling statistics from the labeled CSV dataset
    and replays anomalies along an advancing clock window.
    """
    def __init__(self, config: AlertConfig = CONFIG):
        self.config = config
        self._dataset_anomalies, self._dataset_ohlc = self._precompute_dataset()
        self._sim_window_seconds = 15 * 60

        # Pre-offset by 159 simulated minutes (9540s) so that the default ticker (AAPL)
        # immediately exhibits a genuine anomaly episode (breach at z=+3.10) with visible
        # debounced triangle markers, and lookback windows up to 60m are fully populated.
        # At 5x replay speed, 9540 simulated seconds = 1908 real seconds of pre-advance.
        _warm_offset_sim_seconds = 9540.0
        _warm_offset_real_seconds = _warm_offset_sim_seconds / 5.0
        self._start_wall_time = time.time() - _warm_offset_real_seconds

    def _precompute_dataset(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Precompute both anomaly rows and full 30s OHLC bars from the labeled CSV."""
        csv_path = self.config.mock_csv_path
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Mock dataset not found at {csv_path}")

        raw = pd.read_csv(csv_path)
        raw["dt"] = pd.to_datetime(raw["event_time_ms"], unit="ms", utc=True)
        raw = raw.sort_values(["ticker", "dt"]).reset_index(drop=True)

        # --- Anomaly rows (with near-zero std guard and [-6, 6] clipping) ---
        processed = []
        for sym, group in raw.groupby("ticker"):
            g = group.set_index("dt")
            rolling_2m = g.rolling("2min", closed="left")
            mean_p = rolling_2m["price"].mean()
            std_p = rolling_2m["price"].std()
            eps = 1e-4
            std_safe = std_p.where((std_p >= eps) & (std_p > 0), other=np.nan)
            zscore = ((g["price"] - mean_p) / std_safe).fillna(0.0).clip(-6.0, 6.0)
            zscore = zscore.where(std_p >= eps, other=0.0).clip(-6.0, 6.0)

            cum_pv = (g["price"] * g["volume"]).cumsum()
            cum_v = g["volume"].cumsum()
            vwap = (cum_pv / cum_v).fillna(g["price"])
            vwap_div = (np.abs(g["price"] - vwap) / vwap).fillna(0.0)

            anomalies = g[g["label"] != "normal"].copy()
            anomalies["zscore"] = zscore.loc[anomalies.index]
            anomalies["vwap_divergence"] = vwap_div.loc[anomalies.index]
            anomalies["anomaly_type"] = anomalies["label"]
            processed.append(anomalies)

        df_anom = pd.concat(processed).reset_index()
        df_anom = df_anom.sort_values("dt").reset_index(drop=True)

        # --- Full OHLC bars (resample ALL ticks into 30s candles) ---
        ohlc_frames = []
        for sym, group in raw.groupby("ticker"):
            g = group.set_index("dt").sort_index()
            ohlc = g["price"].resample("30s").ohlc().dropna()
            vol = g["volume"].resample("30s").sum().reindex(ohlc.index).fillna(0)

            # Rolling z-score on 30s closes (window=15 candles, min_periods=5)
            roll_mean = ohlc["close"].rolling(window=15, min_periods=5).mean()
            roll_std = ohlc["close"].rolling(window=15, min_periods=5).std()
            # Guard against near-zero std causing z-score blowup:
            eps = 1e-4
            std_safe = roll_std.where((roll_std >= eps) & (roll_std > 0), other=np.nan)
            zscore_ohlc = ((ohlc["close"] - roll_mean) / std_safe).fillna(0.0).clip(-6.0, 6.0)
            zscore_ohlc = zscore_ohlc.where(roll_std >= eps, other=0.0).clip(-6.0, 6.0)

            bar_df = pd.DataFrame({
                "ticker": sym,
                "dt": ohlc.index,
                "open": ohlc["open"].values,
                "high": ohlc["high"].values,
                "low": ohlc["low"].values,
                "close": ohlc["close"].values,
                "volume": vol.values,
                "zscore": zscore_ohlc.values,
            })
            ohlc_frames.append(bar_df)

        df_ohlc = pd.concat(ohlc_frames).sort_values("dt").reset_index(drop=True)

        # Baseline time offsets for simulated stream progression (shared across both)
        all_dts = pd.concat([df_anom["dt"], df_ohlc["dt"]])
        min_ts = all_dts.min().timestamp()
        df_anom["relative_offset"] = df_anom["dt"].apply(lambda t: t.timestamp() - min_ts)
        df_ohlc["relative_offset"] = df_ohlc["dt"].apply(lambda t: t.timestamp() - min_ts)
        self._max_relative_offset = max(
            df_anom["relative_offset"].max() if not df_anom.empty else 0.0,
            df_ohlc["relative_offset"].max() if not df_ohlc.empty else 0.0,
        )
        return df_anom, df_ohlc

    def _get_replay_state(self, lookback_minutes: int = 15) -> Tuple[datetime, float, float]:
        """Compute synchronized clock and window offsets shared by all query methods."""
        now_utc = datetime.now(timezone.utc)
        elapsed = time.time() - self._start_wall_time
        effective_offset = (elapsed * 5.0) % (self._max_relative_offset or 1.0)
        window_seconds = lookback_minutes * 60
        window_start_offset = max(0.0, effective_offset - window_seconds)
        return now_utc, effective_offset, window_start_offset

    def _get_active_simulated_df(self, lookback_minutes: int = 15) -> pd.DataFrame:
        now_utc, effective_offset, window_start_offset = self._get_replay_state(lookback_minutes)
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
        sub = self._get_active_simulated_df(lookback_minutes=lookback_minutes)
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
            try:
                ohlc = self.fetch_recent_ohlc(ticker=sym, lookback_minutes=lookback_minutes)
                last_price = float(ohlc["close"].iloc[-1]) if not ohlc.empty else 0.0
                first_price = float(ohlc["close"].iloc[0]) if not ohlc.empty else last_price
                pct_chg = ((last_price - first_price) / first_price) * 100 if first_price > 0 else 0.0
            except Exception:
                last_price = 0.0
                pct_chg = 0.0
            metrics[sym] = {
                "anomaly_count": len(sym_df),
                "price_shocks": len(sym_df[sym_df["anomaly_type"] == "price_shock"]),
                "wash_trades": len(sym_df[sym_df["anomaly_type"] == "wash_trade"]),
                "max_zscore": float(sym_df["zscore"].abs().max()) if not sym_df.empty else 0.0,
                "latest_event_time": sym_df["event_time"].max() if not sym_df.empty else None,
                "latest_price": last_price,
                "pct_change": pct_chg,
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

    def fetch_recent_ohlc(
        self,
        ticker: str,
        lookback_minutes: int = 15,
    ) -> pd.DataFrame:
        """Return 30s OHLC bars for `ticker`, re-anchored to the current replay clock."""
        now_utc, effective_offset, window_start_offset = self._get_replay_state(lookback_minutes)
        sub = self._dataset_ohlc[
            (self._dataset_ohlc["ticker"] == ticker) &
            (self._dataset_ohlc["relative_offset"] >= window_start_offset) &
            (self._dataset_ohlc["relative_offset"] <= effective_offset)
        ].copy()

        if sub.empty:
            return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "zscore"])

        # Re-anchor bar timestamps to current UTC time (same logic as anomalies)
        sub["time"] = sub["relative_offset"].apply(
            lambda off: now_utc - timedelta(seconds=(effective_offset - off))
        )

        # Apply lookback filter
        cutoff = now_utc - timedelta(minutes=lookback_minutes)
        sub = sub[sub["time"] >= cutoff]

        cols = ["time", "open", "high", "low", "close", "volume", "zscore"]
        return sub[cols].sort_values("time").reset_index(drop=True)


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
