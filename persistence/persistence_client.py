import os
import time
import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

from .alerting.config import CONFIG, AlertConfig
from .schema_tools import cluster as make_cluster


def _utc_days(start: datetime, end: datetime) -> List:
    """The UTC dates from start's to end's, inclusive: the (ticker, day)
    partitions a time window covers (schema.cql)."""
    day, last = start.astimezone(timezone.utc).date(), end.astimezone(timezone.utc).date()
    days = []
    while day <= last:
        days.append(day)
        day += timedelta(days=1)
    return days


def _as_utc(dt: datetime) -> datetime:
    """The driver returns naive UTC datetimes."""
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


ANOMALY_COLUMNS = [
    "ticker",
    "event_time",
    "tick_id",
    "price",
    "volume",
    "zscore",
    "vwap_divergence",
    "anomaly_type",
    "ewma_divergence",
    "cusum_price",
    "cusum_volume",
    "risk_score",
    "signals",
    "incident_id",
]


def _finite_float(value, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


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
            # every contact point in CASSANDRA_HOST, reads at LOCAL_QUORUM
            self._cluster = make_cluster(
                self.config.cassandra_host,
                self.config.cassandra_port,
                connect_timeout=5.0,
            )
            self._session = self._cluster.connect(self.config.cassandra_keyspace)

            # Select every refined field present in the live table. This keeps
            # reads working during a rolling migration from the original
            # five-column anomaly table.
            legacy_columns = {
                "ticker", "event_time", "zscore", "vwap_divergence", "anomaly_type"
            }
            available_columns = set(legacy_columns)
            try:
                table_meta = self._cluster.metadata.keyspaces.get(self.config.cassandra_keyspace)
                if table_meta and "anomalies" in table_meta.tables:
                    available_columns = set(table_meta.tables["anomalies"].columns)
            except Exception:
                pass

            # Every query names a whole partition key (ticker, day), so it reads
            # one partition: no cluster-wide scan, no ALLOW FILTERING. A time
            # window is read as one query per UTC day it covers.
            self._selected_anomaly_columns = [
                name for name in ANOMALY_COLUMNS if name in available_columns
            ]
            selected = ", ".join(self._selected_anomaly_columns)
            query = f"""
                SELECT {selected}
                FROM anomalies
                WHERE ticker = ? AND day = ? AND event_time >= ?
                ORDER BY event_time DESC
                LIMIT ?
            """
            self._prepared_stmt = self._session.prepare(query)
            self._stmt_raw_since = self._session.prepare(
                "SELECT event_time, price, volume FROM raw_events "
                "WHERE ticker = ? AND day = ? AND event_time >= ? ORDER BY event_time ASC"
            )
            self._stmt_latest = {
                table: self._session.prepare(f"SELECT event_time FROM {table} WHERE ticker = ? AND day = ? LIMIT 1")
                for table in ("anomalies", "raw_events")
            }
            self._latest_days = {}
        except Exception as e:
            self._session = None
            hint = ""
            if "day" in str(e):
                # a keyspace created before the ((ticker, day)) partition key
                hint = (" -- the tables predate the 2026-10-04 ((ticker, day)) partition key; recreate them "
                        "(deletes their data) with: python -m persistence.schema_tools --reset "
                        "(note at the top of persistence/schema.cql)")
            raise ConnectionError(f"Failed to connect to Cassandra at {self.config.cassandra_host}:{self.config.cassandra_port}: {e}{hint}")

    def fetch_recent_anomalies(
        self,
        lookback_minutes: int = 15,
        ticker: Optional[str] = None,
        limit: int = 200,
    ) -> pd.DataFrame:
        if not self._session:
            self._connect()

        now = datetime.now(timezone.utc)
        cutoff_time = now - timedelta(minutes=lookback_minutes)
        tickers_to_query = [ticker] if ticker else list(self.config.monitored_tickers)

        records = []
        for sym in tickers_to_query:
            rows = self._anomalies_since(sym, cutoff_time, now, limit)
            if not rows:
                latest_dt = self._latest_event_time("anomalies", sym)
                if latest_dt and latest_dt < cutoff_time:
                    adjusted_cutoff = latest_dt - timedelta(minutes=lookback_minutes)
                    rows = self._anomalies_since(sym, adjusted_cutoff, latest_dt, limit)
            for row in rows:
                zscore = _finite_float(getattr(row, "zscore", 0.0))
                vwap_divergence = _finite_float(getattr(row, "vwap_divergence", 0.0))
                risk_value = getattr(row, "risk_score", None)
                # Legacy rows predate risk_score. Reconstruct the two original
                # normalized detector signals so dashboard ranking remains useful.
                risk_score = (
                    _finite_float(risk_value)
                    if risk_value is not None
                    else max(abs(zscore) / 5.0, abs(vwap_divergence) / 0.01)
                )
                records.append({
                    "ticker": row.ticker,
                    "event_time": row.event_time,
                    "tick_id": getattr(row, "tick_id", None),
                    "price": _finite_float(getattr(row, "price", 0.0)),
                    "volume": _finite_float(getattr(row, "volume", 0.0)),
                    "zscore": zscore,
                    "vwap_divergence": vwap_divergence,
                    "anomaly_type": str(row.anomaly_type),
                    "ewma_divergence": _finite_float(getattr(row, "ewma_divergence", 0.0)),
                    "cusum_price": _finite_float(getattr(row, "cusum_price", 0.0)),
                    "cusum_volume": _finite_float(getattr(row, "cusum_volume", 0.0)),
                    "risk_score": risk_score,
                    "signals": getattr(row, "signals", None),
                    "incident_id": getattr(row, "incident_id", None),
                })

        if not records:
            return pd.DataFrame(columns=ANOMALY_COLUMNS)

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
                "max_risk_score": float(sym_df["risk_score"].max()) if not sym_df.empty else 0.0,
                "incident_count": int(sym_df["incident_id"].dropna().nunique()) if not sym_df.empty else 0,
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

    def _anomalies_since(self, sym, cutoff, end, limit):
        """Newest-first anomaly rows at or after cutoff, from the day
        partitions up to end's, newest day first, stopping at limit."""
        rows = []
        for day in reversed(_utc_days(cutoff, end)):
            rows += self._session.execute(self._prepared_stmt, (sym, day, cutoff, limit - len(rows)))
            if len(rows) >= limit:
                break
        return rows

    def _latest_event_time(self, table, sym):
        """The ticker's newest event_time in table, or None: its newest day
        partition, then that partition's first (newest) row."""
        day = self._latest_day(table, sym)
        if day is None:
            return None
        row = self._session.execute(self._stmt_latest[table], (sym, day)).one()
        return _as_utc(row.event_time) if row and row.event_time else None

    def _latest_day(self, table, sym, max_age_s=30.0):
        """The ticker's newest day partition in table. Listed from the
        partition keys alone (SELECT DISTINCT reads the partition index,
        not the rows) and cached for max_age_s; only the fallback for an
        empty lookback window needs it."""
        fetched_at, latest = self._latest_days.get(table, (0.0, {}))
        if time.monotonic() - fetched_at > max_age_s:
            latest = {}
            for r in self._session.execute(f"SELECT DISTINCT ticker, day FROM {table}"):
                day = r.day.date()  # cassandra.util.Date -> datetime.date
                if r.ticker not in latest or day > latest[r.ticker]:
                    latest[r.ticker] = day
            self._latest_days[table] = (time.monotonic(), latest)
        return latest.get(sym)

    def _raw_since(self, ticker, cutoff, end):
        """Oldest-first raw_events rows at or after cutoff, from the day
        partitions up to end's, oldest day first."""
        rows = []
        for day in _utc_days(cutoff, end):
            rows += self._session.execute(self._stmt_raw_since, (ticker, day, cutoff))
        return rows

    def fetch_recent_ohlc(
        self,
        ticker: str,
        lookback_minutes: int = 15,
    ) -> pd.DataFrame:
        """Query raw_events from Cassandra and resample into 30s OHLC bars."""
        if not self._session:
            self._connect()

        now = datetime.now(timezone.utc)
        cutoff_time = now - timedelta(minutes=lookback_minutes)
        rows = self._raw_since(ticker, cutoff_time, now)
        if not rows:
            latest_dt = self._latest_event_time("raw_events", ticker)
            if latest_dt and latest_dt < cutoff_time:
                adjusted_cutoff = latest_dt - timedelta(minutes=lookback_minutes)
                rows = self._raw_since(ticker, adjusted_cutoff, latest_dt)

        records = [
            {"event_time": row.event_time, "price": float(row.price), "volume": float(row.volume)}
            for row in rows
        ]

        empty = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "zscore", "vwap", "vwap_divergence"])
        if not records:
            return empty

        df = pd.DataFrame(records)
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
        df = df.set_index("event_time").sort_index()

        ohlc = df["price"].resample("30s").ohlc().dropna()
        vol = df["volume"].resample("30s").sum().reindex(ohlc.index).fillna(0)

        # Rolling z-score on 30s closes (window=15 candles, min_periods=2)
        roll_mean = ohlc["close"].rolling(window=15, min_periods=2).mean()
        roll_std = ohlc["close"].rolling(window=15, min_periods=2).std()
        # Guard against near-zero std causing z-score blowup:
        # suppress where std is below epsilon (price barely moving in window)
        eps = 1e-4
        std_safe = roll_std.where((roll_std >= eps) & (roll_std > 0), other=np.nan)
        zscore = ((ohlc["close"] - roll_mean) / std_safe).fillna(0.0).clip(-6.0, 6.0)
        zscore = zscore.where(roll_std >= eps, other=0.0).clip(-6.0, 6.0)

        # Cumulative VWAP and divergence (%) for the lookback window
        typical_price = (ohlc["high"] + ohlc["low"] + ohlc["close"]) / 3.0
        cum_tp_vol = (typical_price * vol).cumsum()
        cum_vol = vol.cumsum().replace(0, np.nan)
        vwap = (cum_tp_vol / cum_vol).fillna(ohlc["close"])
        vwap_divergence = ((ohlc["close"] - vwap) / vwap * 100).fillna(0.0).clip(-10.0, 10.0)

        result = pd.DataFrame({
            "time": ohlc.index,
            "open": ohlc["open"].values,
            "high": ohlc["high"].values,
            "low": ohlc["low"].values,
            "close": ohlc["close"].values,
            "volume": vol.values,
            "zscore": zscore.values,
            "vwap": vwap.values,
            "vwap_divergence": vwap_divergence.values,
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
            anomalies["ewma_divergence"] = 0.0
            anomalies["cusum_price"] = 0.0
            anomalies["cusum_volume"] = 0.0
            anomalies["risk_score"] = np.maximum(
                np.maximum(anomalies["zscore"].abs() / 5.0, anomalies["vwap_divergence"].abs() / 0.01),
                1.01,
            )
            anomalies["signals"] = np.where(
                anomalies["anomaly_type"] == "wash_trade", "wash_rule", "zscore"
            )
            event_ms = (anomalies.index.astype("int64") // 1_000_000).astype("int64")
            anomalies["tick_id"] = [f"{sym}-{ms}-0" for ms in event_ms]
            new_incident = (
                anomalies.index.to_series().diff().dt.total_seconds().fillna(float("inf")) > 10.0
            ) | anomalies["anomaly_type"].ne(anomalies["anomaly_type"].shift())
            incident_starts = pd.Series(event_ms, index=anomalies.index).where(new_incident).ffill().astype("int64")
            anomalies["incident_id"] = [f"{sym}-{ms}" for ms in incident_starts]
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

            # Cumulative VWAP and divergence (%) per ticker
            typical_price = (ohlc["high"] + ohlc["low"] + ohlc["close"]) / 3.0
            cum_tp_vol = (typical_price * vol).cumsum()
            cum_vol = vol.cumsum().replace(0, np.nan)
            vwap_ohlc = (cum_tp_vol / cum_vol).fillna(ohlc["close"])
            vwap_div_ohlc = ((ohlc["close"] - vwap_ohlc) / vwap_ohlc * 100).fillna(0.0).clip(-10.0, 10.0)

            bar_df = pd.DataFrame({
                "ticker": sym,
                "dt": ohlc.index,
                "open": ohlc["open"].values,
                "high": ohlc["high"].values,
                "low": ohlc["low"].values,
                "close": ohlc["close"].values,
                "volume": vol.values,
                "zscore": zscore_ohlc.values,
                "vwap": vwap_ohlc.values,
                "vwap_divergence": vwap_div_ohlc.values,
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

        cols = ANOMALY_COLUMNS
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
                "max_risk_score": float(sym_df["risk_score"].max()) if not sym_df.empty else 0.0,
                "incident_count": int(sym_df["incident_id"].dropna().nunique()) if not sym_df.empty else 0,
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
            return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "zscore", "vwap", "vwap_divergence"])

        # Re-anchor bar timestamps to current UTC time (same logic as anomalies)
        sub["time"] = sub["relative_offset"].apply(
            lambda off: now_utc - timedelta(seconds=(effective_offset - off))
        )

        # Apply lookback filter
        cutoff = now_utc - timedelta(minutes=lookback_minutes)
        sub = sub[sub["time"] >= cutoff]

        cols = ["time", "open", "high", "low", "close", "volume", "zscore", "vwap", "vwap_divergence"]
        return sub[cols].sort_values("time").reset_index(drop=True)


_CLIENT_SINGLETONS = {}


def get_persistence_client(config: AlertConfig = CONFIG) -> PersistenceClient:
    """Factory function returning the configured persistence client (cached singleton to prevent socket exhaustion)."""
    key = (config.use_mock, config.cassandra_host, config.cassandra_port, config.cassandra_keyspace)
    cached = _CLIENT_SINGLETONS.get(key)
    if cached is not None:
        return cached

    if config.use_mock:
        client = MockReplayClient(config=config)
    else:
        try:
            client = CassandraClient(config=config)
        except Exception as err:
            # Fall back to mock client if Cassandra connection fails during development
            print(f"Warning: Cassandra unavailable ({err}). Falling back to MockReplayClient.")
            client = MockReplayClient(config=config)

    _CLIENT_SINGLETONS[key] = client
    return client
