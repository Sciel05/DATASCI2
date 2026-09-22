"""
inject_anomalies.py

Takes the real-anchored synthetic tick data (from yfinance 1-min bars,
upsampled to tick level) and injects labeled anomalies on top of it:
  - price_shock: a sudden multi-sigma jump/drop that decays back over a
    few ticks, with bid/ask shifted consistently.
  - wash_trade: a burst of elevated volume with near-zero net price
    movement, matching the heuristic your proposal scoped.

This is the missing "hybrid" piece: real price dynamics as the base,
synthetic ground-truth labels for evaluating your Spark detector.

Expects input columns: timestamp, price, bid, ask, size, symbol
Output adds one column: label (normal | price_shock | wash_trade)

Session-aware: real market data has overnight/weekend gaps (markets
close). Anomalies are never injected across those gaps, and local
volatility is estimated per-session so injected shocks are scaled to
that session's actual behavior, not a global average.

Usage:
    python inject_anomalies.py --in AAPL_synthetic_ticks.csv \
        --out AAPL_ticks_labeled.csv --anomaly-rate 0.008
"""

import argparse

import numpy as np
import pandas as pd

SESSION_GAP = pd.Timedelta(minutes=10)  # gap larger than this = new session


def split_sessions(df):
    """Return a list of (start_idx, end_idx) exclusive-end tuples, one per
    contiguous trading session (no overnight/weekend gaps inside)."""
    gaps = df["ts"].diff() > SESSION_GAP
    session_id = gaps.cumsum()
    sessions = []
    for _, group in df.groupby(session_id):
        sessions.append((group.index[0], group.index[-1] + 1))
    return sessions


def inject_for_session(df, start, end, anomaly_rate, rng):
    """Mutates df in place for rows [start, end) of one contiguous session."""
    seg_prices = df.loc[start:end - 1, "price"].to_numpy()
    if len(seg_prices) < 20:
        return  # too short a session to bother

    # local sigma from tick-to-tick price diffs within this session only
    diffs = np.diff(seg_prices)
    local_sigma = max(np.std(diffs), 1e-4)

    labels = df.loc[start:end - 1, "label"].to_numpy()
    prices = df.loc[start:end - 1, "price"].to_numpy().copy()
    bids = df.loc[start:end - 1, "bid"].to_numpy().copy()
    asks = df.loc[start:end - 1, "ask"].to_numpy().copy()
    sizes = df.loc[start:end - 1, "size"].to_numpy().copy()

    n = len(prices)
    i = 0
    while i < n:
        if rng.random() >= anomaly_rate:
            i += 1
            continue

        if rng.random() < 0.5:
            # price_shock: jump, decay over next few ticks, bid/ask follow
            shock_mag = rng.uniform(4, 8) * local_sigma
            direction = 1 if rng.random() > 0.5 else -1
            decay_len = min(5, n - i)
            for j in range(decay_len):
                decay_factor = 1 - (j / decay_len)
                shift = direction * shock_mag * decay_factor
                prices[i + j] += shift
                bids[i + j] += shift
                asks[i + j] += shift
            labels[i] = "price_shock"
            i += decay_len
        else:
            # wash_trade: volume burst, price held ~flat around current level
            burst_len = min(rng.integers(4, 10), n - i)
            anchor = prices[i]
            for j in range(burst_len):
                jitter = rng.normal(0, local_sigma * 0.1)
                prices[i + j] = anchor + jitter
                spread = asks[i + j] - bids[i + j]
                bids[i + j] = prices[i + j] - spread / 2
                asks[i + j] = prices[i + j] + spread / 2
                sizes[i + j] = sizes[i + j] * rng.uniform(6, 15)  # volume spike
            labels[i] = "wash_trade"
            i += burst_len

    df.loc[start:end - 1, "price"] = prices
    df.loc[start:end - 1, "bid"] = bids
    df.loc[start:end - 1, "ask"] = asks
    df.loc[start:end - 1, "size"] = sizes
    df.loc[start:end - 1, "label"] = labels


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", default="AAPL_synthetic_ticks.csv")
    ap.add_argument("--out", default="AAPL_ticks_labeled.csv")
    ap.add_argument("--anomaly-rate", type=float, default=0.008,
                     help="Probability a given tick starts an anomaly event")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    df = pd.read_csv(args.in_path)
    df["ts"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["symbol", "ts"]).reset_index(drop=True)
    df["label"] = "normal"

    rng = np.random.default_rng(args.seed)

    for symbol, sym_df in df.groupby("symbol"):
        sym_df = sym_df.reset_index()  # keep 'index' = original df index
        orig_index = sym_df["index"].to_numpy()
        # work on a local frame with the original df's row labels remapped
        local = sym_df.drop(columns=["index"]).reset_index(drop=True)
        for start, end in split_sessions(local):
            inject_for_session(local, start, end, args.anomaly_rate, rng)
        # write results back to df via the original index mapping
        df.loc[orig_index, ["price", "bid", "ask", "size", "label"]] = \
            local[["price", "bid", "ask", "size", "label"]].to_numpy()

    # dtype-agnostic epoch-ms conversion (pandas may parse timestamps at us
    # or ns resolution depending on version; don't assume int64 // 1e6)
    epoch = pd.Timestamp("1970-01-01", tz="UTC")
    df["event_time_ms"] = ((df["ts"] - epoch) / pd.Timedelta(milliseconds=1)).astype("int64")
    out_df = df[["symbol", "event_time_ms", "price", "bid", "ask", "size", "label"]]
    out_df = out_df.rename(columns={"symbol": "ticker", "size": "volume"})
    out_df.to_csv(args.out, index=False)

    n_anom = (out_df["label"] != "normal").sum()
    print(f"Wrote {len(out_df)} ticks to {args.out}")
    print(f"  Injected anomalies: {n_anom} ({n_anom / len(out_df):.2%})")
    print(out_df["label"].value_counts().to_string())


if __name__ == "__main__":
    main()
