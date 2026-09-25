"""
Unit tests for the per-ticker rolling baseline logic in detect_anomalies.py.

Drives make_baseline_update_fn directly with a fake GroupState, bypassing
Spark's streaming runtime (and its checkpoint directory requirement -- see
the Windows-local dev note in README.md) so the core stateful algorithm can
be iterated on and verified without a Kafka+Spark cluster.

Run with: python -m pytest test_baseline.py -v
"""
import pickle

import pandas as pd
import pytest

from detect_anomalies import make_baseline_update_fn


def state_tuple(state):
    """Unpickle the (window, sum_price, sum_sq, sum_pv, sum_v, wash_hist)
    tuple out of the state blob -- see the BASELINE_STATE_SCHEMA comment in
    detect_anomalies.py for why it's a pickled blob rather than plain fields.
    `window` is a list of (raw_price, clipped_price, volume) triples: raw
    feeds VWAP's accumulators, clipped feeds the mean/variance accumulators
    -- see the bounded-influence comment in make_baseline_update_fn."""
    (blob,) = state.get
    return pickle.loads(blob)


def std_from_state(state):
    window, sum_price, sum_sq, _sum_pv, _sum_v, _wash_hist = state_tuple(state)
    n = len(window)
    mean = sum_price / n
    variance = max(sum_sq / n - mean * mean, 0.0)
    return variance**0.5


class FakeState:
    """Minimal stand-in for pyspark.sql.streaming.state.GroupState."""

    def __init__(self):
        self._value = None

    @property
    def exists(self):
        return self._value is not None

    @property
    def get(self):
        return self._value

    def update(self, value):
        self._value = value


def make_ticks(specs, ticker="AAPL", start_ms=1_700_000_000_000, step_ms=1000):
    """specs: list of (price, volume) tuples -> DataFrame in the shape the
    state function expects (one row per tick, columns matching the parsed
    Kafka payload plus event_time)."""
    rows = []
    for i, (price, volume) in enumerate(specs):
        ms = start_ms + i * step_ms
        rows.append(
            {
                "ticker": ticker,
                "event_time_ms": ms,
                "event_time": pd.Timestamp(ms, unit="ms"),
                "price": price,
                "volume": volume,
            }
        )
    return pd.DataFrame(rows)


def run(fn, pdf, state=None):
    state = state or FakeState()
    out = pd.concat(list(fn(("AAPL",), iter([pdf]), state)), ignore_index=True)
    return out, state


def test_warmup_period_is_unscored():
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=3.0, vwap_threshold=0.003)
    pdf = make_ticks([(100.0, 100.0)] * 9)
    out, _ = run(fn, pdf)
    assert out["zscore"].isna().all()
    assert out["is_anomaly"].eq(False).all()


def test_shock_tick_flagged_with_high_zscore():
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=3.0, vwap_threshold=0.003)
    specs = [(100.0 + (0.1 if i % 2 == 0 else -0.1), 100.0) for i in range(40)]
    specs.append((500.0, 100.0))
    pdf = make_ticks(specs)
    out, _ = run(fn, pdf)
    shock_row = out.iloc[-1]
    assert shock_row["is_anomaly"]
    assert shock_row["anomaly_type"] == "price_shock"
    assert shock_row["zscore"] > 3.0


def test_shock_contamination_is_bounded_not_runaway():
    """Regression test for a bug found by replaying the real labeled dataset
    through this function: an earlier version excluded flagged ticks from
    ever entering the baseline window (intended to stop a shock from
    dragging the baseline for the following window_size ticks). On real
    data that exclusion instead caused an UNBOUNDED feedback loop -- once a
    tick is excluded, the window's variance estimate shrinks, which makes
    the next tick more likely to be excluded too, compounding until ~89%%
    of all ticks were flagged (see stream-processing/README.md). Flagged
    ticks must be included in the window like any other tick (their
    contribution to the mean/variance accumulators clipped, not excluded --
    see test_bounded_influence_limits_baseline_widening_after_shock), so a
    shock's influence decays and disappears once it ages out of the window,
    rather than compounding forever."""
    window_size, min_samples = 100, 30
    fn = make_baseline_update_fn(
        window_size=window_size, min_samples=min_samples, z_threshold=3.0, vwap_threshold=0.01
    )
    stable = [(100.0 + (0.1 if i % 2 == 0 else -0.1), 100.0) for i in range(150)]
    shock = [(500.0, 100.0)]
    recovery = [(100.0 + (0.1 if i % 2 == 0 else -0.1), 100.0) for i in range(150)]
    pdf = make_ticks(stable + shock + recovery)
    out, _ = run(fn, pdf)

    shock_idx = len(stable)
    # Well after the shock has aged out of the window (window_size ticks
    # later), scoring must have fully recovered -- this is the part that a
    # runaway feedback loop would fail.
    tail = out.iloc[shock_idx + window_size + 20 :]
    assert not tail["is_anomaly"].any(), "flagging did not recover after the shock aged out of the window"


def test_bounded_influence_limits_baseline_widening_after_shock():
    """The specific failure mode bounded-influence clipping targets: folding
    a shock's *raw* price into the running mean/variance (rather than
    clipping its deviation first) blows the window's variance estimate wide
    open for as long as the shock sits in the window. Compare the window's
    std immediately before vs. immediately after the shock -- with clipping,
    a single tick can widen std by at most a small, bounded amount (roughly
    clip_k**2 / window_size worth of variance); without clipping, folding in
    a ~500 vs ~100 outlier would widen std by roughly two orders of
    magnitude."""
    window_size, min_samples, clip_k = 100, 30, 5.0
    fn = make_baseline_update_fn(
        window_size=window_size,
        min_samples=min_samples,
        z_threshold=3.0,
        vwap_threshold=0.01,
        clip_k=clip_k,
    )
    stable = [(100.0 + (0.1 if i % 2 == 0 else -0.1), 100.0) for i in range(150)]
    state = FakeState()
    _, state = run(fn, make_ticks(stable), state=state)
    std_before = std_from_state(state)
    assert std_before > 0

    shock_pdf = make_ticks([(500.0, 100.0)], start_ms=1_700_000_000_000 + len(stable) * 1000)
    _, state = run(fn, shock_pdf, state=state)
    std_after = std_from_state(state)

    # Bounded: a single clipped tick nudges std by a small amount. Unbounded
    # inclusion of a raw 500-vs-~100 outlier into a 100-tick window would
    # roughly 400x the std (from ~0.1 to ~40) -- well outside this bound.
    assert std_after < std_before * 2, (
        f"baseline widened too much after one shock: std {std_before:.4f} -> {std_after:.4f} "
        "(expected bounded growth, not the unbounded blow-up clipping is meant to prevent)"
    )


def test_window_evicts_oldest_and_sums_stay_consistent():
    # Small, steady drift (0.001/tick) so neither the Z-score nor VWAP-divergence
    # threshold trips -- a *monotonic* drift on the order of the price itself
    # (e.g. +1 on a ~100 base) diverges from the windowed VWAP fast enough to
    # get every post-warmup tick excluded from the window, which would make
    # this a test of the anomaly gate rather than of FIFO eviction.
    prices_in = [round(100.0 + 0.001 * i, 3) for i in range(12)]
    fn = make_baseline_update_fn(window_size=5, min_samples=3, z_threshold=3.0, vwap_threshold=0.003)
    pdf = make_ticks([(p, 10.0) for p in prices_in])
    out, state = run(fn, pdf)
    assert out["is_anomaly"].eq(False).all(), "small steady drift should not trip either threshold"

    window, sum_price, sum_sq, sum_pv, sum_v, _wash_hist = state_tuple(state)
    raw_prices = [w[0] for w in window]
    clipped_prices = [w[1] for w in window]
    assert len(window) == 5
    assert raw_prices == prices_in[-5:]
    assert abs(sum_price - sum(clipped_prices)) < 1e-6
    assert abs(sum_sq - sum(p * p for p in clipped_prices)) < 1e-6


def test_state_round_trips_across_batches():
    """A second call with a fresh iterator must resume from the previous
    call's saved state rather than starting cold."""
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=3.0, vwap_threshold=0.003)
    first_batch = make_ticks([(100.0, 100.0)] * 15, start_ms=1_700_000_000_000)
    out1, state = run(fn, first_batch)
    assert out1.iloc[-1]["zscore"] is not None

    second_batch = make_ticks([(100.0, 100.0)], start_ms=1_700_000_020_000)
    out2, state = run(fn, second_batch, state=state)
    assert not out2.empty
    window, *_ = state_tuple(state)
    assert len(window) == 16


def test_wash_trade_flagged_for_large_print_into_flat_market():
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01)
    # flat price, modest volume, then one outsized print at the same price
    specs = [(100.0, 100.0)] * 15 + [(100.0, 5000.0)]
    out, _ = run(fn, make_ticks(specs))
    wash_row = out.iloc[-1]
    assert wash_row["is_anomaly"]
    assert wash_row["anomaly_type"] == "wash_trade"
    assert not out.iloc[:-1]["is_anomaly"].any()


def test_wash_trade_not_flagged_when_market_is_moving():
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01)
    # trailing prices span ~1% -- well over the 0.1% flatness limit
    specs = [(100.0 + (0.5 if i % 2 == 0 else -0.5), 100.0) for i in range(15)]
    specs.append((100.0, 5000.0))
    out, _ = run(fn, make_ticks(specs))
    assert out.iloc[-1]["anomaly_type"] != "wash_trade"


def test_wash_trade_lookback_ignores_ticks_older_than_window():
    fn = make_baseline_update_fn(
        window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01, wash_lookback_ms=10_000
    )
    # a big-volume history that ages out of the 10s lookback, then small flat
    # ticks: the final print is only large relative to what's still in range
    old = make_ticks([(100.0, 100_000.0)] * 5, start_ms=1_700_000_000_000)
    recent = make_ticks([(100.0, 100.0)] * 5 + [(100.0, 1000.0)], start_ms=1_700_000_060_000)
    out, _ = run(fn, pd.concat([old, recent], ignore_index=True))
    assert out.iloc[-1]["anomaly_type"] == "wash_trade"


def test_price_shock_takes_precedence_over_wash_trade():
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=3.0, vwap_threshold=0.01)
    specs = [(100.0 + (0.01 if i % 2 == 0 else -0.01), 100.0) for i in range(15)]
    specs.append((150.0, 5000.0))  # both a huge move and an outsized print
    out, _ = run(fn, make_ticks(specs))
    assert out.iloc[-1]["anomaly_type"] == "price_shock"


def test_wash_history_round_trips_across_batches():
    """The trailing wash-trade lookback must survive a micro-batch boundary --
    the reason this heuristic lives in the stateful function rather than a
    per-batch window."""
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01)
    first = make_ticks([(100.0, 100.0)] * 15, start_ms=1_700_000_000_000)
    _, state = run(fn, first)
    second = make_ticks([(100.0, 5000.0)], start_ms=1_700_000_015_000)
    out2, _ = run(fn, second, state=state)
    assert out2.iloc[0]["anomaly_type"] == "wash_trade"


def test_resumes_from_pre_wash_trade_state_blob():
    """State checkpointed before the wash-trade detector moved into this
    function holds a 5-tuple; it must still load."""
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01)
    state = FakeState()
    old_window = [(100.0, 100.0, 100.0)] * 12
    state.update((pickle.dumps((old_window, 1200.0, 120000.0, 120000.0, 1200.0)),))
    out, state = run(fn, make_ticks([(100.0, 100.0)]), state=state)
    assert out.iloc[0]["zscore"] is not None
    assert len(state_tuple(state)) == 6


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
