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

from detect_anomalies import load_state, make_baseline_update_fn


def std_from_state(state):
    st = load_state(state)
    n = len(st["window"])
    mean = st["sum_price"] / n
    variance = max(st["sum_sq"] / n - mean * mean, 0.0)
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

    st = load_state(state)
    raw_prices = [w[0] for w in st["window"]]
    clipped_prices = [w[1] for w in st["window"]]
    assert len(st["window"]) == 5
    assert raw_prices == prices_in[-5:]
    assert abs(st["sum_price"] - sum(clipped_prices)) < 1e-6
    assert abs(st["sum_sq"] - sum(p * p for p in clipped_prices)) < 1e-6


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
    assert len(load_state(state)["window"]) == 16


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


@pytest.mark.parametrize("n_fields", [5, 6])
def test_resumes_from_legacy_tuple_state_blob(n_fields):
    """Checkpoints written by earlier versions hold a positional tuple (5
    fields before the wash-trade detector moved in, 6 after); both must
    still load, and be re-saved in the current dict layout."""
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01)
    state = FakeState()
    legacy = ([(100.0, 100.0, 100.0)] * 12, 1200.0, 120000.0, 120000.0, 1200.0, [])[:n_fields]
    state.update((pickle.dumps(legacy),))
    out, state = run(fn, make_ticks([(100.0, 100.0)]), state=state)
    assert out.iloc[0]["zscore"] is not None, "resumed baseline should already be warm"
    (blob,) = state.get
    saved = pickle.loads(blob)
    assert isinstance(saved, dict)
    assert len(saved["window"]) == 13
    assert saved["late_dropped"] == 0


# --- overnight gap reset


def test_gap_resets_baseline_so_open_is_not_scored_against_prior_close():
    """Feed a day's close, then the next morning's open ~17.5h later at a
    very different price level. Without a reset the first morning ticks
    would be scored against yesterday's close (a huge z-score and a false
    price_shock). With it, the morning re-warms from scratch."""
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01)
    close_ms = 1_700_000_000_000
    close = make_ticks(
        [(100.0 + (0.01 if i % 2 == 0 else -0.01), 100.0) for i in range(30)], start_ms=close_ms
    )
    open_ms = close_ms + 30 * 1000 + int(17.5 * 3600 * 1000)
    morning = make_ticks(
        [(110.0 + (0.01 if i % 2 == 0 else -0.01), 100.0) for i in range(15)], start_ms=open_ms
    )

    _, state = run(fn, close)
    assert len(load_state(state)["window"]) == 20
    out, state = run(fn, morning, state=state)

    # first min_samples morning ticks are warm-up: unscored, never flagged
    assert out.iloc[:10]["zscore"].isna().all()
    assert not out["is_anomaly"].any()
    # the baseline now holds only morning ticks
    assert all(raw >= 109.0 for raw, _clipped, _vol in load_state(state)["window"])


def test_gap_reset_is_configurable_and_short_gaps_keep_the_baseline():
    fn = make_baseline_update_fn(
        window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01, gap_reset_ms=60_000
    )
    first = make_ticks([(100.0, 100.0)] * 12, start_ms=1_700_000_000_000)
    _, state = run(fn, first)
    # 50s later: under the 60s reset, the baseline carries over
    after_short = make_ticks([(100.0, 100.0)], start_ms=1_700_000_011_000 + 50_000)
    out, state = run(fn, after_short, state=state)
    assert out.iloc[0]["zscore"] is not None
    # 2 min later: over it, the baseline resets
    after_long = make_ticks([(100.0, 100.0)], start_ms=1_700_000_061_000 + 120_000)
    out, state = run(fn, after_long, state=state)
    assert out.iloc[0]["zscore"] is None
    assert len(load_state(state)["window"]) == 1


# --- late / out-of-order ticks


def test_out_of_order_ticks_within_a_batch_are_reordered_not_dropped():
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01)
    specs = [(100.0 + 0.01 * (i % 3), 100.0 + i) for i in range(40)]
    in_order = make_ticks(specs)
    shuffled = in_order.sample(frac=1.0, random_state=3).reset_index(drop=True)

    expected, _ = run(fn, in_order)
    out, state = run(fn, shuffled)
    assert len(out) == 40
    assert load_state(state)["late_dropped"] == 0
    pd.testing.assert_frame_equal(out, expected)


def test_tick_older_than_previous_batch_is_dropped_and_counted():
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01)
    ticks = make_ticks([(100.0, 100.0)] * 20)
    _, state = run(fn, ticks.iloc[:15])
    # batch 2 carries one straggler from before batch 1's last tick
    late = ticks.iloc[[7]]
    out, state = run(fn, pd.concat([late, ticks.iloc[15:]], ignore_index=True), state=state)

    assert len(out) == 5, "the straggler must not be emitted"
    assert load_state(state)["late_dropped"] == 1
    assert len(load_state(state)["window"]) == 20  # 15 + 5, straggler excluded


def test_same_timestamp_as_last_tick_is_not_late():
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01)
    ticks = make_ticks([(100.0, 100.0)] * 10)
    _, state = run(fn, ticks)
    same_ms = ticks.iloc[[-1]]
    out, state = run(fn, same_ms, state=state)
    assert len(out) == 1
    assert load_state(state)["late_dropped"] == 0


def test_replay_with_shuffled_stragglers_drops_exactly_the_late_ones():
    """Replay 300 ticks in 50-tick micro-batches, but hold a few ticks back
    and deliver them one batch late -- the way a slow partition or a retry
    would. Exactly those ticks are dropped and counted; everything else is
    emitted, and the running count survives across batches."""
    fn = make_baseline_update_fn(window_size=20, min_samples=10, z_threshold=5.0, vwap_threshold=0.01)
    ticks = make_ticks([(100.0 + 0.01 * (i % 5), 100.0) for i in range(300)])
    batches = [ticks.iloc[i : i + 50] for i in range(0, 300, 50)]
    held_back = {1: [10, 20, 30], 3: [5]}  # batch index -> row offsets delivered a batch late

    state = FakeState()
    emitted = 0
    carry = None
    for b, batch in enumerate(batches):
        late_rows = batch.iloc[held_back.get(b, [])]
        on_time = batch.drop(index=late_rows.index)
        feed = on_time if carry is None else pd.concat([carry, on_time])
        out, state = run(fn, feed.reset_index(drop=True), state=state)
        emitted += len(out)
        carry = late_rows if len(late_rows) else None

    assert load_state(state)["late_dropped"] == 4
    assert emitted == 300 - 4


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
