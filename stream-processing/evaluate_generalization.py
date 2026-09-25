"""
evaluate_generalization.py

Out-of-sample evaluation of detect_anomalies.py's detectors -- the honest
counterpart to calibrate_thresholds.py, which tunes and scores on the same
data. Four checks:

  1. Time split: tune (baseline window, z threshold, wash-trade params) on
     the first 70% of the timeline, score on the remaining 30%. Tick data
     is ordered, so a random split would leak the future into the past.
     Plus leave-one-ticker-out: tune on four tickers, score on the fifth.
  2. Baselines for context: random flagging at the detector's own flag
     rate (expected precision = prevalence), and a naive fixed threshold
     (|tick-to-tick return| for price shocks, raw volume for wash trades)
     tuned on the same training split.
  3. Precision-recall over z (train and test), to show whether the chosen
     z sits on a stable plateau or a lucky point.
  4. Harder injected variants, scored per event and per anomaly type,
     with the detector exactly as the Spark job runs it (its CLI
     defaults): multi-tick price ramps (the same 4-8 sigma move, spread
     over 8-20 ticks) and wash trades split into normal-sized pieces
     (1.5-3x volume over 15-30 ticks instead of 6-15x over 4-9). The
     detector and the original injector were written together, so scoring
     only the original injected styles is circular.

     A second, HELD-OUT set uses different parameters and a different
     seed (ramps over 25-40 ticks; splits into 1.2-2x pieces). It exists
     to check detector changes tuned on the variants above, so it is
     never used for tuning and is only generated with --heldout.

Drives the real make_baseline_update_fn (pandas, no Spark/Kafka) like
calibrate_thresholds.py. The detector is causal -- each tick is scored
only against ticks before it -- so it runs once over the full timeline and
splits are applied when scoring; `label` is only ever read for scoring and
for choosing thresholds on the training split.

Scoring note: the injector labels only the FIRST tick of each anomaly
event, though a price_shock moves prices for 5 ticks and a wash_trade burst
spans 4-9. Tick-level scores (sections 1-3, comparable to
calibrate_thresholds.py) therefore count flags on an event's later ticks as
false positives. Section 4 scores per event instead: an event counts as
detected if the matching detector flags any tick inside its span, and
flags inside another type's span are neither true nor false positives.
Original events' spans are approximated (5 ticks for price_shock, 9 -- the
injector's maximum -- for wash_trade); injected variants' spans are exact.

Run with:
    python evaluate_generalization.py                     # sections 1-4 (~2 min)
    python evaluate_generalization.py --sections variants  # section 4 only
    python evaluate_generalization.py --heldout            # adds the held-out set to section 4
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from detect_anomalies import detector_kwargs, make_baseline_update_fn, parse_args

DATA_PATH = (
    Path(__file__).resolve().parent.parent
    / "data-ingestion"
    / "aapl_msft_googl_tsla_nvda_ticks_labeled.csv"
)

TRAIN_FRACTION = 0.7
VWAP_THRESHOLD = 0.01
CLIP_K = 5.0
SESSION_GAP_MS = 10 * 60 * 1000  # same session split as data-ingestion/inject_anomalies.py

# (window_size, min_samples) candidates for the baseline
WINDOWS = [(10, 5), (15, 10), (20, 10), (25, 10), (30, 10), (50, 20), (100, 30)]
Z_GRID = [3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0]
PR_Z_GRID = [round(z, 2) for z in np.arange(2.0, 8.01, 0.5)]
# (wash_volume_ratio, wash_price_range) candidates
WASH_GRID = [(r, pr) for r in (0.2, 0.3, 0.5) for pr in (0.0005, 0.001, 0.002)]
NAIVE_QUANTILES = [0.9, 0.95, 0.98, 0.99, 0.995, 0.998, 0.999]

# Harder variants used while developing detector changes. Each kind
# belongs to the family of the original label it imitates.
EXISTING_VARIANTS = {
    "shock_ramp": {"family": "price_shock", "ramp_len": (8, 20)},
    "wash_split": {"family": "wash_trade", "mult": (1.5, 3.0), "burst_len": (15, 30)},
}
SEED = 7

# Held-out variants: different parameters and seed. Never used for tuning;
# only generated when --heldout is passed.
HELDOUT_VARIANTS = {
    "shock_ramp_long": {"family": "price_shock", "ramp_len": (25, 40)},
    "wash_split_fine": {"family": "wash_trade", "mult": (1.2, 2.0), "burst_len": (15, 30)},
}
HELDOUT_SEED = 20260925

FAMILY = {
    "price_shock": "price_shock",
    "wash_trade": "wash_trade",
    **{k: v["family"] for k, v in {**EXISTING_VARIANTS, **HELDOUT_VARIANTS}.items()},
}

VARIANT_LABELS = {
    "price_shock": "price_shock (original: 1-tick jump, decays over 5)",
    "wash_trade": "wash_trade (original: 6-15x volume, 4-9 ticks)",
    "shock_ramp": "shock_ramp (4-8 sigma over 8-20 ticks)",
    "wash_split": "wash_split (1.5-3x volume, 15-30 ticks)",
    "shock_ramp_long": "HELD-OUT shock_ramp_long (over 25-40 ticks)",
    "wash_split_fine": "HELD-OUT wash_split_fine (1.2-2x volume, 15-30)",
}


class FakeState:
    """Minimal stand-in for pyspark.sql.streaming.state.GroupState -- see
    test_baseline.py for the same pattern."""

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


# --------------------------------------------------------------------------
# running the detector


def load_data(path=DATA_PATH):
    df = pd.read_csv(path)
    df = df.sort_values(["ticker", "event_time_ms"], kind="stable").reset_index(drop=True)
    df["event_time"] = pd.to_datetime(df["event_time_ms"], unit="ms")
    return df


def raw_scores(df, window, min_samples, wash=(0.3, 0.001)):
    """Per-tick zscore, VWAP divergence and wash-trade flag, aligned to df's
    rows. Price-shock thresholds are applied afterwards (the z-score itself
    doesn't depend on the threshold), so one run serves the whole z grid."""
    kwargs = production_kwargs()
    kwargs.update(
        window_size=window,
        min_samples=min_samples,
        z_threshold=float("inf"),
        vwap_threshold=float("inf"),
        clip_k=CLIP_K,
        wash_volume_ratio=wash[0],
        wash_price_range=wash[1],
        # sections 1-3 study the z-score and original wash components only
        ewma_threshold=float("inf"),
        cusum_h=float("inf"),
        cusum_volume_h=float("inf"),
    )
    fn = make_baseline_update_fn(**kwargs)
    parts = []
    for ticker, g in df.groupby("ticker", sort=True):
        parts.append(pd.concat(list(fn((ticker,), iter([g]), FakeState())), ignore_index=True))
    out = pd.concat(parts, ignore_index=True)
    assert np.array_equal(out["price"].to_numpy(), df["price"].to_numpy()), "row alignment lost"
    return pd.DataFrame(
        {
            "zscore": pd.to_numeric(out["zscore"]).to_numpy(),
            "vwap_div": pd.to_numeric(out["vwap_divergence"]).to_numpy(),
            "wash": (out["anomaly_type"] == "wash_trade").to_numpy(),
        }
    )


def production_kwargs():
    """The detector exactly as the Spark job runs it with default flags --
    chosen in-sample on the full dataset, so test-split scores of these
    settings are optimistic by construction."""
    return detector_kwargs(parse_args([]))


def run_detector(df, kwargs):
    """Runs make_baseline_update_fn per ticker over df (sorted by ticker,
    event_time_ms) and returns its output aligned to df's rows."""
    fn = make_baseline_update_fn(**kwargs)
    parts = []
    for ticker, g in df.groupby("ticker", sort=True):
        parts.append(pd.concat(list(fn((ticker,), iter([g]), FakeState())), ignore_index=True))
    out = pd.concat(parts, ignore_index=True)
    assert np.array_equal(out["price"].to_numpy(), df["price"].to_numpy()), "row alignment lost"
    return out


def shock_flags(scores, z):
    # NaN (warm-up) compares False, i.e. unflagged
    return (np.abs(scores["zscore"].to_numpy()) > z) | (scores["vwap_div"].to_numpy() > VWAP_THRESHOLD)


def flags_for(shock_scores, wash_scores, z):
    """(price_shock flags, wash_trade flags) with detect_anomalies.py's
    precedence: a tick flagged as both is reported as price_shock."""
    shock = shock_flags(shock_scores, z)
    wash = wash_scores["wash"].to_numpy() & ~shock
    return shock, wash


# --------------------------------------------------------------------------
# scoring


def prf(flag, pos):
    flag = np.asarray(flag, bool)
    pos = np.asarray(pos, bool)
    tp = int((flag & pos).sum())
    n_flag = int(flag.sum())
    n_pos = int(pos.sum())
    p = tp / n_flag if n_flag else 0.0
    r = tp / n_pos if n_pos else 0.0
    f1 = 2 * p * r / (p + r) if p + r else 0.0
    return {"flagged": n_flag, "precision": p, "recall": r, "f1": f1}


def tick_metrics(labels, shock, wash, mask):
    lab = labels[mask]
    return {
        "price_shock": prf(shock[mask], lab == "price_shock"),
        "wash_trade": prf(wash[mask], lab == "wash_trade"),
        "any anomaly": prf((shock | wash)[mask], lab != "normal"),
    }


def random_baseline(flag_rate, prevalence):
    """Expected scores of flagging each tick independently at `flag_rate`."""
    p, r = prevalence, flag_rate
    f1 = 2 * p * r / (p + r) if p + r else 0.0
    return {"flagged": None, "precision": p, "recall": r, "f1": f1}


# --------------------------------------------------------------------------
# tuning


def tune(labels, shock_cache, wash_cache, mask):
    """Picks (window, z) by price_shock F1, then wash params by wash_trade F1
    (given that shock setting, since shock flags take precedence), using
    only rows in `mask`."""
    lab = labels[mask]
    best_shock = max(
        ((w, z) for w in shock_cache for z in Z_GRID),
        key=lambda wz: prf(shock_flags(shock_cache[wz[0]], wz[1])[mask], lab == "price_shock")["f1"],
    )
    shock = shock_flags(shock_cache[best_shock[0]], best_shock[1])
    best_wash = max(
        wash_cache,
        key=lambda wp: prf((wash_cache[wp]["wash"].to_numpy() & ~shock)[mask], lab == "wash_trade")["f1"],
    )
    return {"window": best_shock[0], "z": best_shock[1], "wash": best_wash}


def naive_features(df):
    ret = df.groupby("ticker")["price"].pct_change().abs().to_numpy()
    return np.nan_to_num(ret, nan=0.0), df["volume"].to_numpy()


def tune_naive(labels, ret, vol, mask):
    lab = labels[mask]

    def best(feature, target):
        cands = np.quantile(feature[mask], NAIVE_QUANTILES)
        return max(cands, key=lambda t: prf(feature[mask] > t, lab == target)["f1"])

    return best(ret, "price_shock"), best(vol, "wash_trade")


# --------------------------------------------------------------------------
# harder injected variants


def session_ids(df):
    gap = df.groupby("ticker")["event_time_ms"].diff() > SESSION_GAP_MS
    new_ticker = df["ticker"] != df["ticker"].shift()
    return (gap | new_ticker).cumsum().to_numpy()


def original_events(df):
    """Approximate spans of the originally injected events (see module
    docstring): (start_row, end_row_exclusive, kind)."""
    sess = session_ids(df)
    n = len(df)
    events = []
    for i in np.flatnonzero(df["label"].to_numpy() != "normal"):
        kind = df["label"].iat[i]
        end = min(i + (5 if kind == "price_shock" else 9), n)
        while end > i + 1 and sess[end - 1] != sess[i]:
            end -= 1
        events.append((i, end, kind))
    return events


def inject_hard(df, kind, spec, rng):
    """Returns (copy of df with extra events injected, their spans). Places
    one new event per original event of the same family per ticker, only in
    stretches at least 40 ticks clear of any labeled event, never across a
    session gap. Local sigma per session is estimated from tick-to-tick
    price diffs, matching data-ingestion/inject_anomalies.py."""
    df = df.copy()
    prices = df["price"].to_numpy().copy()
    volumes = df["volume"].to_numpy().copy()
    labels = df["label"].to_numpy()
    sess = session_ids(df)
    n = len(df)

    blocked = np.zeros(n, bool)
    for i in np.flatnonzero(labels != "normal"):
        blocked[max(i - 40, 0) : i + 40] = True

    sigma = pd.Series(prices).groupby(sess).transform(lambda s: max(s.diff().std(), 1e-4)).to_numpy()
    family = spec["family"]
    events = []
    for ticker, rows in df.groupby("ticker").groups.items():
        rows = np.asarray(rows)
        target = int((labels[rows] == family).sum())
        placed = 0
        for i in rng.permutation(rows):
            if placed >= target:
                break
            if family == "price_shock":
                ramp = int(rng.integers(spec["ramp_len"][0], spec["ramp_len"][1] + 1))
                length = ramp + 5
            else:
                length = int(rng.integers(spec["burst_len"][0], spec["burst_len"][1] + 1))
            end = i + length
            if end > rows[-1] + 1 or blocked[i:end].any() or sess[end - 1] != sess[i]:
                continue

            if family == "price_shock":
                # same total size as the original shocks (4-8 local sigma),
                # built up gradually over `ramp` ticks, then decaying over 5
                mag = rng.uniform(4, 8) * sigma[i] * (1 if rng.random() > 0.5 else -1)
                for j in range(ramp):
                    prices[i + j] += mag * (j + 1) / ramp
                for k in range(5):
                    prices[i + ramp + k] += mag * (1 - (k + 1) / 5)
            else:
                # flat price like the original wash trades, but each print
                # only a small multiple of its normal size, over many ticks
                anchor = prices[i]
                for j in range(length):
                    prices[i + j] = anchor + rng.normal(0, sigma[i] * 0.1)
                    volumes[i + j] *= rng.uniform(*spec["mult"])

            events.append((i, end, kind))
            blocked[max(i - 40, 0) : end + 40] = True
            placed += 1

    df["price"] = prices
    df["volume"] = volumes
    return df, events


def variant_datasets(df, variants, seed):
    """[(kind, injected copy of df, events)] -- one rng per set, consumed
    in the order the variants are listed, so each set is reproducible."""
    rng = np.random.default_rng(seed)
    out = []
    for kind, spec in variants.items():
        hard_df, events = inject_hard(df, kind, spec, rng)
        out.append((kind, hard_df, events))
    return out


def event_metrics(shock, wash, events, n):
    """Per-kind event recall and precision. Flags inside another kind's
    span are excluded from that kind's precision (neither TP nor FP)."""
    cover = {}
    for s, e, kind in events:
        cover.setdefault(kind, np.zeros(n, bool))[s:e] = True
    any_cover = np.zeros(n, bool)
    for c in cover.values():
        any_cover |= c

    results = {}
    for kind, c in cover.items():
        flags = shock if FAMILY[kind] == "price_shock" else wash
        spans = [(s, e) for s, e, k in events if k == kind]
        detected = sum(flags[s:e].any() for s, e in spans)
        counted = flags & ~(any_cover & ~c)
        tp_flags = int((counted & c).sum())
        n_flags = int(counted.sum())
        p = tp_flags / n_flags if n_flags else 0.0
        r = detected / len(spans) if spans else 0.0
        f1 = 2 * p * r / (p + r) if p + r else 0.0
        results[kind] = {"events": len(spans), "detected": int(detected), "precision": p, "recall": r, "f1": f1}
    return results


# --------------------------------------------------------------------------
# printing


def fmt_params(params):
    return f"window={params['window'][0]} (min_samples={params['window'][1]}), z={params['z']}, " f"wash ratio/range={params['wash'][0]}/{params['wash'][1]}"


def print_rows(title, rows):
    print(f"\n{title}\n")
    print(f"| {'':<44} | {'flagged':>7} | {'precision':>9} | {'recall':>7} | {'F1':>6} |")
    print(f"|{'-' * 46}|{'-' * 9}|{'-' * 11}|{'-' * 9}|{'-' * 8}|")
    for name, m in rows:
        flagged = "-" if m["flagged"] is None else f"{m['flagged']:,}"
        print(f"| {name:<44} | {flagged:>7} | {m['precision']:>9.2%} | {m['recall']:>7.2%} | {m['f1']:>6.3f} |")


def print_event_rows(rows):
    print(f"| {'type':<52} | {'events':>6} | {'detected':>8} | {'precision':>9} | {'recall':>7} | {'F1':>6} |")
    print(f"|{'-' * 54}|{'-' * 8}|{'-' * 10}|{'-' * 11}|{'-' * 9}|{'-' * 8}|")
    for name, m in rows:
        print(f"| {name:<52} | {m['events']:>6} | {m['detected']:>8} | {m['precision']:>9.2%} | "
              f"{m['recall']:>7.2%} | {m['f1']:>6.3f} |")


def detector_flags(out):
    types = out["anomaly_type"].to_numpy()
    return types == "price_shock", types == "wash_trade"


def variant_section(df, include_heldout, naive_thresholds=None):
    """Section 4: per-event scores by anomaly type for the detector as the
    Spark job runs it, on the original data and the harder variants (and the
    held-out variants only when asked). With naive_thresholds, the naive
    fixed-threshold baseline is scored on the same datasets."""
    n = len(df)
    kwargs = production_kwargs()
    sets = [("original", df, original_events(df))]
    sets += [(k, d, original_events(d) + e) for k, d, e in variant_datasets(df, EXISTING_VARIANTS, SEED)]
    if include_heldout:
        sets += [(k, d, original_events(d) + e) for k, d, e in variant_datasets(df, HELDOUT_VARIANTS, HELDOUT_SEED)]

    print("\n## 4. Per-event scores by anomaly type (detector at the job's defaults, all days)\n")
    det_rows, naive_rows = [], []
    for name, data, events in sets:
        kinds = ["price_shock", "wash_trade"] if name == "original" else [name]
        res = event_metrics(*detector_flags(run_detector(data, kwargs)), events, n)
        det_rows += [(VARIANT_LABELS[k], res[k]) for k in kinds]
        if naive_thresholds is not None:
            ret, vol = naive_features(data)
            naive_shock = ret > naive_thresholds[0]
            naive_wash = (vol > naive_thresholds[1]) & ~naive_shock
            res = event_metrics(naive_shock, naive_wash, events, n)
            naive_rows += [(VARIANT_LABELS[k], res[k]) for k in kinds]
    print_event_rows(det_rows)
    if naive_rows:
        print("\nNaive fixed-threshold baseline (thresholds tuned tick-level on the time-split train set):\n")
        print_event_rows(naive_rows)
    return det_rows, naive_rows


# --------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sections", nargs="+", choices=["split", "variants"], default=["split", "variants"],
                    help="split = sections 1-3 (time split, baselines, LOTO, PR over z); variants = section 4")
    ap.add_argument("--heldout", action="store_true",
                    help="also score the held-out variant set in section 4 (never use for tuning)")
    ap.add_argument("--naive", action="store_true", help="also score the naive baseline in section 4")
    cli = ap.parse_args()

    df = load_data()
    labels = df["label"].to_numpy()
    n = len(df)
    cutoff = np.quantile(df["event_time_ms"], TRAIN_FRACTION)
    train = df["event_time_ms"].to_numpy() <= cutoff
    test = ~train
    job = parse_args([])

    print(f"{n:,} ticks, {df['ticker'].nunique()} tickers, "
          f"{df['event_time'].dt.date.nunique()} trading days")
    print(f"Time split at {pd.Timestamp(cutoff, unit='ms')} UTC: "
          f"train {train.sum():,} ticks, test {test.sum():,} ticks")
    for name, m in (("all", np.ones(n, bool)), ("train", train), ("test", test)):
        lab = labels[m]
        print(f"  prevalence ({name:>5}): price_shock {np.mean(lab == 'price_shock'):.2%}, "
              f"wash_trade {np.mean(lab == 'wash_trade'):.2%}, any {np.mean(lab != 'normal'):.2%}")

    ret, vol = naive_features(df)
    naive_thresholds = tune_naive(labels, ret, vol, train)

    if "split" in cli.sections:
        split_sections(df, labels, train, test, job, naive_thresholds)
    if "variants" in cli.sections:
        variant_section(df, cli.heldout, naive_thresholds if cli.naive else None)


def split_sections(df, labels, train, test, job, naive_thresholds):
    """Sections 1-3: the Z-score and original wash-trade components, tuned on
    the training split and scored on the test split (tick-level)."""
    print("\nScoring detector variants (one pass per window / wash setting) ...")
    prod = {"window": (job.baseline_window, job.min_samples), "z": job.zscore_threshold,
            "wash": (job.wash_volume_ratio, job.wash_price_range)}
    windows = list(dict.fromkeys(WINDOWS + [prod["window"]]))
    shock_cache = {w: raw_scores(df, *w) for w in windows}
    wash_cache = {wp: raw_scores(df, 20, 10, wash=wp) for wp in dict.fromkeys(WASH_GRID + [prod["wash"]])}

    # ---- 1 + 2: time split with baselines
    tuned = tune(labels, shock_cache, wash_cache, train)
    print(f"\n## 1. Time split\n\nTuned on train: {fmt_params(tuned)}")
    print(f"Production defaults: {fmt_params(prod)} (chosen on ALL data -- optimistic)")

    tuned_flags = flags_for(shock_cache[tuned["window"]], wash_cache[tuned["wash"]], tuned["z"])
    prod_flags = flags_for(shock_cache[prod["window"]], wash_cache[prod["wash"]], prod["z"])
    tr_m = tick_metrics(labels, *tuned_flags, train)
    te_m = tick_metrics(labels, *tuned_flags, test)
    pr_m = tick_metrics(labels, *prod_flags, test)

    ret, vol = naive_features(df)
    naive_shock = ret > naive_thresholds[0]
    naive_wash = (vol > naive_thresholds[1]) & ~naive_shock
    nv_m = tick_metrics(labels, naive_shock, naive_wash, test)

    lab_te = labels[test]
    for det in ("price_shock", "wash_trade", "any anomaly"):
        prevalence = np.mean(lab_te != "normal") if det == "any anomaly" else np.mean(lab_te == det)
        flag_rate = te_m[det]["flagged"] / test.sum()
        rnd = random_baseline(flag_rate, prevalence)
        print_rows(
            f"### {det} (tick-level)",
            [
                ("tuned detector - train (in-sample)", tr_m[det]),
                ("tuned detector - TEST", te_m[det]),
                ("production defaults - test (leaky)", pr_m[det]),
                ("naive fixed threshold - test", nv_m[det]),
                ("random at detector's flag rate - test", rnd),
            ],
        )
        lift = te_m[det]["precision"] / prevalence if prevalence else float("nan")
        print(f"\nTest precision lift over random: {lift:.1f}x (prevalence {prevalence:.2%})")

    # ---- leave-one-ticker-out
    print("\n## Leave-one-ticker-out (tune on 4 tickers, all days; score the 5th)\n")
    print(f"| {'held out':<8} | {'tuned window/z':<14} | {'shock F1':>8} | {'own-best shock F1':>17} | "
          f"{'wash F1':>7} | {'own-best wash F1':>16} | {'any F1':>6} |")
    print(f"|{'-' * 10}|{'-' * 16}|{'-' * 10}|{'-' * 19}|{'-' * 9}|{'-' * 18}|{'-' * 8}|")
    tickers = df["ticker"].to_numpy()
    for t in sorted(df["ticker"].unique()):
        held = tickers == t
        p = tune(labels, shock_cache, wash_cache, ~held)
        oracle = tune(labels, shock_cache, wash_cache, held)
        m = tick_metrics(labels, *flags_for(shock_cache[p["window"]], wash_cache[p["wash"]], p["z"]), held)
        o = tick_metrics(labels, *flags_for(shock_cache[oracle["window"]], wash_cache[oracle["wash"]], oracle["z"]), held)
        print(f"| {t:<8} | {str(p['window'][0]) + ' / ' + str(p['z']):<14} | {m['price_shock']['f1']:>8.3f} | "
              f"{o['price_shock']['f1']:>17.3f} | {m['wash_trade']['f1']:>7.3f} | "
              f"{o['wash_trade']['f1']:>16.3f} | {m['any anomaly']['f1']:>6.3f} |")

    # ---- 3: precision-recall over z
    w = tuned["window"]
    print(f"\n## 3. price_shock precision-recall over z (window={w[0]}, tick-level)\n")
    print(f"| {'z':>4} | {'train P':>7} | {'train R':>7} | {'train F1':>8} | {'test P':>7} | {'test R':>7} | {'test F1':>7} |")
    print(f"|{'-' * 6}|{'-' * 9}|{'-' * 9}|{'-' * 10}|{'-' * 9}|{'-' * 9}|{'-' * 9}|")
    for z in PR_Z_GRID:
        s = shock_flags(shock_cache[w], z)
        a = prf(s[train], labels[train] == "price_shock")
        b = prf(s[test], labels[test] == "price_shock")
        mark = " <" if z == tuned["z"] else ""
        print(f"| {z:>4} | {a['precision']:>7.2%} | {a['recall']:>7.2%} | {a['f1']:>8.3f} | "
              f"{b['precision']:>7.2%} | {b['recall']:>7.2%} | {b['f1']:>7.3f} |{mark}")


if __name__ == "__main__":
    main()
