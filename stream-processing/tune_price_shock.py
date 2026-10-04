"""
tune_price_shock.py

Sweeps the detector's price-signal thresholds for the injected price
shocks (a 4-8 sigma jump on one tick, decaying over the next 5) and scores
every setting per event with evaluate_generalization.py's section 4 --
the same scoring as its README tables -- on the TRAINING split only.

Why the price CUSUM: each of its steps is the tick-to-tick move in units of
the ticker's tick sigma, which is exactly what the injector scales a shock
by. At the old defaults (k=2, h=6) a single step can add at most
clip_k - k = 3 < h, so the CUSUM could never fire on a one-tick jump; the
level z-score (price vs. a 20-tick window) carried price-shock detection
alone, and a jump is diluted there (median |z| 3.9 on the labeled shock
tick, against 4.0 at the 99th percentile of normal ticks).

Stage 1 sweeps cusum_k x cusum_h at the other defaults; stage 2 sweeps
zscore_threshold x ewma_threshold at the best stage-1 setting. A setting
qualifies only if no other anomaly type (wash_trade, shock_ramp,
wash_split) loses more than 0.005 F1 against the current defaults on the
training split; among those, the highest price_shock F1 wins. Then:

    python tune_price_shock.py                    # sweep on train (~10 min)
    python tune_price_shock.py --check k=1.5,h=2.5,z=5.0,ewma=2.75
        # one setting vs the defaults on train, test and (--heldout) held-out

The held-out variants are never used to choose; --check --heldout only
reports them. Run inside the pyspark container (detect_anomalies imports
pyspark), from stream-processing/.
"""

import argparse
from multiprocessing import Pool

import evaluate_generalization as eg

TYPES = ("price_shock", "wash_trade", "shock_ramp", "wash_split")
LABEL = {eg.VARIANT_LABELS[t]: t for t in eg.VARIANT_LABELS}
MAX_REGRESSION = 0.005
STAGE1 = [dict(cusum_k=k, cusum_h=h) for k in (1.0, 1.5, 2.0, 2.5) for h in (1.5, 2.0, 2.5, 3.0, 4.0, 6.0)]
STAGE2_Z = (4.5, 5.0, 5.5, 6.0, 7.0)
STAGE2_EWMA = (2.75, 3.0, 3.5, 4.0)
SHORT = {"cusum_k": "k", "cusum_h": "h", "z_threshold": "z", "ewma_threshold": "ewma"}

_DF = None


def _init():
    global _DF
    _DF = eg.load_data()


def score(overrides, period="train", heldout=False):
    kwargs = dict(eg.production_kwargs(), **overrides)
    det, _naive, inc = eg.variant_section(_DF, heldout, period=period, kwargs=kwargs, quiet=True)
    out = {LABEL.get(name, name): m for name, m in det}
    original = dict(inc)["original"]
    out["incident_precision"] = original["precision"]
    out["alerts_per_hour"] = original["total_per_hour"]
    return overrides, out


def fmt(overrides):
    return ", ".join(f"{SHORT.get(k, k)}={v}" for k, v in overrides.items()) or "defaults"


def table(rows, base):
    print(f"| {'setting':<34} | " + " | ".join(f"{t:>11}" for t in TYPES)
          + f" | {'inc. prec':>9} | {'alerts/h':>8} | ok |")
    print(f"|{'-' * 36}|" + "|".join("-" * 13 for _ in TYPES) + f"|{'-' * 11}|{'-' * 10}|----|")
    for o, m in rows:
        cells = " | ".join(f"{m[t]['f1']:>11.3f}" for t in TYPES)
        print(f"| {fmt(o):<34} | {cells} | {m['incident_precision']:>9.1%} | {m['alerts_per_hour']:>8.1f} | "
              f"{'yes' if qualifies(m, base) else 'no':>2} |")


def qualifies(m, base):
    return all(m[t]["f1"] >= base[t]["f1"] - MAX_REGRESSION for t in TYPES if t != "price_shock")


def best(rows, base):
    ok = [(o, m) for o, m in rows if qualifies(m, base)]
    return max(ok, key=lambda r: r[1]["price_shock"]["f1"])


def sweep(pool):
    _o, base = pool.apply(score, ({},))
    rows = pool.map(score, STAGE1)
    print("\n## Stage 1: price CUSUM k x h (train, per-event F1)\n")
    table([({}, base)] + rows, base)
    s1, m1 = best(rows + [({}, base)], base)
    print(f"\nStage 1 best: {fmt(s1)} (price_shock F1 {base['price_shock']['f1']:.3f} -> {m1['price_shock']['f1']:.3f})")

    stage2 = [dict(s1, z_threshold=z, ewma_threshold=e) for z in STAGE2_Z for e in STAGE2_EWMA]
    rows2 = pool.map(score, stage2)
    print("\n## Stage 2: z x EWMA threshold at the stage-1 CUSUM (train, per-event F1)\n")
    table([({}, base)] + rows2, base)
    s2, m2 = best(rows2 + [(s1, m1)], base)
    print(f"\nChosen on train: {fmt(s2)} (price_shock F1 {base['price_shock']['f1']:.3f} -> {m2['price_shock']['f1']:.3f})")
    print(f"Check it: python tune_price_shock.py --check {','.join(f'{SHORT.get(k, k)}={v}' for k, v in s2.items())}")


def check(pool, spec, heldout):
    long = {v: k for k, v in SHORT.items()}
    overrides = {long.get(k, k): float(v) for k, v in (p.split("=") for p in spec.split(","))}
    for period in ("train", "test", "all"):
        (_o, base), (_o2, new) = pool.starmap(score, [({}, period, heldout), (overrides, period, heldout)])
        types = list(TYPES) + (["shock_ramp_long", "wash_split_fine"] if heldout else [])
        print(f"\n## {period}: defaults -> {fmt(overrides)} (per event: precision / recall / F1)\n")
        for t in types:
            b, n = base[t], new[t]
            print(f"| {t:<16} | {b['precision']:6.1%} / {b['recall']:6.1%} / {b['f1']:.3f} -> "
                  f"{n['precision']:6.1%} / {n['recall']:6.1%} / {n['f1']:.3f} | {n['f1'] - b['f1']:+.3f} |")
        print(f"| incidents        | precision {base['incident_precision']:.1%} -> {new['incident_precision']:.1%}, "
              f"alerts/h {base['alerts_per_hour']:.1f} -> {new['alerts_per_hour']:.1f} |")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", help="score one setting (k=..,h=..,z=..,ewma=..) against the defaults")
    ap.add_argument("--heldout", action="store_true", help="with --check: also report the held-out variants")
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args()
    with Pool(a.workers, initializer=_init) as pool:
        check(pool, a.check, a.heldout) if a.check else sweep(pool)


if __name__ == "__main__":
    main()
