"""
calibrate_thresholds.py

Sweeps the Z-score and VWAP-divergence thresholds against the labeled
dataset to find where the real precision/recall trade-off curve sits, so
defaults in detect_anomalies.py get picked deliberately rather than guessed.
Drives the REAL make_baseline_update_fn directly (pandas, no Spark/Kafka
needed -- same approach as test_baseline.py) so there's no risk of the
calibration logic drifting from what the Spark job actually runs.

`label` is read here ONLY for offline evaluation, exactly like
test_baseline.py and the README's "Evaluating against ground truth"
section -- never fed into the detector itself.

Two-stage sweep (the full cross product isn't necessary):
  1. z_threshold in [3.0 .. 7.0] at the current default vwap_threshold --
     wide enough to find where F1 actually peaks rather than just hit the
     edge of a narrower range.
  2. vwap_threshold in [0.005, 0.01, 0.015] at the production z_threshold.
     See stream-processing/README.md's Calibration section for why the
     production default sits on the recall side of the F1 peak.

Price-shock precision/recall count only anomaly_type == "price_shock"
flags, so the wash-trade detector (which shares the same state function)
isn't scored as price-shock false positives. Wash-trade and combined
numbers are reported alongside.

window_size, min_samples, and clip_k are held at detect_anomalies.py's
current defaults throughout -- this sweep is scoped to the two flagging
thresholds only. Does NOT change any defaults in detect_anomalies.py; it
only reports numbers.

Run with: python calibrate_thresholds.py
"""

from pathlib import Path

import pandas as pd

from detect_anomalies import make_baseline_update_fn

# resolved relative to this file, so it works natively and inside the Docker
# container (where the repo is mounted at /workspace)
DATA_PATH = (
    Path(__file__).resolve().parent.parent
    / "data-ingestion"
    / "aapl_msft_googl_tsla_nvda_ticks_labeled.csv"
)

WINDOW_SIZE = 20
MIN_SAMPLES = 10
CLIP_K = 5.0

DEFAULT_VWAP_THRESHOLD = 0.01
# The chosen production default (detect_anomalies.py --zscore-threshold) --
# deliberately on the recall side of the F1 peak found by the sweep below.
# See README.md's Calibration section for why.
PRODUCTION_Z_THRESHOLD = 5.0

Z_SWEEP = [3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0]
VWAP_SWEEP = [0.005, 0.01, 0.015]


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


def load_data():
    df = pd.read_csv(DATA_PATH)
    df["event_time"] = pd.to_datetime(df["event_time_ms"], unit="ms")
    return df


def run_detector(df, z_threshold, vwap_threshold):
    fn = make_baseline_update_fn(
        window_size=WINDOW_SIZE,
        min_samples=MIN_SAMPLES,
        z_threshold=z_threshold,
        vwap_threshold=vwap_threshold,
        clip_k=CLIP_K,
    )
    results = []
    for ticker, g in df.groupby("ticker"):
        g = g.sort_values("event_time_ms")
        state = FakeState()
        out = pd.concat(list(fn((ticker,), iter([g]), state)), ignore_index=True)
        out["label"] = g["label"].values
        results.append(out)
    return pd.concat(results, ignore_index=True)


def score(full, flagged=None, positive=None):
    """Defaults to scoring the price-shock detector against price_shock labels."""
    if flagged is None:
        flagged = full["anomaly_type"] == "price_shock"
    if positive is None:
        positive = full["label"] == "price_shock"
    tp = (flagged & positive).sum()
    fp = (flagged & ~positive).sum()
    total_flagged = flagged.sum()
    total_shock = positive.sum()
    precision = tp / total_flagged if total_flagged else 0.0
    recall = tp / total_shock if total_shock else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {
        "flagged": int(total_flagged),
        "flag_rate": total_flagged / len(full),
        "tp": int(tp),
        "fp": int(fp),
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def print_table(rows, param_name):
    header = f"{param_name:>16} | {'flagged':>8} {'flag%':>7} {'tp':>5} {'fp':>7} {'precision':>10} {'recall':>8} {'f1':>7}"
    print(header)
    print("-" * len(header))
    for param_value, s in rows:
        print(
            f"{param_value:>16} | {s['flagged']:>8} {s['flag_rate']:>6.2%} {s['tp']:>5} {s['fp']:>7} "
            f"{s['precision']:>9.2%} {s['recall']:>7.2%} {s['f1']:>7.4f}"
        )
    print()


def main():
    print(f"Loading {DATA_PATH} ...")
    df = load_data()
    print(f"{len(df)} ticks, {(df['label']=='price_shock').sum()} price_shock labels\n")
    print(f"Fixed: window_size={WINDOW_SIZE} min_samples={MIN_SAMPLES} clip_k={CLIP_K}\n")

    print(f"=== Stage 1: z_threshold sweep (vwap_threshold fixed at {DEFAULT_VWAP_THRESHOLD}) ===\n")
    z_rows = []
    for z_t in Z_SWEEP:
        s = score(run_detector(df, z_t, DEFAULT_VWAP_THRESHOLD))
        z_rows.append((z_t, s))
    print_table(z_rows, "z_threshold")

    best_z, best_z_score = max(z_rows, key=lambda item: item[1]["f1"])
    print(f"F1-optimal z_threshold: {best_z} (F1={best_z_score['f1']:.4f}, "
          f"precision={best_z_score['precision']:.2%}, recall={best_z_score['recall']:.2%})")
    print(f"Chosen production default is z={PRODUCTION_Z_THRESHOLD} instead -- "
          f"recall-favoring, see module docstring.\n")

    print(f"=== Stage 2: vwap_threshold sweep (z_threshold fixed at production "
          f"default={PRODUCTION_Z_THRESHOLD}, not the F1-optimal {best_z}) ===\n")
    vwap_rows = []
    for v_t in VWAP_SWEEP:
        s = score(run_detector(df, PRODUCTION_Z_THRESHOLD, v_t))
        vwap_rows.append((v_t, s))
    print_table(vwap_rows, "vwap_threshold")

    best_v, best_v_score = max(vwap_rows, key=lambda item: item[1]["f1"])
    print(f"Best vwap_threshold by F1 (at z_threshold={PRODUCTION_Z_THRESHOLD}): {best_v} "
          f"(F1={best_v_score['f1']:.4f}, precision={best_v_score['precision']:.2%}, "
          f"recall={best_v_score['recall']:.2%})\n")

    full = run_detector(df, PRODUCTION_Z_THRESHOLD, DEFAULT_VWAP_THRESHOLD)
    print(f"=== Production defaults (z={PRODUCTION_Z_THRESHOLD}, vwap={DEFAULT_VWAP_THRESHOLD}): "
          f"every detector ===\n")
    print_table(
        [
            ("price_shock", score(full)),
            ("wash_trade", score(full, full["anomaly_type"] == "wash_trade", full["label"] == "wash_trade")),
            ("any anomaly", score(full, full["is_anomaly"].astype(bool), full["label"] != "normal")),
        ],
        "detector",
    )

    print("Note: this only reports numbers -- detect_anomalies.py's defaults are unchanged by this script.")


if __name__ == "__main__":
    main()

