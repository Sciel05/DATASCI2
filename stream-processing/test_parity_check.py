"""
Unit tests for parity_check.py's comparison logic -- the part that decides
whether the streaming and offline outputs agree. The pipeline run itself
needs Kafka + Spark (see README.md, "Parity check").

Run with: python -m pytest test_parity_check.py -v
"""
import json

import numpy as np
import pandas as pd

from parity_check import compare_frames, parse_spark_rows


def offline_frame(n=5):
    return pd.DataFrame(
        {
            "ticker": ["AAPL"] * n,
            "ts_ms": [1_700_000_000_000 + i * 1000 for i in range(n)],
            "price": [100.0 + i for i in range(n)],
            "volume": [10.0] * n,
            "zscore": [np.nan, np.nan, 0.5, 1.5, 6.0][:n],
            "vwap_divergence": [np.nan, np.nan, 0.001, 0.002, 0.003][:n],
            "is_anomaly": [False, False, False, False, True][:n],
            "anomaly_type": [None, None, None, None, "price_shock"][:n],
        }
    )


def test_identical_frames_have_no_problems():
    off = offline_frame()
    problems, counts, _, n = compare_frames(off, off.copy())
    assert problems == []
    assert n == 5
    assert all(v == 0 for v in counts.values())


def test_tiny_float_noise_is_tolerated_but_real_differences_are_not():
    off = offline_frame()
    spk = off.copy()
    spk.loc[2, "zscore"] += 1e-12
    assert compare_frames(off, spk)[0] == []
    spk.loc[3, "zscore"] += 1e-3
    problems, counts, examples, _ = compare_frames(off, spk)
    assert counts["zscore"] == 1
    assert "zscore" in examples


def test_nan_vs_value_is_a_mismatch():
    off = offline_frame()
    spk = off.copy()
    spk.loc[0, "zscore"] = 0.0
    assert compare_frames(off, spk)[1]["zscore"] == 1


def test_flag_and_type_mismatches_are_reported():
    off = offline_frame()
    spk = off.copy()
    spk.loc[4, "is_anomaly"] = False
    spk.loc[4, "anomaly_type"] = None
    counts = compare_frames(off, spk)[1]
    assert counts["is_anomaly"] == 1
    assert counts["anomaly_type"] == 1


def test_missing_extra_and_duplicate_rows_are_reported():
    off = offline_frame()
    spk = pd.concat([off.iloc[1:], off.iloc[[4]]], ignore_index=True)  # drop row 0, duplicate row 4
    spk = pd.concat([spk, off.iloc[[0]].assign(ts_ms=1)], ignore_index=True)  # a row offline never produced
    problems = compare_frames(off, spk)[0]
    assert any("duplicated" in p for p in problems)
    assert any("missing from spark output" in p for p in problems)
    assert any("no offline counterpart" in p for p in problems)


def test_parse_spark_rows_reads_kafka_json_values():
    rows = [
        {"ticker": "AAPL", "timestamp": "2026-09-16T13:30:00.018Z", "price": 1.0, "volume": 2.0,
         "is_anomaly": False},
        {"ticker": "AAPL", "timestamp": "2026-09-16T13:30:01.000Z", "price": 1.1, "volume": 2.0,
         "zscore": 6.1, "vwap_divergence": 0.0, "is_anomaly": True, "anomaly_type": "price_shock"},
    ]
    out = parse_spark_rows(["Processed a total of 2 messages"] + [json.dumps(r) for r in rows])
    assert list(out["ts_ms"]) == [1789565400018, 1789565401000]
    assert np.isnan(out.loc[0, "zscore"])  # omitted null field
    assert out.loc[1, "anomaly_type"] == "price_shock"
