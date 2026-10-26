"""
Unit and Parity Tests for Real-Time Feature Engineering
Verifies zero training-serving skew between offline batch and online streaming.
"""

import pytest
import numpy as np
import pandas as pd
from src.features import is_vulnerable_type, extract_features, FEATURE_COLUMNS


def test_is_vulnerable_type():
    """Verify early-exit filter correctly separates vulnerable from non-vulnerable types."""
    assert is_vulnerable_type("TRANSFER") is True
    assert is_vulnerable_type("CASH_OUT") is True
    assert is_vulnerable_type("transfer") is True  # Case insensitive
    assert is_vulnerable_type("cash_out") is True

    # Safe types that must bypass ML scoring
    assert is_vulnerable_type("PAYMENT") is False
    assert is_vulnerable_type("CASH_IN") is False
    assert is_vulnerable_type("DEBIT") is False
    assert is_vulnerable_type("UNKNOWN") is False


def test_feature_vector_shape_and_types():
    """Verify single event returns exact (1, 8) float32 vector."""
    event = {
        "type": "TRANSFER",
        "amount": 1000.0,
        "oldbalanceOrg": 5000.0,
        "oldbalanceDest": 0.0
    }
    vec = extract_features(event)
    assert isinstance(vec, np.ndarray)
    assert vec.shape == (1, 8)
    assert vec.dtype == np.float32


def test_exact_drain_edge_cases():
    """Verify edge case logic for penny-exact account emptying."""
    # Case 1: Exact balance drain with positive balance -> exact_drain = 1.0
    drain_event = {"type": "TRANSFER", "amount": 250.0, "oldbalanceOrg": 250.0, "oldbalanceDest": 0.0}
    vec = extract_features(drain_event)
    exact_drain_val = vec[0, FEATURE_COLUMNS.index("exact_drain")]
    assert exact_drain_val == 1.0

    # Case 2: Zero balance with zero amount -> exact_drain = 0.0 (cannot drain an empty account)
    zero_event = {"type": "TRANSFER", "amount": 0.0, "oldbalanceOrg": 0.0, "oldbalanceDest": 0.0}
    vec_zero = extract_features(zero_event)
    assert vec_zero[0, FEATURE_COLUMNS.index("exact_drain")] == 0.0


def test_offline_online_parity_on_dataset():
    """
    CRITICAL AUDIT:
    Compares offline Pandas transformation vs online streaming extract_features()
    across 1,000 real PaySim transactions.
    """
    data_path = "data/PS_20174392719_1491204439457_log.csv"
    sample_df = pd.read_csv(data_path, nrows=1000)

    # 1. Offline Pandas transformation (from Phase 2)
    sample_df["is_transfer"] = (sample_df["type"] == "TRANSFER").astype(np.float32)
    sample_df["orig_diff"] = (sample_df["amount"] - sample_df["oldbalanceOrg"]).astype(np.float32)
    sample_df["dest_zero"] = (sample_df["oldbalanceDest"] == 0.0).astype(np.float32)
    sample_df["orig_zero"] = (sample_df["oldbalanceOrg"] == 0.0).astype(np.float32)
    sample_df["exact_drain"] = (
        (np.abs(sample_df["amount"] - sample_df["oldbalanceOrg"]) < 1.0) & (sample_df["oldbalanceOrg"] > 0.0)
    ).astype(np.float32)

    offline_matrix = sample_df[FEATURE_COLUMNS].to_numpy(dtype=np.float32)

    # 2. Online streaming transformation (one-by-one dictionary ingestion)
    online_vectors = []
    for _, row in sample_df.iterrows():
        event = row.to_dict()
        vec = extract_features(event)
        online_vectors.append(vec.flatten())

    online_matrix = np.array(online_vectors, dtype=np.float32)

    # 3. Assert mathematical parity across all 1,000 rows x 8 features
    np.testing.assert_allclose(
        offline_matrix,
        online_matrix,
        rtol=1e-5,
        atol=1e-5,
        err_msg="Training-Serving Skew detected! Offline and Online features do not match."
    )
