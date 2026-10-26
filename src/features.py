"""
Real-Time Fraud Detection Engine: Feature Engineering Pipeline
Ensures 100% offline-online feature parity between Pandas batch training
and single-event Kafka stream scoring.
"""

from typing import Dict, Any, List
import numpy as np 

# Exact feature ordering expected by the trained LightGBM model
FEATURE_COLUMNS: List[str] = [
    "amount",
    "oldbalanceOrg",
    "oldbalanceDest",
    "is_transfer",
    "orig_diff",
    "dest_zero",
    "orig_zero",
    "exact_drain"
]

VULNERABLE_TYPES = {"TRANSFER", "CASH_OUT"}

def is_vulnerable_type(tx_type: str) -> bool:
    """Fast-path filter: checks if transaction type is prone to fraud."""
    return str(tx_type).upper() in VULNERABLE_TYPES
def extract_features(event:Dict[str, Any]) ->np.ndarray:
    """
    Transforms a single incoming Kafka transaction payload into an ordered 
    (1, 8) NumPy feature vector for model inference.
    """
    amount = float(event.get('amount', 0.0))
    old_orig = float(event.get('oldbalanceOrg', 0.0))
    old_dest = float(event.get('oldbalanceDest', 0.0))

    tx_type = str(event.get('type', '')).upper()

    #is_transfer = 1 if tx_type == 'TRANSFER' else 0
    is_transfer = 1.0 if tx_type == 'TRANSFER' else 0.0
    orig_diff = amount - old_orig
    dest_zero = 1.0 if old_dest == 0.0 else 0.0
    orig_zero = 1.0 if old_orig == 0.0 else 0.0
    exact_drain = 1.0 if (abs(amount-old_orig) < 1.0 and old_orig > 0.0) else 0.0



  # 3. Assemble vector in exact canonical order
    feature_vector = np.array([
        amount,
        old_orig,
        old_dest,
        is_transfer,
        orig_diff,
        dest_zero,
        orig_zero,
        exact_drain
    ], dtype=np.float32)
    # Return reshaped as (1, 8) for model inference
    return feature_vector.reshape(1, -1)

    

    