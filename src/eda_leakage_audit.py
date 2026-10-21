"""
Phase 1: Exploratory Data Analysis & Temporal Leakage Audit
Analyzes the PaySim dataset for:
1. Class Imbalance (fraud vs legitimate)
2. Transaction Type Breakdown (where does fraud happen?)
3. Prediction Moment & Balance Discrepancies
4. Temporal Distribution (step / hours)
"""

import sys
import pandas as pd
import numpy as np

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DATA_PATH = "data/PS_20174392719_1491204439457_log.csv"


def load_data():
    print(f"Loading dataset from: {DATA_PATH}...")
    df = pd.read_csv(DATA_PATH)
    print(f"Dataset loaded successfully: {df.shape[0]:,} rows, {df.shape[1]} columns.\n")
    return df


def audit_class_imbalance(df: pd.DataFrame):
    print("=" * 70)
    print("1. CLASS IMBALANCE AUDIT")
    print("=" * 70)
    counts = df["isFraud"].value_counts()
    percentages = df["isFraud"].value_counts(normalize=True) * 100

    summary = pd.DataFrame({
        "Count": counts,
        "Percentage (%)": percentages.round(4)
    })
    summary.index = ["Legitimate (0)", "Fraud (1)"]
    print(summary.to_string())

    fraud_count = counts.get(1, 0)
    fraud_pct = percentages.get(1, 0.0)
    print(f"\nKey Takeaway: Fraud occurs in only {fraud_pct:.3f}% ({fraud_count:,} out of {len(df):,}) transactions.")
    print("Failure Mode Warning: A naive model predicting '0' for everything would achieve")
    print(f"{100 - fraud_pct:.3f}% accuracy while catching ZERO fraud cases.\n")


def audit_transaction_types(df: pd.DataFrame):
    print("=" * 70)
    print("2. TRANSACTION TYPE BREAKDOWN")
    print("=" * 70)
    type_summary = df.groupby("type").agg(
        Total_Count=("isFraud", "count"),
        Fraud_Count=("isFraud", "sum"),
        Fraud_Rate_Pct=("isFraud", lambda x: (x.sum() / x.count()) * 100)
    ).sort_values("Fraud_Count", ascending=False)

    type_summary["Pct_of_All_Tx"] = (type_summary["Total_Count"] / len(df) * 100).round(2)
    type_summary["Fraud_Rate_Pct"] = type_summary["Fraud_Rate_Pct"].round(4)

    print(type_summary.to_string())

    # Check which types have zero fraud
    clean_types = type_summary[type_summary["Fraud_Count"] == 0].index.tolist()
    fraud_types = type_summary[type_summary["Fraud_Count"] > 0].index.tolist()
    print(f"\nKey Takeaway: Fraud ONLY occurs in: {fraud_types}")
    print(f"No fraud ever occurs in: {clean_types}\n")


def audit_prediction_moment_and_leakage(df: pd.DataFrame):
    print("=" * 70)
    print("3. PREDICTION MOMENT & BALANCE LEAKAGE AUDIT")
    print("=" * 70)

    # Filter to the vulnerable types (TRANSFER & CASH_OUT)
    vuln_df = df[df["type"].isin(["TRANSFER", "CASH_OUT"])].copy()

    # Rule: Does oldbalanceOrg - amount == newbalanceOrig?
    vuln_df["expected_orig_balance"] = vuln_df["oldbalanceOrg"] - vuln_df["amount"]
    vuln_df["orig_balance_discrepancy"] = vuln_df["newbalanceOrig"] - vuln_df["expected_orig_balance"]

    # Emptied sender account check
    vuln_df["account_emptied"] = (vuln_df["oldbalanceOrg"] > 0) & (vuln_df["newbalanceOrig"] == 0)

    print("Checking: How often does fraud completely empty the sender's account?")
    emptied_crosstab = pd.crosstab(
        vuln_df["account_emptied"],
        vuln_df["isFraud"],
        rownames=["Sender Account Emptied to 0"],
        colnames=["isFraud"]
    )
    print(emptied_crosstab.to_string())

    fraud_emptied = vuln_df[(vuln_df["isFraud"] == 1) & (vuln_df["account_emptied"])]
    fraud_total = vuln_df[vuln_df["isFraud"] == 1]
    pct_fraud_emptied = (len(fraud_emptied) / len(fraud_total)) * 100
    print(f"\nKey Takeaway: {pct_fraud_emptied:.1f}% of all frauds wipe the sender balance to exactly 0.")
    print("Production Reality: At the moment of scoring in Kafka, newbalanceOrig represents")
    print("the post-settlement outcome. Using it naively creates temporal leakage.\n")


def audit_temporal_distribution(df: pd.DataFrame):
    print("=" * 70)
    print("4. TEMPORAL DISTRIBUTION (STEP ANALYSIS)")
    print("=" * 70)
    min_step = df["step"].min()
    max_step = df["step"].max()
    unique_steps = df["step"].nunique()

    print(f"Step range: Step {min_step} to Step {max_step} ({unique_steps} total unique hours / ~{max_step / 24:.1f} days)")

    # Group by step to see fraud distribution over time
    step_agg = df.groupby("step").agg(
        Total_Tx=("isFraud", "count"),
        Fraud_Tx=("isFraud", "sum")
    )
    print(f"Average transactions per hour: {step_agg['Total_Tx'].mean():,.0f}")
    print(f"Average frauds per hour:       {step_agg['Fraud_Tx'].mean():.2f}")
    print("\nKey Takeaway: We must split our dataset chronologically by step.")
    print("K-Fold random splitting will shuffle future transactions into the training set (severe leakage).\n")


def main():
    df = load_data()
    audit_class_imbalance(df)
    audit_transaction_types(df)
    audit_prediction_moment_and_leakage(df)
    audit_temporal_distribution(df)
    print("=" * 70)
    print("[SUCCESS] Phase 1 EDA & Leakage Audit complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
