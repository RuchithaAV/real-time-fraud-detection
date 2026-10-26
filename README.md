# Real-Time Fraud Detection Engine

A production-grade, event-driven fraud detection engine built with Apache Kafka, Redis, PostgreSQL, and a gradient-boosted decision tree model (LightGBM/XGBoost), monitored via Streamlit.

The system processes financial transactions in simulated real-time, extracts historical and rolling behavioral features from an in-memory state store without data leakage, performs sub-millisecond fraud scoring, and persists high-risk alerts for investigator triage.

---

## Architecture Overview

```text
                                  +-----------------------------+
                                  |   Historical PaySim Data    |
                                  +-----------------------------+
                                                 |
                                                 v
                                   [ Replay Producer (Python) ]
                                                 |
                                                 v
                                    ( Kafka Topic: transactions )
                                                 |
                                                 v
+-----------------------+           [ Scoring Consumer (Python) ]
|  Redis State Store    | <------->   - State lookup (pre-tx)
| (Rolling aggregations)|             - Feature parity transformation
+-----------------------+             - ML Model Inference
                                      - State update (post-tx)
                                                 |
                          +----------------------+----------------------+
                          |                                             |
                          v                                             v
               [ All Transactions ]                            [ Fraud Alerts ]
                          |                                             |
                          v                                             v
            ( Kafka: processed-tx )                         ( PostgreSQL: alerts )
                                                                        |
                                                                        v
                                                            [ Streamlit Dashboard ]
```

---

## Tech Stack

* **Streaming Backbone**: Apache Kafka (KRaft mode / Zookeeper-less, port `9092`)
* **Real-Time State Store**: Redis 7 Alpine (sliding window transactional metrics, port `6379`)
* **Persistent Storage**: PostgreSQL 16 Alpine (fraud alerts, auditing, triage logs, port `5433`)
* **Kafka Driver**: `confluent-kafka` (official C-based `librdkafka` client)
* **ML Modeling**: LightGBM, Scikit-learn
* **Containerization**: Docker Compose
* **Live Dashboard**: Streamlit
* **Dataset**: PaySim (Synthetic Financial Dataset for Fraud Detection)

---

## Machine Learning & Streaming Principles

1. **Explicit Prediction Moment**:
   * Features represent account history **prior** to the current transaction.
   * Rolling counters (e.g., transaction count in last $N$ hours, velocity) are computed before updating the state store with the current event to avoid temporal leakage.
2. **Temporal Validation**:
   * Strict time-based train/validation/test splits simulating real-world deployment.
   * Preprocessing pipelines fit solely on historical training windows.
3. **Metric Focus Under Severe Imbalance**:
   * Evaluation prioritized on Precision-Recall AUC (PR-AUC), Precision, Recall, and Alert Rate (triage capacity).
   * Threshold tuning guided by an explicit business cost-benefit matrix (cost of false negatives vs. cost of false positives).
4. **Streaming Semantics**:
   * Message partitioning keyed by originating account to guarantee sequential processing order per entity.
   * Consumer deduplication logic to handle at-least-once delivery guarantees.

---

## Project Roadmap & Milestones

- [x] **Phase 0: Infrastructure & Environment Setup**
  * Repository structure, strict `.gitignore` rules, and Git hygiene.
  * Isolated Python 3.13 virtual environment with modern dependencies.
  * Multi-container Docker Compose infrastructure (KRaft Kafka, Redis, PostgreSQL).
  * Resolved native port collisions (PostgreSQL mapped to port `5433:5432`).
  * End-to-end integration sanity check (`src/sanity_check.py`) verifying Python connectivity to Kafka, Redis, and PostgreSQL.

- [x] **Phase 1: PaySim EDA & Temporal Leakage Audit**
  * Ingestion schema optimization: downcasted dtypes (categorical types, int16/int8, float32) reducing in-memory footprint by **47%** (116.4 MB $\rightarrow$ 62.0 MB).
  * Quantified extreme class imbalance: **0.1089% fraud** (1,142 out of 1,048,575 transactions), ruling out accuracy in favor of PR-AUC.
  * Transaction vulnerability profiling: proved **100% of frauds** occur strictly in `TRANSFER` and `CASH_OUT` (zero fraud in `PAYMENT`, `CASH_IN`, `DEBIT`), enabling an early-exit consumer filter for ~56% of traffic.
  * Uncovered post-settlement leakage: 97.5% of frauds wipe sender balance to `$0` (`newbalanceOrig == 0`).
  * Engineered leakage-free pre-transaction feature: $\text{balance\_drain\_ratio} = \frac{\text{amount}}{\text{oldbalanceOrg} + 1.0}$ (fraud median $\approx 1.0$).
  * Chronological out-of-time split aligned with 24-hour diurnal business cycles:
    * **Train**: Steps 1–35 (288,668 rows, 0.138% fraud)
    * **Validation**: Steps 36–42 (112,178 rows, 0.089% fraud)
    * **Test**: Steps 43–95 (59,548 rows, 1.081% fraud held out for final evaluation)

- [x] **Phase 2: Offline Baseline, LightGBM, and Cost-Optimized Threshold Selection**
  * **Baseline Model**: Logistic Regression with `StandardScaler` (fit on Train only) established a performance floor of **0.1422 PR-AUC**, proving linear boundaries fail on multi-condition fraud patterns.
  * **Feature Engineering**: Diagnosed ratio instability (`oldbalanceOrg == 0`) and engineered robust domain signals: `exact_drain` (penny-exact balance theft), `orig_diff` ($\text{amount} - \text{oldbalanceOrg}$), `dest_zero` (new mule accounts), `orig_zero`, and `is_transfer`.
  * **Gradient Boosted Classifier (LightGBM)**: Non-linear decision trees jumped PR-AUC from 0.1422 to **0.9706** on the Validation set.
  * **Business Cost-Matrix Optimization**: Swept decision thresholds against asymmetric loss ($5,000 per missed fraud vs. $25 per false alarm), discovering an optimal alert threshold of **$T^* = 0.4258 \approx 0.43$** (outperforming arbitrary 0.50 cutoff).
  * **Final Held-Out Test Evaluation (Steps 43–95 / 59,548 transactions)**:
    * **Test PR-AUC**: **0.9971**
    * **Recall**: **99.53%** (641 of 644 live frauds intercepted; only 3 missed)
    * **Precision**: **100.00%** (0 false positives; zero innocent customers declined)
    * **Alert Rate**: **1.076%** of total stream traffic (within human investigator triage capacity)
  * **Model Serialization**: Exported production weights to [`models/fraud_detector_lgbm.joblib`](models/fraud_detector_lgbm.joblib) and inference schema to [`models/model_metadata.json`](models/model_metadata.json).

- [x] **Phase 3: Streaming-Safe Feature Pipeline & Offline/Online Parity**
  * **Feature Contract**: Implemented [`src/features.py`](src/features.py) with canonical 8-feature schema matching model serialization metadata.
  * **Fast-Path Early-Exit Filter**: Added `is_vulnerable_type()` routing safe payment types (`PAYMENT`, `CASH_IN`, `DEBIT`) around ML scoring, saving 56% downstream compute overhead.
  * **Single-Event Ingestion**: Built `extract_features(event: dict) -> np.ndarray` supporting single-message Kafka JSON ingestion with defensive casting, zero temporal leakage, and C-contiguous float32 vector generation.
  * **Automated Parity Test Suite**: Developed [`tests/test_features.py`](tests/test_features.py) running a 1,000-transaction audit proving mathematical parity (`atol=1e-5`) between batch Pandas and single-event streaming, formally eliminating Training-Serving Skew.

- [ ] **Phase 4**: Event-driven transaction replay producer with configurable rate limiting.
- [ ] **Phase 5**: Scoring consumer integrating Redis state lookups, real-time inference, and state updates.
- [ ] **Phase 6**: PostgreSQL alert storage and interactive Streamlit monitoring dashboard.
- [ ] **Phase 7**: End-to-end latency profiling, stress testing, and documentation.

---

## Quickstart

### 1. Environment Setup
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Start Infrastructure Containers
```powershell
docker compose -f docker/docker-compose.yml up -d
```

### 3. Run Integration Sanity Check
```powershell
python src/sanity_check.py
```

### 4. Interactive Exploratory Data Analysis & Model Training
* [`notebooks/01_eda_and_leakage_audit.ipynb`](notebooks/01_eda_and_leakage_audit.ipynb)
* [`notebooks/02_model_training_and_threshold_tuning.ipynb`](notebooks/02_model_training_and_threshold_tuning.ipynb)

### 5. Run Parity Test Suite
```powershell
pytest -v
```
