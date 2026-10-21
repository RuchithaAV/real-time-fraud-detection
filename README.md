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
  * Chronological out-of-time split across 95 hours:
    * **Train**: Steps 1–65 (~68% of data)
    * **Validation**: Steps 66–80 (~16% of data)
    * **Test**: Steps 81–95 (~16% of data, held out for final evaluation)

- [ ] **Phase 2**: Offline baseline and boosted model (LightGBM), threshold & cost optimization.
- [ ] **Phase 3**: Streaming-safe feature engineering pipeline ensuring strict offline/online parity.
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

### 4. Interactive Exploratory Data Analysis
Open [`notebooks/01_eda_and_leakage_audit.ipynb`](notebooks/01_eda_and_leakage_audit.ipynb) using the `.venv` Python kernel.
