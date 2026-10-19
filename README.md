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

* **Streaming Backbone**: Apache Kafka (KRaft mode / Zookeeper-less)
* **Real-Time State Store**: Redis (sliding window transactional metrics, account frequency counters)
* **Persistent Storage**: PostgreSQL (fraud alerts, auditing, triage logs)
* **ML Modeling**: LightGBM / XGBoost, Scikit-learn
* **Containerization**: Docker Compose
* **Live Dashboard**: Streamlit
* **Dataset**: PaySim (Synthetic Financial Datasets for Fraud Detection)

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

## Roadmap & Milestones

- [ ] **Phase 0**: Environment setup, Docker Compose infrastructure (Kafka, Redis, PostgreSQL), sanity check.
- [ ] **Phase 1**: PaySim Exploratory Data Analysis (EDA), feature availability, and leakage audit.
- [ ] **Phase 2**: Offline baseline and gradient-boosted model training, threshold & cost optimization.
- [ ] **Phase 3**: Streaming-safe feature engineering pipeline ensuring strict offline/online parity.
- [ ] **Phase 4**: Event-driven transaction replay producer with configurable rate limiting.
- [ ] **Phase 5**: Scoring consumer integrating Redis state lookups, real-time inference, and state updates.
- [ ] **Phase 6**: PostgreSQL alert storage and interactive Streamlit monitoring dashboard.
- [ ] **Phase 7**: End-to-end latency profiling, stress testing, and documentation.
