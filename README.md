## Assumptions

- The `country` column is authoritative for phone number formatting
- City/country mismatches in the sample data do not affect the matching logic
- Focus is on demonstrating the reconciliation approach, not fixing synthetic data issues

## Observations

- **Email matching** was the most effective strategy (46% match rate)
- **Name + address matching** captured 8% additional matches
- **Phone matching** yielded 0 matches due to:
  - Poor phone data quality in source systems
  - Many invalid phone numbers in the sample data
  - Missing country codes for international numbers

  # Golden Customer Record Pipeline

## Overview

This project builds a **Golden Customer Record (GCR)** by reconciling customer data from two independent sources:

* **CRM system** – authoritative customer profiles
* **Transaction system** – behavioral and purchase data

The goal is to produce a **single, reliable, analytics-ready customer dataset** that resolves inconsistencies, deduplicates records, and enriches customer profiles.

---

## Architecture

The pipeline is structured into modular components:

```
DataLoader → DataQualityChecker → CustomerReconciliation → GoldenRecordBuilder
```

### 1. DataLoader

Responsible for:

* Schema enforcement
* Data normalization (email, phone, dates)
* Deduplication using window functions
* Lineage tracking (`source`, `ingestion_timestamp`)

**Design choice:**
Normalization is handled early to ensure downstream joins operate on clean, consistent keys.

---

### 2. DataQualityChecker

Performs validation across four dimensions:

* **Completeness** – null analysis per column
* **Format validity** – regex-based checks (email, phone)
* **Uniqueness** – duplicate detection
* **Freshness** – min/max date tracking

Outputs a structured JSON report for observability.

**Design choice:**
Quality checks are decoupled from transformation logic to support independent monitoring and reuse.

---

### 3. CustomerReconciliation

Implements multi-strategy matching between CRM and transaction data:

| Match Type     | Logic                             | Confidence |
| -------------- | --------------------------------- | ---------- |
| Email          | Exact match on normalized email   | 1.0        |
| Phone          | Exact match on normalized phone   | 0.8        |
| Name + Address | Composite match (name + location) | 0.6        |

Key features:

* Broadcast joins for performance optimization
* Confidence-based filtering
* Match prioritization (email > phone > name/address)
* Window ranking to retain best match per customer
* Explicit handling of unmatched records

**Design choice:**
A **deterministic scoring model** was used instead of fuzzy matching to ensure explainability and reproducibility.

---

### 4. GoldenRecordBuilder

Constructs the final unified dataset by:

* Joining CRM, transaction aggregates, and reconciliation outputs
* Resolving conflicts using **coalesce-based prioritization**
* Enriching with:

  * Transaction summaries
  * Customer tenure
  * Activity status
  * Data quality scoring

**Design choice:**
All joins are explicitly aliased to prevent column ambiguity—a common failure point in Spark pipelines.

---

## Key Engineering Decisions

### 1. Early Column Normalization

Avoids downstream join inconsistencies and reduces reconciliation complexity.

---

### 2. Explicit Aliasing Strategy

All joins use `.alias()` and fully qualified column references.

**Why:**
Prevents Spark `AnalysisException` due to ambiguous columns in multi-join pipelines.

---

### 3. Window Functions for Determinism

Used for:

* Deduplication (`row_number`)
* Best match selection

**Why:**
Ensures deterministic outputs regardless of data ordering.

---

### 4. Separation of Concerns

Each component has a single responsibility:

* Loading
* Quality checking
* Matching
* Final assembly

**Why:**
Improves testability, maintainability, and extensibility.

---

### 5. Data Lineage Tracking

Every dataset includes:

* `source`
* `ingestion_timestamp`

**Why:**
Supports traceability and debugging in production environments.

---

### 6. Defensive Defaults

* Null-safe transformations
* Default values (e.g., `total_transactions = 0`)
* Graceful handling of missing columns

---

## Testing Strategy

The project includes **unit and integration-style tests** using `pytest`.

### Coverage Areas

#### DataLoader

* Email normalization
* Phone normalization
* Date parsing
* Deduplication logic
* Lineage column presence

#### DataQualityChecker

* Completeness calculations
* Format validation
* Uniqueness metrics
* Freshness checks
* Report generation and persistence

#### CustomerReconciliation

* Email, phone, and composite matching
* Confidence scoring
* Match prioritization
* Handling unmatched records

#### GoldenRecordBuilder (implicit via integration)

* Correct field prioritization
* Join correctness
* Derived metrics

---

### Test Design Choices

* **Shared fixtures (`conftest.py`)**

  * Spark session reuse (performance)
  * Sample datasets for deterministic testing

* **Isolation via `spark.newSession()`**

  * Prevents cross-test contamination

* **Realistic edge cases**

  * Missing values
  * Duplicate records
  * Format inconsistencies

---

## Data Model (Golden Record)

The final dataset includes:

### Core Identity

* `gold_customer_id`
* `first_name`, `last_name`
* `email`, `phone`

### Contact & Location

* `address`, `city`, `country`

### Transaction Insights

* `total_transactions`
* `first_purchase_date`, `last_purchase_date`
* `latest_transaction_id`

### Matching Metadata

* `match_type`
* `confidence`
* `match_quality`

### Derived Features

* `customer_tenure_days`
* `customer_tenure_months`
* `is_active`
* `data_quality_score`

### Lineage

* `source`
* `ingestion_timestamp`

---

## Error Handling

* Null-safe transformations across all modules
* Graceful degradation when columns are missing
* Logging at each stage for traceability
* Validation-driven safeguards (e.g., confidence thresholds)

---

## Performance Considerations

* Broadcast joins for small lookup datasets
* Reduced shuffle partitions in test environment
* Column pruning via selective `select()` usage
* Caching of reconciliation output for reuse

---

## How to Run

```bash
pytest tests/
```

Or run the pipeline:

```bash
python playground.py
```

---

## Future Improvements

* Fuzzy matching (e.g., Levenshtein distance) for better recall
* Incremental processing (CDC support)
* Partitioned output for large-scale datasets
* Data quality alerting integration
* Config-driven match rule engine

---

## Summary

This implementation focuses on:

* **Correctness** (deterministic matching, explicit joins)
* **Maintainability** (modular design, clear responsibilities)
* **Observability** (data quality + lineage tracking)
* **Production readiness** (testing, logging, error handling)

It is designed to be easily extensible while remaining robust under real-world data inconsistencies.
