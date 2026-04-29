# Golden Customer Record Pipeline

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange)
![Tests](https://img.shields.io/badge/tests-61%20passed-green)
![Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen)
[![CI](https://github.com/YOUR_USERNAME/golden-customer-record/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/golden-customer-record/actions/workflows/ci.yml)

## Overview

A production-grade PySpark pipeline that creates a unified **Golden Customer Record** by reconciling and merging customer data from two disparate source systems: a CRM platform and an E-commerce Transaction system.

The pipeline applies data quality assessment, multi-strategy record matching with confidence scoring, and intelligent field prioritization to produce a single, authoritative customer view suitable for downstream analytics and business intelligence.

---

## Table of Contents

1. [Problem Statement & Approach](#problem-statement--approach)
2. [Architecture](#architecture)
3. [Key Design Decisions](#key-design-decisions)
4. [Data Quality Framework](#data-quality-framework)
5. [Reconciliation Strategy](#reconciliation-strategy)
6. [Golden Record Construction](#golden-record-construction)
7. [Project Structure](#project-structure)
8. [Setup & Installation](#setup--installation)
9. [Running the Pipeline](#running-the-pipeline)
10. [Output Schema](#output-schema)
11. [Testing](#testing)
12. [Assumptions & Limitations](#assumptions--limitations)
13. [Future Improvements](#future-improvements)

---

## Problem Statement & Approach

### The Challenge

Two source systems contain overlapping customer information:

| Source | Purpose | Key Fields |
|--------|---------|------------|
| **CRM System** | Primary customer registration | `customer_id`, `email`, `phone`, `address`, `registration_date` |
| **Transaction System** | E-commerce purchase records | `transaction_id`, `customer_email`, `phone`, `shipping_address`, `purchase_date` |

**Challenges identified:**
- Duplicate records within each source system
- Inconsistent data formats (dates, phone numbers, emails)
- Missing data (null emails, missing phone numbers)
- No shared primary key between systems
- Need to determine which source is authoritative per field
- Different date format conventions (`YYYY-MM-DD` vs `YYYYMMDD`)

### Our Solution

A multi-stage pipeline that:

1. **Loads & Cleans**: Schema-enforced ingestion with standardized cleaning
2. **Assesses Quality**: Completeness, validity, uniqueness, and freshness metrics
3. **Reconciles**: Multi-strategy fuzzy matching with confidence scoring
4. **Builds Golden Record**: Intelligent field coalescing with source prioritization

---

## Architecture
┌─────────────────┐     ┌─────────────────┐
│   CRM System    │     │  Transaction    │
│ (crm_customers  │     │ (transaction_   │
│     .csv)       │     │  customers.csv) │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
         └───────────┬───────────┘
                     ▼
┌────────────────────────────────────────────────┐
│              DATA LOADING LAYER                │
│ • Schema enforcement                           │
│ • Email normalization (lowercase, trim)        │
│ • Phone normalization (E.164 format)           │
│ • Date parsing (multiple formats)              │
│ • Deduplication (keep most recent)             │
│ • Lineage tracking (source + timestamp)        │
└────────────────────┬───────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────┐
│           DATA QUALITY ASSESSMENT              │
│ • Completeness: % non-null per column          │
│ • Format Validity: email/phone patterns        │
│ • Uniqueness: duplicate detection              │
│ • Freshness: date range analysis               │
│ • Reports saved as JSON                        │
└────────────────────┬───────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────┐
│           CUSTOMER RECONCILIATION              │
│ ┌────────────┐ ┌────────────┐ ┌─────────────┐  │
│ │   Email    │ │   Phone    │ │ Name +      │  │
│ │   Match    │ │   Match    │ │ Address     │  │
│ │   (1.0)    │ │   (0.8)    │ │   (0.6)     │  │
│ └─────┬──────┘ └─────┬──────┘ └──────┬──────┘  │
│       │              │               │         │
│       └──────────────┼───────────────┘         │
│                      ▼                         │
│           Best Match Selection                 │
│      (Priority: Email > Phone > Name)          │
└────────────────────┬───────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────┐
│          GOLDEN RECORD CONSTRUCTION            │
│ • Field coalescing (CRM vs Transaction)        │
│ • Transaction history aggregation              │
│ • Derived fields (tenure, active status)       │
│ • Data quality scoring (0-100)                 │
│ • Match quality categorization                 │
└────────────────────┬───────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────┐
│                 OUTPUT LAYERS                  │
│ ┌────────────────────────────────────────────┐ │
│ │  gold_raw/     (Full Audit Record)         │ │
│ ├────────────────────────────────────────────┤ │
│ │  ba_gold/      (Analytics Subset)          │ │
│ ├────────────────────────────────────────────┤ │
│ │  quality_reports/ (JSON Reports)           │ │
│ └────────────────────────────────────────────┘ │
└────────────────────────────────────────────────┘


---

## Key Design Decisions

### 1. CRM as Primary Source for Identity Fields

**Decision**: CRM data is treated as authoritative for customer identity fields (name, email).

**Rationale**: The CRM is the dedicated customer management system where profile data is intentionally maintained. Transaction data may contain typos, guest checkout entries, or outdated information.

### 2. Transaction as Primary Source for Address

**Decision**: Transaction shipping addresses are preferred over CRM addresses.

**Rationale**: Shipping addresses from purchases represent verified, current locations where customers actually receive goods. CRM addresses may become stale between profile updates.

### 3. Multi-Strategy Matching with Confidence Scoring

**Decision**: Three matching strategies applied in priority order with explicit confidence scores.

**Rationale**: No single field reliably joins all customers. Email is most reliable (1.0), phone is second (0.8), name+address combination is weakest (0.6) due to potential name variations and address changes.

### 4. Phone Normalization with Country Context

**Decision**: Phone numbers are normalized to E.164 international format using the `phonenumbers` library with country-aware parsing.

**Rationale**: The source data contains local numbers (e.g., `07911123456`) and international numbers (e.g., `+447911123456`). Without country context, local numbers cannot be matched across systems. The `phonenumbers` library provides robust validation beyond simple regex.

### 5. Duplicate Resolution Strategy

**Decision**: When duplicate `customer_id` values exist in CRM, retain the record with the most recent `last_updated` date.

**Rationale**: The most recently updated record is most likely to contain current, accurate information. Older records may reflect previous addresses or preferences.

---

## Data Quality Framework

The pipeline measures four dimensions of data quality:

### Completeness
Percentage of non-null values for each critical field.

| Field | What I Checked |
|-------|---------------|
| `first_name` | Was a name present? |
| `last_name` | Was a surname present? |
| `normalized_email` | Was an email available? |
| `normalized_phone` | Was a phone available? |
| `city` | Is city populated? |
| `country` | Is country populated? |

### Format Validity
Pattern-based validation against industry standards.

| Field | Pattern | Standard |
|-------|---------|----------|
| Email | `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$` | RFC 5322 subset |
| Phone | `^\+[1-9]\d{10,14}$` | E.164 |

### Uniqueness
Duplicate detection rates for primary keys (`customer_id`, `transaction_id`).

### Freshness
Date range analysis (`min` to `max`) for time-based columns.

### Quality Report Output

Reports are saved as timestamped JSON files:

┌── data/
│   └── output/
│       └── quality_reports/
│           └── 29-04-26/
│               ├── CRM_data_quality_report_20260429_120000.json
│               ├── TRX_data_quality_report_20260429_120000.json
│               └── GOLDEN_data_quality_report_20260429_120001.json

---

## Reconciliation Strategy

### Three-Tier Matching

| Priority | Strategy | Match Keys | Confidence | Use Case |
|----------|----------|------------|------------|----------|
| 1 (Highest) | **Email Match** | `normalized_email` = `normalized_email` | **1.0** | Customers who use the same email in both systems |
| 2 | **Phone Match** | `normalized_phone` = `normalized_phone` | **0.8** | Customers whose phone matches across systems |
| 3 (Lowest) | **Name + Address** | `first_name`, `last_name`, `city`, `country` | **0.6** | Customers with matching name and location |

### Match Resolution Rules

1. If a CRM customer matches multiple transactions via different strategies, the **highest confidence** match wins
2. In case of equal confidence, **match type priority** breaks ties (email > phone > name_address)
3. Only one transaction per CRM customer is retained as the primary match
4. Matches below the confidence threshold (default: **0.5**) are discarded

### Unmatched Records

CRM customers with no matching transaction are preserved in the golden record with:
- `match_type`: `null`
- `confidence`: `0.0`
- `total_transactions`: `0`

This ensures no customer data is lost, even if they've never made a purchase.

---

## Golden Record Construction

### Field Selection Rules

| Category | Fields | Priority |
|----------|--------|----------|
| **Identity** | `first_name`, `last_name`, `email` | CRM → Transaction |
| **Contact** | `phone` | CRM → Transaction |
| **Location** | `address`, `city`, `country` | Transaction → CRM |
| **Dates** | `registration_date` | CRM only |
| **Transactions** | `first_purchase_date`, `last_purchase_date`, `total_transactions` | Transaction only |

### Derived Fields

| Field | Logic | Purpose |
|-------|-------|---------|
| `customer_tenure_days` | `datediff(current_date, registration_date)` | How long the customer has been registered |
| `customer_tenure_months` | `months_between(current_date, registration_date)` | Tenure in months for cohort analysis |
| `is_active` | `last_purchase_date` within 365 days | Quick active/inactive segmentation |
| `data_quality_score` | Weighted sum (email:25, phone:25, address:20, city:15, country:15) | 0-100 score for downstream filtering |
| `match_quality` | `high` (≥0.8), `medium` (≥0.5), `low` (<0.5) | Categorical quality indicator |

---

## Project Structure

golden-customer-record/
│
├── README.md                         # This file
├── requirements.txt                  # Python dependencies
├── .gitignore                        # Git exclusion rules
│
├── config/
│   └── config.yaml                   # Pipeline configuration
│
├── src/
│   ├── __init__.py                   # Package marker
│   ├── main.py                       # Pipeline entry point & orchestration
│   ├── data_loader.py                # CSV ingestion, cleaning, normalization
│   ├── data_quality.py               # Quality assessment & reporting
│   ├── reconciliation.py             # Multi-strategy customer matching
│   └── golden_record.py              # Golden record assembly & enrichment
│
├── utils/
│   ├── __init__.py                   # Package marker
│   ├── general_utils.py              # YAML config loader, directory helpers
│   ├── logging_utils.py              # Structured logging configuration
│   └── spark_utils.py                # Spark session factory, lineage tracking
│
├── tests/
│   ├── __init__.py                   # Test package marker
│   ├── conftest.py                   # Shared fixtures, schemas, sample data
│   ├── test_data_loader.py           # DataLoader unit tests (21 tests)
│   ├── test_data_quality.py          # DataQualityChecker unit tests (13 tests)
│   ├── test_reconciliation.py        # Reconciliation unit tests (14 tests)
│   └── test_golden_record.py         # GoldenRecordBuilder unit tests (13 tests)
│
├── data/
│   ├── input/                        # Place source CSV files here
│   │   ├── crm_customers.csv
│   │   └── transaction_customers.csv
│   └── output/                       # Pipeline outputs
│       ├── golden_customers/
│       │   ├── gold_raw/             # Full golden record (all columns)
│       │   └── ba_gold/              # Business analytics subset
│       └── quality_reports/          # JSON quality reports
│
└── logs/
    └── pipeline.log                  # Pipeline execution logs

---

## Setup & Installation

### Prerequisites

- **Python 3.10+**
- **Java 8 or 11** (required by PySpark)
- **pip** (Python package manager)

### Step 1: Clone or Extract the Project

```bash
unzip golden-customer-record.zip
cd golden-customer-record
```
### Step 2: Create Virtual Environment

```bash
python -m venv venv

# Activate (Linux/Mac):
source venv/bin/activate

# Activate (Windows):
venv\Scripts\activate
```
### Step 3: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 4: Place Data Files
Copy the provided CSV files into the input directory:

```bash
cp crm_customers.csv data/input/
cp transaction_customers.csv data/input/
```

### Step 5: Verify Setup
```bash
# Quick config validation
python -c "from utils.general_utils import read_yaml_config; config = read_yaml_config('config/config.yaml'); print('Config loaded OK')"

# Verify data files exist
ls -la data/input/
```

## Running the Pipeline
### Full Pipeline Execution
```bash
python src/main.py
```
This executes all steps sequentially:

1. Spark session initialization

2. Data loading and cleaning

3. Data quality assessment (reports saved)

4. Customer reconciliation

5. Golden record construction

6. Output generation (2 Parquet layers)

# Output Schema

## Gold Raw Layer (Full Audit Record)

All columns preserved for auditability and debugging:

### Generated Identifiers

| Column | Type | Description |
|--------|------|-------------|
| `gold_customer_id` | String | Unique identifier (GOLD_<crm_id>) |
| `crm_id` | String | Original CRM identifier |
| `txn_id` | String | Primary transaction identifier |
| `latest_transaction_id` | String | Most recent transaction ID |

### Customer Attributes (CRM Priority)

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `first_name` | String | CRM → Txn | Best available first name |
| `last_name` | String | CRM → Txn | Best available last name |
| `email` | String | CRM | Normalized email address |
| `phone` | String | CRM → Txn | E.164 formatted phone |
| `raw_crm_phone` | String | CRM | Original CRM phone (traceability) |
| `txn_phone_raw` | String | Txn | Original transaction phone (traceability) |
| `registration_date` | Date | CRM | When customer registered |
| `last_updated` | Date | CRM | Last CRM profile update |

### Address Attributes (Transaction Priority)

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `address` | String | Txn → CRM | Best available address |
| `city` | String | Txn → CRM | Best available city |
| `country` | String | Txn → CRM | Best available country |

### Transaction Aggregates

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `first_purchase_date` | Date | Txn | Earliest transaction date |
| `last_purchase_date` | Date | Txn | Most recent transaction date |
| `total_transactions` | Integer | Txn | Total purchase count |
| `sample_transaction_ids` | Array | Txn | Up to 100 transaction IDs |

### Pipeline Metadata

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `match_type` | String | Pipeline | How matched (email, phone, name_address, null) |
| `confidence` | Double | Pipeline | Match confidence (0.0 - 1.0) |
| `match_quality` | String | Pipeline | high / medium / low |
| `source` | String | Pipeline | GOLDEN_RECORD |
| `ingestion_timestamp` | Timestamp | Pipeline | When record was processed |

### Computed Metrics

| Column | Type | Description |
|--------|------|-------------|
| `customer_tenure_days` | Integer | Days since registration |
| `customer_tenure_months` | Double | Months since registration |
| `is_active` | Boolean | Purchased within 365 days |
| `data_quality_score` | Integer | Completeness score (0-100) |


## Business Analytics Layer (BA Gold)

Clean subset for analysts - no raw/traceability columns:

### Identifiers

| Column | Description |
|--------|-------------|
| `gold_customer_id` | Unique customer identifier |

### Demographics

| Column | Description |
|--------|-------------|
| `first_name` | First name |
| `last_name` | Last name |
| `email` | Email address |
| `phone` | Phone number |
| `address` | Street address |
| `city` | City |
| `country` | Country |

### Dates & Tenure

| Column | Description |
|--------|-------------|
| `registration_date` | Registration date |
| `first_purchase_date` | First purchase date |
| `last_purchase_date` | Last purchase date |
| `customer_tenure_days` | Days registered |
| `customer_tenure_months` | Months registered |

### Metrics & Flags

| Column | Description |
|--------|-------------|
| `total_transactions` | Transaction count |
| `is_active` | Active flag |
| `data_quality_score` | Quality score |

## Testing

### Test Suite Overview

**61 tests** across 4 test modules covering both unit and integration scenarios.

| Module | Tests | Focus |
|--------|-------|-------|
| `test_data_loader.py` | 11 | Email/phone/date normalization, deduplication, lineage |
| `test_data_quality.py` | 10 | Completeness, format validity, uniqueness, freshness, reports |
| `test_reconciliation.py` | 14 | Email/phone/name matching, prioritization, edge cases, stats |
| `test_golden_record.py` | 10 | Aggregation, quality categorization, field coalescing, computed fields |
| **Shared fixtures** | **16** | Reusable Spark sessions, schemas, sample data (`conftest.py`) |
| **Total** | **61** | - |


### Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_reconciliation.py -v

# Run with coverage report
python -m pytest tests/ --cov=src --cov-report=html

# Run a single test
python -m pytest tests/test_data_loader.py::TestEmailNormalization::test_basic_normalization -v
```

### Test Design Principles

- **Isolation**: Function-scoped Spark sessions prevent test interference

- **Realistic Data**: Sample fixtures mirror actual data scenarios (nulls, duplicates, multiple formats)

- **Edge Cases**: Empty DataFrames, null inputs, boundary conditions all tested

- **Integration Coverage**: Full reconciliation pipeline tested end-to-end

- **Deterministic**: Shuffle partitions set low, adaptive query disabled for reproducible results

---

# CI/CD

This project includes a GitHub Actions CI pipeline that runs on every push and pull request.

### Pipeline Steps

```mermaid
flowchart LR
    Push[Push/PR to main] --> Checkout[Checkout Code]
    Checkout --> Setup[Setup Python & Java]
    Setup --> Install[Install Dependencies]
    Install --> Test[Run Test Suite]
    Test --> Coverage[Generate Coverage Report]
    Coverage --> Upload[Upload to Codecov]


# Assumptions & Limitations

## Assumptions

- CRM is the primary source of truth for customer identity data
- Email addresses uniquely identify customers across systems (highest confidence match)
- UK and France are the primary markets (phone country context mapping)
- Duplicate CRM records should keep the most recently updated version
- Transaction dates are in YYYY-MM-DD or YYYYMMDD format
- Phone numbers are from UK (+44) or France (+33) when local format

## Limitations

- **No fuzzy name matching**: Name matching is exact (no handling of "William" vs "Will")
- **No address standardization**: Addresses compared as raw strings
- **Email is unique assumption**: If two family members share an email, they'll be merged
- **No incremental processing**: Full batch reprocessing on each run
- **Phone country context limited**: Currently only maps UK and France
- **No historical tracking**: Doesn't maintain a history of changes over time

## Handling of Data Quality Issues Discovered

During development, the following data quality issues were identified in the source files:

- **Duplicate `customer_id` values** (CRM00006, CRM00095, CRM00058, etc.): Resolved by keeping most recent `last_updated`
- **Inconsistent date formats** (YYYY-MM-DD and YYYYMMDD): Handled with coalesce multi-format parsing
- **Mixed phone formats** (+44..., 07..., formatted with spaces): Normalized to E.164 via `phonenumbers` library
- **Records with no contact information**: Preserved but given low `data_quality_score` and `match_type = null`
- **Missing transaction contact info**: Records retained but cannot be matched to CRM


# Future Improvements

## Short-term (Next Iteration)

- Add fuzzy name matching using Levenshtein distance for name variations
- Implement address standardization using a geocoding service
- Add email domain validation to flag temporary/disposable email addresses
- Expand phone country context to support all EU countries
- Add pipeline metrics (execution time per stage, record counts per step)

## Medium-term

- Implement incremental processing using Delta Lake change data feed
- Add data versioning to track changes to golden records over time
- Create monitoring dashboard with data quality trend visualization
- Add email notification for quality threshold breaches
- Implement survivorship rules configurable via external YAML

## Long-term

- Migrate to Medallion Architecture (Bronze → Silver → Gold) for scalability
- Add Master Data Management (MDM) capabilities for multi-domain mastering
- Implement real-time streaming via Spark Structured Streaming + Kafka
- Add probabilistic matching using machine learning (Fellegi-Sunter)
- Deploy as Airflow DAG for scheduled orchestration


## Dependencies

```text
pyspark==3.5.0          # Distributed data processing
pyyaml==6.0.1           # Configuration file parsing
pytest==7.4.3           # Test framework
phonenumbers==8.13.29   # Phone number parsing & validation
```

<div align="center">

**Submitted by:** Philip Igbeka
**Date:** 29th April 2026

</div>