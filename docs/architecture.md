# RecoMart – Project Architecture

## 1. Project objective

RecoMart is a modern data-stack project that turns retail transaction data and product-popularity signals into curated analytical datasets and Top-N product recommendations.

The pipeline is organized as a medallion architecture:

**Source data → Source preparation → Bronze → Silver → Gold → Feature store → Recommendation model → Evaluation and Top-N output**

The repository is developed collaboratively by five members. Each layer has a named owner; the integration owner coordinates interfaces and end-to-end execution rather than duplicating each member's implementation.

## 2. High-level architecture

```text
UCI Online Retail XLSX                 Mock product-popularity REST API
          |                                           |
          v                                           v
  Source preparation                         Paginated JSON snapshots
  (monthly CSV batches,                      (periodic polling)
   product catalog, redelivery)                       |
          |                                           |
          +-------------------+-----------------------+
                              |
                              v
                    BRONZE – raw ingestion
              transactions | product_catalog
                   product_popularity
                 + ingestion_log / rejects
                              |
                              v
                    SILVER – data quality
              schema/type standardization
          null/duplicate/cancelled/invalid handling
                              |
                              v
                    GOLD – analytical features
           customer | product | user-item interactions
                              |
                              v
                  FEATURE STORE / REGISTRY
                 feature definitions and versions
                              |
                              v
                 RECOMMENDATION MODEL (ALS)
                  train | evaluate | Top-N
                              |
                              v
                Monitoring, orchestration and
                   end-to-end execution evidence
```

## 3. Source layer

### 3.1 UCI Online Retail

The source workbook is read as text to preserve raw values. Source preparation:
- normalizes the invoice timestamp representation;
- removes the spreadsheet-style trailing `.0` from CustomerID values;
- partitions transactions into monthly CSV batches;
- creates a deliberate re-delivery batch to test idempotency;
- derives a product catalog with StockCode, modal Description, average unit price and first-seen date.

The generated files are staged under `data/raw/uci_online_retail/`. They are not Bronze tables until delivered to the landing zone and ingested.

### 3.2 Product popularity API

The local mock API exposes paginated product popularity snapshots. It provides product code, popularity score, sentiment score, units sold and an as-of date. The mock source is used to demonstrate API ingestion, pagination, retries and snapshot history; it is not a production third-party API.

## 4. Bronze layer – raw ingestion

Bronze preserves source fields as strings and adds lineage metadata:
- `_source_file`
- `_record_hash`
- `_occurrence_no`
- `_batch_id`
- `_record_type`
- `_source_system`
- `_ingest_ts`

The ingestion framework validates CSV headers, records malformed rows, uses a SHA-256 hash plus occurrence number for duplicate-safe Delta merges, and writes per-batch execution status to an ingestion log. Transient CSV ingestion errors are retried with exponential backoff; failed input files are moved to the rejected area. API ingestion saves raw JSON pages before parsing and retries network/HTTP 5xx failures.

Bronze outputs:
- `transactions`
- `product_catalog`
- `product_popularity`
- `ingestion_log`

**Implementation note:** the currently committed Bronze demo uses Python, PyArrow and the `deltalake` package to create local Delta tables. It does not currently execute on a Databricks Spark cluster. Databricks/Workflows deployment is an integration target and must be validated separately before describing the project as Databricks-native.

## 5. Silver layer – data quality and standardization

Silver consumes the Bronze tables. Its implementation is owned by Member 2 (Aravinth S.). Planned/assigned transformations include:
- applying explicit data types;
- profiling nulls and duplicates;
- handling cancelled invoices and invalid quantities/prices;
- validating source schemas and business rules;
- writing cleaned Silver Delta tables and data-quality results.

Silver must retain traceability to the Bronze source metadata and document its treatment of rejected or excluded records.

## 6. Gold layer – analytical features

Gold implementation is owned by Member 3 (Sonu Singh). It consumes Silver data and produces analytical tables such as:
- customer-level purchase frequency and recency;
- product-level popularity and sales measures;
- customer–product interaction features;
- sales analysis aggregates.

Feature definitions and grain (one row per customer, product or customer-product pair) must be documented in the data dictionary.

## 7. Feature store and recommendation model

Feature store and ML ownership belongs to Member 4 (Rohit Totad). The assigned work includes a feature registry and versioning, a user-item interaction matrix, Spark ML ALS collaborative filtering, MLflow experiment tracking, model evaluation and Top-N recommendations.

The model input contract must be agreed with the Gold owner before model training is integrated.

## 8. Orchestration, monitoring and integration

Integration/testing is owned by Aman. The assigned work includes:
- a master pipeline that invokes completed upstream stages in dependency order;
- Databricks Workflow configuration, subject to environment compatibility;
- execution monitoring and run-level status;
- end-to-end and incremental-ingestion tests;
- execution evidence, deployment notes and support for final report/demo preparation.

The master pipeline should not be treated as complete until it can run from source preparation through recommendation output using documented parameters and repeatable inputs.

## 9. Team ownership and repository interfaces

| Member | Ownership | Main deliverables |
|---|---|---|
| Mouli | Architecture, source preparation, Bronze ingestion and team integration | `src/config.py`, `src/source_preparation.py`, `src/ingestion.py`, notebooks 01–02, architecture and README |
| Aravinth S. | Raw data profiling, validation and Silver | `src/validation.py`, notebook 03, data-quality SQL, data contract and test cases |
| Sonu Singh | Feature engineering and Gold | `src/transformations.py`, `src/analytics.py`, notebook 04, sales SQL and data dictionary |
| Rohit Totad | Feature store and recommendation ML | feature store, recommender, evaluation modules and notebooks 07–09 |
| Aman | Orchestration, integration testing and monitoring | notebooks 05–06, deployment guide, execution results, final report and video script |

### Handoff contracts

1. **Source preparation → Bronze:** CSV file layout and expected raw column names are defined in `src/config.py`.
2. **Bronze → Silver:** three Bronze source tables plus lineage metadata; Bronze does not cast business columns to final types or silently clean source values.
3. **Silver → Gold:** cleaned, typed and quality-assessed tables with documented exclusion rules.
4. **Gold → ML:** documented customer, product and interaction feature tables with stable keys and feature definitions.
5. **ML → orchestration/reporting:** model metrics, evaluation evidence and Top-N recommendation output.

## 10. Repository execution order

The current local Bronze demonstration is run from the repository root:

1. Prepare source batches with notebook 01 / `src/source_preparation.py`.
2. Run notebook 02 to deliver batches, ingest CSV/API data and verify Bronze.
3. Run the Silver, Gold, ML and orchestration components after their owners complete and merge them.

See the root README for setup commands. Do not run downstream notebooks against empty placeholders; first confirm their implementation and input/output contracts.
