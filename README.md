# RecoMart – Modern Data Stack

A collaborative MTech Data Science & Engineering project demonstrating a retail data pipeline from source ingestion through curated analytics and product recommendations.

## Project flow

`UCI Online Retail + product-popularity API → Bronze → Silver → Gold → Feature Store → ALS Recommendations → Monitoring`

See [docs/architecture.md](docs/architecture.md) for the architecture, layer responsibilities and handoff contracts. See [docs/bronze_layer.md](docs/bronze_layer.md) for the implemented Bronze design and evidence.

## Repository layout

```text
.
├── data/                         # Local source, staging, landing and Delta outputs (not committed)
├── docs/
│   ├── architecture.md
│   ├── bronze_layer.md
│   └── evidence/
├── notebooks/
│   ├── 01_data_generation.py
│   ├── 02_bronze_ingestion.py
│   ├── 03_silver_transformation.py
│   ├── 04_gold_analytics.py
│   ├── 05_pipeline_monitoring.py
│   └── 06_master_pipeline.py
├── src/
│   ├── config.py
│   ├── source_preparation.py
│   ├── prepare_batches.py
│   ├── ingestion.py
│   └── mock_api.py
├── sql/
├── reports/
└── presentation/
```

Some downstream files are team-owned placeholders until their respective pull requests are merged. See the architecture document for ownership.

## Current implementation and environment note

The current source-preparation and Bronze demo runs locally with Python, PyArrow and the `deltalake` package. The Bronze demo creates local Delta tables; it is not yet a Databricks Spark job. Databricks Workspace/Workflow execution and downstream layer integration must be validated and documented before claiming a fully deployed Databricks pipeline.

## Prerequisites

- Python 3.10+ recommended.
- The packages listed in [requirements.txt](requirements.txt).
- The UCI Online Retail XLSX workbook saved locally (for example, at `data/source/Online Retail.xlsx`).

Create an environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Run source preparation

From the repository root:

```bash
python notebooks/01_data_generation.py --xlsx "data/source/Online Retail.xlsx"
```

This creates monthly transaction CSV batches, a re-delivery test batch and a product catalog under `data/raw/uci_online_retail/`. The XLSX file is a local input and is not included in the repository.

Equivalent module command:

```bash
python src/source_preparation.py --xlsx "data/source/Online Retail.xlsx" --out data/raw
```

## Run the Bronze demonstration

After source preparation:

```bash
python notebooks/02_bronze_ingestion.py --reset
```

The demo covers initial delivery, incremental batches, re-runs, duplicate re-delivery, bad-header rejection, API pagination/polling, transient API failures, ingestion logging and final row-count/duplicate-key assertions.

**Warning:** `--reset` deletes the local Bronze Delta, landing and rejected directories before running. Use it only when you intend to reset the local demo data.

## Output directories

The demo creates local outputs under `data/`:
- `raw/`: prepared source batches;
- `landing/`: delivered CSV files and raw API JSON pages;
- `lake/bronze/`: Bronze Delta tables and ingestion log;
- `rejected/`: failed files and malformed input rows;
- `logs/`: timestamped execution logs and CSV log export.

These generated data files are local runtime artifacts and should not be committed unless the team explicitly selects small evidence files for the report.

## Team collaboration

- Keep each member's implementation on a dedicated feature branch.
- Open a pull request to `main`; include changed files, test commands and evidence.
- Do not edit another member's owned module without coordinating first.
- Review input/output schemas and contracts before merging dependent layers.
- The integration owner coordinates the end-to-end run after upstream PRs are merged.

## Team

| Member | Responsibility |
|---|---|
| Mouli | Architecture, source preparation, Bronze ingestion and integration coordination |
| Aravinth S. | Data profiling, validation and Silver |
| Sonu Singh | Feature engineering and Gold |
| Rohit Totad | Feature store and recommendation model |
| Aman | Orchestration, integration testing and monitoring |

## Status

- Source preparation and local Bronze ingestion implementation are present on the Bronze development branch.
- Silver, Gold, recommendation ML and orchestration are separate team deliverables and must be checked for implementation before execution.
- Use the evidence under `docs/evidence/` and the Bronze design document when preparing the project report.
