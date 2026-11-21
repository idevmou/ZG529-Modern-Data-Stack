* [ ] 

# RecoMart – Modern Data Stack

A collaborative MTech Data Science & Engineering project demonstrating a retail data pipeline from source ingestion through curated analytics and product recommendations.

## Project flow

`UCI Online Retail + product-popularity API → Bronze → Silver → Gold → Feature Store → ALS Recommendations → Monitoring`

See [docs/architecture.md](docs/architecture.md) for the architecture, layer responsibilities and handoff contracts. See [docs/bronze_layer.md](docs/bronze_layer.md) for the implemented Bronze design and evidence.

## Repository layout

```text
.
├── data/
│   └── lake/bronze/              # Shared Bronze Delta snapshot (committed)
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
- The UCI Online Retail XLSX workbook saved locally (for example, at `data/raw/Online Retail.xlsx`).

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
python notebooks/01_data_generation.py --xlsx "data/raw/Online Retail.xlsx"
```

This creates monthly transaction CSV batches, a re-delivery test batch and a product catalog under `data/raw/uci_online_retail/`. The XLSX file is a local input and is not included in the repository.

Equivalent module command:

```bash
python src/source_preparation.py --xlsx "data/raw/Online Retail.xlsx" --out data/raw
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

The source, staging, landing, rejected and log files are local runtime artifacts; the Bronze Delta snapshot is the exception and is shared through Git as described below.

## Sharing Bronze data through Git

The generated Bronze Delta tables are intended to be shared under `data/lake/bronze/` so downstream members can use the same snapshot without downloading the source workbook or regenerating the batches. Each table folder must be shared in full, including its `_delta_log/` and Parquet files.

Only the Bronze tables are intended to be committed from `data/`. The source workbook, staged CSVs, landing files, rejected inputs and runtime logs are not needed by downstream members and should remain local. Since `data/` is not ignored, stage the Bronze directory explicitly rather than using `git add data`:

```powershell
git add data/lake/bronze
git status --short
```

Before committing, confirm that only intended `data/lake/bronze/` files are staged. After the snapshot is pushed, teammates can pull the branch and read the tables at the same relative paths. Do not run the Bronze demo with `--reset` in a checkout containing the shared snapshot unless you intend to delete its local Bronze tables and landing data.

The shared snapshot uses local Delta Lake files created with `deltalake`; it is not a Spark/Databricks table. If the Bronze data is refreshed, commit and push the complete updated table directories together with any relevant schema or code changes. Repeated large data updates can grow Git history; consider Git LFS or shared object storage if the snapshot becomes substantially larger.

## Team collaboration

- Keep each member's implementation on a dedicated feature branch.
- Open a pull request to `main`; include changed files, test commands and evidence.
- Do not edit another member's owned module without coordinating first.
- Review input/output schemas and contracts before merging dependent layers.
- The integration owner coordinates the end-to-end run after upstream PRs are merged.

## Team

| Member      | Responsibility                                                                  |
| ----------- | ------------------------------------------------------------------------------- |
| Mouli       | Architecture, source preparation, Bronze ingestion and integration coordination |
| Aravinth S. | Data profiling, validation and Silver                                           |
| Sonu Singh  | Feature engineering and Gold                                                    |
| Rohit Totad | Feature store and recommendation model                                          |
| Aman        | Orchestration, integration testing and monitoring                               |

## Status

- Source preparation and local Bronze ingestion implementation are present on the Bronze development branch.
- Silver, Gold, recommendation ML and orchestration are separate team deliverables and must be checked for implementation before execution.
- Use the evidence under `docs/evidence/` and the Bronze design document when preparing the project report.
