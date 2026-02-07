# Airflow Analytics Pipeline (End-to-End)

An end-to-end data engineering pipeline built with Apache Airflow that ingests raw datasets, stages and cleans data, computes customer-level analytics, and generates a static HTML dashboard.

## Architecture Overview

This project implements a simple but realistic data engineering workflow:

source → raw → clean → analytics → dashboard

- **Source**: Public datasets (Online Retail, IMDb reviews)
- **Raw**: Immutable copies of source data
- **Clean**: Normalised, schema-safe datasets
- **Analytics**: Aggregated customer-level metrics
- **Dashboard**: Static HTML report generated from analytics output

Apache Airflow orchestrates each stage as a DAG with explicit task dependencies.

## Data Sources

This project uses public, real-world datasets:

- **Online Retail Dataset**  
  Transaction-level retail data containing invoices, quantities, prices, customer IDs, and countries.

- **IMDb Reviews Dataset**  
  Movie review text with sentiment labels used to generate synthetic user engagement metrics.

The datasets are intentionally messy (missing values, inconsistent types) to reflect real production data.

## Airflow DAGs

The pipeline is split into clear phases:

### Phase 1 — Sanity Check
- Verifies Airflow setup and task execution

### Phase 2 — Extract & Stage
- Copies source CSV files into the raw data layer
- Ensures raw data is immutable and reproducible

### Phase 3 — Transform & Analytics
- Cleans raw datasets
- Computes customer-level metrics:
  - total spend
  - number of transactions
  - number of positive reviews

### Phase 4 — Dashboard
- Generates a static HTML dashboard with summary tables and distributions

## Key Engineering Lessons

- Raw data should never be over-cleaned; aggressive filtering can silently destroy downstream analytics
- Row-count validation between pipeline stages is critical
- Data bugs often originate in staging, not transformation or visualization
- “Green” Airflow tasks do not guarantee correct data
- Minimal, schema-safe cleaning preserves flexibility for downstream logic

## How to Run

### Requirements
- Docker & Docker Compose
- Git

### Start Airflow
```bash
docker compose up -d

Access Airflow UI
- URL: http://localhost:8080
- Username: airflow
- Password: airflow

Trigger the pipeline
Trigger the DAGs manually from the Airflow UI.
The generated dashboard will be written to:
/opt/airflow/dashboards/customer_metrics_dashboard.html


---

## 7️⃣ Optional: portfolio disclaimer (recommended)

```md
## Notes

This project was rebuilt from scratch to understand each stage of an end-to-end data pipeline.  
All code was written and debugged manually, with an emphasis on correctness, data integrity, and observability.
