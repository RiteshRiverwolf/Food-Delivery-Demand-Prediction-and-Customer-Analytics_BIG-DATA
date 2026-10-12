# Food Delivery Demand Prediction and Customer Analytics

A Big Data pipeline on Google Cloud Dataproc that predicts delivery demand, segments customers and recommends products, using 3.4 million real orders from Instacart.

## Problem statement

An online food and grocery delivery platform needs to anticipate demand (when customers will order next and which items they will reorder) and understand customer behaviour, so it can plan delivery capacity, manage inventory and personalise recommendations.

| # | Business question | Approach |
|---|---|---|
| 1 | When and what will customers order? | Spark MLlib regression and classification |
| 2 | Who are our customers? | K-Means segmentation |
| 3 | What should we show each customer? | ALS recommendations, FP-Growth basket analysis |

## Dataset

| Table | Rows | Description |
|---|---|---|
| orders | 3,421,083 | One row per order: user, day of week, hour, days since prior order |
| order_products__prior | 32,434,489 | Products in each historical order |
| order_products__train | 1,384,617 | Products in each user's latest order (labels) |
| products | 49,688 | Product names with aisle and department |
| aisles | 134 | Aisle names |
| departments | 21 | Department names |

Download it with:

```bash
kaggle datasets download -d yasserh/instacart-online-grocery-basket-analysis-dataset -p data/raw --unzip
```

**Source:** Instacart, "The Instacart Online Grocery Shopping Dataset 2017". Accessed via Kaggle (`yasserh/instacart-online-grocery-basket-analysis-dataset`) on 12 Oct 2026.

## Medallion pipeline (mid-term)

```
Kaggle CSV ──► Landing (data/raw) ──► Bronze ──► Silver ──► Gold ──► charts
               checksums + manifest   raw +      cleaned,   analytics
                                      lineage    validated  tables
```

| Stage | Script | What it does |
|---|---|---|
| Ingest | [01_ingest.py](src/pipeline/01_ingest.py) | Downloads from Kaggle if missing; validates headers, MD5, sizes, row counts; writes an ingestion manifest with a batch id |
| Bronze | [02_bronze.py](src/pipeline/02_bronze.py) | Raw columns as strings in Parquet + `_source_system`, `_source_file`, `_batch_id`, `_ingested_at`, `_row_hash`; partitioned by batch; row counts reconciled with the manifest |
| Silver | [03_silver.py](src/pipeline/03_silver.py) | Dedup on business keys (latest ingest wins), type casting, text standardisation, 20 validation rules with a quarantine table, data-quality flags, derived columns |
| Processing | [04_processing.py](src/pipeline/04_processing.py) | Spark SQL, joins (broadcast), aggregations, pivot, window functions (rank, row_number, ntile, lag, rolling) → Gold tables |
| Charts | [05_charts.py](src/pipeline/05_charts.py) | Report figures from the Gold summaries |

Run everything:

```bash
bash src/pipeline/run_pipeline.sh
```

Reports land in `outputs/pipeline/` (manifest, bronze audit, silver data-quality report, Gold summaries, figures).

## Technology stack

Hadoop/HDFS, MapReduce (MRJob), Pig, Hive, Apache Spark (PySpark, Spark SQL, MLlib, Structured Streaming), Google Cloud Storage and Dataproc.

## Project structure

```
src/mapreduce   MRJob jobs
src/pig         Pig cleaning scripts
src/hive        Hive warehouse DDL and queries
src/spark       PySpark jobs (EDA, ML, streaming)
cloud           Google Cloud setup and job submission scripts
outputs         Results and charts
report          Project report
```

## Local setup

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python src/spark/00_smoke_test.py data/raw
```

Requires Java 17.
