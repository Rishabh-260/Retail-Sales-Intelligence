# Retail Sales and Promotion Intelligence Platform

Databricks-first analytics and demand-forecasting project built on the [Corporación Favorita Grocery Sales Forecasting dataset](https://www.kaggle.com/competitions/favorita-grocery-sales-forecasting).

The pipeline ingests the daily sales history and reference files into Delta tables, creates store and product analytics marts, tracks promotion-associated sales differences, and forecasts daily sales for each store and product family. It uses PySpark, Spark SQL, Databricks Workflows, MLflow, and Power BI-ready exports.

> Promotion comparisons in this dataset are observational. They describe sales associated with promotion flags and do not establish causal lift.

## Project layout

```text
.
├── databricks.yml
├── resources/job.yml
├── notebooks/
│   ├── _helpers.py
│   ├── 01_ingest.py
│   ├── 02_build_marts.py
│   ├── 03_train_forecast.py
│   └── 04_export_powerbi.py
├── sql/analytics.sql
├── data/
│   ├── raw/                 # Kaggle downloads (not committed)
│   └── exports/             # Optional local exports (not committed)
└── docs/powerbi-guide.md
```

## Data setup

1. Join the Kaggle competition and accept its rules.
2. Download the competition files. Upload these CSVs to a Databricks Unity Catalog Volume, for example `/Volumes/main/retail_intelligence/raw/`:
   - Required: `train.csv`, `stores.csv`, `items.csv`, `holidays_events.csv`, `oil.csv`, `transactions.csv`.
   - Optional: `test.csv`. When present, it scopes forecast output to the competition's store-family combinations and supplies known promotion mix for those dates.
3. In Databricks, create a Unity Catalog catalog and schema (the defaults in the bundle are `main.retail_intelligence`). Make sure the job principal can read the Volume and create tables in the schema.

The source `train.csv` is large. Use a Unity Catalog Volume or another Databricks-accessible cloud path instead of uploading it through the workspace editor.

## Deploy and run

Install and authenticate the Databricks CLI for your workspace, then from this project directory:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev retail_sales_pipeline
```

The bundle job runs the notebooks in order: ingest, build marts, train and evaluate, then export. Edit `resources/job.yml` to set `catalog`, `schema`, `raw_path`, and a valid `node_type_id` for your cloud/region before deployment. The job writes Delta tables to the selected schema and Power BI exports to a `powerbi_exports` subdirectory under the raw Volume path.

Alternatively, open and run the four notebooks in order in a Databricks workspace, providing the same widget values. No Kaggle credentials are stored in this project.

## Data model

| Layer | Tables | Grain / purpose |
|---|---|---|
| Bronze | `bronze_sales`, `bronze_test`, `bronze_stores`, `bronze_items`, `bronze_holidays`, `bronze_oil`, `bronze_transactions` | Source-shaped Delta tables |
| Silver | `silver_sales`, `silver_test`, `silver_stores`, `silver_items`, `silver_holidays`, `silver_oil`, `silver_transactions` | Typed and normalized data |
| Gold | `gold_daily_family_store`, `gold_promotion_summary`, `gold_store_performance`, `gold_product_performance`, `gold_forecast_validation`, `gold_forecasts` | BI marts, model evaluation, and forecasts |

The primary fact retains date, store, item, observed unit sales, and promotion status. Gold daily sales aggregate to date × store × product family and include store metadata, promotion share, transactions, oil price, and holiday indicators. The forecast target is non-negative family-level daily units; raw negative returns remain available in Silver and net sales marts.

## Forecast and experiment tracking

The forecasting notebook builds a complete daily store-family series from observed family totals, adds lag and calendar features, and compares a 7-day seasonal-naive baseline with Spark `GBTRegressor`. It holds out the last 16 dates for a rolling-origin evaluation: each validation date can use earlier observed dates, matching a daily refresh workflow. RMSLE and MAE are logged to MLflow; the fitted Spark pipeline is logged as an artifact.

After evaluation, the model is refit on all available history and generates forecasts for the next 16 days. If `test.csv` exists, its dates and product assortment define the forecast combinations; otherwise, the latest observed store-family combinations are projected with promotion share set to zero. This is a family-level planning forecast, not a Kaggle item-level submission.

## Power BI

Follow [the Power BI guide](docs/powerbi-guide.md) to import the exports, relate the dimensions, and build the report pages. The report covers demand trends, store and family performance, promotion-associated changes, and forecast versus recent actuals. This project produces Power BI-ready tables, not a `.pbix` file.

## Data quality and interpretation

- Null promotion flags are normalized to `false` for analysis; source records remain available in Bronze.
- Negative sales values represent returns and are retained in the detailed fact. Forecast target values are clamped to zero after family-level aggregation so RMSLE remains defined.
- The source is transaction-activity data, not a customer dataset; it does not support customer-level behavior analysis.
- Promotion flags are not randomized, and availability/stock status is not provided. Promotion summaries should be read as descriptive comparisons, not causal estimates.
- Store type and cluster are provided as source attributes; this project does not infer their business definitions.
