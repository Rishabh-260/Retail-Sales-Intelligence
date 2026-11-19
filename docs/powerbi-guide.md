# Power BI report guide

Import the CSV folders generated in `powerbi_exports` from the project Volume. Each folder contains a Spark CSV part file; use **Get Data → Folder** (or the Databricks connector for Delta tables) and combine the files in that folder. The bundle writes these tables:

- `gold_daily_family_store`: date, store, family, daily net and positive units, promotion share, transactions, oil price, and holiday flag.
- `gold_promotion_summary`: observed promoted versus non-promoted item-day averages and associated percentage change by store and family.
- `gold_store_performance` and `gold_product_performance`: comparison and ranking tables.
- `gold_forecast_validation` and `gold_forecasts`: actual versus predicted holdout values and forward projections.
- `forecast_model_metadata`: MLflow run reference and evaluation metrics.

Recommended report pages:

1. **Demand overview** — total units over time; slicers for date, state, city, store, and family.
2. **Store and product performance** — rank stores and product families by positive units and compare trends.
3. **Promotion patterns** — compare promoted and non-promoted item-day averages and show the associated change. Add a caption explaining that the comparison is observational.
4. **Forecast planning** — plot recent actuals, the 16-day holdout predictions, and future family-level forecasts by store and family.

Keep `net_units` for reporting recorded returns and use `positive_units` for gross positive demand. Forecast target values are non-negative family-level units. The current exports are snapshots written at the end of each pipeline run.

