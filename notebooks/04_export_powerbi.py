# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_helpers

# COMMAND ----------
catalog = widget_value("catalog", "main")
schema = widget_value("schema", "retail_intelligence")
raw_path = widget_value("raw_path", "/Volumes/main/retail_intelligence/raw")
output_path = f"{raw_path.rstrip('/')}/powerbi_exports"
tables = [
    "gold_daily_family_store", "gold_promotion_summary", "gold_store_performance",
    "gold_product_performance", "gold_forecast_validation", "gold_forecasts",
    "forecast_model_metadata",
]
for table in tables:
    df = spark.table(table_name(catalog, schema, table))
    (df.write.mode("overwrite").option("header", "true")
        .csv(f"{output_path}/{table}"))
    print(f"Exported {table} to {output_path}/{table}")

