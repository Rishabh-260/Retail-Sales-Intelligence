# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_helpers

# COMMAND ----------
catalog = widget_value("catalog", "main")
schema = widget_value("schema", "retail_intelligence")
raw_path = widget_value("raw_path", "/Volumes/main/retail_intelligence/raw")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

files = {
    "train.csv": "bronze_sales",
    "stores.csv": "bronze_stores",
    "items.csv": "bronze_items",
    "holidays_events.csv": "bronze_holidays",
    "oil.csv": "bronze_oil",
    "transactions.csv": "bronze_transactions",
}
for filename, table in files.items():
    df = read_csv(raw_path, filename)
    write_delta(df, table_name(catalog, schema, table))
    print(f"Loaded {filename} into {table_name(catalog, schema, table)}")

try:
    test = read_csv(raw_path, "test.csv")
    write_delta(test, table_name(catalog, schema, "bronze_test"))
    print("Loaded test.csv into bronze_test")
except Exception as exc:
    if spark.catalog.tableExists(table_name(catalog, schema, "bronze_test")):
        spark.sql(f"DROP TABLE {table_name(catalog, schema, 'bronze_test')}")
    print(f"Optional test.csv not loaded ({type(exc).__name__}); forecast will use recent store-family combinations.")

