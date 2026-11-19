# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_helpers

# COMMAND ----------
catalog = widget_value("catalog", "main")
schema = widget_value("schema", "retail_intelligence")
create_silver(catalog, schema)
build_family_daily(catalog, schema)
print("Silver dimensions/facts and Gold analytics marts are ready.")

