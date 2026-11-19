# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_helpers

# COMMAND ----------
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import mlflow
import mlflow.spark

catalog = widget_value("catalog", "main")
schema = widget_value("schema", "retail_intelligence")
daily = spark.table(table_name(catalog, schema, "gold_daily_family_store"))

# Fill unrecorded family-store dates with zero aggregate units, then add only past-looking lags.
bounds = daily.agg(F.min("date").alias("start"), F.max("date").alias("end")).first()
calendar = spark.sql(
    f"SELECT explode(sequence(date'{bounds['start']}', date'{bounds['end']}', interval 1 day)) AS date")
series = daily.select("store_nbr", "family").distinct()
grid = series.crossJoin(calendar)
observed = daily.select("date", "store_nbr", "family", "forecast_units", "promotion_share", "is_holiday", "dcoilwtico")
grid = (grid.join(observed, ["date", "store_nbr", "family"], "left")
    .fillna({"forecast_units": 0.0, "promotion_share": 0.0, "is_holiday": 0, "dcoilwtico": 0.0})
    .withColumn("dow", F.dayofweek("date").cast("double"))
    .withColumn("month", F.month("date").cast("double"))
    .withColumn("day_of_year", F.dayofyear("date").cast("double")))
w = Window.partitionBy("store_nbr", "family").orderBy("date")
grid = (grid.withColumn("lag_1", F.lag("forecast_units", 1).over(w))
    .withColumn("lag_7", F.lag("forecast_units", 7).over(w))
    .withColumn("lag_14", F.lag("forecast_units", 14).over(w))
    .withColumn("lag_28", F.lag("forecast_units", 28).over(w))
    .withColumn("roll_mean_7", F.avg("forecast_units").over(w.rowsBetween(-7, -1)))
    .withColumn("roll_mean_28", F.avg("forecast_units").over(w.rowsBetween(-28, -1))))
features = ["store_nbr", "family", "dow", "month", "day_of_year", "promotion_share",
            "is_holiday", "dcoilwtico", "lag_1", "lag_7", "lag_14", "lag_28",
            "roll_mean_7", "roll_mean_28"]
model_data = grid.dropna(subset=["lag_28", "roll_mean_28"])
last_date = model_data.agg(F.max("date")).first()[0]
validation_start = F.date_sub(F.lit(last_date), 15)
train = model_data.filter(F.col("date") < validation_start)
validation = model_data.filter(F.col("date") >= validation_start)

store_indexer = StringIndexer(inputCol="store_nbr", outputCol="store_idx", handleInvalid="keep")
family_indexer = StringIndexer(inputCol="family", outputCol="family_idx", handleInvalid="keep")
encoder = OneHotEncoder(inputCols=["store_idx", "family_idx"],
                        outputCols=["store_vec", "family_vec"], handleInvalid="keep")
assembler = VectorAssembler(
    inputCols=["store_vec", "family_vec", "dow", "month", "day_of_year", "promotion_share",
               "is_holiday", "dcoilwtico", "lag_1", "lag_7", "lag_14", "lag_28",
               "roll_mean_7", "roll_mean_28"], outputCol="features", handleInvalid="keep")
gbt = GBTRegressor(labelCol="forecast_units", featuresCol="features", predictionCol="prediction",
                   maxIter=50, maxDepth=5, stepSize=0.05, seed=42)
pipeline = Pipeline(stages=[store_indexer, family_indexer, encoder, assembler, gbt])

def score(df, prediction_col="prediction"):
    scored = df.withColumn("pred_nonnegative", F.greatest(F.col(prediction_col), F.lit(0.0)))
    return scored.agg(
        F.sqrt(F.avg(F.pow(F.log1p("pred_nonnegative") - F.log1p("forecast_units"), 2))).alias("rmsle"),
        F.avg(F.abs(F.col("pred_nonnegative") - F.col("forecast_units"))).alias("mae"))

baseline = validation.withColumn("baseline_prediction", F.col("lag_7"))
baseline_metrics = score(baseline, "baseline_prediction").first().asDict()

mlflow.set_experiment("/Shared/retail-sales-promotion-intelligence")
with mlflow.start_run(run_name="spark-gbt-family-daily") as run:
    mlflow.log_param("forecast_grain", "store_family_day")
    mlflow.log_param("validation_horizon_days", 16)
    mlflow.log_param("max_depth", 5)
    mlflow.log_param("max_iter", 50)
    mlflow.log_metric("baseline_rmsle", float(baseline_metrics["rmsle"]))
    mlflow.log_metric("baseline_mae", float(baseline_metrics["mae"]))
    fitted = pipeline.fit(train)
    val_predictions = fitted.transform(validation)
    model_metrics = score(val_predictions).first().asDict()
    mlflow.log_metric("gbt_rmsle", float(model_metrics["rmsle"]))
    mlflow.log_metric("gbt_mae", float(model_metrics["mae"]))
    mlflow.spark.log_model(fitted, artifact_path="spark-model")
    validation_output = (val_predictions.select("date", "store_nbr", "family", "forecast_units",
                                                  F.greatest("prediction", F.lit(0.0)).alias("predicted_units"))
                         .withColumn("model", F.lit("spark_gbt")))
    write_delta(validation_output, table_name(catalog, schema, "gold_forecast_validation"))
    # Refit on all labeled data for production forecast inference and retain it in this run.
    final_model = pipeline.fit(model_data)
    mlflow.spark.log_model(final_model, artifact_path="spark-model-final")
    model_uri = f"runs:/{run.info.run_id}/spark-model-final"
    run_id = run.info.run_id

spark.sql(f"CREATE OR REPLACE TABLE {table_name(catalog, schema, 'forecast_model_metadata')} AS "
          f"SELECT '{model_uri}' AS final_model_uri, '{run_id}' AS mlflow_run_id, "
          f"{float(model_metrics['rmsle'])} AS gbt_rmsle, {float(model_metrics['mae'])} AS gbt_mae, "
          f"{float(baseline_metrics['rmsle'])} AS baseline_rmsle, {float(baseline_metrics['mae'])} AS baseline_mae")

# Prepare the 16 forecast dates. Test rows, when present, define the future family assortment.
test_table = table_name(catalog, schema, "silver_test")
if spark.catalog.tableExists(test_table):
    item_map = spark.table(table_name(catalog, schema, "silver_items")).select("item_nbr", "family")
    future = (spark.table(test_table).join(item_map, "item_nbr", "inner")
        .groupBy("date", "store_nbr", "family")
        .agg(F.avg(F.col("onpromotion").cast("double")).alias("promotion_share")))
else:
    recent_pairs = series
    future_dates = spark.sql(f"SELECT explode(sequence(date_add(date'{last_date}', 1), date_add(date'{last_date}', 16), interval 1 day)) AS date")
    future = recent_pairs.crossJoin(future_dates).withColumn("promotion_share", F.lit(0.0))

# Recursive forecasts feed each day's prediction into later-day lag features.
history = grid.select("date", "store_nbr", "family", F.col("forecast_units").alias("units"))
holiday = spark.table(table_name(catalog, schema, "silver_holidays"))
holiday_dates = (holiday.filter(F.col("locale") == "National").groupBy("date")
                 .agg(F.max(F.lit(1)).alias("is_holiday")))
oil = spark.table(table_name(catalog, schema, "silver_oil")).select("date", "dcoilwtico")
oil_default_row = oil.orderBy(F.col("date").desc()).select("dcoilwtico").first()
oil_default = oil_default_row["dcoilwtico"] if oil_default_row else 0.0
future_dates_list = [r[0] for r in future.select("date").distinct().orderBy("date").collect()]
forecast_parts = []
for forecast_date in future_dates_list:
    base = (future.filter(F.col("date") == forecast_date)
        .join(holiday_dates, "date", "left").fillna({"is_holiday": 0})
        .join(oil, "date", "left").fillna({"dcoilwtico": float(oil_default or 0.0)})
        .withColumn("dow", F.dayofweek("date").cast("double"))
        .withColumn("month", F.month("date").cast("double"))
        .withColumn("day_of_year", F.dayofyear("date").cast("double")))
    for lag_n in [1, 7, 14, 28]:
        lagged = history.filter(F.col("date") == F.date_sub(F.lit(forecast_date), lag_n))
        base = base.join(lagged.select("store_nbr", "family", F.col("units").alias(f"lag_{lag_n}")),
                         ["store_nbr", "family"], "left")
    for window_n, name in [(7, "roll_mean_7"), (28, "roll_mean_28")]:
        means = (history.filter((F.col("date") < F.lit(forecast_date)) &
                                (F.col("date") >= F.date_sub(F.lit(forecast_date), window_n)))
                 .groupBy("store_nbr", "family").agg(F.avg("units").alias(name)))
        base = base.join(means, ["store_nbr", "family"], "left")
    base = base.fillna({"lag_1": 0.0, "lag_7": 0.0, "lag_14": 0.0, "lag_28": 0.0,
                        "roll_mean_7": 0.0, "roll_mean_28": 0.0})
    predicted = final_model.transform(base).select(
        "date", "store_nbr", "family", "promotion_share",
        F.greatest(F.col("prediction"), F.lit(0.0)).alias("forecast_units"))
    forecast_parts.append(predicted)
    history = history.unionByName(predicted.select(
        "date", "store_nbr", "family", F.col("forecast_units").alias("units")))

forecasts = forecast_parts[0]
for part in forecast_parts[1:]:
    forecasts = forecasts.unionByName(part)
write_delta(forecasts, table_name(catalog, schema, "gold_forecasts"))
print(f"Validation RMSLE: baseline={baseline_metrics['rmsle']:.4f}, GBT={model_metrics['rmsle']:.4f}")
print(f"Validation MAE: baseline={baseline_metrics['mae']:.4f}, GBT={model_metrics['mae']:.4f}")

