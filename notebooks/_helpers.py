# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def widget_value(name, default):
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name).strip()
    except Exception:
        return default


def table_name(catalog, schema, name):
    return f"{catalog}.{schema}.{name}"


def write_delta(df, table, mode="overwrite"):
    (df.write.format("delta").mode(mode).option("overwriteSchema", "true")
        .saveAsTable(table))


def read_csv(raw_path, filename):
    return (spark.read.option("header", "true").option("mode", "PERMISSIVE")
            .option("quote", '"').option("escape", '"')
            .csv(f"{raw_path.rstrip('/')}/{filename}"))


def create_silver(catalog, schema):
    n = lambda name: table_name(catalog, schema, name)
    sales = (spark.table(n("bronze_sales"))
        .select(F.col("date").cast("date").alias("date"),
                F.col("store_nbr").cast("int").alias("store_nbr"),
                F.col("item_nbr").cast("long").alias("item_nbr"),
                F.col("unit_sales").cast("double").alias("unit_sales"),
                F.when(F.lower(F.col("onpromotion")).isin("true", "1", "t"), True)
                 .otherwise(False).alias("onpromotion"))
        .filter(F.col("date").isNotNull() & F.col("store_nbr").isNotNull()
                & F.col("item_nbr").isNotNull() & F.col("unit_sales").isNotNull()))
    write_delta(sales, n("silver_sales"))

    test_table = n("bronze_test")
    if spark.catalog.tableExists(test_table):
        test = (spark.table(test_table)
            .select(F.col("id").cast("long").alias("id"),
                    F.col("date").cast("date").alias("date"),
                    F.col("store_nbr").cast("int").alias("store_nbr"),
                    F.col("item_nbr").cast("long").alias("item_nbr"),
                    F.when(F.lower(F.col("onpromotion")).isin("true", "1", "t"), True)
                     .otherwise(False).alias("onpromotion"))
            .filter(F.col("date").isNotNull() & F.col("store_nbr").isNotNull()
                    & F.col("item_nbr").isNotNull()))
        write_delta(test, n("silver_test"))
    elif spark.catalog.tableExists(n("silver_test")):
        spark.sql(f"DROP TABLE {n('silver_test')}")

    stores = spark.table(n("bronze_stores")).select(
        F.col("store_nbr").cast("int").alias("store_nbr"), "city", "state",
        "type", F.col("cluster").cast("int").alias("cluster"))
    items = spark.table(n("bronze_items")).select(
        F.col("item_nbr").cast("long").alias("item_nbr"), "family",
        F.col("class").cast("int").alias("class"),
        F.col("perishable").cast("int").alias("perishable"))
    write_delta(stores, n("silver_stores"))
    write_delta(items, n("silver_items"))

    holidays = (spark.table(n("bronze_holidays"))
        .withColumn("date", F.to_date("date"))
        .withColumn("transferred", F.lower(F.col("transferred")).isin("true", "1")))
    write_delta(holidays, n("silver_holidays"))

    oil = (spark.table(n("bronze_oil"))
        .select(F.to_date("date").alias("date"),
                F.col("dcoilwtico").cast("double").alias("dcoilwtico"))
        .withColumn("dcoilwtico", F.last("dcoilwtico", ignorenulls=True)
                    .over(Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0))))
    write_delta(oil, n("silver_oil"))

    transactions = (spark.table(n("bronze_transactions"))
        .select(F.to_date("date").alias("date"),
                F.col("store_nbr").cast("int").alias("store_nbr"),
                F.col("transactions").cast("long").alias("transactions")))
    write_delta(transactions, n("silver_transactions"))


def build_family_daily(catalog, schema):
    n = lambda name: table_name(catalog, schema, name)
    sales = spark.table(n("silver_sales")).alias("s")
    items = spark.table(n("silver_items")).alias("i")
    stores = spark.table(n("silver_stores")).alias("st")
    item_sales = sales.join(items, "item_nbr", "inner")
    family_daily = (item_sales.groupBy("date", "store_nbr", "family")
        .agg(F.sum("unit_sales").alias("net_units"),
             F.sum(F.greatest(F.col("unit_sales"), F.lit(0.0))).alias("positive_units"),
             F.avg(F.col("onpromotion").cast("double")).alias("promotion_share"),
             F.countDistinct("item_nbr").alias("active_items"))
        .withColumn("forecast_units", F.greatest(F.col("net_units"), F.lit(0.0)))
        .join(stores, "store_nbr", "left")
        .join(spark.table(n("silver_transactions")), ["date", "store_nbr"], "left")
        .join(spark.table(n("silver_oil")), "date", "left"))

    holiday = (spark.table(n("silver_holidays"))
        .filter((F.col("locale") == "National") | (F.col("locale") == "Regional"))
        .groupBy("date").agg(F.max(F.lit(1)).alias("is_holiday")))
    family_daily = family_daily.join(holiday, "date", "left").fillna({"is_holiday": 0})
    write_delta(family_daily, n("gold_daily_family_store"))

    # Promotion summaries are computed from item-days so each group's flag is unambiguous.
    promo = (item_sales.groupBy("store_nbr", "family", "onpromotion")
        .agg(F.count(F.lit(1)).alias("item_days"),
             F.avg(F.greatest(F.col("unit_sales"), F.lit(0.0))).alias("avg_units_per_item_day"),
             F.sum(F.greatest(F.col("unit_sales"), F.lit(0.0))).alias("positive_units")))
    promo_window = Window.partitionBy("store_nbr", "family")
    promo = (promo.withColumn("promo_avg", F.max(F.when(F.col("onpromotion"), F.col("avg_units_per_item_day"))).over(promo_window))
        .withColumn("nonpromo_avg", F.max(F.when(~F.col("onpromotion"), F.col("avg_units_per_item_day"))).over(promo_window))
        .withColumn("associated_change_pct",
            F.when(F.col("nonpromo_avg") > 0,
                100.0 * (F.col("promo_avg") - F.col("nonpromo_avg")) / F.col("nonpromo_avg")))
        .filter(F.col("onpromotion") == True))
    write_delta(promo, n("gold_promotion_summary"))

    store_perf = (family_daily.groupBy("store_nbr", "city", "state", "type", "cluster")
        .agg(F.sum("net_units").alias("net_units"),
             F.sum("positive_units").alias("positive_units"),
             F.countDistinct("date").alias("active_days"),
             F.avg("promotion_share").alias("avg_promotion_share")))
    write_delta(store_perf, n("gold_store_performance"))

    product_perf = (family_daily.groupBy("family")
        .agg(F.sum("net_units").alias("net_units"),
             F.sum("positive_units").alias("positive_units"),
             F.countDistinct("store_nbr").alias("store_count"),
             F.countDistinct("date").alias("active_days"),
             F.avg("promotion_share").alias("avg_promotion_share")))
    write_delta(product_perf, n("gold_product_performance"))

