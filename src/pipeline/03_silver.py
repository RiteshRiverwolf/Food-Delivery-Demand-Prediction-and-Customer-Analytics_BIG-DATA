"""Stage 3 - Silver layer (cleaned, typed, validated, conformed).

For each entity:
  1. deduplicate on the business key, keeping the most recently ingested row
     (bronze is append-only, so a key can appear in several batches)
  2. cast strings to proper types and standardise text
  3. run validation rules; rows that fail any rule go to a quarantine table
     with the reason(s) instead of being silently dropped
  4. add data-quality flags and derived columns for analysis

Silver tables: orders, products (joined with aisles and departments),
order_items (prior + train baskets). A data-quality report records what
every rule caught.
"""
from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import parse_args, spark_session, write_report

DAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]  # assumes 0 = Sunday


def latest_per_key(df, keys):
    """Keep one row per business key: the most recently ingested one."""
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc(), F.col("_batch_id").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def bad_type(raw_col, typed_col):
    """Value was present in the source but could not be cast to its type."""
    return F.col(raw_col).isNotNull() & F.col(typed_col).isNull()


def split_valid(df, rules):
    """rules: list of (name, condition that is True when the row is invalid)."""
    reason = F.concat_ws(",", *[F.when(cond, F.lit(name)) for name, cond in rules])
    df = df.withColumn("_dq_reason", reason).persist(StorageLevel.MEMORY_AND_DISK)
    return df.where("_dq_reason = ''").drop("_dq_reason"), df.where("_dq_reason <> ''")


def reason_counts(quarantine):
    rows = quarantine.select(F.explode(F.split("_dq_reason", ",")).alias("r")).groupBy("r").count().collect()
    return {r["r"]: r["count"] for r in rows}


def build_orders(spark, base):
    bronze = spark.read.parquet(f"{base}/bronze/orders")
    deduped = latest_per_key(bronze, ["order_id"])
    typed = (
        deduped.select(
            "*",
            F.col("order_id").cast("int").alias("order_id_t"),
            F.col("user_id").cast("int").alias("user_id_t"),
            F.lower(F.trim("eval_set")).alias("eval_set_t"),
            F.col("order_number").cast("int").alias("order_number_t"),
            F.col("order_dow").cast("int").alias("order_dow_t"),
            F.col("order_hour_of_day").cast("int").alias("order_hour_t"),
            # source stores "15.0"; cast through double so it becomes an int day count
            F.col("days_since_prior_order").cast("double").cast("int").alias("days_t"),
        )
    )
    rules = [
        ("missing_key", F.col("order_id_t").isNull() | F.col("user_id_t").isNull()),
        ("bad_type", bad_type("order_number", "order_number_t") | bad_type("order_dow", "order_dow_t")
         | bad_type("order_hour_of_day", "order_hour_t") | bad_type("days_since_prior_order", "days_t")),
        ("invalid_eval_set", ~F.col("eval_set_t").isin("prior", "train", "test")),
        ("invalid_order_number", F.col("order_number_t") < 1),
        ("dow_out_of_range", ~F.col("order_dow_t").between(0, 6)),
        ("hour_out_of_range", ~F.col("order_hour_t").between(0, 23)),
        ("days_out_of_range", ~F.col("days_t").between(0, 30)),
        ("missing_gap_not_first_order", F.col("days_t").isNull() & (F.col("order_number_t") != 1)),
        ("gap_on_first_order", F.col("days_t").isNotNull() & (F.col("order_number_t") == 1)),
    ]
    valid, quarantine = split_valid(typed, rules)

    silver = valid.select(
        F.col("order_id_t").alias("order_id"),
        F.col("user_id_t").alias("user_id"),
        F.col("eval_set_t").alias("eval_set"),
        F.col("order_number_t").alias("order_number"),
        F.col("order_dow_t").alias("order_dow"),
        F.element_at(F.array(*[F.lit(d) for d in DAY_NAMES]), F.col("order_dow_t") + 1).alias("day_name"),
        F.col("order_dow_t").isin(0, 6).alias("is_weekend"),
        F.col("order_hour_t").alias("order_hour"),
        F.when(F.col("order_hour_t").between(5, 11), "morning")
        .when(F.col("order_hour_t").between(12, 16), "afternoon")
        .when(F.col("order_hour_t").between(17, 21), "evening")
        .otherwise("night").alias("time_of_day"),
        F.col("days_t").alias("days_since_prior_order"),
        (F.col("order_number_t") == 1).alias("is_first_order"),
        (F.col("days_t") == 30).alias("is_gap_capped"),
        "_batch_id", "_ingested_at",
    )
    stats = {
        "bronze_rows": bronze.count(),
        "duplicates_removed": bronze.count() - deduped.count(),
        "quarantined": quarantine.count(),
        "quarantine_reasons": reason_counts(quarantine),
        "flags": {
            "is_first_order (gap legitimately missing)": silver.where("is_first_order").count(),
            "is_gap_capped (gap recorded as 30 = '30 or more')": silver.where("is_gap_capped").count(),
        },
    }
    return silver, quarantine, stats


def clean_text(c):
    return F.trim(F.regexp_replace(F.col(c), r"\s+", " "))


def build_products(spark, base):
    products = latest_per_key(spark.read.parquet(f"{base}/bronze/products"), ["product_id"])
    aisles = latest_per_key(spark.read.parquet(f"{base}/bronze/aisles"), ["aisle_id"])
    departments = latest_per_key(spark.read.parquet(f"{base}/bronze/departments"), ["department_id"])

    aisles = aisles.select(F.col("aisle_id").cast("int").alias("_dim_aisle_id"), F.lower(clean_text("aisle")).alias("aisle"))
    departments = departments.select(F.col("department_id").cast("int").alias("_dim_department_id"),
                                     F.lower(clean_text("department")).alias("department"))
    raw_bronze_rows = spark.read.parquet(f"{base}/bronze/products").count()

    typed = products.select(
        "*",
        F.col("product_id").cast("int").alias("product_id_t"),
        clean_text("product_name").alias("product_name_t"),
        F.col("aisle_id").cast("int").alias("aisle_id_t"),
        F.col("department_id").cast("int").alias("department_id_t"),
    )
    # small dimension tables -> broadcast join, no shuffle of products
    joined = (
        typed.join(F.broadcast(aisles), F.col("aisle_id_t") == F.col("_dim_aisle_id"), "left")
        .join(F.broadcast(departments), F.col("department_id_t") == F.col("_dim_department_id"), "left")
    )
    rules = [
        ("missing_key", F.col("product_id_t").isNull()),
        ("empty_name", F.col("product_name_t").isNull() | (F.col("product_name_t") == "")),
        ("unknown_aisle_id", F.col("_dim_aisle_id").isNull()),
        ("unknown_department_id", F.col("_dim_department_id").isNull()),
    ]
    valid, quarantine = split_valid(joined, rules)

    unknown = F.col("department") == "missing"
    same_name = Window.partitionBy(F.lower(F.col("product_name_t")))
    silver = valid.select(
        F.col("product_id_t").alias("product_id"),
        F.col("product_name_t").alias("product_name"),
        F.col("aisle_id_t").alias("aisle_id"),
        F.when(F.col("aisle") == "missing", "unknown").otherwise(F.col("aisle")).alias("aisle"),
        F.col("department_id_t").alias("department_id"),
        F.when(unknown, "unknown").otherwise(F.col("department")).alias("department"),
        (unknown | (F.col("aisle") == "missing")).alias("is_unknown_category"),
        (F.count("*").over(same_name) > 1).alias("is_duplicate_name"),
        (F.col("product_name") != F.col("product_name_t")).alias("name_was_cleaned"),
        "_batch_id", "_ingested_at",
    )
    stats = {
        "bronze_rows": raw_bronze_rows,
        "duplicates_removed": raw_bronze_rows - products.count(),
        "quarantined": quarantine.count(),
        "quarantine_reasons": reason_counts(quarantine),
        "flags": {
            "name_was_cleaned (extra whitespace removed)": silver.where("name_was_cleaned").count(),
            "is_duplicate_name (same name, different product_id)": silver.where("is_duplicate_name").count(),
            "is_unknown_category ('missing' -> 'unknown')": silver.where("is_unknown_category").count(),
        },
    }
    return silver, quarantine, stats


def build_order_items(spark, base, orders, products):
    bronze = (
        spark.read.parquet(f"{base}/bronze/order_products_prior").withColumn("eval_set", F.lit("prior"))
        .unionByName(spark.read.parquet(f"{base}/bronze/order_products_train").withColumn("eval_set", F.lit("train")))
    )
    deduped = latest_per_key(bronze, ["order_id", "product_id"])
    typed = deduped.select(
        F.col("add_to_cart_order").alias("add_to_cart_order_raw"),
        F.col("reordered").alias("reordered_raw"),
        F.col("order_id").cast("int").alias("order_id"),
        F.col("product_id").cast("int").alias("product_id"),
        F.col("add_to_cart_order").cast("int").alias("add_to_cart_order"),
        F.col("reordered").cast("int").alias("reordered"),
        "eval_set", "_batch_id", "_ingested_at",
    )

    order_keys = orders.select("order_id", "user_id", "order_number")
    product_keys = products.select("product_id", F.lit(True).alias("_product_exists"))
    joined = typed.join(order_keys, "order_id", "left").join(F.broadcast(product_keys), "product_id", "left")
    rules = [
        ("missing_key", F.col("order_id").isNull() | F.col("product_id").isNull()),
        ("bad_type", bad_type("add_to_cart_order_raw", "add_to_cart_order") | bad_type("reordered_raw", "reordered")),
        ("invalid_reordered_flag", ~F.col("reordered").isin(0, 1)),
        ("invalid_cart_position", F.col("add_to_cart_order") < 1),
        ("orphan_order", F.col("user_id").isNull()),
        ("orphan_product", F.col("_product_exists").isNull()),
        ("reorder_on_first_order", (F.col("reordered") == 1) & (F.col("order_number") == 1)),
    ]
    valid, quarantine = split_valid(joined, rules)
    silver = valid.select("order_id", "product_id", "user_id", "order_number", "add_to_cart_order",
                          F.col("reordered").cast("boolean").alias("reordered"), "eval_set", "_batch_id", "_ingested_at")
    bronze_rows = bronze.count()
    stats = {
        "bronze_rows": bronze_rows,
        "duplicates_removed": bronze_rows - deduped.count(),
        "quarantined": quarantine.count(),
        "quarantine_reasons": reason_counts(quarantine),
    }
    return silver, quarantine, stats


def main():
    args = parse_args("Build the Silver layer")
    spark = spark_session("silver")
    base = args.base
    report = {}

    orders, q_orders, report["orders"] = build_orders(spark, base)
    orders.write.mode("overwrite").parquet(f"{base}/silver/orders")
    q_orders.write.mode("overwrite").parquet(f"{base}/silver/_quarantine/orders")
    orders = spark.read.parquet(f"{base}/silver/orders")

    products, q_products, report["products"] = build_products(spark, base)
    products.write.mode("overwrite").parquet(f"{base}/silver/products")
    q_products.write.mode("overwrite").parquet(f"{base}/silver/_quarantine/products")
    products = spark.read.parquet(f"{base}/silver/products")

    items, q_items, report["order_items"] = build_order_items(spark, base, orders, products)
    items.write.mode("overwrite").partitionBy("eval_set").parquet(f"{base}/silver/order_items")
    q_items.write.mode("overwrite").parquet(f"{base}/silver/_quarantine/order_items")

    for table in ["orders", "products", "order_items"]:
        report[table]["silver_rows"] = spark.read.parquet(f"{base}/silver/{table}").count()
        print(f"  {table:<12} bronze={report[table]['bronze_rows']:>11,}  "
              f"dupes={report[table]['duplicates_removed']:,}  quarantined={report[table]['quarantined']:,}  "
              f"silver={report[table]['silver_rows']:>11,}")
    write_report(args.reports, "03_silver_dq_report.json", report)
    spark.stop()


if __name__ == "__main__":
    main()
