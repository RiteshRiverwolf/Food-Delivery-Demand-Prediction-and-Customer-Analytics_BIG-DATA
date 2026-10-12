"""Stage 4 - PySpark processing (Silver -> Gold analytics tables).

Each analysis answers a business question and exercises a Spark technique:

  A  demand_by_time          Spark SQL aggregation + window (share and rank within a day)
  B  department_kpis         broadcast join + aggregations + window over the whole table
  C  top_products_by_dept    join + window row_number (top-N per group)
  D  department_time_mix     filtering + pivot transformation
  E  customer_profile        multi-table aggregation, window "mode", ntile segmentation in Spark SQL
  F  customer_order_rhythm   lag + rolling window to flag customers who are slowing down
  G  first_in_cart           filtering + aggregation (what customers add first)

Customer behaviour uses only "prior" orders, so the "train" orders stay unseen
for the prediction models in the final phase (no data leakage).
Gold tables go to <base>/gold as Parquet; small summaries go to <reports> as CSV.
"""
import os
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import parse_args, spark_session, write_report


def save(df, base, reports, name, csv=True):
    df.write.mode("overwrite").parquet(f"{base}/gold/{name}")
    df = df.sparkSession.read.parquet(f"{base}/gold/{name}")
    if csv:
        df.toPandas().to_csv(os.path.join(reports, f"{name}.csv"), index=False)
    return df


def main():
    args = parse_args("PySpark processing: Silver -> Gold")
    base, reports = args.base, os.path.join(args.reports, "processing")
    os.makedirs(reports, exist_ok=True)
    spark = spark_session("processing")
    timings = {}

    orders = spark.read.parquet(f"{base}/silver/orders")
    products = spark.read.parquet(f"{base}/silver/products")
    items = spark.read.parquet(f"{base}/silver/order_items")
    prior_items = items.where(F.col("eval_set") == "prior")  # partition pruning: only reads eval_set=prior
    orders.createOrReplaceTempView("orders")
    products.createOrReplaceTempView("products")
    prior_items.createOrReplaceTempView("prior_items")

    # A. When do customers order? ------------------------------------------------
    t = time.time()
    demand = spark.sql("""
        SELECT order_dow, day_name, order_hour, orders,
               ROUND(100 * orders / SUM(orders) OVER (PARTITION BY order_dow), 2) AS pct_of_day,
               RANK() OVER (PARTITION BY order_dow ORDER BY orders DESC)           AS rank_in_day
        FROM (SELECT order_dow, day_name, order_hour, COUNT(*) AS orders
              FROM orders GROUP BY order_dow, day_name, order_hour)
        ORDER BY order_dow, order_hour
    """)
    save(demand, base, reports, "demand_by_time")
    timings["A_demand_by_time"] = round(time.time() - t, 1)

    # B. Which departments drive demand and loyalty? ------------------------------
    t = time.time()
    item_dept = prior_items.join(F.broadcast(products.select("product_id", "department", "aisle")), "product_id")
    dept = (
        item_dept.groupBy("department")
        .agg(F.count("*").alias("items_ordered"),
             F.countDistinct("order_id").alias("orders"),
             F.countDistinct("user_id").alias("customers"),
             F.round(F.avg(F.col("reordered").cast("int")), 4).alias("reorder_rate"))
        .withColumn("pct_of_items", F.round(100 * F.col("items_ordered") / F.sum("items_ordered").over(Window.partitionBy()), 2))
        .orderBy(F.desc("items_ordered"))
    )
    with open(os.path.join(reports, "plan_department_kpis.txt"), "w") as fh:
        fh.write(dept._jdf.queryExecution().simpleString())  # shows BroadcastHashJoin in the physical plan
    save(dept, base, reports, "department_kpis")
    timings["B_department_kpis"] = round(time.time() - t, 1)

    # C. Best sellers in every department -----------------------------------------
    t = time.time()
    product_counts = (
        item_dept.groupBy("department", "product_id")
        .agg(F.count("*").alias("times_ordered"), F.round(F.avg(F.col("reordered").cast("int")), 4).alias("reorder_rate"))
    )
    by_dept = Window.partitionBy("department").orderBy(F.desc("times_ordered"))
    top_products = (
        product_counts.withColumn("rank", F.row_number().over(by_dept))
        .where("rank <= 5")
        .join(F.broadcast(products.select("product_id", "product_name")), "product_id")
        .select("department", "rank", "product_name", "times_ordered", "reorder_rate")
        .orderBy("department", "rank")
    )
    save(top_products, base, reports, "top_products_by_dept")
    timings["C_top_products_by_dept"] = round(time.time() - t, 1)

    # D. What sells at which time of day? -----------------------------------------
    t = time.time()
    mix = (
        item_dept.where(F.col("department") != "unknown")
        .join(orders.select("order_id", "time_of_day"), "order_id")
        .groupBy("department").pivot("time_of_day", ["morning", "afternoon", "evening", "night"]).count()
    )
    total = sum(F.col(c) for c in ["morning", "afternoon", "evening", "night"])
    mix = mix.select("department", *[F.round(100 * F.col(c) / total, 2).alias(f"pct_{c}")
                                     for c in ["morning", "afternoon", "evening", "night"]])
    save(mix.orderBy(F.desc("pct_morning")), base, reports, "department_time_mix")
    timings["D_department_time_mix"] = round(time.time() - t, 1)

    # E. Who are our customers? ---------------------------------------------------
    t = time.time()
    prior_orders = orders.where(F.col("eval_set") == "prior")
    order_side = prior_orders.groupBy("user_id").agg(
        F.count("*").alias("n_orders"),
        F.round(F.avg("days_since_prior_order"), 2).alias("avg_days_between_orders"),
        F.round(F.avg(F.col("is_weekend").cast("int")), 3).alias("pct_weekend_orders"),
    )
    item_side = prior_items.groupBy("user_id").agg(
        F.count("*").alias("total_items"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.round(F.avg(F.col("reordered").cast("int")), 3).alias("reorder_ratio"),
    )
    # favourite department / hour = most frequent value per user (window "mode")
    fav_dept = (
        item_dept.groupBy("user_id", "department").count()
        .withColumn("r", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("count"), "department")))
        .where("r = 1").select("user_id", F.col("department").alias("favourite_department"))
    )
    fav_hour = (
        prior_orders.groupBy("user_id", "order_hour").count()
        .withColumn("r", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("count"), "order_hour")))
        .where("r = 1").select("user_id", F.col("order_hour").alias("favourite_hour"))
    )
    features = (
        order_side.join(item_side, "user_id").join(fav_dept, "user_id").join(fav_hour, "user_id")
        .withColumn("avg_basket_size", F.round(F.col("total_items") / F.col("n_orders"), 2))
    )
    features.createOrReplaceTempView("customer_features")
    profile = spark.sql("""
        WITH scored AS (
            SELECT *,
                   NTILE(4) OVER (ORDER BY avg_days_between_orders ASC) AS frequency_quartile,  -- 1 = orders most often
                   NTILE(4) OVER (ORDER BY avg_basket_size DESC)        AS basket_quartile      -- 1 = biggest baskets
            FROM customer_features)
        SELECT *,
               CASE WHEN frequency_quartile = 1 AND basket_quartile <= 2 THEN 'Power Shoppers'
                    WHEN frequency_quartile = 1                          THEN 'Frequent Top-up'
                    WHEN frequency_quartile = 4 AND basket_quartile = 1  THEN 'Occasional Bulk'
                    WHEN frequency_quartile = 4                          THEN 'Light / At-risk'
                    ELSE 'Regulars' END AS segment
        FROM scored
    """)
    profile = save(profile, base, reports, "customer_profile", csv=False)
    segments = (
        profile.groupBy("segment")
        .agg(F.count("*").alias("customers"),
             F.round(F.avg("n_orders"), 1).alias("avg_orders"),
             F.round(F.avg("avg_days_between_orders"), 1).alias("avg_days_between"),
             F.round(F.avg("avg_basket_size"), 1).alias("avg_basket"),
             F.round(F.avg("reorder_ratio"), 3).alias("avg_reorder_ratio"),
             F.sum("total_items").alias("total_items"))
        .withColumn("pct_customers", F.round(100 * F.col("customers") / F.sum("customers").over(Window.partitionBy()), 1))
        .withColumn("pct_items", F.round(100 * F.col("total_items") / F.sum("total_items").over(Window.partitionBy()), 1))
        .orderBy(F.desc("avg_orders"))
    )
    save(segments, base, reports, "customer_segments")
    timings["E_customer_profile"] = round(time.time() - t, 1)

    # F. Is a customer slowing down? (early churn signal) -------------------------
    t = time.time()
    history = Window.partitionBy("user_id").orderBy("order_number")
    rhythm = (
        prior_orders.where(~F.col("is_first_order"))
        .withColumn("prev_gap", F.lag("days_since_prior_order").over(history))
        .withColumn("gap_change", F.col("days_since_prior_order") - F.col("prev_gap"))
        .withColumn("rolling_gap_3", F.avg("days_since_prior_order").over(history.rowsBetween(-2, 0)))
        .withColumn("lifetime_avg_gap", F.avg("days_since_prior_order").over(Window.partitionBy("user_id")))
        .withColumn("is_latest", F.col("order_number") == F.max("order_number").over(Window.partitionBy("user_id")))
    )
    latest = (
        rhythm.where("is_latest")
        .select("user_id", "order_number", F.round("rolling_gap_3", 1).alias("recent_avg_gap"),
                F.round("lifetime_avg_gap", 1).alias("lifetime_avg_gap"))
        # gaps are capped at 30 days in the source, so "1.5x the usual gap" is capped at 30 too;
        # otherwise customers who usually wait 20+ days could never be flagged.
        # Customers whose lifetime average is already 30 have no room to slow down and are left unflagged.
        .withColumn("slowing_threshold", F.least(1.5 * F.col("lifetime_avg_gap"), F.lit(30.0)))
        .withColumn("slowing_down", (F.col("recent_avg_gap") >= F.col("slowing_threshold"))
                    & (F.col("lifetime_avg_gap") < 30))
        .drop("slowing_threshold")
    )
    latest = save(latest, base, reports, "customer_order_rhythm", csv=False)
    rhythm_summary = (
        latest.join(profile.select("user_id", "segment"), "user_id")
        .groupBy("segment")
        .agg(F.count("*").alias("customers"),
             F.sum(F.col("slowing_down").cast("int")).alias("slowing_down"))
        .withColumn("pct_slowing_down", F.round(100 * F.col("slowing_down") / F.col("customers"), 1))
        .orderBy(F.desc("pct_slowing_down"))
    )
    save(rhythm_summary, base, reports, "churn_signal_by_segment")
    timings["F_customer_order_rhythm"] = round(time.time() - t, 1)

    # G. What goes into the cart first? -------------------------------------------
    t = time.time()
    first = (
        prior_items.where(F.col("add_to_cart_order") == 1)
        .groupBy("product_id").agg(F.count("*").alias("times_added_first"))
        .join(prior_items.groupBy("product_id").agg(F.count("*").alias("times_ordered")), "product_id")
        .where(F.col("times_ordered") >= 1000)
        .withColumn("pct_added_first", F.round(100 * F.col("times_added_first") / F.col("times_ordered"), 1))
        .join(F.broadcast(products.select("product_id", "product_name", "department")), "product_id")
        .orderBy(F.desc("pct_added_first"))
        .limit(20)
        .select("product_name", "department", "times_ordered", "times_added_first", "pct_added_first")
    )
    save(first, base, reports, "first_in_cart")
    timings["G_first_in_cart"] = round(time.time() - t, 1)

    for name, secs in timings.items():
        print(f"  {name:<28} {secs:>6}s")
    write_report(args.reports, "04_processing_timings.json", timings)
    spark.stop()


if __name__ == "__main__":
    main()
