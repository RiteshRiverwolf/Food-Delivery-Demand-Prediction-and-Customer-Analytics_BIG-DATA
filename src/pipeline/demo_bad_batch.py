"""Demo: prove the Silver rules work by injecting a batch of deliberately broken records.

  python demo_bad_batch.py inject   # writes batch 'demo_bad_batch' into Bronze
  python 03_silver.py ...           # Silver deduplicates and quarantines it
  python demo_bad_batch.py remove   # deletes the demo batch again (then re-run Silver)

Bronze is partitioned by _batch_id, so the demo batch lives in its own folder
and removing it leaves the real data untouched.
"""
import shutil
import sys

from pyspark.sql import functions as F

from common import TABLES, parse_args, spark_session

BATCH = "demo_bad_batch"

# (record, what it tests). Values are strings, exactly as a CSV would deliver them.
ORDERS = [
    (("2539329", "1", "prior", "1", "2", "08", None), "exact re-send of an existing order -> duplicate removed"),
    (("9000001", "1", "prior", "12", "3", "25", "7.0"), "hour 25 -> hour_out_of_range"),
    (("9000002", "1", "prior", "13", "Funday", "10", "7.0"), "day 'Funday' -> bad_type"),
    (("9000003", "1", "unknown", "14", "3", "10", "7.0"), "eval_set 'unknown' -> invalid_eval_set"),
    (("9000004", "2", "prior", "1", "3", "10", "7.0"), "gap on a first order -> gap_on_first_order"),
    (("9000005", "2", "prior", "5", "3", "10", None), "missing gap on 5th order -> missing_gap_not_first_order"),
    ((None, "3", "prior", "6", "3", "10", "7.0"), "no order_id -> missing_key"),
    (("9000006", "4", "train", "1", "4", "11", None), "valid new first order -> passes to Silver"),
]
PRODUCTS = [
    (("900001", "Mystery Snack", "61", "99"), "department 99 does not exist -> unknown_department_id"),
    (("900002", "   ", "61", "19"), "blank name -> empty_name"),
]
ITEMS = [
    (("99999999", "24852", "1", "0"), "order does not exist -> orphan_order"),
    (("9000006", "999999", "1", "0"), "product does not exist -> orphan_product"),
    (("9000006", "24852", "1", "yes"), "reordered 'yes' -> bad_type"),
    (("9000006", "13176", "0", "0"), "cart position 0 -> invalid_cart_position"),
    (("9000006", "21137", "2", "1"), "reorder on a first order -> reorder_on_first_order"),
    (("9000006", "21903", "3", "0"), "valid item -> passes to Silver"),
]


def inject(spark, base):
    for table, records in [("orders", ORDERS), ("products", PRODUCTS), ("order_products_train", ITEMS)]:
        columns = TABLES[table][1]
        df = spark.createDataFrame([r for r, _ in records], schema=", ".join(f"{c} string" for c in columns))
        df = (
            df.withColumn("_source_system", F.lit("demo:injected"))
            .withColumn("_source_file", F.lit(f"demo://{table}"))
            .withColumn("_batch_id", F.lit(BATCH))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in columns]), 256))
        )
        df.write.mode("overwrite").partitionBy("_batch_id").parquet(f"{base}/bronze/{table}")
        print(f"{table}: injected {len(records)} records")
        for _, why in records:
            print(f"   - {why}")


def remove(base):
    for table in ["orders", "products", "order_products_train"]:
        shutil.rmtree(f"{base}/bronze/{table}/_batch_id={BATCH}", ignore_errors=True)
    print("demo batch removed from Bronze; re-run 03_silver.py to rebuild Silver")


def main():
    action = sys.argv.pop(1) if len(sys.argv) > 1 and sys.argv[1] in ("inject", "remove") else "inject"
    args = parse_args("Inject or remove the demo bad-data batch")
    if action == "remove":
        remove(args.base)
    else:
        spark = spark_session("demo-bad-batch")
        inject(spark, args.base)
        spark.stop()


if __name__ == "__main__":
    main()
