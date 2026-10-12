"""Shared settings for the medallion pipeline (landing -> bronze -> silver -> gold).

Every stage takes --base, the root data folder. Locally that is "data"; on
Dataproc it becomes a gs:// bucket path, so the same code runs in both places.
"""
import argparse
import json
import os

from pyspark.sql import SparkSession

SOURCE = {
    "provider": "Instacart (Maplebear Inc.)",
    "dataset": "The Instacart Online Grocery Shopping Dataset 2017",
    "distribution": "Kaggle",
    "kaggle_ref": "yasserh/instacart-online-grocery-basket-analysis-dataset",
    "url": "https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-dataset",
    "format": "CSV",
}

# table name -> (file name, expected header)
TABLES = {
    "orders": ("orders.csv", ["order_id", "user_id", "eval_set", "order_number", "order_dow",
                              "order_hour_of_day", "days_since_prior_order"]),
    "order_products_prior": ("order_products__prior.csv", ["order_id", "product_id", "add_to_cart_order", "reordered"]),
    "order_products_train": ("order_products__train.csv", ["order_id", "product_id", "add_to_cart_order", "reordered"]),
    "products": ("products.csv", ["product_id", "product_name", "aisle_id", "department_id"]),
    "aisles": ("aisles.csv", ["aisle_id", "aisle"]),
    "departments": ("departments.csv", ["department_id", "department"]),
}


def parse_args(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--base", default="data", help="root data folder or gs:// path")
    parser.add_argument("--reports", default="outputs", help="folder for small report files")
    return parser.parse_args()


def spark_session(app_name):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def write_report(reports_dir, name, obj):
    os.makedirs(reports_dir, exist_ok=True)
    path = os.path.join(reports_dir, name)
    with open(path, "w") as fh:
        json.dump(obj, fh, indent=2, default=str)
    print(f"report -> {path}")
