"""Quick check that Spark can read the raw data."""
import sys
from pyspark.sql import SparkSession

raw = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
spark = SparkSession.builder.appName("smoke-test").getOrCreate()
orders = spark.read.csv(f"{raw}/orders.csv", header=True, inferSchema=True)
orders.printSchema()
print("orders:", orders.count())
orders.groupBy("eval_set").count().show()
spark.stop()
