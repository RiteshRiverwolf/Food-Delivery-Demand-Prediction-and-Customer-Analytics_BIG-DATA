"""Stage 2 - Bronze layer (raw, append-only history).

Every column is kept exactly as it arrived (all strings, no cleaning), stored
as Parquet, and stamped with lineage metadata:
  _source_system  where the data came from
  _source_file    the exact file each row was read from
  _batch_id       the ingestion run (from the manifest)
  _ingested_at    when the row entered the lake
  _row_hash       SHA-256 of the raw row, used later to detect duplicates
Tables are partitioned by _batch_id, so re-running a batch replaces only that
batch and older batches stay available for audit.
Finally row counts are reconciled against the manifest.
"""
import json
import os

from pyspark.sql import functions as F

from common import SOURCE, TABLES, parse_args, spark_session, write_report


def main():
    args = parse_args("Build the Bronze layer")
    with open(os.path.join(args.base, "raw", "_ingestion_manifest.json")) as fh:
        manifest = json.load(fh)
    batch_id = manifest["batch_id"]
    spark = spark_session("bronze")

    audit = {"batch_id": batch_id, "tables": {}}
    for table, (fname, columns) in TABLES.items():
        # escape='"' handles RFC 4180 doubled quotes, e.g. "Scotch Kids 5"" Scissors, Blunted, Red";
        # Spark's default (backslash escape) splits that name across columns
        raw = (spark.read.option("header", True).option("inferSchema", False).option("escape", '"')
               .csv(f"{args.base}/raw/{fname}"))
        bronze = (
            raw.withColumn("_source_system", F.lit(f"kaggle:{SOURCE['kaggle_ref']}"))
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in columns]), 256))
        )
        bronze.write.mode("overwrite").partitionBy("_batch_id").parquet(f"{args.base}/bronze/{table}")

        bronze_rows = spark.read.parquet(f"{args.base}/bronze/{table}").where(F.col("_batch_id") == batch_id).count()
        expected = manifest["files"][table]["rows"]
        audit["tables"][table] = {"landing_rows": expected, "bronze_rows": bronze_rows,
                                  "reconciled": bronze_rows == expected}
        print(f"  {table:<22} {bronze_rows:>12,} rows  reconciled={bronze_rows == expected}")

    if not all(t["reconciled"] for t in audit["tables"].values()):
        raise RuntimeError("Bronze row counts do not match the ingestion manifest")
    write_report(args.reports, "02_bronze_audit.json", audit)
    spark.stop()


if __name__ == "__main__":
    main()
