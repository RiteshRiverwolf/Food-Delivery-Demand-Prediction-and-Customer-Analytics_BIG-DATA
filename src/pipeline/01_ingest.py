"""Stage 1 - Collect & ingest.

Pulls the source CSVs from Kaggle into the landing zone (data/raw) if they are
not already there, then validates every file (exists, header matches the
expected schema, size, MD5 checksum, row count) and writes an ingestion
manifest. The manifest's batch_id ties every later layer back to this run.
"""
import hashlib
import os
import subprocess
import sys
from datetime import datetime, timezone

from common import SOURCE, TABLES, parse_args, write_report


def md5_and_rows(path):
    digest, rows = hashlib.md5(), 0
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
            rows += chunk.count(b"\n")
    return digest.hexdigest(), rows - 1  # minus header


def main():
    args = parse_args("Ingest Instacart CSVs into the landing zone")
    landing = os.path.join(args.base, "raw")
    os.makedirs(landing, exist_ok=True)

    missing = [f for f, _ in TABLES.values() if not os.path.exists(os.path.join(landing, f))]
    if missing:
        print(f"downloading from Kaggle ({len(missing)} files missing)")
        subprocess.run([os.path.join(os.path.dirname(sys.executable), "kaggle"), "datasets", "download",
                        "-d", SOURCE["kaggle_ref"], "-p", landing, "--unzip"], check=True)

    started = datetime.now(timezone.utc)
    files = {}
    for table, (fname, expected_cols) in TABLES.items():
        path = os.path.join(landing, fname)
        with open(path, encoding="utf-8") as fh:
            header = fh.readline().strip().split(",")
        if header != expected_cols:
            raise ValueError(f"{fname}: header {header} does not match expected {expected_cols}")
        checksum, rows = md5_and_rows(path)
        files[table] = {"file": fname, "rows": rows, "bytes": os.path.getsize(path),
                        "md5": checksum, "columns": header}
        print(f"  {table:<22} {rows:>12,} rows  {os.path.getsize(path) / 1e6:8.1f} MB  ok")

    manifest = {
        "batch_id": started.strftime("%Y%m%d_%H%M%S"),
        "ingested_at_utc": started.isoformat(),
        "landing_path": landing,
        "source": SOURCE,
        "files": files,
        "total_rows": sum(f["rows"] for f in files.values()),
    }
    write_report(landing, "_ingestion_manifest.json", manifest)
    write_report(args.reports, "01_ingestion_manifest.json", manifest)


if __name__ == "__main__":
    main()
