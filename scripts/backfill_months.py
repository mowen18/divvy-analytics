"""Backfill a contiguous range of Divvy months into raw.trips_raw.

Reuses the functions in ingestion/load_divvy_month.py (nothing is
reimplemented here) and adds what a multi-month run needs on top:
an upfront availability check for every zip in the range, a schema-drift
check against the existing raw table before any row is written, per-month
row counts, and a final rows-per-source_month summary.

Usage:
    python scripts/backfill_months.py 202407 202506
    python scripts/backfill_months.py 202407 202506 --dbt

--dbt runs `dbt run --full-refresh` then `dbt test` from dbt/ after all
months have loaded — the one-shot bulk build for a backfill. Steady-state
per-month processing stays on the incremental path (the Airflow DAG or
`dbt run --vars '{"source_month": ...}'`).
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
# scripts/ is sys.path[0] when run directly; ingestion/ lives at the repo root.
sys.path.insert(0, str(REPO_ROOT))
# download_zip/extract_csv resolve data/zips and data/csv relative to the cwd.
os.chdir(REPO_ROOT)

from ingestion.load_divvy_month import (  # noqa: E402
    BASE_URL,
    RAW_SCHEMA,
    RAW_TABLE,
    copy_csv,
    download_zip,
    ensure_raw_table,
    extract_csv,
    get_header,
    normalize_col,
)

ZIP_DIR = Path("data") / "zips"
CSV_DIR = Path("data") / "csv"


def parse_month(value: str) -> str:
    if len(value) != 6 or not value.isdigit() or not 1 <= int(value[4:]) <= 12:
        raise argparse.ArgumentTypeError(
            f"{value!r} is not a valid YYYYMM month, e.g. 202407"
        )
    return value


def month_range(start: str, end: str) -> list[str]:
    y, m = int(start[:4]), int(start[4:])
    end_y, end_m = int(end[:4]), int(end[4:])
    months = []
    while (y, m) <= (end_y, end_m):
        months.append(f"{y:04d}{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return months


def preflight(months: list[str]) -> None:
    # Check every zip exists before loading anything, so a missing month
    # at the end of the range can't leave the run half-done.
    print(f"Preflight: checking {len(months)} zips on {BASE_URL} ...")
    missing = []
    for month in months:
        url = f"{BASE_URL}/{month}-divvy-tripdata.zip"
        resp = requests.head(url, timeout=30, allow_redirects=True)
        if resp.status_code == 200:
            print(f"  {month}: OK")
        else:
            print(f"  {month}: MISSING (HTTP {resp.status_code})")
            missing.append(month)
    if missing:
        sys.exit(f"Aborting: no zip published for {', '.join(missing)}.")


def fetch_raw_columns(conn) -> set[str] | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s;",
            (RAW_SCHEMA, RAW_TABLE),
        )
        rows = cur.fetchall()
    return {r[0] for r in rows} if rows else None


def check_schema(month: str, csv_cols: list[str], db_cols: set[str]) -> None:
    # The ingestion script only errors when the CSV has a column the table
    # lacks; a column missing from the CSV would silently load as NULL.
    # Catch drift in both directions before any row is written.
    if len(set(csv_cols)) != len(csv_cols):
        sys.exit(f"Aborting on {month}: duplicate column names after slugify: {csv_cols}")
    expected = db_cols - {"source_month"}
    extra = set(csv_cols) - expected
    lacking = expected - set(csv_cols)
    if extra or lacking:
        lines = [f"Aborting on {month}: CSV header does not match {RAW_SCHEMA}.{RAW_TABLE}."]
        if extra:
            lines.append(f"  CSV has but table lacks: {', '.join(sorted(extra))}")
        if lacking:
            lines.append(f"  Table has but CSV lacks: {', '.join(sorted(lacking))}")
        sys.exit("\n".join(lines))


def count_month(conn, month: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {RAW_SCHEMA}.{RAW_TABLE} WHERE source_month = %s;",
            (month,),
        )
        return cur.fetchone()[0]


def print_summary(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT source_month, count(*) FROM {RAW_SCHEMA}.{RAW_TABLE} "
            "GROUP BY source_month ORDER BY source_month;"
        )
        rows = cur.fetchall()
    print(f"\nRows per source_month in {RAW_SCHEMA}.{RAW_TABLE}:")
    for month, n in rows:
        print(f"  {month}  {n:>9,}")
    print(f"  total   {sum(n for _, n in rows):>9,}")


def run_dbt() -> None:
    dbt_bin = Path(sys.executable).parent / "dbt"
    if not dbt_bin.exists():
        sys.exit(f"dbt not found at {dbt_bin}; run the dbt step manually from dbt/.")
    for cmd in (["run", "--full-refresh"], ["test"]):
        print(f"\nRunning: dbt {' '.join(cmd)}")
        subprocess.run([str(dbt_bin), *cmd], cwd=REPO_ROOT / "dbt", check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill an inclusive range of Divvy months into raw.trips_raw, oldest first."
    )
    parser.add_argument("start", type=parse_month, help="first month, YYYYMM")
    parser.add_argument("end", type=parse_month, help="last month, YYYYMM (inclusive)")
    parser.add_argument(
        "--dbt",
        action="store_true",
        help="after all months load, run `dbt run --full-refresh` then `dbt test`",
    )
    args = parser.parse_args()
    if args.start > args.end:
        parser.error(f"start {args.start} is after end {args.end}")
    months = month_range(args.start, args.end)

    load_dotenv(REPO_ROOT / ".env")
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        sys.exit("DATABASE_URL not found. Did you create a .env file?")
    pg_url = db_url.replace("postgresql+psycopg2://", "postgresql://")

    preflight(months)

    conn = psycopg2.connect(pg_url)
    month = months[0]
    try:
        # Fetched once: ensure_raw_table is CREATE TABLE IF NOT EXISTS, so an
        # existing table's columns cannot change mid-run.
        db_cols = fetch_raw_columns(conn)
        for i, month in enumerate(months, 1):
            print(f"\n[{i}/{len(months)}] {month}")
            zip_path = download_zip(month, ZIP_DIR)
            csv_path = extract_csv(zip_path, CSV_DIR)
            columns = [normalize_col(c) for c in get_header(csv_path)]
            if db_cols is not None:
                check_schema(month, columns, db_cols)
            ensure_raw_table(conn, columns)
            if db_cols is None:
                db_cols = fetch_raw_columns(conn)
            copy_csv(conn, csv_path, month, columns)
            print(f"== {month}: {count_month(conn, month):,} rows now in {RAW_SCHEMA}.{RAW_TABLE} ==")
        print_summary(conn)
    except Exception as exc:
        conn.rollback()
        sys.exit(f"FAILED on {month}: {exc}")
    finally:
        conn.close()

    if args.dbt:
        run_dbt()


if __name__ == "__main__":
    main()
