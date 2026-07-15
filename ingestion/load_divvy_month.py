import csv
import os
import sys
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv
from slugify import slugify
import psycopg2


BASE_URL = "https://divvy-tripdata.s3.amazonaws.com"
RAW_SCHEMA = "raw"
RAW_TABLE = "trips_raw"   # raw landing table; we’ll build cleaned tables later


def download_zip(yyyymm: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / f"{yyyymm}-divvy-tripdata.zip"
    if zip_path.exists():
        print(f"Zip already exists: {zip_path}")
        return zip_path

    url = f"{BASE_URL}/{yyyymm}-divvy-tripdata.zip"
    print(f"Downloading: {url}")
    # Stream to a .part file and rename on success, so an interrupted
    # download never leaves a partial zip at the final path (which the
    # reuse check above would trust on the next run).
    part_path = zip_path.with_suffix(".zip.part")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(part_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    part_path.replace(zip_path)
    print(f"Saved: {zip_path}")
    return zip_path


def extract_csv(zip_path: Path, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        # Divvy zips ship macOS metadata entries (__MACOSX/._foo.csv) that
        # also end in .csv — skip those and hidden files before selecting.
        csv_names = [
            n
            for n in z.namelist()
            if n.lower().endswith(".csv")
            and "__MACOSX" not in Path(n).parts
            and not Path(n).name.startswith(".")
        ]
        if not csv_names:
            raise ValueError("No CSV found in zip.")
        if len(csv_names) > 1:
            raise ValueError(f"Multiple CSVs found in zip, refusing to guess: {csv_names}")
        csv_name = csv_names[0]
        target = out_dir / Path(csv_name).name
        if target.exists():
            print(f"CSV already extracted: {target}")
            return target
        z.extract(csv_name, out_dir)
        extracted = out_dir / csv_name
        extracted.rename(target)
        print(f"Extracted: {target}")
        return target


def get_header(csv_path: Path) -> list[str]:
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
    if not header:
        raise ValueError("Empty header.")
    return header


def normalize_col(col: str) -> str:
    # Make safe SQL identifiers; keep it deterministic
    return slugify(col, separator="_").lower()


def ensure_raw_table(conn, columns: list[str]) -> None:
    cols_sql = ",\n  ".join([f'"{c}" TEXT' for c in columns])
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};

    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{RAW_TABLE} (
      source_month TEXT NOT NULL,
      {cols_sql}
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def copy_csv(conn, csv_path: Path, yyyymm: str, columns: list[str]) -> None:
    # Load into temp table first, then insert with source_month
    tmp = f"{RAW_TABLE}_tmp"

    cols_sql = ", ".join([f'"{c}" TEXT' for c in columns])
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{tmp};")
        cur.execute(f"CREATE TABLE {RAW_SCHEMA}.{tmp} ({cols_sql});")
        conn.commit()

        print("COPY into temp table...")
        with open(csv_path, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f'COPY {RAW_SCHEMA}.{tmp} FROM STDIN WITH (FORMAT csv, HEADER true)',
                f,
            )

        print(f"Replacing existing rows for source_month={yyyymm}...")
        cur.execute(
            f"DELETE FROM {RAW_SCHEMA}.{RAW_TABLE} WHERE source_month = %s;",
            (yyyymm,),
        )
        print(f"Deleted {cur.rowcount} existing rows for source_month={yyyymm}.")

        # Insert into landing table + tag with month
        cols_list = ", ".join([f'"{c}"' for c in columns])
        cur.execute(
            f"""
            INSERT INTO {RAW_SCHEMA}.{RAW_TABLE} (source_month, {cols_list})
            SELECT %s AS source_month, {cols_list}
            FROM {RAW_SCHEMA}.{tmp};
            """,
            (yyyymm,),
        )
        cur.execute(f"DROP TABLE {RAW_SCHEMA}.{tmp};")

    conn.commit()
    print("Load complete.")


def main():
    if len(sys.argv) != 2:
        print("Usage: python ingestion/load_divvy_month.py YYYYMM")
        sys.exit(1)

    yyyymm = sys.argv[1]
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise ValueError("YYYYMM must be 6 digits, e.g. 202401")

    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not found. Did you create a .env file?")

    data_dir = Path("data")
    zip_dir = data_dir / "zips"
    csv_dir = data_dir / "csv"

    zip_path = download_zip(yyyymm, zip_dir)
    csv_path = extract_csv(zip_path, csv_dir)

    raw_header = get_header(csv_path)
    columns = [normalize_col(c) for c in raw_header]

    # Connect using psycopg2 URL (strip sqlalchemy prefix if present)
    pg_url = db_url.replace("postgresql+psycopg2://", "postgresql://")

    with psycopg2.connect(pg_url) as conn:
        ensure_raw_table(conn, columns)
        copy_csv(conn, csv_path, yyyymm, columns)

    print(f"✅ Loaded {yyyymm} into {RAW_SCHEMA}.{RAW_TABLE}")


if __name__ == "__main__":
    main()
