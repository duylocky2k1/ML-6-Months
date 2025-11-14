#!/usr/bin/env python3
"""
ETL Pipeline Pro: Decorator + Logging + Retry + Safe Run
Run: python etl.py --file data/input_sample.csv
"""

import argparse
import pandas as pd
import requests
import sys
from clickhouse_driver import Client
from decorators import log_time, retry, safe_run
from logging_conf import setup_logging
import logging

# Setup logging đầu tiên
setup_logging()
log = logging.getLogger(__name__)

# ClickHouse client
client = Client(
    host='localhost',
    port=9000,
    user='admin',
    password='mypass123'
)

TABLE = 'orders_etl'

# ===================== CREATE TABLE =====================
@log_time
def ensure_table():
    client.execute(f"""
    CREATE TABLE IF NOT EXISTS default.{TABLE} (
        order_id UInt32,
        user_id UInt32,
        amount Decimal(10,2),
        created_at DateTime,
        status LowCardinality(String),
        etl_timestamp DateTime DEFAULT now()
    ) ENGINE = MergeTree
    ORDER BY (created_at, order_id)
    """)
    log.info(f"Table {TABLE} ensured")

# ===================== EXTRACT =====================
@log_time
@retry(max_attempts=3, delay=2)
def extract_csv(file_path: str) -> pd.DataFrame:
    log.info(f"Extracting CSV: {file_path}")
    df = pd.read_csv(file_path)
    log.info(f"Extracted {len(df)} rows | columns: {list(df.columns)}")
    return df

@log_time
@retry(max_attempts=3, delay=2)
def extract_api(url: str) -> pd.DataFrame:
    log.info(f"Extracting API: {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data)
    log.info(f"API extracted {len(df)} rows")
    return df

# ===================== TRANSFORM =====================
@log_time
def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming data...")
    init_count = len(df)

    df.columns = [c.strip().lower() for c in df.columns]
    df['order_id'] = pd.to_numeric(df['order_id'], errors='coerce').fillna(0).astype('int')
    df['user_id'] = pd.to_numeric(df['user_id'], errors='coerce').fillna(0).astype('int')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

    valid = df[
        (df['order_id'] > 0) &
        (df['user_id'] > 0) &
        (df['amount'] > 0) &
        df['created_at'].notna()
    ].copy()

    invalid = df.drop(valid.index)
    log.info(f"Transform: {init_count} → {len(valid)} valid, {len(invalid)} invalid")
    if len(invalid) > 0:
        log.warning(f"Invalid rows dropped:\n{invalid[['order_id', 'user_id', 'amount']].to_string()}")

    return valid

# ===================== LOAD =====================
@log_time
@retry(max_attempts=3, delay=1)
def load_batch(records: list, batch_size: int = 1000):

    if not records:
        return
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        client.execute(
            f"INSERT INTO default.{TABLE} (order_id, user_id, amount, created_at, status) VALUES",
            batch
        )
        log.info(f"Loaded batch {i//batch_size + 1}: {len(batch)} rows")

# ===================== LOAD =====================
@log_time
@retry(max_attempts=3, delay=1)
def load_batch_2(records: list, batch_size: int = 1000):    
    if not records:
        return
    
    client.execute('TRUNCATE TABLE expenses')

    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        client.execute(
            'INSERT INTO expenses (date, category, description, payment_method, amount) VALUES',
            records
        )


        log.info(f"Loaded batch {i//batch_size + 1}: {len(batch)} rows")

# ===================== MAIN JOB =====================
@safe_run
def run_etl(file_path: str = None, api_url: str = None, batch_size: int = 1000):
    log.info("=== ETL JOB START ===")
    ensure_table()

    # EXTRACT
    if file_path:
        df = extract_csv(file_path)
    elif api_url:
        df = extract_api(api_url)
    else:
        raise ValueError("Must provide --file or --api")

    # TRANSFORM
    df_clean = transform(df)
    if df_clean.empty:
        log.warning("No valid data to load")
        return

    # LOAD
    records = df_clean.to_dict(orient='records')
    load_batch(records, batch_size)

    log.info("=== ETL JOB SUCCESS ===")

# ===================== MAIN JOB =====================
@safe_run
def run_etl_2(file_path: str = None, api_url: str = None, batch_size: int = 1000):
    log.info("=== ETL JOB START ===")
    ensure_table()

    # EXTRACT
    if file_path:
        df = extract_csv(file_path)
    elif api_url:
        df = extract_api(api_url)
    else:
        raise ValueError("Must provide --file or --api")
    
    print(df)

    # LOAD
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df= df.rename(columns={"Date": "date", "Category": "category",
                       "Description": "description","Payment Method": "payment_method",
                       "Amount": "amount"})
    records = df.to_dict(orient='records')

    load_batch_2(records, batch_size)

    log.info("=== ETL JOB SUCCESS ===")

# ===================== CLI =====================
def main():
    parser = argparse.ArgumentParser(description="Pro ETL Pipeline")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--file', type=str, help='CSV file path')
    group.add_argument('--api', type=str, help='API URL')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size')
    args = parser.parse_args()

    run_etl_2(file_path=args.file, api_url=args.api, batch_size=args.batch_size)

if __name__ == '__main__':
    main()