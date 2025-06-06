#!/usr/bin/env python3
"""A-share downloader using AKShare with SQLite storage.

Features:
- Incremental download (checks existing data in SQLite).
- Rate limiting and retry logic to avoid request failures.
- Optional multi-threading up to 300 threads with progress display.

Usage:
    python akshare_downloader.py CODE1 CODE2 ... [--start YYYYMMDD] [--end YYYYMMDD]

Example:
    python akshare_downloader.py sz000001 sz000002 --start 20220101 --end 20220301 --threads 5

The database ``stock_data.db`` will be created in the current directory.
"""
from __future__ import annotations

import argparse
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import akshare as ak
import pandas as pd
from sqlalchemy import create_engine
from tenacity import retry, stop_after_attempt, wait_fixed
from tqdm import tqdm

DB_NAME = "stock_data.db"
MAX_THREADS = 300
DEFAULT_SLEEP = 0.5

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download A-share data using AKShare")
    parser.add_argument("codes", nargs="+", help="Stock codes like sz000001")
    parser.add_argument("--start", default="20200101", help="Start date YYYYMMDD")
    parser.add_argument("--end", default=datetime.today().strftime("%Y%m%d"), help="End date YYYYMMDD")
    parser.add_argument("--threads", type=int, default=4, help="Number of download threads (1-300)")
    parser.add_argument("--db", default=DB_NAME, help="SQLite DB file")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Seconds to sleep between requests")
    return parser.parse_args()


def init_db(db_path: str):
    engine = create_engine(f"sqlite:///{db_path}", pool_size=5, max_overflow=10)
    # create index tables if not exists
    with engine.begin() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily (
                ts_code TEXT,
                trade_date TEXT,
                open REAL, high REAL, low REAL, close REAL,
                volume REAL, amount REAL,
                PRIMARY KEY (ts_code, trade_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS weekly (
                ts_code TEXT,
                trade_date TEXT,
                open REAL, high REAL, low REAL, close REAL,
                volume REAL, amount REAL,
                PRIMARY KEY (ts_code, trade_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS minute (
                ts_code TEXT,
                datetime TEXT,
                open REAL, high REAL, low REAL, close REAL,
                volume REAL, amount REAL,
                PRIMARY KEY (ts_code, datetime)
            )
            """
        )
    return engine


def get_last_date(engine, table: str, ts_code: str, date_col: str) -> str | None:
    with engine.begin() as conn:
        result = conn.execute(
            f"SELECT MAX({date_col}) FROM {table} WHERE ts_code=?", (ts_code,)
        ).fetchone()
    return result[0] if result and result[0] else None


@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def fetch_daily(code: str, start: str, end: str) -> pd.DataFrame:
    return ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end)


@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def fetch_weekly(code: str, start: str, end: str) -> pd.DataFrame:
    return ak.stock_zh_a_hist(symbol=code, period="weekly", start_date=start, end_date=end)


@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def fetch_minute(code: str, start: str, end: str) -> pd.DataFrame:
    return ak.stock_zh_a_hist_min_em(symbol=code, period="1", start_date=start, end_date=end)


def append_data(engine, table: str, df: pd.DataFrame, ts_code: str, date_col: str):
    if df.empty:
        return
    df.insert(0, "ts_code", ts_code)
    df.to_sql(table, engine, if_exists="append", index=False, method="multi")


def download_stock(engine, code: str, start: str, end: str, sleep_time: float):
    last_daily = get_last_date(engine, "daily", code, "trade_date")
    sd = last_daily if last_daily else start
    df_daily = fetch_daily(code, sd, end)
    append_data(engine, "daily", df_daily, code, "trade_date")
    time.sleep(sleep_time)

    last_weekly = get_last_date(engine, "weekly", code, "trade_date")
    sw = last_weekly if last_weekly else start
    df_weekly = fetch_weekly(code, sw, end)
    append_data(engine, "weekly", df_weekly, code, "trade_date")
    time.sleep(sleep_time)

    last_min = get_last_date(engine, "minute", code, "datetime")
    sm = last_min if last_min else start
    df_min = fetch_minute(code, sm, end)
    append_data(engine, "minute", df_min, code, "datetime")
    time.sleep(sleep_time)


def main():
    args = parse_args()
    thread_count = max(1, min(MAX_THREADS, args.threads))
    engine = init_db(args.db)

    codes = args.codes
    pbar = tqdm(total=len(codes), desc="Stocks", unit="stock")
    lock = threading.Lock()

    def task(code: str):
        try:
            download_stock(engine, code, args.start, args.end, args.sleep)
        except Exception as exc:
            print(f"Failed to download {code}: {exc}")
        finally:
            with lock:
                pbar.update(1)

    with ThreadPoolExecutor(max_workers=thread_count) as pool:
        for code in codes:
            pool.submit(task, code)
        pool.shutdown(wait=True)
    pbar.close()


if __name__ == "__main__":
    main()
