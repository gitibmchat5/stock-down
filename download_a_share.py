#!/usr/bin/env python3
"""
Download A-share stock data using Tushare.

This script fetches daily, weekly, and 1-minute bars for a given
stock code and stores them as CSV files.

Usage:
    export TUSHARE_TOKEN=your_tushare_token
    python download_a_share.py TS_CODE [START_DATE] [END_DATE]

Example:
    python download_a_share.py 000001.SZ 20230101 20230301

The START_DATE and END_DATE should use the format YYYYMMDD.
If not provided, the script defaults to 20200101 until today.
"""

import os
import sys
from datetime import datetime
import tushare as ts


def init_pro():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("Please set the TUSHARE_TOKEN environment variable")
    ts.set_token(token)
    return ts.pro_api()


def save_csv(df, filename):
    if df.empty:
        print(f"No data returned for {filename}")
    else:
        df.to_csv(filename, index=False)
        print(f"Saved {filename}")


def download_daily(pro, ts_code, start_date, end_date):
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    save_csv(df, f"{ts_code}_daily.csv")


def download_weekly(pro, ts_code, start_date, end_date):
    df = pro.weekly(ts_code=ts_code, start_date=start_date, end_date=end_date)
    save_csv(df, f"{ts_code}_weekly.csv")


def download_minute(pro, ts_code, start_date, end_date, freq="1min"):
    df = ts.pro_bar(api=pro, ts_code=ts_code, start_date=start_date, end_date=end_date, freq=freq)
    save_csv(df, f"{ts_code}_{freq}.csv")


def main():
    if len(sys.argv) < 2:
        print("Usage: python download_a_share.py TS_CODE [START_DATE] [END_DATE]")
        sys.exit(1)

    ts_code = sys.argv[1]
    start_date = sys.argv[2] if len(sys.argv) > 2 else "20200101"
    end_date = sys.argv[3] if len(sys.argv) > 3 else datetime.today().strftime("%Y%m%d")

    pro = init_pro()
    download_daily(pro, ts_code, start_date, end_date)
    download_weekly(pro, ts_code, start_date, end_date)
    download_minute(pro, ts_code, start_date, end_date)


if __name__ == "__main__":
    main()
