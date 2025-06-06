# Stock Data Downloader

This project provides a Python script that downloads A-share (Chinese mainland) stock data using [AKShare](https://akshare.vip). It focuses on reliability and efficient incremental updates.

## Requirements

- Python 3
- `akshare`, `pandas`, `tqdm`, `tenacity`, `sqlalchemy`

Install the dependencies:

```bash
pip install akshare pandas tqdm tenacity sqlalchemy
```

## Usage

```bash
python akshare_downloader.py CODE1 CODE2 ... [--start YYYYMMDD] [--end YYYYMMDD] [--threads N]
```

Example:

```bash
python akshare_downloader.py sz000001 sz000002 --start 20220101 --end 20220301 --threads 10
```

The script downloads daily, weekly and 1-minute data for each stock code and stores them in the SQLite database `stock_data.db` (customizable via `--db`). It performs incremental downloads by checking existing records, applies retries and rate limiting to avoid errors, supports up to 300 concurrent threads, and shows progress with a bar. The data can later be exported to CSV using standard SQLite tools.
