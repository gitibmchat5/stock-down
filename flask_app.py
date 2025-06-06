import os
from datetime import datetime
from flask import Flask, request, render_template_string

from akshare_downloader import init_db, download_stock, DEFAULT_SLEEP

app = Flask(__name__)
engine = init_db(os.getenv("STOCK_DB", "stock_data.db"))

FORM_HTML = """
<!doctype html>
<title>Stock Downloader</title>
<h1>Download Stock Data</h1>
<form method=post enctype=multipart/form-data action="/download">
  <label>Stock Codes (数字, 用 ; 分隔):</label><br>
  <input type=text name=codes style="width:300px"><br><br>
  <label>或上传CSV文件 (代码,名称):</label>
  <input type=file name=file><br><br>
  <label>开始日期 YYYYMMDD:</label>
  <input type=text name=start value="{start}"><br>
  <label>结束日期 YYYYMMDD:</label>
  <input type=text name=end value="{end}"><br><br>
  <input type=submit value="Download">
</form>
"""


def adapt_code(code: str) -> str:
    code = code.strip()
    if not code.isdigit():
        return ""
    prefix = "sh" if code.startswith("6") else "sz"
    return prefix + code.zfill(6)


@app.route("/")
def index():
    today = datetime.today().strftime("%Y%m%d")
    html = FORM_HTML.format(start="20200101", end=today)
    return render_template_string(html)


@app.route("/download", methods=["POST"])
def download():
    codes = []
    text = request.form.get("codes", "")
    if text:
        codes.extend([c.strip() for c in text.split(";") if c.strip()])

    file = request.files.get("file")
    if file and file.filename:
        content = file.read().decode("utf-8").splitlines()
        for line in content:
            parts = line.split(',')
            if parts and parts[0].strip().isdigit():
                codes.append(parts[0].strip())

    codes = [c for c in {c for c in codes if c.isdigit()}]

    start = request.form.get("start", "20200101")
    end = request.form.get("end", datetime.today().strftime("%Y%m%d"))

    downloaded = []
    for c in codes:
        symbol = adapt_code(c)
        if not symbol:
            continue
        try:
            download_stock(engine, symbol, start, end, DEFAULT_SLEEP)
            downloaded.append(symbol)
        except Exception as exc:
            print(f"Failed {symbol}: {exc}")

    return f"Downloaded {len(downloaded)} stocks." if downloaded else "No valid codes provided." 


if __name__ == "__main__":
    app.run(debug=True)
