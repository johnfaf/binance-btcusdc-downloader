from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import HTMLResponse
import os
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta
import sys
import uvicorn

app = FastAPI(title="Binance BTCUSDC Downloader")

DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)

# ====================== Core Functions ======================

def get_existing_dates(product: str, symbol: str):
    dates = set()
    path = f"{DATA_DIR}/{product}/{symbol}"
    if not os.path.exists(path):
        return dates
    for f in os.listdir(path):
        if f.endswith(".parquet"):
            try:
                date_str = f.split("-")[-1].replace(".parquet", "")
                dates.add(date_str)
            except:
                continue
    return dates

def download_1s_klines(symbol="BTCUSDC", product="spot", days=400):
    base = "https://data.binance.vision"
    prefix = f"/data/{product}/daily/klines/{symbol}/1s/" if product == "spot" else f"/data/futures/um/daily/klines/{symbol}/1s/"
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    current = start_date

    existing = get_existing_dates(product, symbol)
    downloaded = 0

    print(f"🚀 Starting {product} download...")

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        if date_str in existing:
            current += timedelta(days=1)
            continue

        filename = f"{symbol}-1s-{date_str}.zip"
        url = f"{base}{prefix}{filename}"
        save_zip = f"{DATA_DIR}/{product}/{symbol}/{filename}"

        os.makedirs(os.path.dirname(save_zip), exist_ok=True)

        print(f"📥 Downloading {date_str}")
        try:
            r = requests.get(url, stream=True, timeout=60)
            if r.status_code == 200:
                with open(save_zip, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

                csv_path = save_zip.replace(".zip", ".csv")
                with zipfile.ZipFile(save_zip) as z:
                    z.extractall(os.path.dirname(save_zip))

                df = pd.read_csv(csv_path)
                parquet_path = save_zip.replace(".zip", ".parquet")
                df.to_parquet(parquet_path, compression='gzip', index=False)

                # Cleanup
                os.remove(save_zip)
                if os.path.exists(csv_path):
                    os.remove(csv_path)

                downloaded += 1
                print(f"✅ {date_str} completed")
            else:
                print(f"⚠️ No data for {date_str}")
        except Exception as e:
            print(f"❌ Error {date_str}: {e}")

        current += timedelta(days=1)

    print(f"🎉 {product} download finished: {downloaded} new days")
    return downloaded

# ====================== Web Routes ======================

@app.get("/", response_class=HTMLResponse)
async def home():
    html = """
    <html>
    <head><title>Binance BTCUSDC Downloader</title></head>
    <body style="font-family: Arial; padding: 20px;">
        <h1>🚀 Binance BTCUSDC 1s Data Downloader</h1>
        <p><strong>Status:</strong> Running</p>
        
        <h2>Quick Actions</h2>
        <p>
            <a href="/download/spot?days=400" target="_blank"><button>Download Spot (Last 400 days)</button></a><br><br>
            <a href="/download/futures?days=400" target="_blank"><button>Download Perpetual Futures (Last 400 days)</button></a>
        </p>
        
        <h2>Check Data</h2>
        <p>
            <a href="/list" target="_blank"><button>View Downloaded Files</button></a>
        </p>
        
        <hr>
        <p><small>Tip: Use Railway Cron Job with command <code>python main.py download</code> for daily auto-update.</small></p>
    </body>
    </html>
    """
    return html

@app.get("/download/{product}")
async def start_download(product: str, days: int = 400, background_tasks: BackgroundTasks = None):
    if product not in ["spot", "futures"]:
        return {"error": "Use 'spot' or 'futures'"}
    
    background_tasks.add_task(download_1s_klines, "BTCUSDC", product, days)
    return {"status": f"✅ Background download started for {product} (last {days} days). Check logs."}

@app.get("/list")
async def list_files():
    files = []
    for root, _, fs in os.walk(DATA_DIR):
        for f in fs:
            if f.endswith(".parquet"):
                size_mb = round(os.path.getsize(os.path.join(root, f)) / (1024*1024), 2)
                files.append({"file": os.path.join(root, f).replace(DATA_DIR, ""), "size_mb": size_mb})
    return {"total_files": len(files), "files": files[-30:]}

@app.get("/status")
async def status():
    return {"status": "running", "data_dir": DATA_DIR}

# ====================== CLI for Cron ======================

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "download":
        print("🔄 Running automated daily download...")
        download_1s_klines("BTCUSDC", "spot", 400)
        download_1s_klines("BTCUSDC", "futures", 400)
        print("✅ Daily job completed")
    else:
        port = int(os.getenv("PORT", 8000))
        uvicorn.run(app, host="0.0.0.0", port=port)
