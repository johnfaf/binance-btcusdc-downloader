from fastapi import FastAPI, BackgroundTasks
import os
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta
import sys
import uvicorn
from contextlib import asynccontextmanager

app = FastAPI(title="Binance BTCUSDC Data Downloader")

DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)

def get_existing_dates(product: str, symbol: str):
    """Return set of dates that already have parquet files"""
    dates = set()
    path = f"{DATA_DIR}/{product}/{symbol}"
    if not os.path.exists(path):
        return dates
    
    for file in os.listdir(path):
        if file.endswith(".parquet"):
            try:
                # filename example: BTCUSDC-1s-2025-05-10.parquet
                date_str = file.split("-")[-1].replace(".parquet", "")
                dates.add(date_str)
            except:
                continue
    return dates

def download_1s_klines(symbol="BTCUSDC", product="spot", days=400):
    """Smart downloader - only downloads missing dates"""
    base = "https://data.binance.vision"
    if product == "spot":
        prefix = f"/data/spot/daily/klines/{symbol}/1s/"
    elif product == "futures":
        prefix = f"/data/futures/um/daily/klines/{symbol}/1s/"
    else:
        print("❌ Invalid product. Use 'spot' or 'futures'")
        return

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    current = start_date

    existing_dates = get_existing_dates(product, symbol)
    downloaded = 0

    print(f"🚀 Starting download for {product.upper()} {symbol} (last {days} days)")

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        
        # Skip if already downloaded
        if date_str in existing_dates:
            current += timedelta(days=1)
            continue

        filename = f"{symbol}-1s-{date_str}.zip"
        url = f"{base}{prefix}{filename}"
        save_zip = f"{DATA_DIR}/{product}/{symbol}/{filename}"
        
        os.makedirs(os.path.dirname(save_zip), exist_ok=True)

        print(f"📥 Downloading {date_str} ...")
        try:
            r = requests.get(url, stream=True, timeout=60)
            
            if r.status_code == 200:
                # Save zip
                with open(save_zip, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

                # Extract CSV
                csv_path = save_zip.replace(".zip", ".csv")
                with zipfile.ZipFile(save_zip) as z:
                    z.extractall(os.path.dirname(save_zip))

                # Convert to Parquet (much better for backtesting)
                df = pd.read_csv(csv_path)
                parquet_path = save_zip.replace(".zip", ".parquet")
                df.to_parquet(parquet_path, compression='gzip', index=False)

                # Optional: remove zip and csv to save space
                os.remove(save_zip)
                if os.path.exists(csv_path):
                    os.remove(csv_path)

                downloaded += 1
                print(f"✅ Completed {date_str}")
            else:
                print(f"⚠️  No data for {date_str} (HTTP {r.status_code})")
        except Exception as e:
            print(f"❌ Error on {date_str}: {e}")

        current += timedelta(days=1)

    print(f"🎉 Finished! Downloaded {downloaded} new days for {product}.")
    return downloaded

# ==================== FastAPI Endpoints ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 App started")
    yield
    print("👋 App shutting down")

app = FastAPI(lifespan=lifespan)

@app.get("/download/{product}")
async def start_download(product: str, days: int = 400, background_tasks: BackgroundTasks = None):
    if product not in ["spot", "futures"]:
        return {"error": "Use 'spot' or 'futures'"}
    
    background_tasks.add_task(download_1s_klines, "BTCUSDC", product, days)
    return {"status": f"Background download started for {product} (last {days} days)"}

@app.get("/list")
async def list_files():
    files = []
    for root, _, fs in os.walk(DATA_DIR):
        for f in fs:
            if f.endswith(".parquet"):
                size_mb = round(os.path.getsize(os.path.join(root, f)) / (1024*1024), 2)
                files.append({"file": f, "size_mb": size_mb})
    return {"total_files": len(files), "recent_files": files[-15:]}

@app.get("/status")
async def status():
    return {"status": "running", "data_dir": DATA_DIR}

# ==================== CLI for Cron Jobs ====================

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "download":
        print("🔄 Running automated download job...")
        download_1s_klines("BTCUSDC", "spot", 400)
        download_1s_klines("BTCUSDC", "futures", 400)
        print("✅ Automated download job completed")
    else:
        # Normal web server
        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
