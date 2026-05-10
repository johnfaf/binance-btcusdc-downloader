from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import HTMLResponse
import os
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta
import sys
import uvicorn
import glob

app = FastAPI(title="Binance BTCUSDC 1s Data Downloader")

DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)

# ====================== Core Functions ======================

def get_existing_dates(product: str, symbol: str):
    """Return set of dates that already have parquet files"""
    dates = set()
    path = f"{DATA_DIR}/{product}/{symbol}"
    if not os.path.exists(path):
        return dates
    for file in os.listdir(path):
        if file.endswith(".parquet"):
            try:
                date_str = file.split("-")[-1].replace(".parquet", "")
                dates.add(date_str)
            except:
                continue
    return dates

def download_1s_klines(symbol="BTCUSDC", product="spot", days=400):
    """Download 1s klines - only missing dates"""
    base = "https://data.binance.vision"
    if product == "spot":
        prefix = f"/data/spot/daily/klines/{symbol}/1s/"
    elif product == "futures":
        prefix = f"/data/futures/um/daily/klines/{symbol}/1s/"
    else:
        print("❌ Invalid product")
        return 0

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    current = start_date

    existing = get_existing_dates(product, symbol)
    downloaded = 0

    print(f"🚀 Starting download for {product.upper()} {symbol}...")

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

                # Extract and convert to Parquet
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
                print(f"✅ Completed {date_str}")
            else:
                print(f"⚠️ No data for {date_str}")
        except Exception as e:
            print(f"❌ Error downloading {date_str}: {e}")

        current += timedelta(days=1)

    print(f"🎉 Finished {product}! Downloaded {downloaded} new days.")
    return downloaded


def merge_parquet_files(product: str, symbol="BTCUSDC"):
    """Merge all daily parquet files into one big file"""
    folder = f"{DATA_DIR}/{product}/{symbol}"
    if not os.path.exists(folder):
        print(f"❌ No data folder for {product}")
        return None

    parquet_files = sorted(glob.glob(f"{folder}/*.parquet"))
    if not parquet_files:
        print("❌ No parquet files found")
        return None

    print(f"🔄 Merging {len(parquet_files)} files for {product}...")

    dfs = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dfs.append(df)
            print(f"✓ Loaded {os.path.basename(file)}")
        except Exception as e:
            print(f"⚠️ Error reading {file}: {e}")

    if not dfs:
        return None

    final_df = pd.concat(dfs, ignore_index=True)
    final_df = final_df.sort_values(by=final_df.columns[0])  # Sort by timestamp

    output_path = f"{DATA_DIR}/{product}/{symbol}_1s_full.parquet"
    final_df.to_parquet(output_path, compression='gzip', index=False)

    size_mb = round(os.path.getsize(output_path) / (1024 * 1024), 2)
    print(f"🎉 Merge completed! File saved: {output_path} ({size_mb} MB)")
    return output_path


# ====================== Web Interface ======================

@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <html>
    <head><title>Binance BTCUSDC Downloader</title></head>
    <body style="font-family: Arial; padding: 30px; line-height: 1.6;">
        <h1>🚀 Binance BTCUSDC 1s Data Downloader</h1>
        <p><strong>Status:</strong> Running on Railway</p>
        
        <h2>Download Data</h2>
        <p>
            <a href="/download/spot?days=400" target="_blank"><button style="padding:10px 20px; font-size:16px;">📥 Download Spot (Last 400 days)</button></a><br><br>
            <a href="/download/futures?days=400" target="_blank"><button style="padding:10px 20px; font-size:16px;">📥 Download Perpetual Futures (Last 400 days)</button></a>
        </p>

        <h2>Merge Files</h2>
        <p>
            <a href="/merge/spot" target="_blank"><button style="padding:10px 20px; font-size:16px;">🔗 Merge Spot into One File</button></a><br><br>
            <a href="/merge/futures" target="_blank"><button style="padding:10px 20px; font-size:16px;">🔗 Merge Futures into One File</button></a>
        </p>

        <h2>Check Status</h2>
        <p>
            <a href="/list" target="_blank"><button>📋 View All Files</button></a><br><br>
            <a href="/merged" target="_blank"><button>📊 View Merged Files</button></a>
        </p>
        
        <hr>
        <small>Tip: Set up a Railway Cron Job with command <code>python main.py download</code> for daily updates.</small>
    </body>
    </html>
    """


@app.get("/download/{product}")
async def start_download(product: str, days: int = 400, background_tasks: BackgroundTasks = None):
    if product not in ["spot", "futures"]:
        return {"error": "Use 'spot' or 'futures'"}
    background_tasks.add_task(download_1s_klines, "BTCUSDC", product, days)
    return {"status": f"Download started for {product} (last {days} days). Check Railway Logs."}


@app.get("/merge/{product}")
async def start_merge(product: str, background_tasks: BackgroundTasks = None):
    if product not in ["spot", "futures"]:
        return {"error": "Use 'spot' or 'futures'"}
    background_tasks.add_task(merge_parquet_files, product)
    return {"status": f"Merge process started for {product}. Check logs."}


@app.get("/list")
async def list_files():
    files = []
    for root, _, fs in os.walk(DATA_DIR):
        for f in fs:
            if f.endswith(".parquet"):
                full_path = os.path.join(root, f)
                size_mb = round(os.path.getsize(full_path) / (1024*1024), 2)
                files.append({"file": full_path.replace(DATA_DIR, ""), "size_mb": size_mb})
    return {"total_files": len(files), "files": sorted(files, key=lambda x: x["file"], reverse=True)[:50]}


@app.get("/merged")
async def list_merged():
    files = []
    for product in ["spot", "futures"]:
        path = f"{DATA_DIR}/{product}/BTCUSDC_1s_full.parquet"
        if os.path.exists(path):
            size_mb = round(os.path.getsize(path) / (1024*1024), 2)
            files.append({"product": product, "file": "BTCUSDC_1s_full.parquet", "size_mb": size_mb})
    return {"merged_files": files}


@app.get("/status")
async def status():
    return {"status": "running", "data_dir": DATA_DIR}


# ====================== CLI Support (for Cron) ======================

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "download":
        print("🔄 Running automated download job...")
        download_1s_klines("BTCUSDC", "spot", 400)
        download_1s_klines("BTCUSDC", "futures", 400)
        print("✅ Automated download completed")
    else:
        port = int(os.getenv("PORT", 8000))
        uvicorn.run(app, host="0.0.0.0", port=port)
