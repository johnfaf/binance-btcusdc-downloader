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
from sqlalchemy import create_engine
import sqlalchemy as sa

app = FastAPI(title="Binance BTCUSDC 1s Data Manager")

DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)

# ====================== Core Functions ======================

def get_existing_dates(product: str, symbol: str):
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

                csv_path = save_zip.replace(".zip", ".csv")
                with zipfile.ZipFile(save_zip) as z:
                    z.extractall(os.path.dirname(save_zip))

                df = pd.read_csv(csv_path)
                parquet_path = save_zip.replace(".zip", ".parquet")
                df.to_parquet(parquet_path, compression='gzip', index=False)

                os.remove(save_zip)
                if os.path.exists(csv_path):
                    os.remove(csv_path)

                downloaded += 1
                print(f"✅ Completed {date_str}")
            else:
                print(f"⚠️ No data for {date_str}")
        except Exception as e:
            print(f"❌ Error {date_str}: {e}")

        current += timedelta(days=1)

    print(f"🎉 Finished {product}! Downloaded {downloaded} new days.")
    return downloaded


def merge_parquet_files(product: str, symbol="BTCUSDC"):
    folder = f"{DATA_DIR}/{product}/{symbol}"
    if not os.path.exists(folder):
        print(f"❌ No data for {product}")
        return None

    parquet_files = sorted(glob.glob(f"{folder}/*.parquet"))
    if not parquet_files:
        print("❌ No parquet files found")
        return None

    print(f"🔄 Merging {len(parquet_files)} files for {product}...")

    dfs = [pd.read_parquet(f) for f in parquet_files]
    final_df = pd.concat(dfs, ignore_index=True)
    final_df = final_df.sort_values(by=final_df.columns[0])

    output_path = f"{DATA_DIR}/{product}/{symbol}_1s_full.parquet"
    final_df.to_parquet(output_path, compression='gzip', index=False)

    size_mb = round(os.path.getsize(output_path) / (1024*1024), 2)
    print(f"🎉 Merge completed! ({size_mb} MB)")
    return output_path


def get_db_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not set in environment variables")
        return None
    return create_engine(db_url, pool_size=10, max_overflow=20)


def create_ohlcv_table():
    engine = get_db_engine()
    if not engine:
        return
    with engine.connect() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS btcusdc_1s_ohlcv (
                open_time TIMESTAMPTZ NOT NULL,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                quote_volume DOUBLE PRECISION,
                count INTEGER,
                taker_buy_volume DOUBLE PRECISION,
                taker_buy_quote_volume DOUBLE PRECISION,
                product TEXT NOT NULL,
                PRIMARY KEY (product, open_time)
            );
            CREATE INDEX IF NOT EXISTS idx_btc_time ON btcusdc_1s_ohlcv (open_time);
            CREATE INDEX IF NOT EXISTS idx_btc_product_time ON btcusdc_1s_ohlcv (product, open_time);
        """))
        print("✅ Table btcusdc_1s_ohlcv ready")


def import_parquet_to_postgres(product: str, symbol="BTCUSDC"):
    engine = get_db_engine()
    if not engine:
        return

    parquet_path = f"{DATA_DIR}/{product}/{symbol}_1s_full.parquet"
    if not os.path.exists(parquet_path):
        print(f"❌ Merged file not found for {product}")
        return

    create_ohlcv_table()
    print(f"📤 Importing {product} data to PostgreSQL...")

    chunk_size = 300_000
    for i, chunk in enumerate(pd.read_parquet(parquet_path, chunksize=chunk_size)):
        chunk = chunk.rename(columns={chunk.columns[0]: "open_time"})
        chunk['product'] = product
        
        chunk.to_sql(
            name="btcusdc_1s_ohlcv",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=chunk_size
        )
        print(f"✅ Imported chunk {i+1}")

    print(f"🎉 Successfully imported {product} data into PostgreSQL!")


# ====================== Routes ======================

@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <html>
    <head><title>Binance BTCUSDC Manager</title></head>
    <body style="font-family: Arial; padding: 30px;">
        <h1>🚀 Binance BTCUSDC 1s Data Manager</h1>
        
        <h2>1. Download Data</h2>
        <a href="/download/spot?days=400"><button>Download Spot</button></a><br><br>
        <a href="/download/futures?days=400"><button>Download Futures</button></a>

        <h2>2. Merge Files</h2>
        <a href="/merge/spot"><button>Merge Spot</button></a><br><br>
        <a href="/merge/futures"><button>Merge Futures</button></a>

        <h2>3. Import to PostgreSQL</h2>
        <a href="/import-to-db/spot"><button>Import Spot to DB</button></a><br><br>
        <a href="/import-to-db/futures"><button>Import Futures to DB</button></a>

        <h2>Check</h2>
        <a href="/list"><button>View Files</button></a>
    </body>
    </html>
    """


@app.get("/download/{product}")
async def start_download(product: str, days: int = 400, background_tasks: BackgroundTasks = None):
    background_tasks.add_task(download_1s_klines, "BTCUSDC", product, days)
    return {"status": f"Download started for {product}"}


@app.get("/merge/{product}")
async def start_merge(product: str, background_tasks: BackgroundTasks = None):
    background_tasks.add_task(merge_parquet_files, product)
    return {"status": f"Merge started for {product}"}


@app.get("/import-to-db/{product}")
async def start_import(product: str, background_tasks: BackgroundTasks = None):
    background_tasks.add_task(import_parquet_to_postgres, product)
    return {"status": f"Import to PostgreSQL started for {product}"}


@app.get("/list")
async def list_files():
    files = []
    for root, _, fs in os.walk(DATA_DIR):
        for f in fs:
            if f.endswith(".parquet"):
                size_mb = round(os.path.getsize(os.path.join(root, f)) / (1024*1024), 2)
                files.append({"file": f, "size_mb": size_mb})
    return {"total": len(files), "files": files}


# ====================== CLI for Cron ======================

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "download":
        download_1s_klines("BTCUSDC", "spot", 400)
        download_1s_klines("BTCUSDC", "futures", 400)
    else:
        port = int(os.getenv("PORT", 8000))
        uvicorn.run(app, host="0.0.0.0", port=port)
