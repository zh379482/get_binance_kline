import os
import sys
import requests
import zipfile
import pandas as pd
import numpy as np
from io import BytesIO
from tqdm import tqdm

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

def get_all_usdt_symbols():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    try:
        res = requests.get(url, timeout=10).json()
        symbols = [
            s["symbol"] for s in res["symbols"]
            if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"
        ]
        return symbols
    except Exception as e:
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]

def download_and_convert_kline(symbol, interval, year_month, output_dir):
    file_name = f"{symbol}-{interval}-{year_month}"
    url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/{interval}/{file_name}.zip"
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 404: return False
        response.raise_for_status()
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            if not csv_files: return False
            with z.open(csv_files[0]) as f:
                df = pd.read_csv(f, header=None)
                if pd.to_numeric(df.iloc[0, 0], errors='coerce') is np.nan:
                    df = df.iloc[1:].reset_index(drop=True)
                cols_count = min(df.shape[1], len(KLINE_COLUMNS))
                df = df.iloc[:, :cols_count]
                df.columns = KLINE_COLUMNS[:cols_count]
                df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
                df["close_time"] = pd.to_numeric(df["close_time"], errors="coerce")
                df.dropna(subset=["open_time", "close_time"], inplace=True)
                if df.empty: return False
                df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
                df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
                num_cols = ["open", "high", "low", "close", "volume"]
                for col in num_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype("float32")
                if "ignore" in df.columns: df.drop(columns=["ignore"], inplace=True)
                symbol_dir = os.path.join(output_dir, symbol)
                os.makedirs(symbol_dir, exist_ok=True)
                df.to_parquet(os.path.join(symbol_dir, f"{file_name}.parquet"), engine="pyarrow", compression="snappy", index=False)
                return True
    except:
        return False

if __name__ == "__main__":
    INTERVAL = "1d"
    # 云端相对路径，直接生成在项目根目录下
    BASE_DIR = "binance_parquet_data"

    # 生成 2025 全年 + 2026 至今的月份
    months_2025 = [f"2025-{str(i).zfill(2)}" for i in range(1, 13)]
    months_2026 = [f"2026-{str(i).zfill(2)}" for i in range(1, 6)]
    months_to_download = months_2025 + months_2026

    all_symbols = get_all_usdt_symbols()
    
    # 🚨 重点：既然在云端跑，不占用你本地网络和电脑，我们直接切片前 50 个币种！
    target_symbols = all_symbols[:50]

    for symbol in target_symbols:
        for ym in months_to_download:
            download_and_convert_kline(symbol, INTERVAL, ym, output_dir=BASE_DIR)
