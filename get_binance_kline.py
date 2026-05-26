import os
import requests
import zipfile
import pandas as pd
import numpy as np
from io import BytesIO

# 币安 K线数据的标准列名
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]


def get_all_usdt_symbols():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    try:
        res = requests.get(url, timeout=10).json()
        return [s["symbol"] for s in res["symbols"] if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"]
    except Exception as e:
        print(f"获取列表失败: {e}")
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]


def download_and_convert_kline(symbol, interval, year_month, output_dir="binance_parquet_data"):
    file_name = f"{symbol}-{interval}-{year_month}"
    url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/{interval}/{file_name}.zip"

    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 404:
            return False
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

                # 限制合法的时间戳范围，过滤错位脏数据
                valid_time_mask = (df["open_time"] >= 1.5e12) & (df["open_time"] <= 1.9e12)
                df = df[valid_time_mask].copy()

                if df.empty: return False

                df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
                df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

                num_cols = ["open", "high", "low", "close", "volume"]
                for col in num_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype("float32")

                if "ignore" in df.columns:
                    df.drop(columns=["ignore"], inplace=True)

                symbol_dir = os.path.join(output_dir, symbol)
                os.makedirs(symbol_dir, exist_ok=True)

                df.to_parquet(os.path.join(symbol_dir, f"{file_name}.parquet"), engine="pyarrow", compression="snappy",
                              index=False)
                return True
    except:
        return False


if __name__ == "__main__":
    INTERVAL = "1h"
    BASE_DIR = "binance_parquet_data"

    months_to_download = [f"2025-{str(m).zfill(2)}" for m in range(1, 13)] + ["2026-01", "2026-02", "2026-03",
                                                                              "2026-04"]

    all_symbols = get_all_usdt_symbols()

    target_symbols = all_symbols[:10]

    print(f"开始云端下载并转换以下币种: {target_symbols}")
    for symbol in target_symbols:
        success_count = 0
        for ym in months_to_download:
            if download_and_convert_kline(symbol, INTERVAL, ym, output_dir=BASE_DIR):
                success_count += 1
        print(f"{symbol} 转换成功 {success_count} 个月份。")