import os
import requests
import zipfile
import pandas as pd
import numpy as np
from io import BytesIO

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

def get_all_usdt_symbols():
    """从币安官方接口动态获取目前在线的所有 USDT 交易对"""
    url = "https://api.binance.com/api/v3/exchangeInfo"
    try:
        res = requests.get(url, timeout=10).json()
        symbols = [
            s["symbol"] for s in res["symbols"]
            if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"
        ]
        # 排除掉一些类似 BEAR/BULL 的杠杆代币，只留正经现货，缩短下载时间
        symbols = [s for s in symbols if "UPUSDT" not in s and "DOWNUSDT" not in s]
        print(f"🔥 成功获取币安现货列表，当前共有 {len(symbols)} 个活跃的 USDT 交易对。")
        return symbols
    except Exception as e:
        print(f"⚠️ 动态获取失败（可能由于接口风控），启用内置保底主力币种列表。")
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT"]

def download_and_convert_kline(symbol, interval, year_month, output_dir):
    file_name = f"{symbol}-{interval}-{year_month}"
    url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/{interval}/{file_name}.zip"
    try:
        response = requests.get(url, timeout=10)
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
    BASE_DIR = "binance_parquet_data"

    # 📅 时间范围：2025 全年 + 2026 至今
    months_2025 = [f"2025-{str(i).zfill(2)}" for i in range(1, 13)]
    months_2026 = [f"2026-{str(i).zfill(2)}" for i in range(1, 6)]
    months_to_download = months_2025 + months_2026

    # 1. 动态抓取币安此时此刻上架的所有币种
    all_symbols = get_all_usdt_symbols()
    
    # 2. 🛡️ 智能去重机制 🛡️
    # 检查仓库里哪些币种已经下载过了（比如之前成功的15个币），这次直接跳过，绝不重复劳动！
    existing_symbols = []
    if os.path.exists(BASE_DIR):
        existing_symbols = [f for f in os.listdir(BASE_DIR) if os.path.isdir(os.path.join(BASE_DIR, f))]
    
    # 过滤出还没下载的币种
    target_symbols = [s for s in all_symbols if s not in existing_symbols]
    print(f"📊 检查完毕：已有 {len(existing_symbols)} 个币种，本次将冲刺剩余的 {len(target_symbols)} 个新币种！")

    # 3. 🚨 分批控速机制：单次最多跑 80 个币，防止卡死超时
    # 如果没跑完，下次你再点一下 “Run workflow” 开关，它会接着后面的币继续下载！
    BATCH_SIZE = 80
    run_pool = target_symbols[:BATCH_SIZE]
    
    if not run_pool:
        print("🎉 奇迹发生！币安所有币种的数据已经全部收集完毕，无需重复运行！")
        exit(0)

    print(f"🚀 本批次正在全速轰鸣下载以下 {len(run_pool)} 个币种...")
    
    for symbol in run_pool:
        success_count = 0
        for ym in months_to_download:
            if download_and_convert_kline(symbol, INTERVAL, ym, output_dir=BASE_DIR):
                success_count += 1
        if success_count > 0:
            print(f"✨ {symbol} 成功同步 {success_count} 个月份。")
