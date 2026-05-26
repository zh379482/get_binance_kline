import os
import requests
import zipfile
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

def get_all_usdt_symbols():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    try:
        res = requests.get(url, timeout=10).json()
        symbols = [s["symbol"] for s in res["symbols"] if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"]
        return [s for s in symbols if "UPUSDT" not in s and "DOWNUSDT" not in s]
    except:
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]

def clean_and_parse_csv(file_content):
    """核心：清洗币安原始CSV数据的公共逻辑"""
    df = pd.read_csv(BytesIO(file_content), header=None)
    if pd.to_numeric(df.iloc[0, 0], errors='coerce') is np.nan:
        df = df.iloc[1:].reset_index(drop=True)
    cols_count = min(df.shape[1], len(KLINE_COLUMNS))
    df = df.iloc[:, :cols_count]
    df.columns = KLINE_COLUMNS[:cols_count]
    
    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
    df["close_time"] = pd.to_numeric(df["close_time"], errors="coerce")
    df.dropna(subset=["open_time", "close_time"], inplace=True)
    if df.empty: return None
    
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    
    num_cols = ["open", "high", "low", "close", "volume"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype("float32")
    if "ignore" in df.columns: df.drop(columns=["ignore"], inplace=True)
    return df

def sync_historical_months(symbol, months, output_dir):
    """第一部分：同步已经过完的历史整月包（有去重保护）"""
    for ym in months:
        file_name = f"{symbol}-1d-{ym}"
        target_parquet = os.path.join(output_dir, symbol, f"{file_name}.parquet")
        
        # 如果已经存在，说明是铁打的历史数据，一秒钟跳过
        if os.path.exists(target_parquet):
            continue
            
        url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/1d/{file_name}.zip"
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 404: continue
            res.raise_for_status()
            with zipfile.ZipFile(BytesIO(res.content)) as z:
                csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                if not csv_files: continue
                with z.open(csv_files[0]) as f:
                    df = clean_and_parse_csv(f.read())
                    if df is not None:
                        os.makedirs(os.path.dirname(target_parquet), exist_ok=True)
                        df.to_parquet(target_parquet, engine="pyarrow", compression="snappy", index=False)
                        print(f"  ✨ 历史月包补全: {ym}")
        except:
            pass

def sync_current_running_month(symbol, current_ym, output_dir):
    """第二部分：疯狂续写未完结的当前月份（每日更新核心逻辑）"""
    target_parquet = os.path.join(output_dir, symbol, f"{symbol}-1d-{current_ym}.parquet")
    
    # 自动探测今天几号，我们需要把1号到昨天的所有天度数据全部抓下来合体
    today = datetime.now()
    all_day_dfs = []
    
    print(f"  ⏳ 正在拼命追击 {current_ym} 本月最新每日数据...")
    for day in range(1, today.day):
        date_str = f"{current_ym}-{str(day).zfill(2)}"
        file_name = f"{symbol}-1d-{date_str}"
        url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/1d/{file_name}.zip"
        try:
            res = requests.get(url, timeout=5)
            if res.status_code == 404: continue  # 币安还没生成昨天的包就先跳过
            res.raise_for_status()
            with zipfile.ZipFile(BytesIO(res.content)) as z:
                csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                if not csv_files: continue
                with z.open(csv_files[0]) as f:
                    day_df = clean_and_parse_csv(f.read())
                    if day_df is not None:
                        all_day_dfs.append(day_df)
        except:
            pass
            
    # 如果抓到了本月的天度数据，直接融合成一个本月的 Parquet 
    if all_day_dfs:
        merged_df = pd.concat(all_day_dfs, ignore_index=True)
        # 按时间排序并去重
        merged_df.drop_duplicates(subset=["open_time"], inplace=True)
        merged_df.sort_values(by="open_time", inplace=True)
        
        os.makedirs(os.path.dirname(target_parquet), exist_ok=True)
        merged_df.to_parquet(target_parquet, engine="pyarrow", compression="snappy", index=False)
        print(f"  🚀 {current_ym} 动态Parquet已更新成功（包含至 {date_str} 的最新K线）")

if __name__ == "__main__":
    BASE_DIR = "binance_parquet_data"

    # 1. 精准划分时间
    # 过去的硬核整月
    months_2025 = [f"2025-{str(i).zfill(2)}" for i in range(1, 13)]
    months_2026_past = [f"2026-{str(i).zfill(2)}" for i in range(1, 5)] # 1-4月
    historical_months = months_2025 + months_2026_past
    
    # 当前正在过、没有完整数据的月份（动态获取当前年-月，比如 2026-05）
    current_month_str = datetime.now().strftime("%Y-%m")

    # 2. 抓取币全代币
    all_symbols = get_all_usdt_symbols()
    
    # 单批次控制在 60 个币种，防止一天塞太多东西
    BATCH_SIZE = 60
    
    print(f"🚀 云端『历史固定 + 每日增量续写』双引擎流水线启动...")
    
    processed = 0
    for symbol in all_symbols:
        if processed >= BATCH_SIZE:
            print(f"🛑 已达到本批次上限（{BATCH_SIZE}个币种），收工保存。")
            break
            
        # 🛡️ 智能判定：如果这个币连最新的动态5月Parquet都已经有了，说明这一轮已经宠幸过它了，直接跳过
        current_month_parquet = os.path.join(BASE_DIR, symbol, f"{symbol}-1d-{current_month_str}.parquet")
        if os.path.exists(current_month_parquet):
            # 顺便检查一下是不是今天更新的，如果是今天更新的就彻底跳过
            file_time = datetime.fromtimestamp(os.path.getmtime(current_month_parquet))
            if file_time.date() == datetime.now().date():
                continue
        
        print(f"==============================")
        print(f"正在全自动化攻坚币种: {symbol}")
        
        # 引擎一：把这个币以前缺的历史整月全部补齐（有文件的瞬间闪过，不花时间）
        sync_historical_months(symbol, historical_months, BASE_DIR)
        
        # 引擎二：针对没过完的本月，去币安日度区把1号到昨天的所有K线揪过来合体覆盖
        sync_current_running_month(symbol, current_month_str, BASE_DIR)
        
        processed += 1
