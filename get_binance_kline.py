import os
import requests
import zipfile
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime

# 币安标准 K 线列名
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

def clean_and_parse_csv(file_content):
    """公共清洗逻辑：将币安原始K线CSV字节流转换为轻量高效的 DataFrame"""
    try:
        df = pd.read_csv(BytesIO(file_content), header=None)
        # 如果第一行是字符（列名）而非数字，自动剔除首行
        if pd.to_numeric(df.iloc[0, 0], errors='coerce') is np.nan:
            df = df.iloc[1:].reset_index(drop=True)
        
        # 裁剪并对齐列数
        cols_count = min(df.shape[1], len(KLINE_COLUMNS))
        df = df.iloc[:, :cols_count]
        df.columns = KLINE_COLUMNS[:cols_count]
        
        # 清洗非法/空时间戳
        df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
        df["close_time"] = pd.to_numeric(df["close_time"], errors="coerce")
        df.dropna(subset=["open_time", "close_time"], inplace=True)
        if df.empty: return None
        
        # 转换标准日期格式
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
        
        # 核心价格/成交量转换为 float32 节省云端与本地空间
        num_cols = ["open", "high", "low", "close", "volume"]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype("float32")
        
        if "ignore" in df.columns: 
            df.drop(columns=["ignore"], inplace=True)
        return df
    except:
        return None

def sync_all_data_for_symbol(symbol, historical_months, current_ym, output_dir):
    """核心：对单个币种进行全历史整月补漏 + 当月天度合体大扫荡"""
    print(f"------------------------------------------------------------")
    print(f"🕵️ 正在多维度扫荡币种: {symbol}")
    
    # ================= 【第一步：下载历史硬核整月包】 =================
    for ym in historical_months:
        file_name = f"{symbol}-1d-{ym}"
        target_parquet = os.path.join(output_dir, symbol, f"{file_name}.parquet")
        
        # 精准补漏：如果这个月的 Parquet 已经躺在仓库里了，0毫秒闪过
        if os.path.exists(target_parquet):
            continue
            
        url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/1d/{file_name}.zip"
        try:
            res = requests.get(url, timeout=5)
            if res.status_code == 200:
                with zipfile.ZipFile(BytesIO(res.content)) as z:
                    csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                    if csv_files:
                        with z.open(csv_files[0]) as f:
                            df = clean_and_parse_csv(f.read())
                            if df is not None:
                                os.makedirs(os.path.dirname(target_parquet), exist_ok=True)
                                df.to_parquet(target_parquet, engine="pyarrow", compression="snappy", index=False)
                                print(f"  ✨ [历史月包] 成功补全归档: {ym}")
        except:
            pass

    # ================= 【第二步：动态攻坚未结月/新币真空期】 =================
    # 无论是新上线的婴儿币，还是还没过完的本月，都通过“1号到昨天的天度合体”死磕出最新的 Parquet
    target_current_parquet = os.path.join(output_dir, symbol, f"{symbol}-1d-{current_ym}.parquet")
    today = datetime.now()
    all_day_dfs = []
    
    # 扫描从1号到昨天的所有天度压缩包
    for day in range(1, today.day):
        date_str = f"{current_ym}-{str(day).zfill(2)}"
        file_name = f"{symbol}-1d-{date_str}"
        url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/1d/{file_name}.zip"
        try:
            res = requests.get(url, timeout=3)
            if res.status_code == 200:
                with zipfile.ZipFile(BytesIO(res.content)) as z:
                    csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                    if csv_files:
                        with z.open(csv_files[0]) as f:
                            day_df = clean_and_parse_csv(f.read())
                            if day_df is not None:
                                all_day_dfs.append(day_df)
        except:
            pass
            
    if all_day_dfs:
        merged_df = pd.concat(all_day_dfs, ignore_index=True)
        merged_df.drop_duplicates(subset=["open_time"], inplace=True)
        merged_df.sort_values(by="open_time", inplace=True)
        
        os.makedirs(os.path.dirname(target_current_parquet), exist_ok=True)
        merged_df.to_parquet(target_current_parquet, engine="pyarrow", compression="snappy", index=False)
        print(f"  🚀 [动态天度] 实时合体成功 -> 更新至昨日最新K线")

if __name__ == "__main__":
    BASE_DIR = "binance_parquet_data"

    # 1. 划分硬核时间跨度
    months_2025 = [f"2025-{str(i).zfill(2)}" for i in range(1, 13)]
    months_2026_past = [f"2026-{str(i).zfill(2)}" for i in range(1, 5)] # 1-4月
    historical_months = months_2025 + months_2026_past
    
    # 动态获取当前的年份和月份 (例如当前 2026-05)
    current_month_str = datetime.now().strftime("%Y-%m")

    # 2. 🚨 【币安死活新旧全币种白名单】
    # 融合了历史上大名鼎鼎已退市的死币、近期刚上线几天毫无月度包的新币
    history_and_active_symbols = [
        # 历史上著名的退市币、死币、更名币（让它们强制显形去撞墙历史包）
        "LUNAUSDT", "LUNCUSDT", "FTTUSDT", "BTTUSDT", "SRMUSDT", "ANCUSDT", "MIRUSDT", "YFIIUSDT", "WAVESUSDT", "OMGUSDT",
        "WNXMUSDT", "XEMUSDT", "ANTUSDT", "POLYUSDT", "IDRTUSDT", "KP3RUSDT", "OOKIUSDT", "UNFIUSDT", "FORUSDT", "AKROUSDT",
        # 近期或 2026 年最新暴涨上线的超级新币（强制去日K区挖天度线，彻底粉碎新币数据真空期）
        "WUSDT", "TNSRUSDT", "TAOUSDT", "OMNIUSDT", "REZUSDT", "BBUSDT", "NOTUSDT", "IOUSDT", "ATHUSDT", "ZKUSDT",
        "RENDERUSDT", "EIGENUSDT", "SCRUSDT", "COWUSDT", "CETUSDT", "PNUTUSDT", "ACTUSDT", "THEUSDT", "ACXUSDT", "ORCAUSDT"
    ]
    
    # 3. 动态补充：在线调取此时此刻币安在线的所有活跃代币
    try:
        active_res = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=5).json()
        active_symbols = [s["symbol"] for s in active_res["symbols"] if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"]
        # 过滤掉非正经现货的杠杆代币
        active_symbols = [s for s in active_symbols if "UPUSDT" not in s and "DOWNUSDT" not in s]
        # 合并手写白名单与在线活跃名单，并去重
        full_universe = list(set(history_and_active_symbols + active_symbols))
    except Exception as e:
        print(f"⚠️ 在线获取活跃代币失败: {e}，将全力攻坚预设生死簿白名单。")
        full_universe = history_and_active_symbols

    # 4. 🚀 批次吞吐量：调大到 150 个币种！
    # 得益于“精准去重”，已经完美下载的币种会在 0.001 秒内闪过，不占单次运行名额
    BATCH_SIZE = 150
    
    print(f"🔥 『历史归档+每日动态续写』全宇宙死活全收录流水线正式启动...")
    print(f"📊 经全盘盘点，待处理的宇宙总币种数: {len(full_universe)}")
    
    processed = 0
    # 按照字母顺序从 A 到 Z 排序盘查，让进度一目了然
    for symbol in sorted(full_universe):
        if processed >= BATCH_SIZE:
            print(f"\n🛑 已达到本批次安全冲刺上限（{BATCH_SIZE}个币种），收工并提交仓库，防止超时崩溃。")
            break
            
        # 🛡️ 智能终极判定：如果该币种最新的动态月份Parquet已经在仓库里，且是今天刚刚更新过的，直接宣告此币彻底通关
        current_month_parquet = os.path.join(BASE_DIR, symbol, f"{symbol}-1d-{current_month_str}.parquet")
        if os.path.exists(current_month_parquet):
            file_time = datetime.fromtimestamp(os.path.getmtime(current_month_parquet))
            if file_time.date() == datetime.now().date():
                continue # 绝对不干重复劳动，瞬间跳过
                
        # 启动双引擎大扫荡
        sync_all_data_for_symbol(symbol, historical_months, current_month_str, BASE_DIR)
        processed += 1

    print(f"\n🎉 本批次同步清洗结束！成功对 {processed} 个币种进行了深度数据合体。")
