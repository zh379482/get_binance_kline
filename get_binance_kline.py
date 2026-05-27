import os
import requests
import zipfile
import pandas as pd
from io import BytesIO
from datetime import datetime

# 币安标准 K 线列名，仅做标签映射
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

def parse_raw_csv_to_df(file_content):
    """【零清洗零改变】一字不差地读取币安原始CSV，保留所有空值、格式与行数"""
    try:
        df = pd.read_csv(BytesIO(file_content), header=None, keep_default_na=False)
        cols_count = df.shape[1]
        if cols_count <= len(KLINE_COLUMNS):
            df.columns = KLINE_COLUMNS[:cols_count]
        else:
            df.columns = KLINE_COLUMNS + [f"extra_{i}" for i in range(cols_count - len(KLINE_COLUMNS))]
        return df
    except:
        return None

def sync_all_data_for_symbol(session, symbol, historical_months, current_ym, output_dir):
    """核心：利用Session复用通道，全量原装抓取单个币种历史"""
    print(f"------------------------------------------------------------")
    print(f"🕵️ 正在原装抓取币种历史 (2020年起): {symbol}")
    
    # ================= 【第一步：下载历史纯原始整月包】 =================
    for ym in historical_months:
        file_name = f"{symbol}-1d-{ym}"
        target_parquet = os.path.join(output_dir, symbol, f"{file_name}.parquet")
        
        # 🛡️ 智能判定：如果这个月的 Parquet 已经躺在仓库里了，直接跳过！
        if os.path.exists(target_parquet):
            continue
            
        url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/1d/{file_name}.zip"
        try:
            res = session.get(url, timeout=5) # 使用复用 Session 请求
            if res.status_code == 200:
                with zipfile.ZipFile(BytesIO(res.content)) as z:
                    csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                    if csv_files:
                        with z.open(csv_files[0]) as f:
                            df = parse_raw_csv_to_df(f.read())
                            if df is not None:
                                os.makedirs(os.path.dirname(target_parquet), exist_ok=True)
                                df.to_parquet(target_parquet, engine="pyarrow", compression="snappy", index=False)
                                print(f"  ✨ [原始月包] 成功归档: {ym}")
        except:
            pass

    # ================= 【第二步：动态攻坚未结月/新币真空期】 =================
    target_current_parquet = os.path.join(output_dir, symbol, f"{symbol}-1d-{current_ym}.parquet")
    today = datetime.now()
    all_day_dfs = []
    
    for day in range(1, today.day):
        date_str = f"{current_ym}-{str(day).zfill(2)}"
        file_name = f"{symbol}-1d-{date_str}"
        url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/1d/{file_name}.zip"
        try:
            res = session.get(url, timeout=3) # 使用复用 Session 请求
            if res.status_code == 200:
                with zipfile.ZipFile(BytesIO(res.content)) as z:
                    csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                    if csv_files:
                        with z.open(csv_files[0]) as f:
                            day_df = parse_raw_csv_to_df(f.read())
                            if day_df is not None:
                                all_day_dfs.append(day_df)
        except:
            pass
            
    if all_day_dfs:
        merged_df = pd.concat(all_day_dfs, ignore_index=True)
        if "open_time" in merged_df.columns:
            merged_df.sort_values(by="open_time", inplace=True)
        
        os.makedirs(os.path.dirname(target_current_parquet), exist_ok=True)
        merged_df.to_parquet(target_current_parquet, engine="pyarrow", compression="snappy", index=False)
        print(f"  🚀 [动态天度] 原始合体成功 -> 更新至昨日")

if __name__ == "__main__":
    BASE_DIR = "binance_parquet_data"

    # 1. 生成自2020年起所有的历史月份
    historical_months = []
    for year in range(2020, 2026):
        for month in range(1, 13):
            historical_months.append(f"{year}-{str(month).zfill(2)}")
    for month in range(1, 5):
        historical_months.append(f"2026-{str(month).zfill(2)}")
    
    current_month_str = datetime.now().strftime("%Y-%m")

    # 2. 【生死簿白名单】：已退市及新上线的特殊币种
    history_and_active_symbols = [
        "LUNAUSDT", "LUNCUSDT", "FTTUSDT", "BTTUSDT", "SRMUSDT", "ANCUSDT", "MIRUSDT", "YFIIUSDT", "WAVESUSDT", "OMGUSDT",
        "WNXMUSDT", "XEMUSDT", "ANTUSDT", "POLYUSDT", "IDRTUSDT", "KP3RUSDT", "OOKIUSDT", "UNFIUSDT", "FORUSDT", "AKROUSDT",
        "WUSDT", "TNSRUSDT", "TAOUSDT", "OMNIUSDT", "REZUSDT", "BBUSDT", "NOTUSDT", "IOUSDT", "ATHUSDT", "ZKUSDT",
        "RENDERUSDT", "EIGENUSDT", "SCRUSDT", "COWUSDT", "CETUSDT", "PNUTUSDT", "ACTUSDT", "THEUSDT", "ACXUSDT", "ORCAUSDT"
    ]
    
    # 初始化复用型网络会话池
    http_session = requests.Session()
    
    # 3. 动态获取全币安在线活跃币
    try:
        active_res = http_session.get("https://api.binance.com/api/v3/exchangeInfo", timeout=5).json()
        active_symbols = [s["symbol"] for s in active_res["symbols"] if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"]
        active_symbols = [s for s in active_symbols if "UPUSDT" not in s and "DOWNUSDT" not in s]
        full_universe = list(set(history_and_active_symbols + active_symbols))
    except Exception as e:
        full_universe = history_and_active_symbols

    print(f"🔥 『2020纪元：狂暴全量数据下载版』流水线正式启动...")
    print(f"📊 待处理宇宙代币总数: {len(full_universe)} (已彻底解除单次运行数量限制！)")
    
    # 4. 🎛️ 彻底砸碎限制，开启大水漫灌循环
    for symbol in sorted(full_universe):
        # 🛡️ 跳过检查：如果这个币种今天已经完全扫过一遍了，秒跳过
        current_month_parquet = os.path.join(BASE_DIR, symbol, f"{symbol}-1d-{current_month_str}.parquet")
        if os.path.exists(current_month_parquet):
            file_time = datetime.fromtimestamp(os.path.getmtime(current_month_parquet))
            if file_time.date() == datetime.now().date():
                continue # 0.001秒闪过
                
        # 全力开工
        sync_all_data_for_symbol(http_session, symbol, historical_months, current_month_str, BASE_DIR)

    print(f"\n🎉 恭喜！全网所有币种（共计 {len(full_universe)} 个）全历史原始数据同步大获全胜！")
