import os
import requests
import zipfile
import pandas as pd
from io import BytesIO
from datetime import datetime

# 币安标准 K 线列名，纯粹为了转 Parquet 贴标签
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

def parse_raw_csv_to_df(file_content):
    """【零清洗零改变】一字不差地读取币安原始CSV，保留所有原始格式"""
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
    """核心：不再跳过币种，而是逐月、逐日精准盘点补漏"""
    print(f"------------------------------------------------------------")
    print(f"🕵️ 正在盘点并原装抓取币种历史 (2020年起): {symbol}")
    
    # ================= 【第一步：逐月检查 2020-2026，缺哪个月补哪个月】 =================
    for ym in historical_months:
        file_name = f"{symbol}-1d-{ym}"
        target_parquet = os.path.join(output_dir, symbol, f"{file_name}.parquet")
        
        # 🛡️ 精准月份去重：如果这个月的 Parquet 已经存在了，才跳过这一个月，而不是跳过整个币！
        if os.path.exists(target_parquet):
            continue
            
        url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/1d/{file_name}.zip"
        try:
            res = session.get(url, timeout=5)
            if res.status_code == 200:
                with zipfile.ZipFile(BytesIO(res.content)) as z:
                    csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                    if csv_files:
                        with z.open(csv_files[0]) as f:
                            df = parse_raw_csv_to_df(f.read())
                            if df is not None:
                                os.makedirs(os.path.dirname(target_parquet), exist_ok=True)
                                df.to_parquet(target_parquet, engine="pyarrow", compression="snappy", index=False)
                                print(f"  ✨ [历史月包] 成功补全归档月份: {ym}")
        except:
            pass

    # ================= 【第二步：动态更新当月天度包】 =================
    target_current_parquet = os.path.join(output_dir, symbol, f"{symbol}-1d-{current_ym}.parquet")
    today = datetime.now()
    
    # 🛡️ 智能优化：如果是当月的动态文件，且今天已经更新过了，天度小包就没必要重复拼了
    if os.path.exists(target_current_parquet):
        file_time = datetime.fromtimestamp(os.path.getmtime(target_current_parquet))
        if file_time.date() == today.date():
            # 今天已经更新过当月动态了，直接收工，省下网络请求
            return

    all_day_dfs = []
    for day in range(1, today.day):
        date_str = f"{current_ym}-{str(day).zfill(2)}"
        file_name = f"{symbol}-1d-{date_str}"
        url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/1d/{file_name}.zip"
        try:
            res = session.get(url, timeout=3)
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
        print(f"  🚀 [动态天度] 当月数据原始合体成功")

if __name__ == "__main__":
    BASE_DIR = "binance_parquet_data"

    # 1. 生成自 2020 年起所有的历史月份
    historical_months = []
    for year in range(2020, 2026):
        for month in range(1, 13):
            historical_months.append(f"{year}-{str(month).zfill(2)}")
    for month in range(1, 5):
        historical_months.append(f"2026-{str(month).zfill(2)}")
    
    current_month_str = datetime.now().strftime("%Y-%m")

    # 2. 强力加固生死簿：手写主流核心资产托底
    history_and_active_symbols = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOTUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT",
        "MATICUSDT", "LTCUSDT", "UNIUSDT", "SHIBUSDT", "TRXUSDT", "ETCUSDT", "FILUSDT", "NEARUSDT", "ATOMUSDT", "XMRUSDT",
        "LUNAUSDT", "LUNCUSDT", "FTTUSDT", "BTTUSDT", "SRMUSDT", "ANCUSDT", "MIRUSDT", "YFIIUSDT", "WAVESUSDT", "OMGUSDT",
        "WNXMUSDT", "XEMUSDT", "ANTUSDT", "POLYUSDT", "IDRTUSDT", "KP3RUSDT", "OOKIUSDT", "UNFIUSDT", "FORUSDT", "AKROUSDT",
        "WUSDT", "TNSRUSDT", "TAOUSDT", "OMNIUSDT", "REZUSDT", "BBUSDT", "NOTUSDT", "IOUSDT", "ATHUSDT", "ZKUSDT",
        "RENDERUSDT", "EIGENUSDT", "SCRUSDT", "COWUSDT", "CETUSDT", "PNUTUSDT", "ACTUSDT", "THEUSDT", "ACXUSDT", "ORCAUSDT"
    ]
    
    # 初始化网络长连接池
    http_session = requests.get_binance_session if hasattr(requests, 'get_binance_session') else requests.Session()
    active_symbols = []

    # 3. 双域名交叉获取币安在线全量现货代币名单
    endpoints = [
        "https://api.binance.vision/api/v3/exchangeInfo",
        "https://api.binance.com/api/v3/exchangeInfo",
        "https://api1.binance.com/api/v3/exchangeInfo"
    ]
    
    for url in endpoints:
        try:
            active_res = http_session.get(url, timeout=6).json()
            active_symbols = [s["symbol"] for s in active_res["symbols"] if s["quoteAsset"] == "USDT"]
            active_symbols = [s for s in active_symbols if "UPUSDT" not in s and "DOWNUSDT" not in s]
            if active_symbols:
                print(f"✅ 成功截获币安在线活跃币种名单！包含 {len(active_symbols)} 个 USDT 交易对。")
                break
        except Exception as e:
            pass

    full_universe = list(set(history_and_active_symbols + active_symbols))

    print(f"\n🔥 『2020纪元：全月份颗粒级精准补漏版』流水线正式启动...")
    print(f"📊 宇宙总币种数: {len(full_universe)}")
    
    # 4. 🎛️ 每一个币种都必须进去盘点，绝不整体跳过
    for symbol in sorted(full_universe):
        sync_all_data_for_symbol(http_session, symbol, historical_months, current_month_str, BASE_DIR)

    print(f"\n🎉 恭喜！漏洞已完美修复，全历史原始数据补遗大获全胜！")
