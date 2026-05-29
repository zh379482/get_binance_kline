import os
import pandas as pd
import numpy as np

RAW_DIR = "binance_parquet_data"
CLEAN_DIR = "cleaned_parquet_data"

def clean_and_derive_features(df):
    """【量化特征衍生核心引擎】"""
    try:
        # 1. 确保核心数值列转为 float 类型
        num_cols = ["open", "high", "low", "close", "volume", "quote_asset_volume"]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        
        # 2. 清洗时间戳：如果是原始未清洗的毫秒戳，转为 Y-m-d H:M:S
        if "timestamp" not in df.columns and "open_time" in df.columns:
            df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms").dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 3. 提取核心 OHLCV
        clean_df = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
        
        # 4. 衍生指标 1: is_missing (volume 为 0 则为 1，否则为 0)
        clean_df["is_missing"] = np.where(clean_df["volume"] <= 1e-8, 1, 0)

        clean_df["vwap"] = np.where(
            clean_df["is_missing"] == 0,
            clean_df["dollar_volume"] / (clean_df["volume"] + 1e-12),
            clean_df["close"]
        )
        
        if "quote_asset_volume" in df.columns:
            clean_df["dollar_volume"] = df["quote_asset_volume"]
        else:
            clean_df["dollar_volume"] = clean_df["volume"] * clean_df["vwap"]
    
        
        clean_df["returns"] = np.log(clean_df["close"] / clean_df["close"].shift(1))
        clean_df["returns"] = clean_df["returns"].fillna(0.0)
        
        return clean_df
    except Exception as e:
        print(f"  ⚠️ 特征清洗失败: {e}")
        return None

if __name__ == "__main__":
    if not os.path.exists(RAW_DIR):
        print(f"❌ 找不到原始数据文件夹 {RAW_DIR}，请确保已经下载了数据。")
        exit()

    # 1. 扫描当前已经下载的所有币种
    existing_symbols = [d for d in os.listdir(RAW_DIR) if os.path.isdir(os.path.join(RAW_DIR, d))]
    print(f"🔍 检查到本地原始数据中包含 {len(existing_symbols)} 个币种。")

    # 🚨 安全去重与批次限制：单次只清洗 20 个币种，防止 GitHub 推送文件过多被熔断
    BATCH_SIZE = 20
    processed_symbols = 0

    for symbol in sorted(existing_symbols):
        if processed_symbols >= BATCH_SIZE:
            print(f"\n🛑 已达到本批次安全清洗上限（{BATCH_SIZE}个币种），收工提交！")
            break

        raw_symbol_dir = os.path.join(RAW_DIR, symbol)
        clean_symbol_dir = os.path.join(CLEAN_DIR, symbol)

        if os.path.exists(clean_symbol_dir):
            raw_files_count = len([f for f in os.listdir(raw_symbol_dir) if f.endswith('.parquet')])
            clean_files_count = len([f for f in os.listdir(clean_symbol_dir) if f.endswith('.parquet')])
            if clean_files_count >= raw_files_count and clean_files_count > 0:
                continue

        print(f"------------------------------------------------------------")
        print(f"⚙️ 正在就地清洗并构建特征: {symbol}")

        raw_files = [f for f in os.listdir(raw_symbol_dir) if f.endswith('.parquet')]
        
        for file in raw_files:
            raw_file_path = os.path.join(raw_symbol_dir, file)
            
            clean_file_name = file.replace(".parquet", "_clean.parquet")
            clean_file_path = os.path.join(clean_symbol_dir, clean_file_name)
            if os.path.exists(clean_file_path):
                continue

            try:
                df = pd.read_parquet(raw_file_path)
                clean_df = clean_and_derive_features(df)
                
                if clean_df is not None:
                    os.makedirs(os.path.dirname(clean_file_path), exist_ok=True)
                    clean_df.to_parquet(clean_file_path, engine="pyarrow", compression="snappy", index=False)
            except Exception as e:
                print(f"  ❌ 文件 {file} 读取或清洗失败: {e}")

        print(f"  ✨ {symbol} 历史全部文件清洗完毕！")
        processed_symbols += 1

    print(f"\n🎉 本批次本地清洗工作顺利结束！")
