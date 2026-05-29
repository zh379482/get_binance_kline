import os
import pandas as pd
import numpy as np

# 📁 路径配置
RAW_DIR = "binance_parquet_data"
CLEAN_DIR = "cleaned_data"

def clean_and_derive_features(df):
    """
    【量化特征衍生核心引擎】
    输入：币安标准12列原始 DataFrame
    输出：包含衍生特征的量化标准 DataFrame
    """
    try:
        # 1. 强制数值类型转换（防范原始数据存在字符串或 NaN 导致矩阵报错）
        num_cols = ["open", "high", "low", "close", "volume", "quote_asset_volume"]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        
        # 2. 生成标准可读时间戳列
        if "timestamp" not in df.columns and "open_time" in df.columns:
            df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms").dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 3. 筛选并提取核心矩阵
        clean_df = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
        
        # 4. 【特征衍生一】：判定流动性断流/缺失信号 (1代表无成交量，0代表正常)
        clean_df["is_missing"] = np.where(clean_df["volume"] <= 1e-8, 1, 0)
        
        # 5. 【特征衍生二】：成交额 (Dollar Volume)
        if "quote_asset_volume" in df.columns:
            clean_df["dollar_volume"] = df["quote_asset_volume"]
        else:
            clean_df["dollar_volume"] = clean_df["volume"] * clean_df["close"]
        
        # 6. 【特征衍生三】：VWAP (成交量加权平均价)
        # 🛡️ 防御逻辑：若当天没交易量，VWAP 强行等于 Close，防止除以0出现 Inf/NaN
        clean_df["vwap"] = np.where(
            clean_df["is_missing"] == 0,
            clean_df["dollar_volume"] / (clean_df["volume"] + 1e-12),
            clean_df["close"]
        )
        
        # 7. 【特征衍生四】：对数收益率 (Log Returns)
        clean_df["returns"] = np.log(clean_df["close"] / clean_df["close"].shift(1)).fillna(0.0)
        
        return clean_df
    except Exception as e:
        print(f"  ⚠️ 特征清洗计算异常: {e}")
        return None

if __name__ == "__main__":
    if not os.path.exists(RAW_DIR):
        print(f"❌ 错误：找不到原始数据文件夹【{RAW_DIR}】，请先确保本地有原始 Parquet 数据。")
        exit()

    # 扫描所有的币种文件夹
    existing_symbols = [d for d in os.listdir(RAW_DIR) if os.path.isdir(os.path.join(RAW_DIR, d))]
    print(f"🔍 检查到本地共有 {len(existing_symbols)} 个币种的原始数据。")

    total_files_cleaned = 0

    for symbol in sorted(existing_symbols):
        raw_symbol_dir = os.path.join(RAW_DIR, symbol)
        clean_symbol_dir = os.path.join(CLEAN_DIR, symbol)

        # 获取当前币种下所有的原始 Parquet 文件
        raw_files = [f for f in os.listdir(raw_symbol_dir) if f.endswith('.parquet')]
        if not raw_files:
            continue

        print(f"⚙️ 正在清洗币种特征: {symbol} ...")
        
        for file in raw_files:
            raw_file_path = os.path.join(raw_symbol_dir, file)
            # 保持相同的文件命名，方便后续回测时对齐
            clean_file_path = os.path.join(clean_symbol_dir, file)

            # 🛡️ 智能增量跳过：如果该文件已经洗过了，直接秒跳过，不重复做无用功
            if os.path.exists(clean_file_path):
                continue

            try:
                # 读取原始 Parquet
                df = pd.read_parquet(raw_file_path)
                
                # 特征工程加工
                clean_df = clean_and_derive_features(df)
                
                if clean_df is not None:
                    # 创建目标币种特征文件夹
                    os.makedirs(os.path.dirname(clean_file_path), exist_ok=True)
                    # 存储为精简、带压缩的高效特征 Parquet
                    clean_df.to_parquet(clean_file_path, engine="pyarrow", compression="snappy", index=False)
                    total_files_cleaned += 1
            except Exception as e:
                print(f"  ❌ 文件 {file} 读取或加工失败: {e}")

    print(f"\n🎉 大功告成！本次共清洗并生成了 {total_files_cleaned} 个增量特征 Parquet 文件。")
    print(f"📁 特征数据已安全存入：【{CLEAN_DIR}/】")
