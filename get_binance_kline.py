import os
import requests
import zipfile
import pandas as pd
from io import BytesIO
from datetime import datetime

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

CUSTOM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def parse_raw_csv_to_df(file_content):
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
    for ym in historical_months:
        file_name = f"{symbol}-1d-{ym}"
        target_parquet = os.path.join(output_dir, symbol, f"{file_name}.parquet")
        
        if os.path.exists(target_parquet):
            continue
            
        url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/1d/{file_name}.zip"
        try:
            res = session.get(url, headers=CUSTOM_HEADERS, timeout=5)
            if res.status_code == 200:
                with zipfile.ZipFile(BytesIO(res.content)) as z:
                    csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                    if csv_files:
                        with z.open(csv_files[0]) as f:
                            df = parse_raw_csv_to_df(f.read())
                            if df is not None:
                                os.makedirs(os.path.dirname(target_parquet), exist_ok=True)
                                df.to_parquet(target_parquet, engine="pyarrow", compression="snappy", index=False)
        except:
            pass

    target_current_parquet = os.path.join(output_dir, symbol, f"{symbol}-1d-{current_ym}.parquet")
    today = datetime.now()
    all_day_dfs = []
    for day in range(1, today.day):
        date_str = f"{current_ym}-{str(day).zfill(2)}"
        file_name = f"{symbol}-1d-{date_str}"
        url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/1d/{file_name}.zip"
        try:
            res = session.get(url, headers=CUSTOM_HEADERS, timeout=3)
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
if __name__ == "__main__":
    BASE_DIR = "binance_parquet_data"

    historical_months = []
    for year in range(2020, 2026):
        for month in range(1, 13):
            historical_months.append(f"{year}-{str(month).zfill(2)}")
    for month in range(1, 5):
        historical_months.append(f"2026-{str(month).zfill(2)}")
    
    current_month_str = datetime.now().strftime("%Y-%m")

    full_universe = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOTUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT",
        "MATICUSDT", "LTCUSDT", "UNIUSDT", "SHIBUSDT", "TRXUSDT", "ETCUSDT", "FILUSDT", "NEARUSDT", "ATOMUSDT", "XMRUSDT",
        "LUNAUSDT", "LUNCUSDT", "FTTUSDT", "BTTUSDT", "SRMUSDT", "ANCUSDT", "MIRUSDT", "YFIIUSDT", "WAVESUSDT", "OMGUSDT",
        "WNXMUSDT", "XEMUSDT", "ANTUSDT", "POLYUSDT", "IDRTUSDT", "KP3RUSDT", "OOKIUSDT", "UNFIUSDT", "FORUSDT", "AKROUSDT",
        "WUSDT", "TNSRUSDT", "TAOUSDT", "OMNIUSDT", "REZUSDT", "BBUSDT", "NOTUSDT", "IOUSDT", "ATHUSDT", "ZKUSDT",
        "RENDERUSDT", "EIGENUSDT", "SCRUSDT", "COWUSDT", "CETUSDT", "PNUTUSDT", "ACTUSDT", "THEUSDT", "ACXUSDT", "ORCAUSDT",
        "1INCHUSDT", "AAVEUSDT", "ACHUSDT", "ACACM", "AERGOUSDT", "AGLDUSDT", "AIOZUSDT", "ALICEUSDT", "ALGOUSDT", "ALCXUSDT",
        "ALPHAUSDT", "ALTUSDT", "AMBUSDT", "AMPUSDT", "ANKRUSDT", "APEUSDT", "API3USDT", "APTUSDT", "ARUSDT", "ARBMUSDT",
        "ARKUSDT", "ARKMUSDT", "ARPAUSDT", "ASRUSDT", "ASTRUSDT", "ATAUSDT", "AUDIOUSDT", "AUCTIONUSDT", "AXSUSDT",
        "BADGERUSDT", "BAKEUSDT", "BALUSDT", "BANDUSDT", "BATUSDT", "BCHUSDT", "BEAMXUSDT", "BELUSDT", "BICOUSDT",
        "BIFIUSDT", "BLURUSDT", "BLZUSDT", "BNXUSDT", "BOBAUSDT", "BONDUSDT", "BOSONUSDT", "BSVUSDT", "BSWUSDT",
        "BURGERUSDT", "C98USDT", "CAKEUSDT", "CELOUSDT", "CELRUSDT", "CHZUSDT", "CHRUSDT", "CKBUSDT", "CLVUSDT",
        "COMBOUSDT", "COMPUSDT", "COSUSDT", "COTIUSDT", "CRVUSDT", "CTSIUSDT", "CTKUSDT", "CVCUSDT", "CVXUSDT", "CYBERUSDT",
        "DARUSDT", "DASHUSDT", "DATAUSDT", "DCRUSDT", "DEGOUSDT", "DENTUSDT", "DGBUSDT", "DIAUSDT", "DOCKUSDT", "DODOUSDT",
        "DYDXUSDT", "DEXEUSDT", "DYMUSDT", "EDUUSDT", "EGLDUSDT", "ELFUSDT", "ENJUSDT", "ENSUSDT", "EOSUSDT", "EPXUSDT",
        "ERNUSDT", "ETHFIUSDT", "SUIUSDT", "SEIUSDT", "TIAUSDT", "MANTAUSDT", "STRKUSDT", "AXLUSDT", "METISUSDT", "AEVOUSDT",
        "FIDAUSDT", "FIOUSDT", "FLOKIUSDT", "FLOWUSDT", "FLUXUSDT", "FRONTUSDT", "FXSUSDT", "GALUSDT", "GALAUSDT", "GFTUSDT",
        "GHSTUSDT", "GLMRUSDT", "GMTUSDT", "GMXUSDT", "GNSUSDT", "GRTUSDT", "GTCUSDT", "HARDUSDT", "HBARUSDT", "HFTUSDT",
        "HIFIUSDT", "HIGHUSDT", "HIVEUSDT", "HOOKUSDT", "HOTUSDT", "ICPUSDT", "ICXUSDT", "IDUSDT", "IDEXUSDT", "ILVUSDT",
        "IMXUSDT", "INJUSDT", "IOSTUSDT", "IOTAUSDT", "IOTXUSDT", "IQUSDT", "IRISUSDT", "JASMYUSDT", "JOEUSDT", "JSTUSDT",
        "JUPUSDT", "KAVAUSDT", "KDAUSDT", "KEYUSDT", "KMDUSDT", "KNCUSDT", "KSMUSDT", "LDOUSDT", "LEVERUSDT", "LINAUSDT",
        "LQTYUSDT", "LRCUSDT", "LSKUSDT", "LTOUSDT", "MAGICUSDT", "MAVUSDT", "MBLUSDT", "MBOXUSDT", "MDTUSDT", "MDXUSDT",
        "MINAUSDT", "MKRUSDT", "MOVRUSDT", "MTLUSDT", "NKNUSDT", "NMRUSDT", "NTRNUSDT", "NULSUSDT", "OCEANUSDT", "OGUSDT",
        "OGNUSDT", "ONEUSDT", "ONGUSDT", "ONTUSDT", "OXTUSDT", "PAXGUSDT", "PEPEUSDT", "PERPUSDT", "PHBUSDT", "PIVXUSDT",
        "PIXELUSDT", "POLYXUSDT", "PONDUSDT", "POWRUSDT", "PROMUSDT", "PROSUSDT", "PYTHUSDT", "QNTUSDT", "QTUMUSDT",
        "QUICKUSDT", "RADUSDT", "RAREUSDT", "RAYUSDT", "REEFUSDT", "REIUSDT", "RENUSDT", "REQUSDT", "RIFUSDT", "RLCUSDT",
        "RONINUSDT", "ROSEUSDT", "RPLUSDT", "RSRUSDT", "RUNEUSDT", "RVNUSDT", "SANDUSDT", "SANTUSDT", "SCUSDT", "SFPUSDT",
        "SKLUSDT", "SLPUSDT", "SNTUSDT", "SNXUSDT", "SPELLUSDT", "STEEMUSDT", "STGUSDT", "STMXUSDT", "STORJUSDT", "STPTUSDT",
        "STRAXUSDT", "STXUSDT", "SUNUSDT", "SUPERUSDT", "SUSHIUSDT", "SXPUSDT", "SYNUSDT", "SYSUSDT", "TUSDT", "TWTUSDT",
        "THETAUSDT", "TKOUSDT", "TLMUSDT", "TRBUSDT", "TROYUSDT", "TRUUSDT", "TURBOUSDT", "UMAUSDT", "UNFIUSDT", "USDCUSDT",
        "USTCUSDT", "UTKUSDT", "VETUSDT", "VGXUSDT", "VIBUSDT", "VIDTUSDT", "VITEUSDT", "VOXELUSDT", "VTHOUSDT", "WANUSDT",
        "WAXPUSDT", "WIFUSDT", "WINGUSDT", "WOOUSDT", "WRXUSDT", "XECUSDT", "XLMUSDT", "XNOUSDT", "XTZUSDT", "XVGUSDT",
        "XVSUSDT", "YFIUSDT", "YGGUSDT", "ZECUSDT", "ZENUSDT", "ZILUSDT"
    ]

    http_session = requests.Session()
    BATCH_SIZE = 15
    
    processed = 0
    for symbol in sorted(list(set(full_universe))):
        if processed >= BATCH_SIZE:
            break
            
        symbol_dir = os.path.join(BASE_DIR, symbol)
        
        if os.path.exists(symbol_dir):
            existing_files = [f for f in os.listdir(symbol_dir) if f.endswith('.parquet')]
            if len(existing_files) >= 10:
                continue
                
        sync_all_data_for_symbol(http_session, symbol, historical_months, current_month_str, BASE_DIR)
        processed += 1
