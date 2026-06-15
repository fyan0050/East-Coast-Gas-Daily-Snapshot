import os
import io
import json
import time
import argparse
import sqlite3
import logging
import datetime
 
import requests
import pandas as pd
 
DB_PATH = "data/processed/east_coast_gas.db"
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
 
CITIES = {
    "Sydney":    {"lat": -33.87, "lon": 151.21},
    "Brisbane":  {"lat": -27.47, "lon": 153.03},
    "Adelaide":  {"lat": -34.93, "lon": 138.60},
    "Melbourne": {"lat": -37.81, "lon": 144.96},
}
HDD_BASE = 18.0
HEADERS = {"User-Agent": "Mozilla/5.0"}
 
# DWGM 历史参考价单文件：price_bod_gst_ex 列 = 6am/BOD 价
DWGM_INT041_URL = "https://nemweb.com.au/Reports/CURRENT/VicGas/int041_v4_market_and_reference_prices_1.csv"
 
 
def fetch(url, tries=3):
    for i in range(tries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.content
        except requests.exceptions.RequestException as e:
            logging.warning(f"fetch attempt {i+1} failed: {url} :: {e}")
            if i < tries - 1:
                time.sleep(2 ** i)
    return None
 
 
# ============================================================
# 天气回填：Open-Meteo archive (观测 HDD)
# ============================================================
def backfill_weather(start_date, end_date=None):
    end_date = end_date or (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    records = []
 
    for city, geo in CITIES.items():
        url = (
            "https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={geo['lat']}&longitude={geo['lon']}"
            f"&start_date={start_date}&end_date={end_date}"
            "&daily=temperature_2m_max,temperature_2m_min"
            "&timezone=Australia%2FSydney"
        )
        content = fetch(url)
        if not content:
            logging.error(f"weather backfill failed: {city}")
            continue
        try:
            data = json.loads(content)
            daily = data["daily"]
            for i, d in enumerate(daily["time"]):
                mx, mn = daily["temperature_2m_max"][i], daily["temperature_2m_min"][i]
                tmean = None if (mx is None or mn is None) else (mx + mn) / 2.0
                hdd = None if tmean is None else max(0.0, HDD_BASE - tmean)
                records.append((
                    d, city,
                    None if mx is None else float(mx),
                    None if mn is None else float(mn),
                    None if tmean is None else float(tmean),
                    None if hdd is None else float(hdd),
                    0,  # is_forecast=0：archive 观测真值
                    "open-meteo-archive", ingested_at,
                ))
            logging.info(f"weather {city}: {len(daily['time'])} days")
        except Exception as e:
            logging.exception(f"weather parse error {city}: {e}")
 
    if records:
        conn = sqlite3.connect(DB_PATH)
        conn.executemany(
            'INSERT OR REPLACE INTO weather '
            '(date, city, temp_max, temp_min, temp_mean, hdd, is_forecast, source, ingested_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', records)
        conn.commit(); conn.close()
        logging.info(f"Weather backfill saved: {len(records)} rows.")
    return len(records)
 
 
# ============================================================
# DWGM 回填：解析 int041 单文件，取 price_bod (6am/BOD) 价
# ============================================================
def backfill_dwgm():
    content = fetch(DWGM_INT041_URL)
    if not content:
        logging.error("DWGM int041 download failed.")
        return 0
 
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        logging.exception(f"int041 read failed: {e}")
        return 0
 
    needed = {'gas_date', 'price_bod_gst_ex', 'current_date'}
    missing = needed - set(df.columns)
    if missing:
        logging.error(f"int041 缺少必需列: {missing}。实际列: {list(df.columns)}")
        return 0
 
    ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    source_file = "int041_v4_market_and_reference_prices_1.csv"
 
    df = df.copy()
    df['gas_date_dt'] = pd.to_datetime(df['gas_date'], dayfirst=True, errors='coerce')
    cd = pd.to_datetime(df['current_date'], dayfirst=True, errors='coerce').max()
    current_date = cd.normalize() if pd.notna(cd) else pd.Timestamp(datetime.date.today())
 
    n_bad = int(df['gas_date_dt'].isna().sum())
    if n_bad:
        logging.warning(f"int041: {n_bad} 行 gas_date 解析失败，已剔除。")
        df = df[df['gas_date_dt'].notna()]
 
    records = []
    for _, r in df.iterrows():
        gdate = r['gas_date_dt']
        price = r['price_bod_gst_ex']
        if pd.isna(price):
            continue
        is_forecast = 1 if gdate.normalize() > current_date else 0
        records.append((
            gdate.strftime('%Y-%m-%d'),
            float(price),
            "BOD (6am)",          # approval_datetime 位记口径标记，区别于 daily 真实时间戳
            is_forecast,
            source_file,
            ingested_at,
        ))
 
    if records:
        conn = sqlite3.connect(DB_PATH)
        conn.executemany(
            'INSERT OR REPLACE INTO dwgm_prices '
            '(gas_date, price_6am_schedule, approval_datetime, is_forecast, source_file, ingested_at) '
            'VALUES (?, ?, ?, ?, ?, ?)', records)
        conn.commit(); conn.close()
        logging.info(f"DWGM backfill saved: {len(records)} rows "
                     f"({records[0][0]} .. {records[-1][0]}).")
    return len(records)
 
 
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--weather-start', help='天气回填起始日 YYYY-MM-DD')
    ap.add_argument('--weather-end', default=None)
    ap.add_argument('--dwgm', action='store_true', help='回填 DWGM (int041)')
    args = ap.parse_args()
 
    did = False
    if args.weather_start:
        n = backfill_weather(args.weather_start, args.weather_end)
        print(f"天气回填完成：{n} 行")
        did = True
    if args.dwgm:
        n = backfill_dwgm()
        print(f"DWGM 回填完成：{n} 行")
        did = True
    if not did:
        ap.print_help()
 
 
if __name__ == "__main__":
    main()
 