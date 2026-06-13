import os
import datetime
import time
import requests
import pandas as pd
import zipfile
import io
import logging
import re
import yaml
import sqlite3

# 基础配置
MELBOURNE_TZ = datetime.timezone(datetime.timedelta(hours=10)) # AEST
TODAY_DT = datetime.datetime.now(MELBOURNE_TZ)
TODAY_STR = TODAY_DT.strftime('%Y-%m-%d')
RAW_DIR = f"data/raw/{TODAY_STR}"
REPORT_DIR = "reports"
DB_PATH = "data/processed/east_coast_gas.db"

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def setup_directories():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def fetch_with_retries(url, max_retries=3, backoff_factor=2):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                logging.error(f"All {max_retries} attempts failed for {url}")
                return None

# ==========================================
# 数据库 Upsert 函数
# ==========================================
def save_prices_to_db(records):
    if not records: return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT OR REPLACE INTO prices (gas_date, hub, price_type, price, source_file, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', records)
    conn.commit()
    conn.close()
    logging.info(f"Saved {len(records)} price records to DB.")

def save_storage_to_db(records):
    if not records: return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT OR REPLACE INTO storage (gas_date, facility_id, held_in_storage, source_file, ingested_at)
        VALUES (?, ?, ?, ?, ?)
    ''', records)
    conn.commit()
    conn.close()
    logging.info(f"Saved {len(records)} storage records to DB.")

def save_flows_to_db(records):
    if not records: return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT OR REPLACE INTO flows (gas_date, facility_id, facility_type, supply, transfer_in, transfer_out, source_file, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', records)
    conn.commit()
    conn.close()
    logging.info(f"Saved {len(records)} flow records to DB.")


# ==========================================
# 数据提取与清洗逻辑
# ==========================================
def fetch_gbb_data():
    """拉取 GBB 数据，返回 Markdown 报告字符串和 DB 插入元组"""
    url = "https://nemweb.com.au/Reports/Current/GBB/GasBBActualFlowStorageLast31.CSV"
    content = fetch_with_retries(url)
    
    gbb_summary = {"storage": "Data unavailable", "flows": "Data unavailable"}
    storage_records = []
    flows_records = []
    
    if content:
        raw_path = os.path.join(RAW_DIR, "gbb_last31.csv")
        with open(raw_path, "wb") as f:
            f.write(content)
        
        try:
            with open("facilities.yaml", "r", encoding="utf-8") as ymlfile:
                facilities = yaml.safe_load(ymlfile)
            
            df = pd.read_csv(io.StringIO(content.decode('utf-8')))
            df['GasDate_dt'] = pd.to_datetime(df['GasDate'])
            latest_date_dt = df['GasDate_dt'].max()
            latest_date_str = latest_date_dt.strftime('%Y-%m-%d')
            today_df = df[df['GasDate_dt'] == latest_date_dt]
            
            # 定义审计字段 (UTC 时间)
            ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
            source_file = "gbb_last31.csv"
            
            # 1. 储气库
            storage_results = []
            for k, v in facilities.get('storage', {}).items():
                f_data = today_df[today_df['FacilityId'] == v['id']]
                if not f_data.empty:
                    val = float(f_data['HeldInStorage'].sum())
                    storage_results.append(f"**{v['name']}**: {val:,.0f} TJ")
                    # 入库：gas_date, facility_id, value, source_file, ingested_at
                    storage_records.append((latest_date_str, v['id'], val, source_file, ingested_at))
                else:
                    storage_results.append(f"**{v['name']}**: N/A")
            gbb_summary["storage"] = f"As of {latest_date_str}: " + " | ".join(storage_results)
            
            # 2. 流向数据
            flow_results = []
            
            for k, v in facilities.get('production', {}).items():
                f_data = today_df[today_df['FacilityId'] == v['id']]
                if not f_data.empty:
                    val = float(f_data['Supply'].sum())
                    flow_results.append(f"* **{v['name']}** (Production): {val:,.0f} TJ")
                    # 入库增加 facility_type: 'production'
                    flows_records.append((latest_date_str, v['id'], 'production', val, 0.0, 0.0, source_file, ingested_at))
                else:
                    flow_results.append(f"* **{v['name']}** (Production): N/A")

            for k, v in facilities.get('demand', {}).items():
                f_data = today_df[today_df['FacilityId'] == v['id']]
                if not f_data.empty:
                    val = float(f_data['Demand'].sum())
                    flow_results.append(f"* **{v['name']}** (LNG Export): {val:,.0f} TJ")
                    # 入库增加 facility_type: 'lng_export'
                    flows_records.append((latest_date_str, v['id'], 'lng_export', 0.0, 0.0, val, source_file, ingested_at)) 
                else:
                    flow_results.append(f"* **{v['name']}** (LNG Export): N/A")
                    
            for k, v in facilities.get('pipelines', {}).items():
                f_data = today_df[today_df['FacilityId'] == v['id']]
                if not f_data.empty:
                    t_in = float(f_data['TransferIn'].sum() + f_data['Supply'].sum())
                    t_out = float(f_data['TransferOut'].sum() + f_data['Demand'].sum())
                    flow_results.append(f"* **{v['name']}** (Pipeline): Flow In {t_in:,.0f} TJ | Flow Out {t_out:,.0f} TJ")
                    # 入库增加 facility_type: 'pipeline'
                    flows_records.append((latest_date_str, v['id'], 'pipeline', 0.0, t_in, t_out, source_file, ingested_at))
                else:
                    flow_results.append(f"* **{v['name']}** (Pipeline): N/A")
                    
            gbb_summary["flows"] = f"As of {latest_date_str}:\n" + "\n".join(flow_results)
            
        except Exception as e:
            error_msg = f"Parse error: {e}"
            gbb_summary["storage"] = error_msg
            gbb_summary["flows"] = error_msg
            
    return gbb_summary, storage_records, flows_records

def fetch_sttm_data():
    """拉取 STTM 目录，包含防幽灵数据（Stale Data）补丁"""
    folder_url = "https://nemweb.com.au/Reports/Current/STTM/"
    folder_content = fetch_with_retries(folder_url)
    
    sttm_summary = "Data unavailable"
    sttm_records = []
    
    if not folder_content:
        return "Failed to reach STTM directory.", []
        
    html = folder_content.decode('utf-8', errors='ignore')
    zip_files = re.findall(r'href="([^"]+\.zip)"', html, re.IGNORECASE)
    
    if not zip_files:
        return "No ZIP files found.", []
        
    # 构建最近 3 天的文件名探针
    days_to_try = [TODAY_DT, TODAY_DT - datetime.timedelta(days=1), TODAY_DT - datetime.timedelta(days=2)]
    valid_content = None
    
    for dt in days_to_try:
        candidate_name = f"Day{dt.day:02d}.zip"
        target_zip = next((zf for zf in zip_files if zf.upper().endswith(candidate_name.upper())), None)
        
        if target_zip:
            download_url = f"https://nemweb.com.au{target_zip}" if target_zip.startswith('/') else f"{folder_url}{target_zip}"
            content = fetch_with_retries(download_url)
            if content:
                try:
                    with zipfile.ZipFile(io.BytesIO(content)) as z:
                        int651_files = [f for f in z.namelist() if f.startswith('int651_')]
                        if int651_files:
                            df_peek = pd.read_csv(z.open(int651_files[0]), nrows=5)
                            df_peek['gas_date_dt'] = pd.to_datetime(df_peek['gas_date'])
                            peek_date = df_peek['gas_date_dt'].max().date()
                            days_old = (TODAY_DT.date() - peek_date).days
                            if -2 <= days_old <= 5:
                                valid_content = content
                                break
                except Exception as e:
                    logging.warning(f"Peek error {candidate_name}: {e}")
                    
    if not valid_content:
        return "Failed to find fresh STTM data (within 5 days).", []
        
    raw_path = os.path.join(RAW_DIR, "sttm_raw.zip")
    with open(raw_path, "wb") as f:
        f.write(valid_content)
            
    try:
        with open("facilities.yaml", "r", encoding="utf-8") as ymlfile:
            facilities = yaml.safe_load(ymlfile)
        hub_ids = [v['id'] for k, v in facilities.get('hubs', {}).items()]
        
        int651_dfs, int657_dfs = [], []
        with zipfile.ZipFile(io.BytesIO(valid_content)) as z:
            for filename in z.namelist():
                if filename.startswith('int651_'):
                    int651_dfs.append(pd.read_csv(z.open(filename)))
                elif filename.startswith('int657_'):
                    int657_dfs.append(pd.read_csv(z.open(filename)))
        
        # 捕获刚刚下载的文件名作为审计来源
        source_file_sttm = target_zip.split('/')[-1] if target_zip else "unknown_sttm.zip"
        ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        def process_price_dfs(dfs, date_col, price_col, label, price_type):
            if not dfs: return f"{label}: Not found.", []
            
            df = pd.concat(dfs, ignore_index=True)
            df['gas_date_dt'] = pd.to_datetime(df[date_col])
            df = df.sort_values('report_datetime') 
            latest_date = df['gas_date_dt'].max()
            today_df = df[df['gas_date_dt'] == latest_date]
            
            results = []
            db_rows = []
            for hub in hub_ids:
                hub_data = today_df[today_df['hub_identifier'] == hub]
                if not hub_data.empty:
                    price = float(hub_data.iloc[-1][price_col])
                    results.append(f"**{hub}**: ${price:.2f}/GJ")
                    # 入库：加入源文件和时间戳
                    db_rows.append((latest_date.strftime('%Y-%m-%d'), hub, price_type, price, source_file_sttm, ingested_at))
                else:
                    results.append(f"**{hub}**: N/A")
            return f"{label} ({latest_date.strftime('%Y-%m-%d')}): " + " | ".join(results), db_rows

        ante_summary, ante_rows = process_price_dfs(int651_dfs, 'gas_date', 'ex_ante_market_price', 'Ex-Ante', 'Ex-Ante')
        post_summary, post_rows = process_price_dfs(int657_dfs, 'gas_date', 'ex_post_imbalance_price', 'Ex-Post', 'Ex-Post')
        
        sttm_records.extend(ante_rows)
        sttm_records.extend(post_rows)
        sttm_summary = f"{ante_summary}\n* **Status:** {post_summary}"
            
    except Exception as e:
        sttm_summary = f"Parse error: {e}"
        
    return sttm_summary, sttm_records

def generate_report(gbb_data, sttm_status):
    """生成每日 Markdown 简报"""
    run_time = datetime.datetime.now(MELBOURNE_TZ).strftime('%Y-%m-%d %H:%M:%S AEST')
    
    md_content = f"""# East Coast Gas Daily Snapshot

**Gas Date / Run Date:** {TODAY_STR}
**Run Timestamp:** {run_time}
**Status:** F2 (Database Persistence Active)

---

## 1. Anomaly Summary
* Pipeline is in F2 phase. Historical data is now being stored locally.

## 2. Prices (STTM)
* **Status:** {sttm_status}

## 3. Storage
* **Status:** {gbb_data['storage']}

## 4. Flows (Major Facilities & Pipelines)
{gbb_data['flows']}

---
*Disclaimer: Personal learning project. Public AEMO data. Not investment advice.*
"""
    daily_report_path = os.path.join(REPORT_DIR, f"{TODAY_STR}.md")
    with open(daily_report_path, "w", encoding='utf-8') as f:
        f.write(md_content)
        
    latest_report_path = os.path.join(REPORT_DIR, "latest.md")
    with open(latest_report_path, "w", encoding='utf-8') as f:
        f.write(md_content)

def main():
    logging.info("Starting East Coast Gas Daily Pipeline...")
    setup_directories()
    
    logging.info("Fetching GBB Data...")
    gbb_summary, storage_records, flows_records = fetch_gbb_data()
    save_storage_to_db(storage_records)
    save_flows_to_db(flows_records)
    
    logging.info("Fetching STTM Data...")
    sttm_summary, sttm_records = fetch_sttm_data()
    save_prices_to_db(sttm_records)
    
    logging.info("Generating Report...")
    generate_report(gbb_summary, sttm_summary)
    
    logging.info("Pipeline run completed. Data persisted to SQLite.")

if __name__ == "__main__":
    main()