import os
import datetime
import time
import requests
import pandas as pd
import zipfile
import io
import logging

# 基础配置
MELBOURNE_TZ = datetime.timezone(datetime.timedelta(hours=10)) # AEST
TODAY_DT = datetime.datetime.now(MELBOURNE_TZ)
TODAY_STR = TODAY_DT.strftime('%Y-%m-%d')
RAW_DIR = f"data/raw/{TODAY_STR}"
REPORT_DIR = "reports"

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def setup_directories():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(REPORT_DIR, exist_ok=True)

def fetch_with_retries(url, max_retries=3, backoff_factor=2):
    """带指数退避的下载机制 (F1要求)"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                logging.error(f"All {max_retries} attempts failed for {url}")
                return None

def fetch_gbb_data():
    """拉取 GBB 数据并寻找 Iona 储气量"""
    url = "https://nemweb.com.au/Reports/Current/GBB/GasBBActualFlowStorageLast31.CSV"
    content = fetch_with_retries(url)
    
    iona_summary = "Data unavailable"
    if content:
        # 1. 归档原始数据
        raw_path = os.path.join(RAW_DIR, "gbb_last31.csv")
        with open(raw_path, "wb") as f:
            f.write(content)
        
        # 2. 尝试初步解析 (P1 仅抓取 Iona 相关行)
        try:
            df = pd.read_csv(io.StringIO(content.decode('utf-8')))
            # 注：这里仅作模糊匹配，实际列名和ID将在P2阶段从保存的raw_path中查证并固化
            iona_rows = df[df.apply(lambda row: row.astype(str).str.contains('Iona', case=False).any(), axis=1)]
            if not iona_rows.empty:
                iona_summary = f"Found {len(iona_rows)} rows mentioning 'Iona'. Awaiting P2 exact mapping."
            else:
                iona_summary = "Iona data not found in rough parse. Check raw CSV."
        except Exception as e:
            iona_summary = f"Parse error: {e}"
            
    return iona_summary

def fetch_sttm_data():
    """拉取 STTM 价格数据 (DAY01.ZIP)"""
    url = "https://www.nemweb.com.au/Reports/CURRENT/STTM/DAY01.ZIP"
    content = fetch_with_retries(url)
    
    sttm_summary = "Data unavailable"
    if content:
        # 1. 归档原始数据
        raw_path = os.path.join(RAW_DIR, "sttm_day01.zip")
        with open(raw_path, "wb") as f:
            f.write(content)
            
        # 2. 尝试初步解析 ZIP 寻找价格 CSV
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                csv_files = [f for f in z.namelist() if f.endswith('.CSV')]
                sttm_summary = f"Downloaded ZIP successfully. Contains {len(csv_files)} CSV files. "
                sttm_summary += f"Example files: {', '.join(csv_files[:3])}. Awaiting P2 mapping."
        except Exception as e:
            sttm_summary = f"Unzip/Parse error: {e}"
            
    return sttm_summary

def generate_report(iona_status, sttm_status):
    """生成每日 Markdown 简报 (F4 P1瘦身版)"""
    run_time = datetime.datetime.now(MELBOURNE_TZ).strftime('%Y-%m-%d %H:%M:%S AEST')
    
    md_content = f"""# East Coast Gas Daily Snapshot

**Gas Date / Run Date:** {TODAY_STR}
**Run Timestamp:** {run_time}
**Status:** P1 Thin Pipeline

---

## 1. Anomaly Summary
* Pipeline is in P1 phase. Anomaly detection logic will be implemented in P2.

## 2. Prices (STTM)
* **Status:** {sttm_status}
* *Raw ZIP file has been saved to `data/raw/` for structural inspection.*

## 3. Storage (Iona)
* **Status:** {iona_status}
* *Raw GBB CSV has been saved to `data/raw/` for structural inspection.*

---
*Disclaimer: Personal learning project. Public AEMO data. Not investment advice.*
"""
    # 写入特定日期文件
    daily_report_path = os.path.join(REPORT_DIR, f"{TODAY_STR}.md")
    with open(daily_report_path, "w", encoding='utf-8') as f:
        f.write(md_content)
        
    # 覆盖 latest.md
    latest_report_path = os.path.join(REPORT_DIR, "latest.md")
    with open(latest_report_path, "w", encoding='utf-8') as f:
        f.write(md_content)

def main():
    logging.info("Starting East Coast Gas Daily Pipeline (P1)...")
    setup_directories()
    
    logging.info("Fetching GBB Data...")
    iona_status = fetch_gbb_data()
    
    logging.info("Fetching STTM Data...")
    sttm_status = fetch_sttm_data()
    
    logging.info("Generating Report...")
    generate_report(iona_status, sttm_status)
    logging.info("Pipeline run completed.")

if __name__ == "__main__":
    main()