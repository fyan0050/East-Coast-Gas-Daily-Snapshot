import os
import datetime
import time
import requests
import pandas as pd
import zipfile
import io
import logging
import re

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
    """带指数退避的下载机制 (加入 User-Agent 伪装突破 AEMO 拦截)"""
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

def fetch_gbb_data():
    """拉取 GBB 数据并寻找 Iona 储气量"""
    url = "https://nemweb.com.au/Reports/Current/GBB/GasBBActualFlowStorageLast31.CSV"
    content = fetch_with_retries(url)
    
    iona_summary = "Data unavailable"
    if content:
        raw_path = os.path.join(RAW_DIR, "gbb_last31.csv")
        with open(raw_path, "wb") as f:
            f.write(content)
        
        try:
            df = pd.read_csv(io.StringIO(content.decode('utf-8')))
            iona_rows = df[df.apply(lambda row: row.astype(str).str.contains('Iona', case=False).any(), axis=1)]
            if not iona_rows.empty:
                iona_summary = f"Found {len(iona_rows)} rows mentioning 'Iona'. Awaiting P2 exact mapping."
            else:
                iona_summary = "Iona data not found in rough parse. Check raw CSV."
        except Exception as e:
            iona_summary = f"Parse error: {e}"
            
    return iona_summary

def fetch_sttm_data():
    """动态拉取 STTM 目录，寻找最新的 ZIP 文件，解决硬编码失效问题"""
    # 修正：将 CURRENT 改为首字母大写 Current，并去掉 www
    folder_url = "https://nemweb.com.au/Reports/Current/STTM/"
    logging.info(f"Scanning STTM directory: {folder_url}")
    
    folder_content = fetch_with_retries(folder_url)
    sttm_summary = "Data unavailable"
    
    if not folder_content:
        return "Failed to reach STTM directory. Check Github Actions log for HTTP error."
        
    html = folder_content.decode('utf-8', errors='ignore')
    
    # 使用正则解析 HTML 目录，寻找所有 ZIP 文件链接
    zip_files = re.findall(r'href="([^"]+\.zip)"', html, re.IGNORECASE)
    
    if not zip_files:
        return "Folder reached, but no ZIP files found in directory listing."
        
    # 寻找包含 DAY01 或 CURRENTDAY 的文件；如果没有，默认取列表最后一个（NEMWEB中通常是最新的）
    target_zip = None
    for zf in reversed(zip_files):
        if 'DAY01' in zf.upper() or 'CURRENTDAY' in zf.upper():
            target_zip = zf
            break
            
    if not target_zip:
        target_zip = zip_files[-1]
        
    # 拼接完整下载链接
    if target_zip.startswith('/'):
        download_url = f"https://nemweb.com.au{target_zip}"
    else:
        download_url = f"{folder_url}{target_zip}"
        
    logging.info(f"Dynamically found STTM ZIP target: {download_url}")
    
    # 再次请求下载真实的 ZIP 文件
    content = fetch_with_retries(download_url)
    if content:
        raw_path = os.path.join(RAW_DIR, "sttm_raw.zip")
        with open(raw_path, "wb") as f:
            f.write(content)
            
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                csv_files = [f for f in z.namelist() if f.upper().endswith('.CSV')]
                file_name = target_zip.split('/')[-1]
                sttm_summary = f"Success! Downloaded {file_name}. Contains {len(csv_files)} CSV files. Awaiting P2."
        except Exception as e:
            sttm_summary = f"Unzip error: {e}"
            
    return sttm_summary

def generate_report(iona_status, sttm_status):
    """生成每日 Markdown 简报"""
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

## 3. Storage (Iona)
* **Status:** {iona_status}

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