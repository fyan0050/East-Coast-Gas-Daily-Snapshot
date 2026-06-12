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
    """拉取 GBB 数据并精确提取 Iona 最新储气量"""
    url = "https://nemweb.com.au/Reports/Current/GBB/GasBBActualFlowStorageLast31.CSV"
    content = fetch_with_retries(url)
    
    iona_summary = "Data unavailable"
    if content:
        raw_path = os.path.join(RAW_DIR, "gbb_last31.csv")
        with open(raw_path, "wb") as f:
            f.write(content)
        
        try:
            # 读取参考表
            with open("facilities.yaml", "r") as ymlfile:
                facilities = yaml.safe_load(ymlfile)
            iona_id = facilities['storage']['iona_ugs']['id']
            
            # 解析 CSV
            df = pd.read_csv(io.StringIO(content.decode('utf-8')))
            
            # 过滤 Iona 数据
            iona_df = df[df['FacilityId'] == iona_id]
            
            if not iona_df.empty:
                # 寻找最新的一天
                latest_date = iona_df['GasDate'].max()
                latest_row = iona_df[iona_df['GasDate'] == latest_date].iloc[0]
                
                # 提取具体数值并进行格式化
                storage_val = latest_row['HeldInStorage']
                iona_summary = f"**{storage_val:,.0f} TJ** (as of {latest_date[:10]})"
            else:
                iona_summary = "Iona Facility ID not found in current data."
        except Exception as e:
            iona_summary = f"Parse error: {e}"
            
    return iona_summary

def fetch_sttm_data():
    """动态拉取 STTM 目录，提取三大 Hub 的 Ex-Ante 价格"""
    folder_url = "https://nemweb.com.au/Reports/Current/STTM/"
    folder_content = fetch_with_retries(folder_url)
    sttm_summary = "Data unavailable"
    
    if not folder_content:
        return "Failed to reach STTM directory."
        
    html = folder_content.decode('utf-8', errors='ignore')
    zip_files = re.findall(r'href="([^"]+\.zip)"', html, re.IGNORECASE)
    
    if not zip_files:
        return "No ZIP files found in directory listing."
        
    target_zip = None
    for zf in reversed(zip_files):
        if 'DAY01' in zf.upper() or 'CURRENTDAY' in zf.upper():
            target_zip = zf
            break
            
    if not target_zip:
        target_zip = zip_files[-1]
        
    download_url = f"https://nemweb.com.au{target_zip}" if target_zip.startswith('/') else f"{folder_url}{target_zip}"
    content = fetch_with_retries(download_url)
    
    if content:
        raw_path = os.path.join(RAW_DIR, "sttm_raw.zip")
        with open(raw_path, "wb") as f:
            f.write(content)
            
        try:
            # 1. 加载 YAML 配置中的 Hub ID
            with open("facilities.yaml", "r") as ymlfile:
                facilities = yaml.safe_load(ymlfile)
            hub_ids = [v['id'] for k, v in facilities.get('hubs', {}).items()]
            
            # 2. 遍历 ZIP，寻找所有的 int651 文件
            int651_dfs = []
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                for filename in z.namelist():
                    if filename.startswith('int651_'):
                        with z.open(filename) as csv_file:
                            int651_dfs.append(pd.read_csv(csv_file))
            
            # 3. 数据清洗与提取
            if int651_dfs:
                # 合并所有同类文件
                df = pd.concat(int651_dfs, ignore_index=True)
                df['gas_date_dt'] = pd.to_datetime(df['gas_date'])
                # 按报告时间排序，确保最后出现的是最新数据
                df = df.sort_values('report_datetime') 
                
                # 获取数据集中最新的天然气日期
                latest_date = df['gas_date_dt'].max()
                today_df = df[df['gas_date_dt'] == latest_date]
                
                results = []
                for hub in hub_ids:
                    hub_data = today_df[today_df['hub_identifier'] == hub]
                    if not hub_data.empty:
                        price = hub_data.iloc[-1]['ex_ante_market_price']
                        results.append(f"**{hub}**: ${price:.2f}/GJ")
                    else:
                        results.append(f"**{hub}**: N/A")
                        
                sttm_summary = f"Ex-Ante ({latest_date.strftime('%Y-%m-%d')}): " + " | ".join(results)
            else:
                sttm_summary = "Price data (int651) not found in today's ZIP."
                
        except Exception as e:
            sttm_summary = f"Parse error: {e}"
            
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