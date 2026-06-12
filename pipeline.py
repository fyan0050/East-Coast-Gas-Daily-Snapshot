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
    """拉取 GBB 数据并精确提取所有指定设施的最新指标 (已修复聚合与业务逻辑)"""
    url = "https://nemweb.com.au/Reports/Current/GBB/GasBBActualFlowStorageLast31.CSV"
    content = fetch_with_retries(url)
    
    gbb_summary = {"storage": "Data unavailable", "flows": "Data unavailable"}
    
    if content:
        raw_path = os.path.join(RAW_DIR, "gbb_last31.csv")
        with open(raw_path, "wb") as f:
            f.write(content)
        
        try:
            with open("facilities.yaml", "r") as ymlfile:
                facilities = yaml.safe_load(ymlfile)
            
            df = pd.read_csv(io.StringIO(content.decode('utf-8')))
            df['GasDate_dt'] = pd.to_datetime(df['GasDate'])
            latest_date_dt = df['GasDate_dt'].max()
            latest_date_str = latest_date_dt.strftime('%Y-%m-%d')
            today_df = df[df['GasDate_dt'] == latest_date_dt]
            
            # 1. 储气库 (Sum)
            storage_results = []
            for k, v in facilities.get('storage', {}).items():
                f_data = today_df[today_df['FacilityId'] == v['id']]
                if not f_data.empty:
                    val = f_data['HeldInStorage'].sum() # 修复：使用 sum()
                    storage_results.append(f"**{v['name']}**: {val:,.0f} TJ")
                else:
                    storage_results.append(f"**{v['name']}**: N/A")
            gbb_summary["storage"] = f"As of {latest_date_str}: " + " | ".join(storage_results)
            
            # 2. 流向数据
            flow_results = []
            
            # 生产设施 (Supply -> Sum)
            for k, v in facilities.get('production', {}).items():
                f_data = today_df[today_df['FacilityId'] == v['id']]
                if not f_data.empty:
                    val = f_data['Supply'].sum()
                    flow_results.append(f"* **{v['name']}** (Production): {val:,.0f} TJ")
                else:
                    flow_results.append(f"* **{v['name']}** (Production): N/A")

            # 需求设施 - 如 LNG 出口 (Demand -> Sum)
            for k, v in facilities.get('demand', {}).items():
                f_data = today_df[today_df['FacilityId'] == v['id']]
                if not f_data.empty:
                    val = f_data['Demand'].sum()
                    flow_results.append(f"* **{v['name']}** (LNG Export): {val:,.0f} TJ")
                else:
                    flow_results.append(f"* **{v['name']}** (LNG Export): N/A")
                    
            # 管道设施 (修复 AEMO 混合记账规则: True In = TransferIn + Supply, True Out = TransferOut + Demand)
            for k, v in facilities.get('pipelines', {}).items():
                f_data = today_df[today_df['FacilityId'] == v['id']]
                if not f_data.empty:
                    # 将 Supply 和 Demand 纳入管道的物理进出流计算
                    t_in = f_data['TransferIn'].sum() + f_data['Supply'].sum()
                    t_out = f_data['TransferOut'].sum() + f_data['Demand'].sum()
                    flow_results.append(f"* **{v['name']}** (Pipeline): Flow In {t_in:,.0f} TJ | Flow Out {t_out:,.0f} TJ")
                else:
                    flow_results.append(f"* **{v['name']}** (Pipeline): N/A")
                    
            gbb_summary["flows"] = f"As of {latest_date_str}:\n" + "\n".join(flow_results)
            
        except Exception as e:
            error_msg = f"Parse error: {e}"
            gbb_summary["storage"] = error_msg
            gbb_summary["flows"] = error_msg
            
    return gbb_summary

def fetch_sttm_data():
    """动态拉取 STTM 目录，包含防幽灵数据（Stale Data）的日期探测逻辑"""
    folder_url = "https://nemweb.com.au/Reports/Current/STTM/"
    folder_content = fetch_with_retries(folder_url)
    
    if not folder_content:
        return "Failed to reach STTM directory."
        
    html = folder_content.decode('utf-8', errors='ignore')
    zip_files = re.findall(r'href="([^"]+\.zip)"', html, re.IGNORECASE)
    
    if not zip_files:
        return "No ZIP files found in directory listing."
        
    # 1. 构造最近 3 天的候选名单 (完美处理月初的跨月问题)
    days_to_try = [
        TODAY_DT,
        TODAY_DT - datetime.timedelta(days=1),
        TODAY_DT - datetime.timedelta(days=2)
    ]
    
    valid_content = None
    
    for dt in days_to_try:
        candidate_name = f"Day{dt.day:02d}.zip"
        target_zip = next((zf for zf in zip_files if zf.upper().endswith(candidate_name.upper())), None)
        
        if target_zip:
            download_url = f"https://nemweb.com.au{target_zip}" if target_zip.startswith('/') else f"{folder_url}{target_zip}"
            logging.info(f"Probing STTM file: {candidate_name}")
            content = fetch_with_retries(download_url)
            
            if content:
                # 💡 核心防御：打开 ZIP 偷偷看一眼日期
                try:
                    with zipfile.ZipFile(io.BytesIO(content)) as z:
                        int651_files = [f for f in z.namelist() if f.startswith('int651_')]
                        if int651_files:
                            # 只读取前 5 行进行极速探测
                            df_peek = pd.read_csv(z.open(int651_files[0]), nrows=5)
                            df_peek['gas_date_dt'] = pd.to_datetime(df_peek['gas_date'])
                            peek_date = df_peek['gas_date_dt'].max().date()
                            today_date = TODAY_DT.date()
                            
                            # 计算文件内的真实数据距离今天有多久
                            days_old = (today_date - peek_date).days
                            
                            # Ex-ante 价格通常是提前发布的 (D+1)，所以有负数。
                            # 如果数据日期在今天前后几天内，说明是新鲜的！
                            if -2 <= days_old <= 5:
                                valid_content = content
                                logging.info(f"✅ Success: {candidate_name} is fresh (Contains {peek_date})")
                                break
                            else:
                                logging.warning(f"❌ Discarded: {candidate_name} is a stale file from last month (Contains {peek_date})")
                except Exception as e:
                    logging.warning(f"Failed to peek into {candidate_name}: {e}")
                    
    if not valid_content:
        return "Failed to find any fresh STTM data (within 5 days) in the directory."
        
    # --- 找到新鲜数据后，执行正常解析 ---
    raw_path = os.path.join(RAW_DIR, "sttm_raw.zip")
    with open(raw_path, "wb") as f:
        f.write(valid_content)
            
    try:
        with open("facilities.yaml", "r") as ymlfile:
            facilities = yaml.safe_load(ymlfile)
        hub_ids = [v['id'] for k, v in facilities.get('hubs', {}).items()]
        
        int651_dfs, int657_dfs = [], []
        with zipfile.ZipFile(io.BytesIO(valid_content)) as z:
            for filename in z.namelist():
                if filename.startswith('int651_'):
                    int651_dfs.append(pd.read_csv(z.open(filename)))
                elif filename.startswith('int657_'):
                    int657_dfs.append(pd.read_csv(z.open(filename)))
        
        def process_price_dfs(dfs, date_col, price_col, label):
            if not dfs: 
                return f"{label}: Data not found in today's ZIP."
            df = pd.concat(dfs, ignore_index=True)
            df['gas_date_dt'] = pd.to_datetime(df[date_col])
            df = df.sort_values('report_datetime') 
            latest_date = df['gas_date_dt'].max()
            today_df = df[df['gas_date_dt'] == latest_date]
            
            results = []
            for hub in hub_ids:
                hub_data = today_df[today_df['hub_identifier'] == hub]
                if not hub_data.empty:
                    price = hub_data.iloc[-1][price_col]
                    results.append(f"**{hub}**: ${price:.2f}/GJ")
                else:
                    results.append(f"**{hub}**: N/A")
            return f"{label} ({latest_date.strftime('%Y-%m-%d')}): " + " | ".join(results)

        ante_summary = process_price_dfs(int651_dfs, 'gas_date', 'ex_ante_market_price', 'Ex-Ante')
        post_summary = process_price_dfs(int657_dfs, 'gas_date', 'ex_post_imbalance_price', 'Ex-Post')
        
        return f"{ante_summary}\n* **Status:** {post_summary}"
            
    except Exception as e:
        return f"Parse error: {e}"

def generate_report(gbb_data, sttm_status):
    """生成每日 Markdown 简报 (完美匹配 PRD 章节结构)"""
    run_time = datetime.datetime.now(MELBOURNE_TZ).strftime('%Y-%m-%d %H:%M:%S AEST')
    
    md_content = f"""# East Coast Gas Daily Snapshot

**Gas Date / Run Date:** {TODAY_STR}
**Run Timestamp:** {run_time}
**Status:** P1 Thin Pipeline (All Core Facilities Live)

---

## 1. Anomaly Summary
* Pipeline is in P1 phase. Anomaly detection logic will be implemented in P2.

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
    logging.info("Starting East Coast Gas Daily Pipeline (P1)...")
    setup_directories()
    
    logging.info("Fetching GBB Data (All Facilities)...")
    gbb_data = fetch_gbb_data()
    
    logging.info("Fetching STTM Data...")
    sttm_status = fetch_sttm_data()
    
    logging.info("Generating Report...")
    generate_report(gbb_data, sttm_status)
    logging.info("Pipeline run completed.")

if __name__ == "__main__":
    main()