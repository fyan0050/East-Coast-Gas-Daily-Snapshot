import os
import sys
import io
import re
import time
import zipfile
import logging
import datetime

import requests
import pandas as pd
import yaml
import sqlite3

# ==========================================
# 基础配置
# ==========================================
try:
    from zoneinfo import ZoneInfo
    MELBOURNE_TZ = ZoneInfo("Australia/Melbourne")
except ImportError:
    MELBOURNE_TZ = datetime.timezone(datetime.timedelta(hours=10))

TODAY_DT = datetime.datetime.now(MELBOURNE_TZ)
TODAY_STR = TODAY_DT.strftime('%Y-%m-%d')
RAW_DIR = f"data/raw/{TODAY_STR}"
REPORT_DIR = "reports"
DB_PATH = "data/processed/east_coast_gas.db"

GBB_URL = "https://nemweb.com.au/Reports/Current/GBB/GasBBActualFlowStorageLast31.CSV"
STTM_FOLDER_URL = "https://nemweb.com.au/Reports/Current/STTM/"

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
PIPELINE_ERRORS = []

# 表结构假定（建表在本文件之外）：
#   storage(gas_date, facility_id, held_in_storage, source_file, ingested_at)
#           UNIQUE(gas_date, facility_id)
#   flows(gas_date, facility_id, facility_type, supply, demand, transfer_in,
#         transfer_out, source_file, ingested_at)
#           UNIQUE(gas_date, facility_id)
#   prices(gas_date, hub, price_type, price, source_file, ingested_at)
#           UNIQUE(gas_date, hub, price_type)
# UNIQUE 约束是 INSERT OR REPLACE 幂等(修订自愈)的前提。

# ==========================================
# 目录
# ==========================================
def setup_directories():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ==========================================
# 网络
# ==========================================
def fetch_with_retries(url, max_retries=3, backoff_factor=2):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/120.0.0.0 Safari/537.36"}
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(backoff_factor ** attempt)
    logging.error(f"All {max_retries} attempts failed for {url}")
    return None

def load_facilities():
    with open("facilities.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# ==========================================
# 聚合规则
# ==========================================
def _dedup_latest_per_location(df, point_col='LocationId', sort_col='LastUpdated'):
    """跨 location 聚合前，先在每个 location 内取最新修订版本，防修订行被重复加。"""
    if sort_col in df.columns and point_col in df.columns:
        return df.sort_values(sort_col).groupby(point_col, as_index=False).last()
    return df

def storage_latest(group_df, value_col='HeldInStorage', sort_col='LastUpdated'):
    """存量(瞬时水位)：取最新一行，绝不 sum。同设施若多 location 取全部行里最新一条。"""
    if group_df.empty:
        return None
    df = group_df
    if sort_col in df.columns:
        df = df.sort_values(sort_col)
    val = df.iloc[-1][value_col]
    return None if pd.isna(val) else float(val)

def flow_sums(group_df):
    """流量类(production/lng_export/pipeline)：每 location 取最新版本后，四列全部跨 location 求和。
    存量取最新、流量求和——两类规则相反。"""
    if group_df.empty:
        return None
    df = _dedup_latest_per_location(group_df)
    def s(col):
        return float(df[col].sum()) if col in df.columns else 0.0
    return {
        'supply':       s('Supply'),
        'demand':       s('Demand'),
        'transfer_in':  s('TransferIn'),
        'transfer_out': s('TransferOut'),
    }

# ==========================================
# 数据库写入
# ==========================================
def save_storage(records):
    if not records: return
    conn = sqlite3.connect(DB_PATH)
    conn.executemany(
        'INSERT OR REPLACE INTO storage '
        '(gas_date, facility_id, held_in_storage, source_file, ingested_at) '
        'VALUES (?, ?, ?, ?, ?)', records)
    conn.commit(); conn.close()
    logging.info(f"Saved {len(records)} storage records.")

def save_flows(records):
    if not records: return
    conn = sqlite3.connect(DB_PATH)
    conn.executemany(
        'INSERT OR REPLACE INTO flows '
        '(gas_date, facility_id, facility_type, supply, demand, transfer_in, '
        ' transfer_out, source_file, ingested_at) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', records)
    conn.commit(); conn.close()
    logging.info(f"Saved {len(records)} flow records.")

def save_prices(records):
    if not records: return
    conn = sqlite3.connect(DB_PATH)
    conn.executemany(
        'INSERT OR REPLACE INTO prices '
        '(gas_date, hub, price_type, price, source_file, ingested_at) '
        'VALUES (?, ?, ?, ?, ?, ?)', records)
    conn.commit(); conn.close()
    logging.info(f"Saved {len(records)} price records.")

# ==========================================
# GBB：库存与流量（入库前聚合到 facility 粒度）
# ==========================================
def fetch_gbb_data():
    gbb_summary = {"storage": "Data unavailable", "flows": "Data unavailable"}
    storage_records, flows_records = [], []

    content = fetch_with_retries(GBB_URL)
    if not content:
        PIPELINE_ERRORS.append("GBB download failed (network).")
        return gbb_summary, storage_records, flows_records

    raw_path = os.path.join(RAW_DIR, "gbb_last31.csv")
    with open(raw_path, "wb") as f:
        f.write(content)

    source_file = "GasBBActualFlowStorageLast31.CSV"
    ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    try:
        fac = load_facilities()
        df = pd.read_csv(raw_path)
        df['GasDate_dt'] = pd.to_datetime(df['GasDate'])
        latest_dt = df['GasDate_dt'].max()
        latest_str = latest_dt.strftime('%Y-%m-%d')

        def fid(v):
            return str(v['id'])

        # ---------- 全量解析 31 天滚动窗口，聚合后入库 ----------
        for date_dt, group in df.groupby('GasDate_dt'):
            d_str = date_dt.strftime('%Y-%m-%d')
            group = group.copy()
            group['FacilityId'] = group['FacilityId'].astype(str)

            for _, v in fac.get('storage', {}).items():
                sub = group[group['FacilityId'] == fid(v)]
                val = storage_latest(sub)
                if val is not None:
                    storage_records.append((d_str, fid(v), val, source_file, ingested_at))

            for ftype, key in [('production', 'production'),
                               ('lng_export', 'demand'),
                               ('pipeline', 'pipelines')]:
                for _, v in fac.get(key, {}).items():
                    sub = group[group['FacilityId'] == fid(v)]
                    fs = flow_sums(sub)
                    if fs is not None:
                        flows_records.append((
                            d_str, fid(v), ftype,
                            fs['supply'], fs['demand'], fs['transfer_in'], fs['transfer_out'],
                            source_file, ingested_at))

        # ---------- 最新一天 → 日报文本 ----------
        today = df.copy()
        today['FacilityId'] = today['FacilityId'].astype(str)
        today = today[today['GasDate_dt'] == latest_dt]

        storage_lines = []
        for _, v in fac.get('storage', {}).items():
            val = storage_latest(today[today['FacilityId'] == fid(v)])
            storage_lines.append(f"* **{v['name']}**: "
                                 f"{f'{val:,.0f} TJ' if val is not None else 'N/A'}")
        gbb_summary["storage"] = f"As of {latest_str}:\n" + "\n".join(storage_lines)

        flow_lines = []
        for _, v in fac.get('production', {}).items():
            fs = flow_sums(today[today['FacilityId'] == fid(v)])
            val = fs['supply'] if fs else None
            flow_lines.append(f"* **{v['name']}** (Production): "
                              f"{f'{val:,.0f} TJ' if val is not None else 'N/A'}")
        for _, v in fac.get('demand', {}).items():
            fs = flow_sums(today[today['FacilityId'] == fid(v)])
            val = fs['demand'] if fs else None
            flow_lines.append(f"* **{v['name']}** (LNG Export): "
                              f"{f'{val:,.0f} TJ' if val is not None else 'N/A'}")
        for _, v in fac.get('pipelines', {}).items():
            fs = flow_sums(today[today['FacilityId'] == fid(v)])
            if fs is not None:
                # 管道展示对齐 AEMO dashboard：Actual Demand=demand, Actual TransferOut=transfer_out
                flow_lines.append(f"* **{v['name']}** (Pipeline): "
                                  f"Actual Demand {fs['demand']:,.0f} TJ | "
                                  f"Actual TransferOut {fs['transfer_out']:,.0f} TJ")
            else:
                flow_lines.append(f"* **{v['name']}** (Pipeline): N/A")
        gbb_summary["flows"] = (f"As of {latest_str} "
                                f"(pipeline figures aligned to AEMO GBB dashboard):\n"
                                + "\n".join(flow_lines))

        save_storage(storage_records)
        save_flows(flows_records)
        logging.info(f"GBB OK. storage={len(storage_records)} flows={len(flows_records)}")

    except Exception as e:
        logging.exception("GBB parse/persist error")
        PIPELINE_ERRORS.append(f"GBB parse error: {e}")
        gbb_summary["storage"] = f"Parse error: {e}"
        gbb_summary["flows"] = f"Parse error: {e}"

    return gbb_summary, storage_records, flows_records

# ==========================================
# STTM：枢纽价格
# ==========================================
def _parse_sttm_zip(content):
    ante, post, max_date = [], [], None
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        for fn in z.namelist():
            low = fn.lower()
            if low.startswith('int651_'):
                ante.append(pd.read_csv(z.open(fn)))
            elif low.startswith('int657_'):
                post.append(pd.read_csv(z.open(fn)))
        for d in ante + post:
            if 'gas_date' in d.columns:
                md = pd.to_datetime(d['gas_date']).max().date()
                max_date = md if max_date is None else max(max_date, md)
    return ante, post, max_date

def fetch_sttm_data():
    sttm_records = []
    folder = fetch_with_retries(STTM_FOLDER_URL)
    if not folder:
        PIPELINE_ERRORS.append("STTM directory unreachable (network).")
        return "Failed to reach STTM directory.", []

    html = folder.decode('utf-8', errors='ignore')
    zip_files = re.findall(r'href="([^"]+\.zip)"', html, re.IGNORECASE)
    if not zip_files:
        PIPELINE_ERRORS.append("STTM: no ZIP files listed.")
        return "No ZIP files found.", []

    best = None
    candidates = sorted(set(zip_files), key=lambda z: (0 if 'CURRENTDAY' in z.upper() else 1))
    for zf in candidates:
        url = f"https://nemweb.com.au{zf}" if zf.startswith('/') else f"{STTM_FOLDER_URL}{zf}"
        content = fetch_with_retries(url)
        if not content:
            continue
        try:
            _, _, md = _parse_sttm_zip(content)
        except Exception as e:
            logging.warning(f"STTM peek failed {zf}: {e}")
            continue
        if md is None:
            continue
        days_old = (TODAY_DT.date() - md).days
        if -2 <= days_old <= 5:
            if best is None or md > best[0]:
                best = (md, content, zf.split('/')[-1])
            if 'CURRENTDAY' in zf.upper():
                break

    if best is None:
        PIPELINE_ERRORS.append("STTM: no fresh data within window.")
        return "Failed to find fresh STTM data (within 5 days).", []

    _, valid_content, source_file_sttm = best
    with open(os.path.join(RAW_DIR, "sttm_raw.zip"), "wb") as f:
        f.write(valid_content)

    try:
        fac = load_facilities()
        hub_ids = [v['id'] for _, v in fac.get('hubs', {}).items()]
        int651_dfs, int657_dfs, _ = _parse_sttm_zip(valid_content)
        ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        def process(dfs, price_col, label, price_type):
            if not dfs:
                return f"{label}: Not found.", []
            df = pd.concat(dfs, ignore_index=True)
            df['gas_date_dt'] = pd.to_datetime(df['gas_date'])
            if 'report_datetime' in df.columns:
                df = df.sort_values('report_datetime')
            latest = df['gas_date_dt'].max()
            today = df[df['gas_date_dt'] == latest]
            res, rows = [], []
            for hub in hub_ids:
                hd = today[today['hub_identifier'] == hub]
                if not hd.empty:
                    price = float(hd.iloc[-1][price_col])
                    res.append(f"**{hub}**: ${price:.2f}/GJ")
                    rows.append((latest.strftime('%Y-%m-%d'), hub, price_type,
                                 price, source_file_sttm, ingested_at))
                else:
                    res.append(f"**{hub}**: N/A")
            return (f"{label} ({latest.strftime('%Y-%m-%d')}): " + " | ".join(res)), rows

        ante_s, ante_r = process(int651_dfs, 'ex_ante_market_price', 'Ex-Ante', 'Ex-Ante')
        post_s, post_r = process(int657_dfs, 'ex_post_imbalance_price', 'Ex-Post', 'Ex-Post')

        sttm_records.extend(ante_r)
        sttm_records.extend(post_r)
        save_prices(sttm_records)
        logging.info(f"STTM OK. prices={len(sttm_records)}")
        return f"{ante_s}\n* **Status:** {post_s}", sttm_records

    except Exception as e:
        logging.exception("STTM parse/persist error")
        PIPELINE_ERRORS.append(f"STTM parse error: {e}")
        return f"Parse error: {e}", []


DWGM_URL = "https://nemweb.com.au/Reports/CURRENT/VicGas/CurrentDay.zip"
 
def _file_timestamp(fn):
    """从 int037c_..._N~<timestamp>.csv 提取末尾时间戳数字，用于选最新文件。"""
    m = re.search(r'~(\d+)\.csv$', fn, re.IGNORECASE)
    return int(m.group(1)) if m else -1
 
def fetch_dwgm_data():
    """返回 (dwgm_summary_str, records_list)。"""
    content = fetch_with_retries(DWGM_URL)
    if not content:
        PIPELINE_ERRORS.append("DWGM download failed (network).")
        return "Data unavailable", []
 
    ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
 
    try:
        # 1. 文件层：选时间戳最大的 int037c indicative_price 文件
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            candidates = [f for f in z.namelist()
                          if f.lower().startswith('int037c')
                          and 'indicative_price' in f.lower()
                          and f.lower().endswith('.csv')]
            if not candidates:
                PIPELINE_ERRORS.append("DWGM parse error: no int037c file in zip.")
                return "No int037c file found", []
 
            latest_file = max(candidates, key=_file_timestamp)
            with z.open(latest_file) as fh:
                df = pd.read_csv(fh)
 
        source_file = latest_file.split('/')[-1]
 
        # 2. 解析：日期与 approval 时间
        # gas_date 形如 14-Jun-26；approval/current_date 形如 14/06/26 5:09
        # 显式格式避免 dateutil 逐行推断(慢且可能不一致)；若 AEMO 改格式会明确报错而非静默错解
        df['gas_date_dt'] = pd.to_datetime(df['gas_date'], format='%d-%b-%y', errors='coerce')
        df['approval_dt'] = pd.to_datetime(df['approval_datetime'], format='%d/%m/%y %H:%M', errors='coerce')
        current_date = pd.to_datetime(df['current_date'], format='%d/%m/%y %H:%M',
                                      errors='coerce').max().normalize()
 
        # 行层：每个 gas_date 取 approval 最早的一行(6am schedule)
        records, rows_for_report = [], []
        for gdate, grp in df.groupby('gas_date_dt'):
            earliest = grp.sort_values('approval_dt').iloc[0]
            price = float(earliest['price_value_gst_ex'])
            is_forecast = 1 if gdate.normalize() > current_date else 0
            g_str = gdate.strftime('%Y-%m-%d')
            records.append((
                g_str, price,
                earliest['approval_dt'].strftime('%Y-%m-%d %H:%M'),
                is_forecast, source_file, ingested_at))
            rows_for_report.append((g_str, price, is_forecast))
 
        # 3. 入库
        if records:
            conn = sqlite3.connect(DB_PATH)
            conn.executemany(
                'INSERT OR REPLACE INTO dwgm_prices '
                '(gas_date, price_6am_schedule, approval_datetime, is_forecast, '
                ' source_file, ingested_at) VALUES (?, ?, ?, ?, ?, ?)', records)
            conn.commit(); conn.close()
            logging.info(f"Saved {len(records)} DWGM records.")
 
        summary = _build_dwgm_summary(rows_for_report)
        return summary, records
 
    except Exception as e:
        logging.exception("DWGM parse/persist error")
        PIPELINE_ERRORS.append(f"DWGM parse error: {e}")
        return f"Parse error: {e}", []

def _build_dwgm_summary(rows):
    """报告文本：当日实际价 + 明后日预测价。"""
    if not rows:
        return "No DWGM data."
    rows = sorted(rows, key=lambda r: r[0])
    actual = [(d, p) for (d, p, f) in rows if f == 0]
    forecast = [(d, p) for (d, p, f) in rows if f == 1]
 
    parts = []
    if actual:
        # 最新的实际日(通常就是当天 gas date)
        d, p = actual[-1]
        parts.append(f"VIC (DWGM) {d}: **${p:.2f}/GJ** ")
    if forecast:
        fc = " | ".join(f"{d}: ${p:.2f}" for d, p in forecast)
        parts.append(f"Forecast — {fc}")
    return " \n* **Status:** ".join(parts)


# ==========================================
# 报告
# ==========================================
def generate_report(gbb_data, sttm_status, dwgm_status, weather_status=None):
    run_time = datetime.datetime.now(MELBOURNE_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
    health = "OK" if not PIPELINE_ERRORS else "DEGRADED: " + "; ".join(PIPELINE_ERRORS)

    # weather 段可选：未拼接 weather 模块时传 None，则不显示该段
    weather_section = ""
    if weather_status:
        weather_section = f"""## 5. Weather & Heating Demand Signal
* **Status:** {weather_status}

"""

    md = f"""# East Coast Gas Daily Snapshot

**Gas Date / Run Date:** {TODAY_STR}
**Run Timestamp:** {run_time}
**Pipeline health:** {health}

---

## 1. Anomaly Summary
* (anomaly module pending)

## 2. Prices — STTM Hubs (Sydney / Brisbane / Adelaide)
* **Status:** {sttm_status}

## 3. Price — DWGM (Victoria, single price)
* **Status:** {dwgm_status}

  _Figure shown is the 6am schedule price — the ASX Victorian gas futures reference price._

## 4. Storage
* **Status:** {gbb_data['storage']}

## 5. Flows (Major Facilities & Pipelines)
{gbb_data['flows']}

{weather_section}---
*Disclaimer: Personal learning project. Public AEMO data. Not investment advice.*
"""
    with open(os.path.join(REPORT_DIR, f"{TODAY_STR}.md"), "w", encoding='utf-8') as f:
        f.write(md)
    with open(os.path.join(REPORT_DIR, "latest.md"), "w", encoding='utf-8') as f:
        f.write(md)

# ==========================================
# 主流程
# ==========================================
def main():
    logging.info("Starting East Coast Gas Daily Pipeline...")
    setup_directories()

    gbb_summary, _, _ = fetch_gbb_data()
    sttm_summary, _ = fetch_sttm_data()
    dwgm_summary, _ = fetch_dwgm_data()
    generate_report(gbb_summary, sttm_summary)

    fatal = [e for e in PIPELINE_ERRORS if "parse error" in e.lower()]
    if fatal:
        logging.error("FATAL: " + "; ".join(fatal))
        sys.exit(1)
    if PIPELINE_ERRORS:
        logging.warning("Non-fatal degradation: " + "; ".join(PIPELINE_ERRORS))
    logging.info("Pipeline run completed.")

if __name__ == "__main__":
    main()