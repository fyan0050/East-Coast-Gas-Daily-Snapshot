import pandas as pd
import yaml
import sqlite3
import os
import datetime

# 强制使用跨平台安全的路径拼接方式
DB_PATH = os.path.join("data", "processed", "east_coast_gas.db")

def backfill():
    raw_base = os.path.join("data", "raw")
    
    # 自动去 raw 目录下寻找最新的一天（比如 2026-06-13 的文件夹）
    folders = sorted([f for f in os.listdir(raw_base) if os.path.isdir(os.path.join(raw_base, f))], reverse=True)
    if not folders:
        print("🚨 未找到任何原始数据文件夹，请先运行 pipeline.py")
        return

    latest_folder = folders[0]
    csv_path = os.path.join(raw_base, latest_folder, "gbb_last31.csv")

    if not os.path.exists(csv_path):
        print(f"🚨 在最新文件夹 {latest_folder} 中未找到 gbb_last31.csv")
        return

    # 读取设施配置文件
    with open("facilities.yaml", "r", encoding="utf-8") as f:
        facilities = yaml.safe_load(f)

    print(f"📦 正在读取 30 天历史数据: {csv_path}")
    df = pd.read_csv(csv_path)
    df['GasDate_dt'] = pd.to_datetime(df['GasDate'])

    storage_records = []
    flows_records = []
    
    # 统一打上回填时间戳和源文件标记
    ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    source_file = f"backfill_{latest_folder}"

    # 按业务日期进行分组，循环处理每一天的数据
    for date_dt, group in df.groupby('GasDate_dt'):
        date_str = date_dt.strftime('%Y-%m-%d')

        # 1. 提取 Storage
        for k, v in facilities.get('storage', {}).items():
            f_data = group[group['FacilityId'] == v['id']]
            if not f_data.empty:
                val = float(f_data['HeldInStorage'].sum())
                storage_records.append((date_str, v['id'], val, source_file, ingested_at))

        # 2. 提取 Production
        for k, v in facilities.get('production', {}).items():
            f_data = group[group['FacilityId'] == v['id']]
            if not f_data.empty:
                val = float(f_data['Supply'].sum())
                flows_records.append((date_str, v['id'], 'production', val, 0.0, 0.0, source_file, ingested_at))

        # 3. 提取 Demand (LNG Export)
        for k, v in facilities.get('demand', {}).items():
            f_data = group[group['FacilityId'] == v['id']]
            if not f_data.empty:
                val = float(f_data['Demand'].sum())
                flows_records.append((date_str, v['id'], 'lng_export', 0.0, 0.0, val, source_file, ingested_at))

        # 4. 提取 Pipelines
        for k, v in facilities.get('pipelines', {}).items():
            f_data = group[group['FacilityId'] == v['id']]
            if not f_data.empty:
                t_in = float(f_data['TransferIn'].sum() + f_data['Supply'].sum())
                t_out = float(f_data['TransferOut'].sum() + f_data['Demand'].sum())
                flows_records.append((date_str, v['id'], 'pipeline', 0.0, t_in, t_out, source_file, ingested_at))

    print(f"⚙️ 数据解析完毕。准备写入: 库存 {len(storage_records)} 条, 流量 {len(flows_records)} 条...")
    
    # 建立数据库连接并执行批量写入
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 使用 INSERT OR REPLACE：如果这一天的数据已经存在（比如刚才流水线跑的 12 号数据），直接覆盖刷新
    cursor.executemany('INSERT OR REPLACE INTO storage (gas_date, facility_id, held_in_storage, source_file, ingested_at) VALUES (?, ?, ?, ?, ?)', storage_records)
    cursor.executemany('INSERT OR REPLACE INTO flows (gas_date, facility_id, facility_type, supply, transfer_in, transfer_out, source_file, ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', flows_records)
    
    conn.commit()
    conn.close()

    print("✅ 回填大成功！你的数据库现在拥有了坚实的 30 天历史底座！")

if __name__ == "__main__":
    backfill()