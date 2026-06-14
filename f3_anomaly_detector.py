import sqlite3
import pandas as pd
import os
import numpy as np

DB_PATH = os.path.join("data", "processed", "east_coast_gas.db")

# 业务防噪底板 (Materiality Threshold): 变化量低于 50 TJ 无论如何不报警
MATERIALITY_THRESHOLD_TJ = 50

def analyze_storage(conn):
    print("\n🔍 启动 Storage 异常检测 (基于 30 天日变动速率 StdDev)...")
    
    # 提取库存数据并计算日变动量 (Drawdown/Injection Rate)
    query = """
        SELECT gas_date, facility_id, held_in_storage
        FROM storage
        ORDER BY facility_id, gas_date ASC
    """
    df = pd.read_sql(query, conn)
    if df.empty: return []

    df['gas_date'] = pd.to_datetime(df['gas_date'])
    latest_date = df['gas_date'].max()

    # 按设施计算每天的绝对变动量 (今天 - 昨天)
    # 注意：正数是注气，负数是抽气，我们看的是绝对波动速率
    df['daily_change'] = df.groupby('facility_id')['held_in_storage'].diff()
    
    # 分离目标日与历史基准
    history_df = df[df['gas_date'] < latest_date]
    today_df = df[df['gas_date'] == latest_date].copy()

    # 主判据 (自适应)：计算过去 30 天的"日变化量"的标准差和均值
    baseline = history_df.groupby('facility_id')['daily_change'].agg(
        mean_change=lambda x: x.mean(),
        std_change=lambda x: x.std(ddof=1) # 样本标准差
    ).reset_index()

    merged = pd.merge(today_df, baseline, on='facility_id', how='inner')
    
    anomalies = []
    for _, row in merged.iterrows():
        fac_id = row['facility_id']
        current_change = row['daily_change']
        mean_change = row['mean_change']
        std_change = row['std_change']
        
        # 数据不足无法算 StdDev，或没有发生变化，则跳过
        if pd.isna(current_change) or pd.isna(std_change) or pd.isna(mean_change):
            continue

        # 判断 1: 是否越过了统计主判据 (±2 倍标准差)
        upper_limit = mean_change + (2 * std_change)
        lower_limit = mean_change - (2 * std_change)
        is_stat_anomaly = (current_change > upper_limit) or (current_change < lower_limit)

        # 判断 2: 是否越过了重要性地板 (绝对变化量 > 50 TJ)
        is_material = abs(current_change) > MATERIALITY_THRESHOLD_TJ

        # 必须同时满足：统计显著 AND 业务有感
        if is_stat_anomaly and is_material:
            direction = "异常注气" if current_change > 0 else "异常抽气"
            msg = (f"[🔴 RATE ALERT] 设施 {fac_id} {direction}: 突变 {current_change:+,.0f} TJ "
                   f"(30天正常变动范围: {lower_limit:,.0f} 至 {upper_limit:,.0f} TJ, "
                   f"当前水位: {row['held_in_storage']:,.0f} TJ)")
            anomalies.append(msg)
            
    return anomalies

def analyze_flows(conn):
    print("\n🔍 启动 Flows 异常检测 (基于 30 天绝对水位 P10/P90)...")
    
    # 我们以 pipeline 的 transfer_out (流出量) 或者 production 的 supply (供应量) 为监控目标
    # 这里合并一个 'volume' 列方便统一计算
    query = """
        SELECT gas_date, facility_id, facility_type, 
               CASE 
                   WHEN facility_type = 'production' THEN supply
                   WHEN facility_type = 'lng_export' THEN transfer_out
                   WHEN facility_type = 'pipeline' THEN transfer_out
                   ELSE supply
               END as volume
        FROM flows
        ORDER BY facility_id, gas_date ASC
    """
    df = pd.read_sql(query, conn)
    if df.empty: return []

    df['gas_date'] = pd.to_datetime(df['gas_date'])
    latest_date = df['gas_date'].max()

    history_df = df[df['gas_date'] < latest_date]
    today_df = df[df['gas_date'] == latest_date].copy()

    # 主判据 (自适应)：计算过去 30 天的绝对水位 P10, P90 中位数
    baseline = history_df.groupby(['facility_id', 'facility_type'])['volume'].agg(
        p10=lambda x: x.quantile(0.10),
        p90=lambda x: x.quantile(0.90),
        median=lambda x: x.median()
    ).reset_index()

    merged = pd.merge(today_df, baseline, on=['facility_id', 'facility_type'], how='inner')
    
    anomalies = []
    for _, row in merged.iterrows():
        fac_id = row['facility_id']
        f_type = row['facility_type']
        vol = row['volume']
        p10 = row['p10']
        p90 = row['p90']
        median = row['median']

        if pd.isna(vol) or pd.isna(median):
            continue

        # 判断 1: 是否越过分位数红线
        is_high_stat = vol > p90
        is_low_stat = vol < p10

        # 判断 2: 是否越过重要性地板 (和中枢的偏离绝对值 > 50 TJ)
        is_material = abs(vol - median) > MATERIALITY_THRESHOLD_TJ

        if is_low_stat and is_material:
            msg = (f"[🔴 LOW FLOW] 设施 {fac_id} ({f_type}) 流量暴跌: {vol:,.0f} TJ "
                   f"(30天底线: {p10:,.0f} TJ, 偏离中枢 {abs(vol-median):,.0f} TJ)")
            anomalies.append(msg)
        elif is_high_stat and is_material:
            msg = (f"[🟠 HIGH FLOW] 设施 {fac_id} ({f_type}) 流量暴涨: {vol:,.0f} TJ "
                   f"(30天红线: {p90:,.0f} TJ, 偏离中枢 {abs(vol-median):,.0f} TJ)")
            anomalies.append(msg)

    return anomalies

def run_f3_pipeline():
    conn = sqlite3.connect(DB_PATH)
    
    storage_alerts = analyze_storage(conn)
    flow_alerts = analyze_flows(conn)
    
    conn.close()

    print("\n" + "="*60)
    print("🚨 F3 异常检测简报 (含业务底板过滤)")
    print("="*60)
    
    all_alerts = storage_alerts + flow_alerts
    if not all_alerts:
        print("✅ 今日全网设施运行平稳，所有波动均在统计预期或业务容忍度内。")
    else:
        for alert in all_alerts:
            print(alert)
    print("="*60)

if __name__ == "__main__":
    run_f3_pipeline()