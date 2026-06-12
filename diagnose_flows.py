import pandas as pd
import os

# 根据你之前的日志，这里的路径应该是你本地今天抓取的 GBB 文件
# 如果报错找不到文件，请替换为你电脑上的绝对路径
csv_path = "C:\github\East-Coast-Gas-Daily-Snapshot/data/raw/2026-06-12/gbb_last31.csv"

def run_diagnosis():
    if not os.path.exists(csv_path):
        print(f"❌ 找不到文件: {csv_path}")
        return

    print(f"📂 读取原始数据: {csv_path}")
    df = pd.read_csv(csv_path)
    
    # 找到最新的一天
    df['GasDate_dt'] = pd.to_datetime(df['GasDate'])
    latest_date = df['GasDate_dt'].max()
    print(f"\n📅 诊断日期: {latest_date.strftime('%Y-%m-%d')}")

    # 目标管道 ID
    targets = {
        520047: "EGP (Eastern Gas Pipeline)", 
        540060: "SWQP (South West Queensland Pipeline)"
    }

    # 我们关心的核心列
    cols_to_show = ['LocationId', 'LocationName', 'Demand', 'Supply', 'TransferIn', 'TransferOut']

    for fid, fname in targets.items():
        print(f"\n{'='*50}")
        print(f"🔍 {fname} - Raw CSV Rows")
        print(f"{'='*50}")
        
        # 提取该管道当天的所有行
        facility_data = df[(df['GasDate_dt'] == latest_date) & (df['FacilityId'] == fid)]
        
        if facility_data.empty:
            print("  ⚠️ 没有找到该设施当天的任何数据。")
        else:
            # 打印明细
            print(facility_data[cols_to_show].to_string(index=False))
            
            # 打印汇总
            print("-" * 50)
            total_in = facility_data['TransferIn'].sum()
            total_out = facility_data['TransferOut'].sum()
            total_demand = facility_data['Demand'].sum()
            print(f"📊 汇总 -> TransferIn: {total_in:,.0f} | TransferOut: {total_out:,.0f} | Demand: {total_demand:,.0f}")

if __name__ == "__main__":
    run_diagnosis()