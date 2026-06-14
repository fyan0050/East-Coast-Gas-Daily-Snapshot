import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

DB_PATH = os.path.join("data", "processed", "east_coast_gas.db")

def plot_facility_histograms():
    conn = sqlite3.connect(DB_PATH)
    
    # 提取所有库存数据
    df = pd.read_sql("SELECT gas_date, facility_id, held_in_storage FROM storage ORDER BY facility_id, gas_date", conn)
    conn.close()

    if df.empty:
        print("数据库为空")
        return

    # 计算日变动量 (与我们 F3 的逻辑完全一致)
    df['daily_change'] = df.groupby('facility_id')['held_in_storage'].diff()

    # 剔除空值
    df = df.dropna(subset=['daily_change'])

    # 获取所有独特的设施 ID
    facilities = df['facility_id'].unique()

    # 为每个设施画一张直方图
    for fac_id in facilities:
        fac_data = df[df['facility_id'] == fac_id]
        
        plt.figure(figsize=(10, 6))
        # 使用 seaborn 画直方图和核密度估计曲线(KDE)
        sns.histplot(fac_data['daily_change'], bins=20, kde=True, color='skyblue')
        
        # 计算该设施的统计特征
        mean_val = fac_data['daily_change'].mean()
        std_val = fac_data['daily_change'].std()
        
        # 画出均值和 2 倍标准差的参考线
        plt.axvline(mean_val, color='black', linestyle='dashed', linewidth=1.5, label=f'Mean ({mean_val:.1f})')
        plt.axvline(mean_val + 2*std_val, color='red', linestyle='dotted', linewidth=2, label=f'+2 StdDev ({mean_val + 2*std_val:.1f})')
        plt.axvline(mean_val - 2*std_val, color='red', linestyle='dotted', linewidth=2, label=f'-2 StdDev ({mean_val - 2*std_val:.1f})')
        
        plt.title(f'设施 {fac_id} 历史 30 天日变动量分布直方图')
        plt.xlabel('每日变动量 (TJ)')
        plt.ylabel('出现天数 (频次)')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # 保存图片到本地
        os.makedirs('reports/charts', exist_ok=True)
        save_path = f'reports/charts/hist_{fac_id}.png'
        plt.savefig(save_path)
        print(f"📊 已生成设施 {fac_id} 的分布图: {save_path}")
        plt.close()

if __name__ == "__main__":
    plot_facility_histograms()