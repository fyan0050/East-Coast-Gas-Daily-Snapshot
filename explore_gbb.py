import pandas as pd

# 请将这里的路径替换为你今天真实下载的 CSV 路径
csv_path = "C:\github\East-Coast-Gas-Daily-Snapshot/data/raw/2026-06-12/gbb_last31.csv" 

# 读取原始数据
df = pd.read_csv(csv_path)

# 1. 打印所有的列名：我们需要确认“日期”、“设施ID”、“实际流量”的确切拼写
print("=== Column Names ===")
print(df.columns.tolist())

# 2. 提取并打印所有不重复的设施名单：用来寻找 PRD 中要求的靶向目标
print("\n=== Unique Facilities ===")
# 尝试使用常见的列名提取，如果报错 KeyError，请根据上一步打印出的实际列名进行替换
try:
    facilities = df[['FacilityId', 'FacilityName']].drop_duplicates().sort_values('FacilityName')
    print(facilities.to_string())
except KeyError:
    print("Column names didn't match 'FacilityId' and 'FacilityName'. Please check the printed columns above.")