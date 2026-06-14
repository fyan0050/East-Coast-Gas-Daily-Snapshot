import sqlite3
import os

DB_PATH = os.path.join("data", "processed", "east_coast_gas.db")

def add_column():
    print("⚙️ 正在连接数据库...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 使用 ALTER TABLE 为 flows 表新增 demand 列
        # DEFAULT 0.0 保证了历史旧数据不会变成讨厌的 NULL，而是默认填充 0
        cursor.execute("ALTER TABLE flows ADD COLUMN demand REAL DEFAULT 0.0;")
        print("✅ 大成功！已经在 flows 表中成功添加 'demand' 列。")
        
    except sqlite3.OperationalError as e:
        # 捕捉列已存在的报错，防止脚本炸毁
        if "duplicate column name" in str(e).lower():
            print("⚠️ 'demand' 列已经存在了，无需重复添加。")
        else:
            print(f"❌ 发生未知错误: {e}")
            
    conn.commit()
    conn.close()

if __name__ == "__main__":
    add_column()