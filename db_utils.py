import sqlite3
import os
import logging

DB_DIR = "data/processed"
DB_PATH = os.path.join(DB_DIR, "east_coast_gas.db")

def init_db():
    """初始化 SQLite 数据库和表结构"""
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Prices (价格表)
    # 联合主键: 确保同一天、同一个Hub、同一种价格类型只有一条记录
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            gas_date TEXT,
            hub TEXT,
            price_type TEXT, 
            price REAL,
            PRIMARY KEY (gas_date, hub, price_type)
        )
    ''')

    # 2. Storage (储气表)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS storage (
            gas_date TEXT,
            facility_name TEXT,
            held_in_storage REAL,
            PRIMARY KEY (gas_date, facility_name)
        )
    ''')

    # 3. Flows (流量表)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS flows (
            gas_date TEXT,
            facility_name TEXT,
            supply REAL,
            transfer_in REAL,
            transfer_out REAL,
            PRIMARY KEY (gas_date, facility_name)
        )
    ''')

    conn.commit()
    conn.close()
    logging.info(f"Database initialized at {DB_PATH}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()