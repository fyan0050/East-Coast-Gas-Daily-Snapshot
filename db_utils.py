import sqlite3
import os
import logging

DB_DIR = "C:\github\East-Coast-Gas-Daily-Snapshot/data/processed"
DB_PATH = os.path.join(DB_DIR, "east_coast_gas.db")

def init_db():
    """初始化升级版 SQLite 数据库（支持审计追踪与严格分层）"""
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Prices: 增加审计列
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            gas_date TEXT,
            hub TEXT,
            price_type TEXT, 
            price REAL,
            source_file TEXT,
            ingested_at TEXT,
            PRIMARY KEY (gas_date, hub, price_type)
        )
    ''')

    # 2. Storage: 主键彻底改为 facility_id
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS storage (
            gas_date TEXT,
            facility_id INTEGER,
            held_in_storage REAL,
            source_file TEXT,
            ingested_at TEXT,
            PRIMARY KEY (gas_date, facility_id)
        )
    ''')

    # 3. Flows: 增加 facility_type 鉴别列，主键改为 facility_id
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS flows (
            gas_date TEXT,
            facility_id INTEGER,
            facility_type TEXT,
            supply REAL,
            transfer_in REAL,
            transfer_out REAL,
            source_file TEXT,
            ingested_at TEXT,
            PRIMARY KEY (gas_date, facility_id)
        )
    ''')

    conn.commit()
    conn.close()
    logging.info(f"Database schema initialized with Data Lineage fields at {DB_PATH}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()