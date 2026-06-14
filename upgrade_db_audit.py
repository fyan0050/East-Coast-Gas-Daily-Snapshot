import sqlite3
import os

DB_PATH = os.path.join("data", "processed", "east_coast_gas.db")

def init_audit_system():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("🛠️  正在创建数据修订审计表 (data_revisions)...")
    # 1. 创建审计表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,         -- 'storage' 或 'flows'
            gas_date TEXT NOT NULL,           -- 被修改的数据业务日期 (Event Time)
            facility_id INTEGER NOT NULL,     -- 关联的设施ID
            metric_name TEXT NOT NULL,        -- 被修改的指标名 (如 'held_in_storage', 'supply' 等)
            old_value REAL,                   -- 修改前的旧数值
            new_value REAL,                   -- 修改后的新数值
            changed_at TEXT NOT NULL          -- 审计记录发生的时间 (Processing Time)
        )
    ''')

    print("⚙️  正在为 storage 表挂载自动化修订触发器...")
    # 2. 为 storage 表创建触发器：当 held_in_storage 发生变化时自动触发
    cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS audit_storage_update
        AFTER UPDATE ON storage
        FOR EACH ROW
        WHEN OLD.held_in_storage <> NEW.held_in_storage
        BEGIN
            INSERT INTO data_revisions (table_name, gas_date, facility_id, metric_name, old_value, new_value, changed_at)
            VALUES ('storage', OLD.gas_date, OLD.facility_id, 'held_in_storage', OLD.held_in_storage, NEW.held_in_storage, datetime('now'));
        END;
    ''')

    print("⚙️  正在为 flows 表挂载自动化修订触发器...")
    # 3. 为 flows 表创建触发器：分别监控 supply, transfer_in, transfer_out 变动
    cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS audit_flows_supply_update
        AFTER UPDATE ON flows
        FOR EACH ROW
        WHEN OLD.supply <> NEW.supply
        BEGIN
            INSERT INTO data_revisions (table_name, gas_date, facility_id, metric_name, old_value, new_value, changed_at)
            VALUES ('flows', OLD.gas_date, OLD.facility_id, 'supply', OLD.supply, NEW.supply, datetime('now'));
        END;
    ''')

    cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS audit_flows_transfer_in_update
        AFTER UPDATE ON flows
        FOR EACH ROW
        WHEN OLD.transfer_in <> NEW.transfer_in
        BEGIN
            INSERT INTO data_revisions (table_name, gas_date, facility_id, metric_name, old_value, new_value, changed_at)
            VALUES ('flows', OLD.gas_date, OLD.facility_id, 'transfer_in', OLD.transfer_in, NEW.transfer_in, datetime('now'));
        END;
    ''')

    cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS audit_flows_transfer_out_update
        AFTER UPDATE ON flows
        FOR EACH ROW
        WHEN OLD.transfer_out <> NEW.transfer_out
        BEGIN
            INSERT INTO data_revisions (table_name, gas_date, facility_id, metric_name, old_value, new_value, changed_at)
            VALUES ('flows', OLD.gas_date, OLD.facility_id, 'transfer_out', OLD.transfer_out, NEW.transfer_out, datetime('now'));
        END;
    ''')
    cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS audit_flows_demand_update
        AFTER UPDATE ON flows
        FOR EACH ROW
        WHEN OLD.demand <> NEW.demand
        BEGIN
            INSERT INTO data_revisions (table_name, gas_date, facility_id, metric_name, old_value, new_value, changed_at)
            VALUES ('flows', OLD.gas_date, OLD.facility_id, 'demand', OLD.demand, NEW.demand, datetime('now'));
        END;
    ''')
    conn.commit()
    conn.close()
    print("✅ 数据库审计追踪系统部署完毕！触发器已在底层隐形实时监控。")

if __name__ == "__main__":
    init_audit_system()