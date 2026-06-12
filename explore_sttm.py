
import zipfile
import io

# 请根据你本地的真实日期修改路径
zip_path = "C:\github\East-Coast-Gas-Daily-Snapshot/data/raw/2026-06-12/sttm_raw.zip" 

try:
    with zipfile.ZipFile(zip_path, 'r') as z:
        file_names = z.namelist()
        
        # 专门寻找 Ex-Post 事后价格文件 (int657)
        expost_files = [f for f in file_names if 'INT657' in f.upper()]
        
        print("=== Peeking into Ex-Post Price Files (int657) ===")
        for pf in expost_files:
            print(f"\n--- File: {pf} ---")
            with z.open(pf) as f:
                # 只读取前 3 行原始文本，看看列名是什么
                lines = f.readlines()
                for line in lines[:3]:
                    print(line.decode('utf-8', errors='ignore').strip())
                    
except Exception as e:
    print(f"Error: {e}")