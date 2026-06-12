
import zipfile
import io

# 请根据你本地的真实日期修改路径
zip_path = "C:\github\East-Coast-Gas-Daily-Snapshot/data/raw/2026-06-12/sttm_raw.zip" 

try:
    with zipfile.ZipFile(zip_path, 'r') as z:
        file_names = z.namelist()
        
        print("=== All Files inside STTM ZIP ===")
        for f in file_names:
            print(f)
            
        print("\n======================================================\n")
        
        # 筛选可能包含“价格 (PRICE)”相关的文件名
        price_files = [f for f in file_names if 'PRICE' in f.upper()]
        
        print("=== Peeking into Candidate Price Files ===")
        for pf in price_files:
            print(f"\n--- File: {pf} ---")
            with z.open(pf) as f:
                # 只读取前 3 行原始文本，看看列名是什么
                lines = f.readlines()
                for line in lines[:3]:
                    # AEMO数据有时带有特殊编码，用 errors='ignore' 防止报错
                    print(line.decode('utf-8', errors='ignore').strip())
                    
except FileNotFoundError:
    print(f"Error: Could not find the ZIP file at {zip_path}. Check your path!")
except Exception as e:
    print(f"An error occurred: {e}")