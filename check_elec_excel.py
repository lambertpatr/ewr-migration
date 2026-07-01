import pandas as pd
import glob

files = glob.glob("/Users/lambert/Desktop/madam-lucia/applications/Electricity*.xlsx")
targets = ['ACRG/2025/00002', 'ACRG/2023/00001', 'AESOL/2025/0001', 'ACRGD/2021/00014', 'PEL/2013/002', 'AEPDL/2021/0001', 'ACRG/2022/00003', 'ACRGD/2024/00001']

for f in files:
    try:
        df = pd.read_excel(f)
        print(f"\nChecking file: {f}")
        for col in df.columns:
            matches = df[df[col].astype(str).str.contains('|'.join(targets), case=False, na=False)]
            if not matches.empty:
                print(f"  Found in column '{col}':")
                for _, row in matches.iterrows():
                    print(f"    App: {row[col]}, Category ID: {row.get('license_category_id', 'Not Found')}")
    except Exception as e:
        print(f"Error reading {f}: {e}")

