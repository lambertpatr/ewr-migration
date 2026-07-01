import pandas as pd
import sys

file_path = "/Users/lambert/Desktop/madam-lucia/applications/Natural Gas Active Licenses as At 2026-02-19.xlsx"
df = pd.read_excel(file_path)

# Let's print the columns to see where the application number might be
print("Columns in Natural Gas:", df.columns.tolist())

# Let's look for our specific applications
targets = ['NGPO/2023/0003', 'CNGFS/2026/000001', 'CNGPR/2026/0000003']

# Search across all columns for these targets
for col in df.columns:
    matches = df[df[col].astype(str).str.contains('|'.join(targets), case=False, na=False)]
    if not matches.empty:
        print(f"\nFound matches in column '{col}':")
        # Print relevant info
        for _, row in matches.iterrows():
            print(f"App: {row[col]}, Category ID: {row.get('license_category_id', 'Not Found')}, Category Name: {row.get('license_category_name', 'Not Found')}")

