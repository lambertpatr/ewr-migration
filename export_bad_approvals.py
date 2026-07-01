import psycopg2
import pandas as pd

conn_str = "dbname='eservice_applications' user='appuser' host='10.1.8.166' password='ewura@123'"
query = """
SELECT id, application_number, approval_no, license_type, created_at, updated_at
FROM public.applications 
WHERE approval_no ~ '^[0-9]+$'
ORDER BY approval_no::int;
"""

try:
    conn = psycopg2.connect(conn_str)
    df = pd.read_sql_query(query, conn)
    output_file = "/Users/lambert/Desktop/fast-api/ewura-migration/bad_approval_numbers.xlsx"
    df.to_excel(output_file, index=False)
    print(f"Exported {len(df)} records to {output_file}")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
