import os
import pandas as pd
from sqlalchemy import create_engine

# Database Connection (Internal Docker Connection)
host     = os.getenv("POSTGRES_HOST", "postgres")
port     = os.getenv("POSTGRES_PORT", "5432")
db       = os.getenv("POSTGRES_DB", "de_poc")
user     = os.getenv("POSTGRES_USER", "de_user")
password = os.getenv("POSTGRES_PASSWORD", "de_password123")
conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

engine = create_engine(conn_str)

# Save to the mounted data folder so Windows can see it
output_dir = "/opt/airflow/data/powerbi_data"
os.makedirs(output_dir, exist_ok=True)

print("Exporting Gold Layer tables for Power BI...")

tables_to_export = [
    "fact_sales",
    "dim_customer",
    "dim_seller",
    "dim_product",
    "dim_date"
]

for table in tables_to_export:
    print(f"Exporting {table}...")
    try:
        df = pd.read_sql(f"SELECT * FROM gold.{table}", engine)
        csv_path = os.path.join(output_dir, f"{table}.csv")
        df.to_csv(csv_path, index=False)
        print(f"  -> Saved {len(df)} rows to {csv_path}")
    except Exception as e:
        print(f"  -> Error exporting {table}: {e}")

print(f"\nAll done! CSV files are saved in the data folder")
