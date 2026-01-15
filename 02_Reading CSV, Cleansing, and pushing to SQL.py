import pandas as pd
from sqlalchemy import create_engine, text
import traceback

# ------------------------------
# CONFIG
# ------------------------------
csv_path = r'C:\Users\Admin\Desktop\DWBI\all_hubs_orders.csv'

username = "python_user"
password = "StrongPassword123"
host = "127.0.0.1"
port = "3306"
database = "dwbi"

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")

# ------------------------------
# DROP OLD TABLES
# ------------------------------
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS fact_sales_order"))
    conn.execute(text("DROP TABLE IF EXISTS dim_hub"))
    conn.execute(text("DROP TABLE IF EXISTS dim_warehouse_associate"))
    conn.execute(text("DROP TABLE IF EXISTS dim_delivery_associate"))

print("Old tables dropped.")

# ------------------------------
# LOAD FACT IN CHUNKS WITH CLEANSING
# ------------------------------
chunk_size = 200000
first = True

for chunk in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):

    # ------------------ DATA CLEANSING ------------------

    # normalize column names
    chunk.columns = chunk.columns.str.lower().str.replace(' ', '_')

    # remove duplicated column names
    chunk = chunk.loc[:, ~chunk.columns.duplicated()]

    # drop unwanted column if exists
    if 'hub_name' in chunk.columns:
        chunk = chunk.drop(columns=['hub_name'])

    # rename
    rename_map = {
        'city': 'hub_name',
        'expedited_delivery_ts': 'expected_delivery_ts',
        'order_amount': 'order_value'
    }
    chunk = chunk.rename(columns={k:v for k,v in rename_map.items() if k in chunk.columns})

    # lower + underscore clean again
    chunk.columns = chunk.columns.str.lower().str.replace(' ', '_')

    # remove null hub/order ids
    chunk = chunk.dropna(subset=['hub_id', 'order_id'])

    # enforce datatypes
    num_cols = ['order_quantity','order_value','distance_km']
    for col in num_cols:
        if col in chunk.columns:
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

    # timestamp columns
    time_cols = [
        'order_placed_ts','order_assigned_ts','picker_start_ts','picker_end_ts',
        'expected_delivery_ts','rider_pickup_ts','rider_drop_ts'
    ]
    for col in time_cols:
        if col in chunk.columns:
            chunk[col] = pd.to_datetime(chunk[col], errors='coerce')

    # ------------------ SELECT FACT FIELDS ------------------

    fact_fields = [
        'hub_id','hub_name','order_id','customer_id','picker_id','picker_name',
        'rider_id','rider_name','order_date','order_quantity','order_value',
        'order_placed_ts','order_assigned_ts','picker_start_ts','picker_end_ts',
        'expected_delivery_ts','rider_pickup_ts','rider_drop_ts','distance_km',
        'issue_flag'
    ]

    fact_chunk = chunk[[c for c in fact_fields if c in chunk.columns]]

    # load to MySQL
    fact_chunk.to_sql(
        'fact_sales_order',
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=5000
    )

    print("Loaded chunk to fact table")

print("Fact table finished ✔")

# CREATE DIMENSIONS FROM FACT
with engine.begin() as conn:

    # HUB
    conn.execute(text("""
        CREATE TABLE dim_hub AS
        SELECT DISTINCT hub_id,
               COALESCE(hub_name, CONCAT('Hub_', hub_id)) AS hub_name
        FROM fact_sales_order
        WHERE hub_id IS NOT NULL
    """))

    # WAREHOUSE ASSOCIATE
    conn.execute(text("""
        CREATE TABLE dim_warehouse_associate AS
        SELECT DISTINCT hub_id, picker_id,
               COALESCE(picker_name, CONCAT('Picker_', picker_id)) AS picker_name
        FROM fact_sales_order
        WHERE picker_id IS NOT NULL
    """))

    # DELIVERY ASSOCIATE
    conn.execute(text("""
        CREATE TABLE dim_delivery_associate AS
        SELECT DISTINCT hub_id, rider_id,
               COALESCE(rider_name, CONCAT('Rider_', rider_id)) AS rider_name
        FROM fact_sales_order
        WHERE rider_id IS NOT NULL
    """))

print("Dimension tables created")
print("COMPLETE ETL SUCCESS")
