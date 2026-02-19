"""
Migrate all data from local PostgreSQL to Render PostgreSQL.
Usage: python migrate_to_render.py
Requires RENDER_DATABASE_URL in .env (External Database URL from Render).
"""
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# 1. Connection setup
# ---------------------------------------------------------------------------

# Local database (individual vars)
LOCAL_URL = "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
    user=os.getenv("POSTGRES_USER", "prediction"),
    password=os.getenv("POSTGRES_PASSWORD", "prediction123"),
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    dbname=os.getenv("POSTGRES_DB", "prediction"),
)

# Render database (External URL)
RENDER_URL = os.getenv("RENDER_DATABASE_URL")
if not RENDER_URL:
    print("[ERROR] Set RENDER_DATABASE_URL in .env with the External Database URL from Render.")
    print("  Example: RENDER_DATABASE_URL=postgresql://user:pass@dpg-xxx.oregon-postgres.render.com/dbname")
    exit(1)

if RENDER_URL.startswith("postgres://"):
    RENDER_URL = RENDER_URL.replace("postgres://", "postgresql://", 1)

print(f"  Local DB: {LOCAL_URL.split('@')[1]}")
print(f"  Render DB: {RENDER_URL.split('@')[1]}")

local_engine = create_engine(LOCAL_URL)
render_engine = create_engine(RENDER_URL)

# ---------------------------------------------------------------------------
# 2. Test connections
# ---------------------------------------------------------------------------

print("\n[1/5] Testing connections...")
try:
    with local_engine.connect() as c:
        c.execute(text("SELECT 1"))
    print("  [OK] Local database connected.")
except Exception as e:
    print(f"  [ERROR] Local database: {e}")
    exit(1)

try:
    with render_engine.connect() as c:
        c.execute(text("SELECT 1"))
    print("  [OK] Render database connected.")
except Exception as e:
    print(f"  [ERROR] Render database: {e}")
    exit(1)

# ---------------------------------------------------------------------------
# 3. Create tables on Render (using db.py schema)
# ---------------------------------------------------------------------------

print("\n[2/5] Creating tables on Render...")
import db
SCHEMA = db.SCHEMA_SQL + db._MIGRATIONS_SQL

with render_engine.connect() as conn:
    conn.execute(text(SCHEMA))
    conn.commit()
print("  [OK] Tables created on Render.")

# ---------------------------------------------------------------------------
# 4. Tables to migrate
# ---------------------------------------------------------------------------

TABLES = [
    "products",
    "orders",
    "daily_sales",
    "predictions",
    "prediction_metrics",
    "geocache",
    "low_stock_archived",
    "stock_manager",
]

print("\n[3/5] Reading data from local database...")
local_data = {}
for table in TABLES:
    try:
        df = pd.read_sql(text(f"SELECT * FROM {table}"), local_engine)
        local_data[table] = df
        print(f"  {table}: {len(df):,} rows")
    except Exception as e:
        print(f"  {table}: SKIP ({e})")
        local_data[table] = pd.DataFrame()

# ---------------------------------------------------------------------------
# 5. Write data to Render
# ---------------------------------------------------------------------------

print("\n[4/5] Writing data to Render database...")
for table in TABLES:
    df = local_data[table]
    if df.empty:
        print(f"  {table}: skipped (empty)")
        continue

    try:
        # Clear existing data first
        with render_engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
            conn.commit()

        # Insert data
        df.to_sql(table, render_engine, if_exists="append", index=False, method="multi", chunksize=500)
        print(f"  {table}: {len(df):,} rows migrated")
    except Exception as e:
        print(f"  {table}: ERROR - {e}")

# ---------------------------------------------------------------------------
# 6. Verify
# ---------------------------------------------------------------------------

print("\n[5/5] Verifying Render database...")
with render_engine.connect() as conn:
    for table in TABLES:
        try:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"  {table}: {count:,} rows")
        except Exception as e:
            print(f"  {table}: ERROR ({e})")

print("\n[DONE] Migration complete! Restart the Render Web Service to see the data.")
