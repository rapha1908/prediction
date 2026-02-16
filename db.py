"""
Modulo de acesso ao banco de dados PostgreSQL.
Gerencia conexao, criacao de tabelas, e operacoes CRUD.
"""

import os
import uuid
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONEXAO
# ============================================================

# Support Render's DATABASE_URL (single connection string) or individual vars
_DATABASE_URL = os.getenv("DATABASE_URL")

if _DATABASE_URL:
    # Render provides postgres:// but SQLAlchemy needs postgresql://
    if _DATABASE_URL.startswith("postgres://"):
        _DATABASE_URL = _DATABASE_URL.replace("postgres://", "postgresql://", 1)
    _PG_URL = _DATABASE_URL
    DB_CONFIG = None  # use connection string directly
else:
    DB_CONFIG = {
        "dbname": os.getenv("POSTGRES_DB", "prediction"),
        "user": os.getenv("POSTGRES_USER", "prediction"),
        "password": os.getenv("POSTGRES_PASSWORD", "prediction123"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }
    _PG_URL = "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**DB_CONFIG)

_engine = None


def _get_engine():
    """Retorna SQLAlchemy engine (singleton) para uso com pandas."""
    global _engine
    if _engine is None:
        _engine = create_engine(_PG_URL)
    return _engine


def get_connection():
    """Retorna uma conexao psycopg2 com o PostgreSQL (para escrita)."""
    if DB_CONFIG:
        return psycopg2.connect(**DB_CONFIG)
    return psycopg2.connect(_PG_URL)


def test_connection() -> bool:
    """Testa se a conexao com o banco esta funcionando."""
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception as e:
        print(f"  [ERRO] Nao foi possivel conectar ao PostgreSQL: {e}")
        return False


# ============================================================
# DDL - CRIACAO DE TABELAS
# ============================================================

SCHEMA_SQL = """
-- Catalogo de produtos
CREATE TABLE IF NOT EXISTS products (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    category        TEXT DEFAULT 'Sem categoria',
    price           TEXT,
    regular_price   TEXT,
    sale_price      TEXT,
    total_sales     INTEGER,
    stock_quantity  INTEGER,
    status          TEXT,
    ticket_start_date TIMESTAMP,
    ticket_end_date   TIMESTAMP,
    event_id        INTEGER,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Itens de pedido (granularidade: order_id + product_id)
CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL,
    order_date      DATE NOT NULL,
    order_time      TIMESTAMP,
    product_id      INTEGER NOT NULL,
    product_name    TEXT,
    quantity        INTEGER NOT NULL DEFAULT 0,
    total           NUMERIC(12, 2) NOT NULL DEFAULT 0,
    currency        TEXT NOT NULL DEFAULT 'USD',
    order_status    TEXT,
    billing_country TEXT,
    billing_state   TEXT,
    billing_city    TEXT,
    order_source    TEXT,
    synced_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (order_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders (order_date);
CREATE INDEX IF NOT EXISTS idx_orders_product_id ON orders (product_id);

-- Vendas diarias agregadas (materializada a partir de orders)
CREATE TABLE IF NOT EXISTS daily_sales (
    order_date      DATE NOT NULL,
    product_id      INTEGER NOT NULL,
    product_name    TEXT,
    category        TEXT,
    ticket_end_date TIMESTAMP,
    ticket_start_date TIMESTAMP,
    quantity_sold   INTEGER NOT NULL DEFAULT 0,
    revenue         NUMERIC(12, 2) NOT NULL DEFAULT 0,
    currency        TEXT NOT NULL DEFAULT 'USD',
    PRIMARY KEY (order_date, product_id, currency)
);

-- Previsoes geradas (historico completo por run)
CREATE TABLE IF NOT EXISTS predictions (
    id              SERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    run_date        TIMESTAMP NOT NULL DEFAULT NOW(),
    product_id      INTEGER NOT NULL,
    product_name    TEXT,
    category        TEXT,
    forecast_date   DATE NOT NULL,
    predicted_quantity NUMERIC(10, 2),
    yhat_lower      NUMERIC(10, 2),
    yhat_upper      NUMERIC(10, 2),
    ticket_end_date TIMESTAMP,
    method          TEXT
);

CREATE INDEX IF NOT EXISTS idx_predictions_run_id ON predictions (run_id);
CREATE INDEX IF NOT EXISTS idx_predictions_product ON predictions (product_id);

-- Metricas dos modelos (historico completo por run)
CREATE TABLE IF NOT EXISTS prediction_metrics (
    id              SERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    run_date        TIMESTAMP NOT NULL DEFAULT NOW(),
    product_id      INTEGER NOT NULL,
    product_name    TEXT,
    category        TEXT,
    mae             NUMERIC(10, 4),
    rmse            NUMERIC(10, 4),
    r2_score        NUMERIC(10, 4),
    train_size      INTEGER,
    test_size       INTEGER,
    method          TEXT,
    ticket_end_date TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metrics_run_id ON prediction_metrics (run_id);

-- Cache de geocoding (evita chamadas repetidas ao Google Maps)
CREATE TABLE IF NOT EXISTS geocache (
    location_key    TEXT PRIMARY KEY,
    lat             DOUBLE PRECISION,
    lng             DOUBLE PRECISION,
    formatted_addr  TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""


_MIGRATIONS_SQL = """
-- Add currency column to orders (if missing)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'orders' AND column_name = 'currency'
    ) THEN
        ALTER TABLE orders ADD COLUMN currency TEXT NOT NULL DEFAULT 'USD';
    END IF;
END $$;

-- Add billing location columns to orders (if missing)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'orders' AND column_name = 'billing_country'
    ) THEN
        ALTER TABLE orders ADD COLUMN billing_country TEXT;
        ALTER TABLE orders ADD COLUMN billing_state TEXT;
        ALTER TABLE orders ADD COLUMN billing_city TEXT;
    END IF;
END $$;

-- Add order_source column to orders (if missing)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'orders' AND column_name = 'order_source'
    ) THEN
        ALTER TABLE orders ADD COLUMN order_source TEXT;
    END IF;
END $$;

-- Add order_time column to orders (if missing)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'orders' AND column_name = 'order_time'
    ) THEN
        ALTER TABLE orders ADD COLUMN order_time TIMESTAMP;
    END IF;
END $$;

-- Add currency column to daily_sales (if missing)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'daily_sales' AND column_name = 'currency'
    ) THEN
        ALTER TABLE daily_sales ADD COLUMN currency TEXT NOT NULL DEFAULT 'USD';
        -- Recreate primary key to include currency
        ALTER TABLE daily_sales DROP CONSTRAINT IF EXISTS daily_sales_pkey;
        ALTER TABLE daily_sales ADD PRIMARY KEY (order_date, product_id, currency);
    END IF;
END $$;
"""


def create_tables():
    """Cria todas as tabelas se nao existirem."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            cur.execute(_MIGRATIONS_SQL)
        conn.commit()
        print("  [OK] Tabelas do banco de dados verificadas/criadas.")
    finally:
        conn.close()


# ============================================================
# PRODUTOS
# ============================================================

def upsert_products(df: pd.DataFrame):
    """Insere ou atualiza produtos no banco."""
    if df.empty:
        return 0

    conn = get_connection()
    count = 0
    try:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO products (id, name, category, price, regular_price,
                                          sale_price, total_sales, stock_quantity,
                                          status, ticket_start_date, ticket_end_date,
                                          event_id, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        category = EXCLUDED.category,
                        price = EXCLUDED.price,
                        regular_price = EXCLUDED.regular_price,
                        sale_price = EXCLUDED.sale_price,
                        total_sales = EXCLUDED.total_sales,
                        stock_quantity = EXCLUDED.stock_quantity,
                        status = EXCLUDED.status,
                        ticket_start_date = EXCLUDED.ticket_start_date,
                        ticket_end_date = EXCLUDED.ticket_end_date,
                        event_id = EXCLUDED.event_id,
                        updated_at = NOW()
                """, (
                    int(row.get("id", 0)),
                    str(row.get("name", "")),
                    str(row.get("category", "Sem categoria")),
                    str(row.get("price", "")),
                    str(row.get("regular_price", "")),
                    str(row.get("sale_price", "")),
                    int(row["total_sales"]) if pd.notna(row.get("total_sales")) else None,
                    int(row["stock_quantity"]) if pd.notna(row.get("stock_quantity")) else None,
                    str(row.get("status", "")),
                    _parse_ts(row.get("ticket_start_date")),
                    _parse_ts(row.get("ticket_end_date")),
                    int(row["event_id"]) if pd.notna(row.get("event_id")) else None,
                ))
                count += 1
        conn.commit()
    finally:
        conn.close()

    return count


def _parse_ts(val):
    """Converte um valor para timestamp ou None."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s == "nan" or s == "None":
        return None
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


# ============================================================
# PEDIDOS
# ============================================================

def get_last_sync_date():
    """Retorna a data do ultimo pedido salvo no banco."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(order_date) FROM orders")
            result = cur.fetchone()
            if result and result[0]:
                return result[0]
    finally:
        conn.close()
    return None


def insert_orders(orders_raw: list, products_df: pd.DataFrame) -> int:
    """
    Insere itens de pedido no banco a partir da resposta bruta da API.
    Retorna quantidade de linhas inseridas.
    """
    if not orders_raw:
        return 0

    # Construir mapa de nomes de produto
    name_map = {}
    if not products_df.empty and "id" in products_df.columns and "name" in products_df.columns:
        name_map = dict(zip(products_df["id"], products_df["name"]))

    seen = {}  # (order_id, product_id) -> row tuple, to avoid duplicates
    for order in orders_raw:
        order_id = order.get("id")
        order_date = order.get("date_created")
        order_status = order.get("status", "")
        order_currency = order.get("currency", "USD")
        billing = order.get("billing", {})
        b_country = billing.get("country", "") or ""
        b_state = billing.get("state", "") or ""
        b_city = billing.get("city", "") or ""

        # Extract order attribution source from meta_data
        # Priority: pys_enrich_data.pys_utm (utm_source) > pys_enrich_data.pys_source > WC attribution
        meta = order.get("meta_data", [])
        o_source = None
        if isinstance(meta, list):
            # 1) PixelYourSite enriched data (most accurate)
            for m in meta:
                if m.get("key") == "pys_enrich_data":
                    pys = m.get("value", {})
                    if isinstance(pys, dict):
                        # Parse utm_source from pys_utm string like "utm_source:facebook|utm_medium:paid|..."
                        pys_utm = pys.get("pys_utm", "")
                        if isinstance(pys_utm, str) and "utm_source:" in pys_utm:
                            for part in pys_utm.split("|"):
                                if part.startswith("utm_source:"):
                                    val = part.split(":", 1)[1].strip()
                                    if val and val != "undefined":
                                        o_source = val
                                    break
                        # Fallback to pys_source
                        if not o_source:
                            ps = pys.get("pys_source", "")
                            if isinstance(ps, str) and ps and ps != "undefined":
                                o_source = ps
                    break

            # 2) WooCommerce native attribution (fallback)
            if not o_source:
                for m in meta:
                    k = m.get("key", "")
                    v = str(m.get("value", "")).strip()
                    if k == "_wc_order_attribution_utm_source" and v and v not in ("(direct)", ""):
                        o_source = v
                        break
            if not o_source:
                for m in meta:
                    if m.get("key") == "_wc_order_attribution_source_type":
                        v = str(m.get("value", "")).strip()
                        if v:
                            o_source = v
                        break
        o_source = (o_source or "direct").strip()

        if not order_id or not order_date:
            continue

        try:
            od = pd.to_datetime(order_date).date()
        except Exception:
            continue

        # Use date_completed for the full timestamp (has actual hour)
        # Fall back to date_created if date_completed is missing
        date_completed = order.get("date_completed") or order_date
        try:
            ot = pd.to_datetime(date_completed)
        except Exception:
            ot = None

        line_items = order.get("line_items", [])
        if not isinstance(line_items, list):
            continue

        # Aggregate line items with same product_id within the same order
        items_by_pid = {}
        for item in line_items:
            pid = item.get("product_id")
            qty = item.get("quantity", 0)
            total = float(item.get("total", 0))

            if not pid or qty <= 0:
                continue

            pname = name_map.get(pid, item.get("name", "Desconhecido"))
            if pid in items_by_pid:
                items_by_pid[pid] = (
                    items_by_pid[pid][0] + qty,
                    items_by_pid[pid][1] + total,
                    pname,
                )
            else:
                items_by_pid[pid] = (qty, total, pname)

        for pid, (qty, total, pname) in items_by_pid.items():
            # Deduplicate by (order_id, product_id) – last occurrence wins
            seen[(order_id, pid)] = (
                order_id, od, ot, pid, pname,
                qty, total, order_currency, order_status,
                b_country, b_state, b_city, o_source,
            )

    rows = list(seen.values())

    if not rows:
        return 0

    conn = get_connection()
    inserted = 0
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO orders (order_id, order_date, order_time, product_id,
                                    product_name, quantity, total, currency, order_status,
                                    billing_country, billing_state, billing_city, order_source)
                VALUES %s
                ON CONFLICT (order_id, product_id) DO UPDATE SET
                    currency = EXCLUDED.currency,
                    order_time = EXCLUDED.order_time,
                    billing_country = EXCLUDED.billing_country,
                    billing_state = EXCLUDED.billing_state,
                    billing_city = EXCLUDED.billing_city,
                    order_source = EXCLUDED.order_source
            """
            execute_values(cur, sql, rows, page_size=500)
            inserted = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    return inserted


def get_order_count() -> int:
    """Retorna a quantidade total de itens de pedido no banco."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders")
            return cur.fetchone()[0]
    finally:
        conn.close()


# ============================================================
# DAILY SALES (agregacao)
# ============================================================

def refresh_daily_sales():
    """
    Recalcula a tabela daily_sales a partir de orders + products.
    Usa TRUNCATE + INSERT para garantir consistencia.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE daily_sales")
            cur.execute("""
                INSERT INTO daily_sales
                    (order_date, product_id, product_name, category,
                     ticket_end_date, ticket_start_date, quantity_sold, revenue,
                     currency)
                SELECT
                    o.order_date,
                    o.product_id,
                    COALESCE(p.name, o.product_name)    AS product_name,
                    COALESCE(p.category, 'Sem categoria') AS category,
                    p.ticket_end_date,
                    p.ticket_start_date,
                    SUM(o.quantity)                      AS quantity_sold,
                    SUM(o.total)                         AS revenue,
                    o.currency
                FROM orders o
                LEFT JOIN products p ON p.id = o.product_id
                GROUP BY o.order_date, o.product_id,
                         COALESCE(p.name, o.product_name),
                         COALESCE(p.category, 'Sem categoria'),
                         p.ticket_end_date, p.ticket_start_date,
                         o.currency
                ORDER BY o.order_date, o.product_id
            """)
            rows = cur.rowcount
        conn.commit()
        print(f"  [OK] daily_sales atualizada: {rows} registros.")
        return rows
    finally:
        conn.close()


def load_daily_sales() -> pd.DataFrame:
    """Carrega daily_sales do banco como DataFrame."""
    engine = _get_engine()
    df = pd.read_sql("""
        SELECT order_date, product_id, product_name, category,
               ticket_end_date, ticket_start_date, quantity_sold,
               revenue::float AS revenue, currency
        FROM daily_sales
        ORDER BY order_date
    """, engine, parse_dates=["order_date", "ticket_end_date", "ticket_start_date"])
    return df


def load_hourly_sales() -> pd.DataFrame:
    """Carrega vendas por hora a partir da tabela orders (usa order_time)."""
    engine = _get_engine()
    df = pd.read_sql("""
        SELECT
            EXTRACT(HOUR FROM order_time)::int AS hour,
            o.product_id,
            COALESCE(p.name, o.product_name) AS product_name,
            COALESCE(p.category, 'Sem categoria') AS category,
            p.ticket_end_date,
            p.ticket_start_date,
            SUM(o.quantity) AS quantity_sold,
            SUM(o.total::float) AS revenue,
            o.currency
        FROM orders o
        LEFT JOIN products p ON p.id = o.product_id
        WHERE o.order_time IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6, 9
        ORDER BY 1
    """, engine, parse_dates=["ticket_end_date", "ticket_start_date"])
    return df


def load_sales_by_location() -> pd.DataFrame:
    """Carrega vendas agregadas por pais/estado/cidade com info de produto."""
    engine = _get_engine()
    df = pd.read_sql("""
        SELECT
            o.billing_country AS country,
            o.billing_state   AS state,
            o.billing_city    AS city,
            o.product_id,
            COALESCE(p.name, o.product_name) AS product_name,
            COALESCE(p.category, 'Sem categoria') AS category,
            SUM(o.quantity)     AS quantity_sold,
            SUM(o.total::float) AS revenue,
            o.currency
        FROM orders o
        LEFT JOIN products p ON p.id = o.product_id
        WHERE o.billing_country IS NOT NULL
          AND o.billing_country != ''
        GROUP BY 1, 2, 3, 4, 5, 6, 9
        ORDER BY quantity_sold DESC
    """, engine)
    return df


def load_sales_by_source() -> pd.DataFrame:
    """Carrega vendas agregadas por source (canal de aquisicao)."""
    engine = _get_engine()
    try:
        df = pd.read_sql("""
            SELECT
                COALESCE(NULLIF(order_source, ''), 'Direct') AS source,
                SUM(quantity)       AS quantity_sold,
                SUM(total::float)   AS revenue,
                COUNT(DISTINCT order_id) AS order_count
            FROM orders
            GROUP BY 1
            ORDER BY quantity_sold DESC
        """, engine)
        return df
    except Exception:
        # Column may not exist yet (before first sync with new schema)
        return pd.DataFrame(columns=["source", "quantity_sold", "revenue", "order_count"])


def load_all_orders() -> pd.DataFrame:
    """Load all individual orders for the orders table display."""
    engine = _get_engine()
    try:
        df = pd.read_sql("""
            SELECT
                o.order_id,
                o.order_date,
                o.product_id,
                o.product_name,
                o.quantity,
                o.total,
                o.currency,
                o.order_status,
                o.billing_country,
                o.billing_city,
                o.order_source,
                p.category
            FROM orders o
            LEFT JOIN products p ON o.product_id = p.id
            ORDER BY o.order_date DESC, o.order_id DESC
        """, engine)
        df["order_date"] = pd.to_datetime(df["order_date"])
        return df
    except Exception as e:
        print(f"  [WARNING] Could not load orders: {e}")
        return pd.DataFrame(columns=[
            "order_id", "order_date", "product_id", "product_name",
            "quantity", "total", "currency", "order_status",
            "billing_country", "billing_city", "order_source", "category",
        ])


def load_low_stock(threshold: int = 5) -> pd.DataFrame:
    """Retorna produtos com stock_quantity < threshold (e não nulo)."""
    engine = _get_engine()
    df = pd.read_sql("""
        SELECT id AS product_id, name AS product_name, category,
               stock_quantity, status, price
        FROM products
        WHERE stock_quantity IS NOT NULL
          AND stock_quantity < %(threshold)s
        ORDER BY stock_quantity ASC, name ASC
    """, engine, params={"threshold": threshold})
    return df


# ============================================================
# GEOCODING (Google Maps API + cache)
# ============================================================

def _geocache_lookup(keys: list[str]) -> dict[str, tuple[float, float]]:
    """Busca coordenadas já cacheadas no banco."""
    if not keys:
        return {}
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT location_key, lat, lng FROM geocache WHERE location_key = ANY(%s)",
                (keys,),
            )
            return {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    finally:
        conn.close()


def _geocache_save(location_key: str, lat: float, lng: float, formatted_addr: str = ""):
    """Salva resultado de geocoding no cache."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO geocache (location_key, lat, lng, formatted_addr)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (location_key) DO UPDATE SET
                    lat = EXCLUDED.lat, lng = EXCLUDED.lng,
                    formatted_addr = EXCLUDED.formatted_addr
            """, (location_key, lat, lng, formatted_addr))
        conn.commit()
    finally:
        conn.close()


def _ensure_geocache_table():
    """Cria a tabela geocache se nao existir."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS geocache (
                    location_key    TEXT PRIMARY KEY,
                    lat             DOUBLE PRECISION,
                    lng             DOUBLE PRECISION,
                    formatted_addr  TEXT,
                    created_at      TIMESTAMP DEFAULT NOW()
                )
            """)
        conn.commit()
    finally:
        conn.close()


def load_geocache() -> dict[str, tuple[float, float]]:
    """Carrega todo o cache de geocoding do banco (leitura rapida, sem API calls)."""
    _ensure_geocache_table()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT location_key, lat, lng FROM geocache WHERE lat != 0 OR lng != 0")
            return {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    finally:
        conn.close()


def _geocode_single(args):
    """Geocode a single location (used by thread pool)."""
    import requests as _req
    key, country, state, city, api_key = args
    parts = [p for p in [city, state, country] if p]
    address = ", ".join(parts)
    try:
        resp = _req.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": address, "key": api_key},
            timeout=10,
        )
        data = resp.json()
        if data.get("status") == "OK" and data.get("results"):
            loc = data["results"][0]["geometry"]["location"]
            fmt = data["results"][0].get("formatted_address", "")
            return key, loc["lat"], loc["lng"], fmt
        return key, 0.0, 0.0, f"FAILED:{data.get('status', 'UNKNOWN')}"
    except Exception:
        return key, None, None, None


def geocode_new_orders():
    """
    Geocodifica localizacoes de pedidos que ainda nao estao no cache.
    Usa threads paralelas para velocidade. Chamado durante sync (main.py).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    _ensure_geocache_table()

    api_key = os.getenv("GOOGLE_MAPS_API_KEY", "")
    if not api_key:
        print("  [WARNING] GOOGLE_MAPS_API_KEY not set, skipping geocoding.")
        return 0

    # Get unique locations from orders that are not yet cached
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT billing_country, billing_state, billing_city
                FROM orders
                WHERE billing_country IS NOT NULL AND billing_country != ''
            """)
            all_locs = cur.fetchall()
    finally:
        conn.close()

    if not all_locs:
        return 0

    unique = {}
    for country, state, city in all_locs:
        c = str(country or "").strip()
        s = str(state or "").strip()
        ci = str(city or "").strip()
        key = f"{c}|{s}|{ci}"
        if key not in unique and c:
            unique[key] = (c, s, ci)

    # Check which ones are already cached
    cached = _geocache_lookup(list(unique.keys()))
    to_geocode = {k: v for k, v in unique.items() if k not in cached}

    if not to_geocode:
        print(f"  [Geocoding] All {len(cached)} locations already cached.")
        return 0

    total = len(to_geocode)
    print(f"  [Geocoding] {total} new locations to resolve ({len(cached)} cached)...")

    # Build task list
    tasks = [
        (key, country, state, city, api_key)
        for key, (country, state, city) in to_geocode.items()
    ]

    ok_count = 0
    fail_count = 0
    done = 0

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_geocode_single, t): t[0] for t in tasks}
        for future in as_completed(futures):
            result = future.result()
            done += 1
            if result is None or result[1] is None:
                fail_count += 1
            else:
                key, lat, lng, fmt = result
                _geocache_save(key, lat, lng, fmt or "")
                if lat != 0.0 or lng != 0.0:
                    ok_count += 1
                else:
                    fail_count += 1

            if done % 100 == 0 or done == total:
                print(f"    [{done}/{total}] OK: {ok_count} | Failed: {fail_count}")

    print(f"  [Geocoding] Done: {ok_count} resolved, {fail_count} failed out of {total}.")
    return ok_count


# ============================================================
# PREVISOES E METRICAS
# ============================================================

def generate_run_id() -> str:
    """Gera um ID unico para esta execucao."""
    return datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]


def save_predictions(df: pd.DataFrame, run_id: str):
    """Salva previsoes no banco."""
    if df.empty:
        return 0

    conn = get_connection()
    try:
        rows = []
        for _, r in df.iterrows():
            rows.append((
                run_id,
                int(r["product_id"]),
                str(r.get("product_name", "")),
                str(r.get("category", "")),
                pd.to_datetime(r["order_date"]).date(),
                float(r.get("predicted_quantity", 0)),
                float(r.get("yhat_lower", 0)),
                float(r.get("yhat_upper", 0)),
                _parse_ts(r.get("ticket_end_date")),
                str(r.get("method", "")),
            ))

        with conn.cursor() as cur:
            sql = """
                INSERT INTO predictions
                    (run_id, product_id, product_name, category, forecast_date,
                     predicted_quantity, yhat_lower, yhat_upper, ticket_end_date, method)
                VALUES %s
            """
            execute_values(cur, sql, rows, page_size=500)
            inserted = cur.rowcount
        conn.commit()
        return inserted
    finally:
        conn.close()


def save_metrics(df: pd.DataFrame, run_id: str):
    """Salva metricas no banco."""
    if df.empty:
        return 0

    conn = get_connection()
    try:
        rows = []
        for _, r in df.iterrows():
            rows.append((
                run_id,
                int(r["product_id"]),
                str(r.get("product_name", "")),
                str(r.get("category", "")),
                float(r.get("mae", 0)),
                float(r.get("rmse", 0)),
                float(r.get("r2_score", 0)),
                int(r.get("train_size", 0)),
                int(r.get("test_size", 0)),
                str(r.get("method", "")),
                _parse_ts(r.get("ticket_end_date")),
            ))

        with conn.cursor() as cur:
            sql = """
                INSERT INTO prediction_metrics
                    (run_id, product_id, product_name, category,
                     mae, rmse, r2_score, train_size, test_size,
                     method, ticket_end_date)
                VALUES %s
            """
            execute_values(cur, sql, rows, page_size=500)
            inserted = cur.rowcount
        conn.commit()
        return inserted
    finally:
        conn.close()


def get_latest_run_id() -> str | None:
    """Retorna o run_id mais recente."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT run_id FROM predictions
                ORDER BY run_date DESC LIMIT 1
            """)
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


# ============================================================
# CARREGAMENTO PARA DASHBOARD
# ============================================================

def load_for_dashboard(run_id: str | None = None):
    """
    Carrega os 3 DataFrames que o dashboard precisa.
    Se run_id nao for fornecido, usa o mais recente.
    Retorna (hist_df, pred_df, metrics_df).
    """
    if run_id is None:
        run_id = get_latest_run_id()

    if run_id is None:
        raise ValueError("Nenhuma previsao encontrada no banco.")

    engine = _get_engine()

    hist_df = pd.read_sql("""
        SELECT order_date, product_id, product_name, category,
               ticket_end_date, ticket_start_date,
               quantity_sold, revenue::float AS revenue, currency
        FROM daily_sales
        ORDER BY order_date
    """, engine, parse_dates=["order_date", "ticket_end_date", "ticket_start_date"])

    pred_df = pd.read_sql("""
        SELECT forecast_date AS order_date,
               product_id, product_name, category,
               predicted_quantity::float AS predicted_quantity,
               yhat_lower::float AS yhat_lower,
               yhat_upper::float AS yhat_upper,
               ticket_end_date, method
        FROM predictions
        WHERE run_id = %(run_id)s
        ORDER BY forecast_date
    """, engine, params={"run_id": run_id},
        parse_dates=["order_date", "ticket_end_date"])

    metrics_df = pd.read_sql("""
        SELECT product_id, product_name, category,
               mae::float AS mae, rmse::float AS rmse,
               r2_score::float AS r2_score,
               train_size, test_size, method,
               ticket_end_date
        FROM prediction_metrics
        WHERE run_id = %(run_id)s
    """, engine, params={"run_id": run_id},
        parse_dates=["ticket_end_date"])

    return hist_df, pred_df, metrics_df


def get_run_history(limit: int = 10) -> pd.DataFrame:
    """Retorna historico de execucoes de previsao."""
    engine = _get_engine()
    return pd.read_sql("""
        SELECT run_id, MIN(run_date) AS run_date,
               COUNT(DISTINCT product_id) AS n_products,
               COUNT(*) AS n_predictions
        FROM predictions
        GROUP BY run_id
        ORDER BY MIN(run_date) DESC
        LIMIT %(limit)s
    """, engine, params={"limit": limit})
