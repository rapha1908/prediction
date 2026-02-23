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
    """Retorna SQLAlchemy engine (singleton) com connection pooling."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            _PG_URL,
            pool_size=3,
            max_overflow=5,
            pool_recycle=1800,
            pool_pre_ping=True,
        )
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
CREATE INDEX IF NOT EXISTS idx_orders_date_product ON orders (order_date, product_id);

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
CREATE INDEX IF NOT EXISTS idx_predictions_forecast_date ON predictions (forecast_date);

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


_SEQUENCE_RESET_SQL = """
-- Reset SERIAL sequences to max(id) to avoid conflicts after data migration
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM orders LIMIT 1) THEN
        PERFORM setval(pg_get_serial_sequence('orders', 'id'),
                        COALESCE((SELECT MAX(id) FROM orders), 1));
    END IF;
    IF EXISTS (SELECT 1 FROM predictions LIMIT 1) THEN
        PERFORM setval(pg_get_serial_sequence('predictions', 'id'),
                        COALESCE((SELECT MAX(id) FROM predictions), 1));
    END IF;
    IF EXISTS (SELECT 1 FROM prediction_metrics LIMIT 1) THEN
        PERFORM setval(pg_get_serial_sequence('prediction_metrics', 'id'),
                        COALESCE((SELECT MAX(id) FROM prediction_metrics), 1));
    END IF;
END $$;

-- Produtos arquivados no alerta de estoque baixo
CREATE TABLE IF NOT EXISTS low_stock_archived (
    product_id      INTEGER PRIMARY KEY REFERENCES products(id),
    archived_at     TIMESTAMP DEFAULT NOW()
);

-- Gerenciamento automatico de estoque (escassez artificial)
CREATE TABLE IF NOT EXISTS stock_manager (
    product_id          INTEGER PRIMARY KEY,
    product_name        TEXT,
    total_stock         INTEGER NOT NULL DEFAULT 0,
    replenish_amount    INTEGER NOT NULL DEFAULT 20,
    low_threshold       INTEGER NOT NULL DEFAULT 5,
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- HubSpot Forms Manager: scraped events/courses
CREATE TABLE IF NOT EXISTS form_items (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    item_type       TEXT NOT NULL CHECK (item_type IN ('event', 'course')),
    first_seen_at   TIMESTAMP DEFAULT NOW(),
    last_seen_at    TIMESTAMP DEFAULT NOW(),
    active          BOOLEAN DEFAULT TRUE
);

-- HubSpot Forms Manager: which items are assigned to which forms
CREATE TABLE IF NOT EXISTS form_item_assignments (
    form_key        TEXT NOT NULL,
    item_id         INTEGER NOT NULL,
    enabled         BOOLEAN DEFAULT FALSE,
    assigned_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (form_key, item_id)
);

CREATE INDEX IF NOT EXISTS idx_fia_form_key ON form_item_assignments (form_key);
CREATE INDEX IF NOT EXISTS idx_fia_item_id ON form_item_assignments (item_id);

-- RBAC: roles
CREATE TABLE IF NOT EXISTS roles (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    description     TEXT DEFAULT '',
    created_at      TIMESTAMP DEFAULT NOW()
);

-- RBAC: users (replaces env-var based auth)
CREATE TABLE IF NOT EXISTS users (
    id              SERIAL PRIMARY KEY,
    username        TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    display_name    TEXT DEFAULT '',
    role_id         INTEGER REFERENCES roles(id) ON DELETE SET NULL,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    last_login      TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users (username);

-- RBAC: permissions granted to each role
CREATE TABLE IF NOT EXISTS role_permissions (
    role_id         INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    permission_key  TEXT NOT NULL,
    PRIMARY KEY (role_id, permission_key)
);

CREATE INDEX IF NOT EXISTS idx_rp_role ON role_permissions (role_id);

-- RBAC: per-user permission overrides (grant or deny beyond role)
CREATE TABLE IF NOT EXISTS user_permission_overrides (
    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    permission_key  TEXT NOT NULL,
    granted         BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (user_id, permission_key)
);

CREATE INDEX IF NOT EXISTS idx_upo_user ON user_permission_overrides (user_id);
"""


def create_tables():
    """Cria todas as tabelas se nao existirem."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            cur.execute(_MIGRATIONS_SQL)
            cur.execute(_SEQUENCE_RESET_SQL)
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
                    quantity = EXCLUDED.quantity,
                    total = EXCLUDED.total,
                    order_status = EXCLUDED.order_status,
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
    """Carrega vendas agregadas por source (canal de aquisicao) com categoria."""
    engine = _get_engine()
    try:
        df = pd.read_sql("""
            SELECT
                COALESCE(NULLIF(o.order_source, ''), 'Direct') AS source,
                COALESCE(p.category, 'Sem categoria')          AS category,
                SUM(o.quantity)       AS quantity_sold,
                SUM(o.total::float)   AS revenue,
                COUNT(DISTINCT o.order_id) AS order_count
            FROM orders o
            LEFT JOIN products p ON o.product_id = p.id
            GROUP BY 1, 2
            ORDER BY quantity_sold DESC
        """, engine)
        return df
    except Exception:
        return pd.DataFrame(columns=["source", "category", "quantity_sold", "revenue", "order_count"])


def load_cross_sell_data() -> pd.DataFrame:
    """
    Load product co-occurrence pairs from multi-product orders.
    Returns pairs (product_a, product_b) with frequency and revenue.
    Only considers orders with 2+ distinct products.
    """
    engine = _get_engine()
    try:
        df = pd.read_sql("""
            WITH multi_orders AS (
                SELECT order_id
                FROM orders
                WHERE order_status NOT IN ('cancelled', 'refunded', 'failed')
                GROUP BY order_id
                HAVING COUNT(DISTINCT product_id) >= 2
            )
            SELECT
                a.product_id   AS product_a_id,
                a.product_name AS product_a_name,
                b.product_id   AS product_b_id,
                b.product_name AS product_b_name,
                COALESCE(pa.category, 'Sem categoria') AS category_a,
                COALESCE(pb.category, 'Sem categoria') AS category_b,
                COUNT(DISTINCT a.order_id) AS pair_count,
                SUM(a.quantity + b.quantity) AS total_qty,
                SUM(a.total + b.total)::float AS total_revenue
            FROM orders a
            JOIN orders b ON a.order_id = b.order_id
                AND a.product_id < b.product_id
            JOIN multi_orders mo ON a.order_id = mo.order_id
            LEFT JOIN products pa ON a.product_id = pa.id
            LEFT JOIN products pb ON b.product_id = pb.id
            GROUP BY a.product_id, a.product_name,
                     b.product_id, b.product_name,
                     pa.category, pb.category
            ORDER BY pair_count DESC
        """, engine)
        return df
    except Exception as e:
        print(f"  [WARNING] Could not load cross-sell data: {e}")
        return pd.DataFrame(columns=[
            "product_a_id", "product_a_name", "product_b_id", "product_b_name",
            "category_a", "category_b", "pair_count", "total_qty", "total_revenue",
        ])


def load_multi_order_stats() -> dict:
    """Load summary stats about multi-product orders."""
    engine = _get_engine()
    try:
        row = pd.read_sql("""
            WITH order_products AS (
                SELECT order_id, COUNT(DISTINCT product_id) AS n_products
                FROM orders
                WHERE order_status NOT IN ('cancelled', 'refunded', 'failed')
                GROUP BY order_id
            )
            SELECT
                COUNT(*) AS total_orders,
                SUM(CASE WHEN n_products >= 2 THEN 1 ELSE 0 END) AS multi_orders,
                MAX(n_products) AS max_products,
                AVG(n_products)::float AS avg_products
            FROM order_products
        """, engine).iloc[0]
        return row.to_dict()
    except Exception:
        return {"total_orders": 0, "multi_orders": 0, "max_products": 0, "avg_products": 0}


def load_multi_product_orders() -> pd.DataFrame:
    """Load all orders that contain 2+ distinct products, with their line items."""
    engine = _get_engine()
    try:
        df = pd.read_sql("""
            WITH multi AS (
                SELECT order_id
                FROM orders
                WHERE order_status NOT IN ('cancelled', 'refunded', 'failed')
                GROUP BY order_id
                HAVING COUNT(DISTINCT product_id) >= 2
            )
            SELECT
                o.order_id,
                o.order_date,
                o.product_id,
                o.product_name,
                o.quantity,
                o.total::float AS total,
                o.currency,
                o.billing_country,
                o.billing_city,
                COALESCE(p.category, 'Sem categoria') AS category
            FROM orders o
            JOIN multi m ON o.order_id = m.order_id
            LEFT JOIN products p ON o.product_id = p.id
            ORDER BY o.order_date DESC, o.order_id DESC, o.product_name
        """, engine)
        df["order_date"] = pd.to_datetime(df["order_date"])
        return df
    except Exception as e:
        print(f"  [WARNING] Could not load multi-product orders: {e}")
        return pd.DataFrame(columns=[
            "order_id", "order_date", "product_id", "product_name",
            "quantity", "total", "currency", "billing_country",
            "billing_city", "category",
        ])


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
    """Retorna produtos com stock_quantity < threshold, excluindo arquivados."""
    _ensure_archived_table()
    engine = _get_engine()
    df = pd.read_sql("""
        SELECT p.id AS product_id, p.name AS product_name, p.category,
               p.stock_quantity, p.status, p.price
        FROM products p
        LEFT JOIN low_stock_archived a ON p.id = a.product_id
        WHERE p.stock_quantity IS NOT NULL
          AND p.stock_quantity < %(threshold)s
          AND a.product_id IS NULL
        ORDER BY p.stock_quantity ASC, p.name ASC
    """, engine, params={"threshold": threshold})
    return df


def load_low_stock_archived(threshold: int = 5) -> pd.DataFrame:
    """Retorna produtos arquivados que ainda tem estoque baixo."""
    _ensure_archived_table()
    engine = _get_engine()
    df = pd.read_sql("""
        SELECT p.id AS product_id, p.name AS product_name, p.category,
               p.stock_quantity, p.status, p.price, a.archived_at
        FROM products p
        INNER JOIN low_stock_archived a ON p.id = a.product_id
        WHERE p.stock_quantity IS NOT NULL
          AND p.stock_quantity < %(threshold)s
        ORDER BY a.archived_at DESC
    """, engine, params={"threshold": threshold})
    return df


def _ensure_archived_table():
    """Create the low_stock_archived table if it doesn't exist."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS low_stock_archived (
                    product_id  INTEGER PRIMARY KEY,
                    archived_at TIMESTAMP DEFAULT NOW()
                )
            """)
        conn.commit()
    finally:
        conn.close()


def archive_low_stock(product_id: int):
    """Arquiva um produto do alerta de estoque baixo."""
    _ensure_archived_table()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO low_stock_archived (product_id)
                VALUES (%s)
                ON CONFLICT (product_id) DO NOTHING
            """, (product_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def unarchive_low_stock(product_id: int):
    """Desarquiva um produto do alerta de estoque baixo."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM low_stock_archived WHERE product_id = %s", (product_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============================================================
# STOCK MANAGER (automatic scarcity replenishment)
# ============================================================

def _ensure_stock_manager_table():
    """Create the stock_manager table if it doesn't exist."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_manager (
                    product_id       INTEGER PRIMARY KEY,
                    product_name     TEXT,
                    total_stock      INTEGER NOT NULL DEFAULT 0,
                    replenish_amount INTEGER NOT NULL DEFAULT 20,
                    low_threshold    INTEGER NOT NULL DEFAULT 5,
                    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at       TIMESTAMP DEFAULT NOW(),
                    updated_at       TIMESTAMP DEFAULT NOW()
                )
            """)
        conn.commit()
    finally:
        conn.close()


def load_stock_manager() -> pd.DataFrame:
    """Load all products managed by the stock manager."""
    _ensure_stock_manager_table()
    engine = _get_engine()
    df = pd.read_sql("""
        SELECT sm.product_id, sm.product_name, sm.total_stock,
               sm.replenish_amount, sm.low_threshold, sm.enabled,
               sm.created_at, sm.updated_at,
               p.stock_quantity AS current_wc_stock,
               p.total_sales
        FROM stock_manager sm
        LEFT JOIN products p ON sm.product_id = p.id
        ORDER BY sm.enabled DESC, sm.product_name ASC
    """, engine)
    return df


def add_stock_manager(product_id: int, product_name: str, total_stock: int,
                      replenish_amount: int = 20, low_threshold: int = 5):
    """Add a product to the stock manager."""
    _ensure_stock_manager_table()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stock_manager (product_id, product_name, total_stock,
                                           replenish_amount, low_threshold)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    total_stock = EXCLUDED.total_stock,
                    replenish_amount = EXCLUDED.replenish_amount,
                    low_threshold = EXCLUDED.low_threshold,
                    updated_at = NOW()
            """, (product_id, product_name, total_stock, replenish_amount, low_threshold))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_stock_manager(product_id: int, **kwargs):
    """Update specific fields of a stock manager entry."""
    _ensure_stock_manager_table()
    allowed = {"total_stock", "replenish_amount", "low_threshold", "enabled"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [product_id]
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE stock_manager SET {set_clause}, updated_at = NOW() WHERE product_id = %s",
                values,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def remove_stock_manager(product_id: int):
    """Remove a product from the stock manager."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM stock_manager WHERE product_id = %s", (product_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_products_for_stock_picker() -> pd.DataFrame:
    """Load products available to add to stock manager (not already managed)."""
    _ensure_stock_manager_table()
    engine = _get_engine()
    df = pd.read_sql("""
        SELECT p.id AS product_id, p.name AS product_name,
               COALESCE(p.stock_quantity, 0) AS stock_quantity,
               COALESCE(p.total_sales, 0) AS total_sales,
               p.category, p.status
        FROM products p
        LEFT JOIN stock_manager sm ON p.id = sm.product_id
        WHERE sm.product_id IS NULL
        ORDER BY p.name ASC
    """, engine)
    return df


def wc_get_stock(product_id: int) -> dict | None:
    """Fetch live stock info from WooCommerce for a single product.
    Returns dict with stock_quantity and total_sales, or None on failure."""
    import requests as _req
    wc_url = os.getenv("WOOCOMMERCE_URL", "https://tcche.org/wp-json/wc/v3/")
    wc_key = os.getenv("WOOCOMMERCE_KEY", "")
    wc_secret = os.getenv("WOOCOMMERCE_SECRET", "")
    if not wc_key or not wc_secret:
        return None
    try:
        resp = _req.get(
            f"{wc_url}products/{product_id}",
            params={"consumer_key": wc_key, "consumer_secret": wc_secret},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            stock = data.get("stock_quantity") or 0
            sold = data.get("total_sales") or 0
            # Sync local DB
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE products SET stock_quantity = %s, total_sales = %s, updated_at = NOW() WHERE id = %s",
                        (stock, sold, product_id),
                    )
                conn.commit()
            finally:
                conn.close()
            return {"stock_quantity": int(stock), "total_sales": int(sold)}
    except Exception as e:
        print(f"  [WC] Could not fetch stock for {product_id}: {e}")
    return None


def wc_get_stock_bulk(product_ids: list[int]) -> dict[int, dict]:
    """Fetch live stock for multiple products. Returns {pid: {stock_quantity, total_sales}}."""
    import requests as _req
    wc_url = os.getenv("WOOCOMMERCE_URL", "https://tcche.org/wp-json/wc/v3/")
    wc_key = os.getenv("WOOCOMMERCE_KEY", "")
    wc_secret = os.getenv("WOOCOMMERCE_SECRET", "")
    if not wc_key or not wc_secret or not product_ids:
        return {}
    result = {}
    # WC API supports fetching multiple products with include parameter (max 100 per page)
    try:
        ids_str = ",".join(str(p) for p in product_ids)
        resp = _req.get(
            f"{wc_url}products",
            params={
                "consumer_key": wc_key, "consumer_secret": wc_secret,
                "include": ids_str, "per_page": 100,
            },
            timeout=15,
        )
        if resp.status_code == 200:
            updates = []
            for p in resp.json():
                pid = p["id"]
                stock = p.get("stock_quantity") or 0
                sold = p.get("total_sales") or 0
                result[pid] = {"stock_quantity": int(stock), "total_sales": int(sold)}
                updates.append((int(stock), int(sold), pid))
            # Bulk sync local DB
            if updates:
                conn = get_connection()
                try:
                    with conn.cursor() as cur:
                        for stock_val, sold_val, pid_val in updates:
                            cur.execute(
                                "UPDATE products SET stock_quantity = %s, total_sales = %s, updated_at = NOW() WHERE id = %s",
                                (stock_val, sold_val, pid_val),
                            )
                    conn.commit()
                finally:
                    conn.close()
            print(f"  [WC] Synced live stock for {len(result)} products.")
    except Exception as e:
        print(f"  [WC] Bulk stock fetch failed: {e}")
    return result


def wc_update_stock(product_id: int, new_quantity: int) -> bool:
    """Update stock quantity on WooCommerce via REST API.
    Handles Tribe Tickets products by also updating _tribe_ticket_capacity.
    Returns True on success, False on failure."""
    import requests as _req
    wc_url = os.getenv("WOOCOMMERCE_URL", "https://tcche.org/wp-json/wc/v3/")
    wc_key = os.getenv("WOOCOMMERCE_KEY", "")
    wc_secret = os.getenv("WOOCOMMERCE_SECRET", "")
    if not wc_key or not wc_secret:
        print(f"  [ERROR] WooCommerce credentials not configured.")
        return False
    auth = (wc_key, wc_secret)
    try:
        url = f"{wc_url}products/{product_id}"

        # First GET to check if it's a Tribe Ticket product
        get_resp = _req.get(url, auth=auth, timeout=10)
        if get_resp.status_code != 200:
            print(f"  [ERROR] WC GET failed for {product_id}: HTTP {get_resp.status_code}")
            return False

        product_data = get_resp.json()
        meta = {m["key"]: m["value"] for m in product_data.get("meta_data", [])}
        total_sold = int(product_data.get("total_sales", 0) or 0)
        is_tribe = "_tribe_ticket_capacity" in meta

        # Build update payload
        payload = {
            "manage_stock": True,
            "stock_quantity": new_quantity,
        }
        if is_tribe:
            # Tribe Tickets: capacity must be >= stock + sold
            new_capacity = new_quantity + total_sold
            payload["meta_data"] = [
                {"key": "_tribe_ticket_capacity", "value": str(new_capacity)},
            ]
            print(f"  [WC] Tribe product {product_id}: setting stock={new_quantity}, capacity={new_capacity}")

        resp = _req.put(url, json=payload, auth=auth, timeout=15)
        if resp.status_code == 200:
            result_stock = resp.json().get("stock_quantity")
            if result_stock != new_quantity:
                print(f"  [WARNING] WC returned stock={result_stock}, expected {new_quantity}")
            # Update local DB
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE products SET stock_quantity = %s, updated_at = NOW() WHERE id = %s",
                        (new_quantity, product_id),
                    )
                conn.commit()
            finally:
                conn.close()
            return result_stock == new_quantity
        else:
            print(f"  [ERROR] WC stock update failed for {product_id}: HTTP {resp.status_code}")
            return False
    except Exception as e:
        print(f"  [ERROR] WC stock update failed for {product_id}: {e}")
        return False


def auto_replenish_stock() -> list[dict]:
    """Check all enabled stock_manager products and replenish if needed.
    Fetches LIVE stock from WooCommerce before deciding.
    Returns list of actions taken."""
    _ensure_stock_manager_table()
    engine = _get_engine()
    df = pd.read_sql("""
        SELECT sm.product_id, sm.product_name, sm.total_stock,
               sm.replenish_amount, sm.low_threshold
        FROM stock_manager sm
        WHERE sm.enabled = TRUE
    """, engine)

    if df.empty:
        return []

    # Fetch live stock from WooCommerce for all managed products
    pids = df["product_id"].astype(int).tolist()
    live_stock = wc_get_stock_bulk(pids)
    print(f"  [REPLENISH] Fetched live stock for {len(live_stock)}/{len(pids)} products.")

    actions = []
    for _, row in df.iterrows():
        pid = int(row["product_id"])
        total = int(row["total_stock"])
        replenish = int(row["replenish_amount"])
        threshold = int(row["low_threshold"])

        # Use live WC data if available, otherwise fall back to local DB
        if pid in live_stock:
            current = live_stock[pid]["stock_quantity"]
            sold = live_stock[pid]["total_sales"]
        else:
            # Fallback: read from local DB
            local = pd.read_sql(
                "SELECT COALESCE(stock_quantity, 0) AS sq, COALESCE(total_sales, 0) AS ts FROM products WHERE id = %(pid)s",
                engine, params={"pid": pid},
            )
            current = int(local["sq"].iloc[0]) if not local.empty else 0
            sold = int(local["ts"].iloc[0]) if not local.empty else 0

        remaining = max(0, total - sold)

        if current <= threshold and remaining > 0:
            add_qty = min(replenish, remaining - current)
            if add_qty > 0:
                new_stock = current + add_qty
                success = wc_update_stock(pid, new_stock)
                actions.append({
                    "product_id": pid,
                    "product_name": row["product_name"],
                    "old_stock": current,
                    "new_stock": new_stock,
                    "added": add_qty,
                    "remaining": remaining - new_stock + current,
                    "success": success,
                })
                print(f"  [REPLENISH] {row['product_name']}: {current} -> {new_stock} (+{add_qty})"
                      f" | sold={sold}, remaining={remaining} | {'OK' if success else 'FAILED'}")
            else:
                print(f"  [REPLENISH] {row['product_name']}: stock={current}, threshold={threshold}"
                      f" -> no room to add (remaining={remaining})")
        else:
            reason = "stock OK" if current > threshold else "sold out"
            print(f"  [REPLENISH] {row['product_name']}: stock={current}, threshold={threshold} -> {reason}")

    return actions


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


# ============================================================
# HUBSPOT FORMS MANAGER
# ============================================================

def _ensure_form_items_tables():
    """Create form_items and form_item_assignments tables if missing."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS form_items (
                    id            SERIAL PRIMARY KEY,
                    name          TEXT NOT NULL UNIQUE,
                    item_type     TEXT NOT NULL CHECK (item_type IN ('event', 'course')),
                    first_seen_at TIMESTAMP DEFAULT NOW(),
                    last_seen_at  TIMESTAMP DEFAULT NOW(),
                    active        BOOLEAN DEFAULT TRUE
                );
                CREATE TABLE IF NOT EXISTS form_item_assignments (
                    form_key  TEXT NOT NULL,
                    item_id   INTEGER NOT NULL,
                    enabled   BOOLEAN DEFAULT FALSE,
                    assigned_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (form_key, item_id)
                );
            """)
        conn.commit()
    finally:
        conn.close()


def upsert_form_items(items: list[dict]) -> tuple[int, int]:
    """
    Insert or update scraped items.
    items: list of {"name": str, "item_type": "event"|"course"}
    Returns (new_count, updated_count).
    """
    _ensure_form_items_tables()
    conn = get_connection()
    new_count = 0
    updated_count = 0
    try:
        with conn.cursor() as cur:
            for item in items:
                cur.execute("""
                    INSERT INTO form_items (name, item_type)
                    VALUES (%s, %s)
                    ON CONFLICT (name) DO UPDATE SET
                        last_seen_at = NOW(),
                        active = TRUE
                    RETURNING (xmax = 0) AS is_new
                """, (item["name"], item["item_type"]))
                row = cur.fetchone()
                if row and row[0]:
                    new_count += 1
                else:
                    updated_count += 1
        conn.commit()
    finally:
        conn.close()
    return new_count, updated_count


def load_form_items() -> pd.DataFrame:
    """Load all form items (events and courses)."""
    _ensure_form_items_tables()
    engine = _get_engine()
    return pd.read_sql("""
        SELECT id, name, item_type, first_seen_at, last_seen_at, active
        FROM form_items
        ORDER BY item_type, name
    """, engine)


def load_form_assignments() -> pd.DataFrame:
    """Load all form item assignments."""
    _ensure_form_items_tables()
    engine = _get_engine()
    return pd.read_sql("""
        SELECT fa.form_key, fa.item_id, fa.enabled,
               fi.name AS item_name, fi.item_type
        FROM form_item_assignments fa
        JOIN form_items fi ON fi.id = fa.item_id
        ORDER BY fa.form_key, fi.item_type, fi.name
    """, engine)


def ensure_assignments_for_all_forms(form_keys: list[str]):
    """
    Make sure every form_items row has an assignment row for each form_key.
    New assignments default to enabled=FALSE.
    """
    _ensure_form_items_tables()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for fk in form_keys:
                cur.execute("""
                    INSERT INTO form_item_assignments (form_key, item_id, enabled)
                    SELECT %s, fi.id, FALSE
                    FROM form_items fi
                    WHERE fi.id NOT IN (
                        SELECT item_id FROM form_item_assignments WHERE form_key = %s
                    )
                """, (fk, fk))
        conn.commit()
    finally:
        conn.close()


def set_assignment_enabled(form_key: str, item_id: int, enabled: bool):
    """Toggle an assignment on or off."""
    _ensure_form_items_tables()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO form_item_assignments (form_key, item_id, enabled)
                VALUES (%s, %s, %s)
                ON CONFLICT (form_key, item_id) DO UPDATE SET enabled = EXCLUDED.enabled
            """, (form_key, item_id, enabled))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_enabled_items_for_form(form_key: str) -> pd.DataFrame:
    """Get all enabled items for a specific form."""
    _ensure_form_items_tables()
    engine = _get_engine()
    return pd.read_sql("""
        SELECT fi.id, fi.name, fi.item_type
        FROM form_items fi
        JOIN form_item_assignments fa ON fa.item_id = fi.id
        WHERE fa.form_key = %(fk)s
          AND fa.enabled = TRUE
          AND fi.active = TRUE
        ORDER BY fi.item_type, fi.name
    """, engine, params={"fk": form_key})


def deactivate_missing_items(current_names: list[str]):
    """
    Mark items as inactive if they are no longer found on the website.
    Does NOT delete them so assignments are preserved.
    """
    if not current_names:
        return
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE form_items SET active = FALSE
                WHERE name NOT IN %s AND active = TRUE
            """, (tuple(current_names),))
            deactivated = cur.rowcount
        conn.commit()
        if deactivated:
            print(f"  [FORMS DB] Deactivated {deactivated} items no longer on website.")
    finally:
        conn.close()


def sync_assignments_from_hubspot(hubspot_state: dict[str, dict[str, list[str]]]):
    """
    Sync assignments from HubSpot's current state.
    hubspot_state: {form_key: {"events": [names], "courses": [names]}}
    For each form, enables items that are currently in HubSpot and
    disables items that are not.
    Only updates items that exist in form_items table.
    Returns count of assignments set.
    """
    _ensure_form_items_tables()
    conn = get_connection()
    count = 0
    try:
        with conn.cursor() as cur:
            # Load all items into a name -> (id, item_type) map
            cur.execute("SELECT id, name, item_type FROM form_items")
            item_map = {}
            for row in cur.fetchall():
                item_map[row[1]] = (row[0], row[2])  # name -> (id, type)

            for form_key, data in hubspot_state.items():
                event_names = set(data.get("events", []))
                course_names = set(data.get("courses", []))

                for item_name, (item_id, item_type) in item_map.items():
                    # Determine if this item is currently enabled in HubSpot
                    if item_type == "event":
                        enabled = item_name in event_names
                    else:
                        enabled = item_name in course_names

                    cur.execute("""
                        INSERT INTO form_item_assignments (form_key, item_id, enabled)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (form_key, item_id) DO UPDATE SET enabled = EXCLUDED.enabled
                    """, (form_key, item_id, enabled))
                    count += 1

        conn.commit()
        print(f"  [FORMS DB] Synced {count} assignments from HubSpot state.")
    finally:
        conn.close()
    return count


def has_any_assignments() -> bool:
    """Check if there are any assignments already in the DB."""
    _ensure_form_items_tables()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM form_item_assignments LIMIT 1)")
            return cur.fetchone()[0]
    finally:
        conn.close()


# ============================================================
# RBAC: ROLES, USERS, PERMISSIONS
# ============================================================

ALL_PERMISSIONS = [
    # Pages
    ("page:dashboard",       "Dashboard",        "page"),
    ("page:stock",           "Stock Manager",     "page"),
    ("page:forms",           "Forms Manager",     "page"),
    ("page:crosssell",       "Cross-Sell",        "page"),
    ("page:settings",        "Settings",          "page"),
    # Features
    ("feature:sync",             "Sync & Retrain",        "feature"),
    ("feature:chat",             "AI Chat",               "feature"),
    ("feature:report",           "Generate Report",       "feature"),
    ("feature:stock_replenish",  "Replenish Stock",       "feature"),
    ("feature:forms_push",       "Push to HubSpot",       "feature"),
    ("feature:order_bumps",      "Order Bumps",           "feature"),
]

ALL_PERMISSION_KEYS = [p[0] for p in ALL_PERMISSIONS]

DEFAULT_ROLES = {
    "admin": {
        "description": "Full access to everything",
        "permissions": ALL_PERMISSION_KEYS,
    },
    "manager": {
        "description": "All pages and features except Settings",
        "permissions": [k for k in ALL_PERMISSION_KEYS if k != "page:settings"],
    },
    "viewer": {
        "description": "Dashboard read-only, no destructive actions",
        "permissions": [
            "page:dashboard",
            "feature:chat",
            "feature:report",
        ],
    },
}


# --- Roles CRUD ---

def list_roles() -> list[dict]:
    """Return all roles with their permissions."""
    engine = _get_engine()
    roles_df = pd.read_sql("SELECT id, name, description, created_at FROM roles ORDER BY id", engine)
    perms_df = pd.read_sql("SELECT role_id, permission_key FROM role_permissions", engine)
    perms_map: dict[int, list[str]] = {}
    for _, row in perms_df.iterrows():
        perms_map.setdefault(int(row["role_id"]), []).append(row["permission_key"])
    result = []
    for _, r in roles_df.iterrows():
        result.append({
            "id": int(r["id"]),
            "name": r["name"],
            "description": r["description"] or "",
            "permissions": sorted(perms_map.get(int(r["id"]), [])),
            "created_at": str(r["created_at"]),
        })
    return result


def create_role(name: str, description: str = "", permissions: list[str] | None = None) -> int:
    """Create a role and return its id."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO roles (name, description) VALUES (%s, %s) RETURNING id",
                (name.strip(), description.strip()),
            )
            role_id = cur.fetchone()[0]
            if permissions:
                for pk in permissions:
                    if pk in ALL_PERMISSION_KEYS:
                        cur.execute(
                            "INSERT INTO role_permissions (role_id, permission_key) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (role_id, pk),
                        )
        conn.commit()
        return role_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_role(role_id: int, name: str | None = None, description: str | None = None):
    """Update role name and/or description."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if name is not None:
                cur.execute("UPDATE roles SET name = %s WHERE id = %s", (name.strip(), role_id))
            if description is not None:
                cur.execute("UPDATE roles SET description = %s WHERE id = %s", (description.strip(), role_id))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def delete_role(role_id: int):
    """Delete a role (cascades to role_permissions). Users with this role get role_id=NULL."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM roles WHERE id = %s", (role_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def set_role_permissions(role_id: int, permission_keys: list[str]):
    """Replace all permissions for a role."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM role_permissions WHERE role_id = %s", (role_id,))
            for pk in permission_keys:
                if pk in ALL_PERMISSION_KEYS:
                    cur.execute(
                        "INSERT INTO role_permissions (role_id, permission_key) VALUES (%s, %s)",
                        (role_id, pk),
                    )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --- Users CRUD ---

def list_users() -> list[dict]:
    """Return all users (without password hashes)."""
    engine = _get_engine()
    df = pd.read_sql("""
        SELECT u.id, u.username, u.display_name, u.role_id,
               COALESCE(r.name, '') AS role_name,
               u.is_active, u.created_at, u.last_login
        FROM users u
        LEFT JOIN roles r ON u.role_id = r.id
        ORDER BY u.id
    """, engine)
    result = []
    for _, row in df.iterrows():
        result.append({
            "id": int(row["id"]),
            "username": row["username"],
            "display_name": row["display_name"] or "",
            "role_id": int(row["role_id"]) if pd.notna(row["role_id"]) else None,
            "role_name": row["role_name"],
            "is_active": bool(row["is_active"]),
            "created_at": str(row["created_at"]),
            "last_login": str(row["last_login"]) if pd.notna(row["last_login"]) else None,
        })
    return result


def create_user(username: str, password_hash: str, display_name: str = "",
                role_id: int | None = None) -> int:
    """Create a user and return the new id."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (username, password_hash, display_name, role_id)
                VALUES (%s, %s, %s, %s) RETURNING id
            """, (username.strip().lower(), password_hash, display_name.strip(), role_id))
            uid = cur.fetchone()[0]
        conn.commit()
        return uid
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_user(user_id: int, **kwargs):
    """Update user fields. Allowed: display_name, role_id, is_active, password_hash."""
    allowed = {"display_name", "role_id", "is_active", "password_hash"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [user_id]
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE users SET {set_clause} WHERE id = %s", values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def delete_user(user_id: int):
    """Delete a user (cascades to permission overrides)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_last_login(username: str):
    """Set last_login to now for a user."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET last_login = NOW() WHERE username = %s", (username,))
        conn.commit()
    finally:
        conn.close()


# --- User lookup for auth ---

def load_user_by_username(username: str) -> dict | None:
    """Load a single user by username. Returns dict or None."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id, u.username, u.password_hash, u.display_name,
                       u.role_id, COALESCE(r.name, '') AS role_name, u.is_active
                FROM users u
                LEFT JOIN roles r ON u.role_id = r.id
                WHERE u.username = %s
            """, (username.strip().lower(),))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "username": row[1],
                "password_hash": row[2],
                "display_name": row[3] or "",
                "role_id": row[4],
                "role_name": row[5],
                "is_active": row[6],
            }
    finally:
        conn.close()


def get_user_permissions(user_id: int) -> set[str]:
    """
    Compute effective permissions for a user:
    Start with role permissions, then apply per-user overrides (grant/deny).
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Role permissions
            cur.execute("""
                SELECT rp.permission_key
                FROM role_permissions rp
                JOIN users u ON u.role_id = rp.role_id
                WHERE u.id = %s
            """, (user_id,))
            perms = {row[0] for row in cur.fetchall()}

            # Per-user overrides
            cur.execute(
                "SELECT permission_key, granted FROM user_permission_overrides WHERE user_id = %s",
                (user_id,),
            )
            for pk, granted in cur.fetchall():
                if granted:
                    perms.add(pk)
                else:
                    perms.discard(pk)
            return perms
    finally:
        conn.close()


def get_user_overrides(user_id: int) -> list[dict]:
    """Get per-user permission overrides."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT permission_key, granted FROM user_permission_overrides WHERE user_id = %s ORDER BY permission_key",
                (user_id,),
            )
            return [{"permission_key": r[0], "granted": r[1]} for r in cur.fetchall()]
    finally:
        conn.close()


def set_user_overrides(user_id: int, overrides: list[dict]):
    """
    Replace all overrides for a user.
    overrides: list of {"permission_key": str, "granted": bool}
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM user_permission_overrides WHERE user_id = %s", (user_id,))
            for ov in overrides:
                pk = ov.get("permission_key", "")
                if pk in ALL_PERMISSION_KEYS:
                    cur.execute(
                        "INSERT INTO user_permission_overrides (user_id, permission_key, granted) VALUES (%s, %s, %s)",
                        (user_id, pk, bool(ov.get("granted", True))),
                    )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def user_count() -> int:
    """Return the number of users in the DB."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users")
            return cur.fetchone()[0]
    except Exception:
        return 0
    finally:
        conn.close()


def _ensure_new_permissions():
    """Add any new permission keys to existing roles (idempotent migration)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for role_name, role_def in DEFAULT_ROLES.items():
                cur.execute("SELECT id FROM roles WHERE name = %s", (role_name,))
                row = cur.fetchone()
                if row:
                    rid = row[0]
                    for pk in role_def["permissions"]:
                        cur.execute(
                            "INSERT INTO role_permissions (role_id, permission_key) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (rid, pk),
                        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()


def seed_default_roles_and_users():
    """
    Seed default roles and migrate users from DASHBOARD_USERS env var on first run.
    Only runs when the users table is empty.
    """
    import bcrypt as _bcrypt

    if user_count() > 0:
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # 1. Create default roles
            role_ids = {}
            for role_name, role_def in DEFAULT_ROLES.items():
                cur.execute(
                    "INSERT INTO roles (name, description) VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                    (role_name, role_def["description"]),
                )
                rid = cur.fetchone()[0]
                role_ids[role_name] = rid
                for pk in role_def["permissions"]:
                    cur.execute(
                        "INSERT INTO role_permissions (role_id, permission_key) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (rid, pk),
                    )

            # 2. Migrate users from env var
            import json as _json
            admin_role_id = role_ids.get("admin", 1)
            users_json = os.getenv("DASHBOARD_USERS")
            raw_users = {}
            if users_json:
                try:
                    raw_users = _json.loads(users_json)
                except (ValueError, TypeError):
                    pass

            if not raw_users:
                default_pass = os.getenv("DASHBOARD_PASSWORD", "tcche2025")
                raw_users = {"admin": default_pass}

            for uname, pwd in raw_users.items():
                uname = uname.strip().lower()
                if pwd.startswith("$2b$"):
                    pw_hash = pwd
                else:
                    pw_hash = _bcrypt.hashpw(pwd.encode("utf-8"), _bcrypt.gensalt()).decode("utf-8")
                cur.execute("""
                    INSERT INTO users (username, password_hash, display_name, role_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (username) DO NOTHING
                """, (uname, pw_hash, uname.capitalize(), admin_role_id))

        conn.commit()
        print(f"  [OK] Seeded {len(DEFAULT_ROLES)} roles and {len(raw_users)} users from env vars.")
    except Exception as e:
        conn.rollback()
        print(f"  [WARNING] Could not seed users/roles: {e}")
    finally:
        conn.close()

    # Always sync new permissions to existing default roles
    _ensure_new_permissions()
