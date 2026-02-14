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
    return psycopg2.connect(**DB_CONFIG)


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
    product_id      INTEGER NOT NULL,
    product_name    TEXT,
    quantity        INTEGER NOT NULL DEFAULT 0,
    total           NUMERIC(12, 2) NOT NULL DEFAULT 0,
    currency        TEXT NOT NULL DEFAULT 'USD',
    order_status    TEXT,
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

    rows = []
    for order in orders_raw:
        order_id = order.get("id")
        order_date = order.get("date_created")
        order_status = order.get("status", "")
        order_currency = order.get("currency", "USD")
        if not order_id or not order_date:
            continue

        try:
            od = pd.to_datetime(order_date).date()
        except Exception:
            continue

        line_items = order.get("line_items", [])
        if not isinstance(line_items, list):
            continue

        for item in line_items:
            pid = item.get("product_id")
            qty = item.get("quantity", 0)
            total = float(item.get("total", 0))

            if not pid or qty <= 0:
                continue

            pname = name_map.get(pid, item.get("name", "Desconhecido"))
            rows.append((order_id, od, pid, pname, qty, total, order_currency, order_status))

    if not rows:
        return 0

    conn = get_connection()
    inserted = 0
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO orders (order_id, order_date, product_id, product_name,
                                    quantity, total, currency, order_status)
                VALUES %s
                ON CONFLICT (order_id, product_id) DO UPDATE SET
                    currency = EXCLUDED.currency
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
