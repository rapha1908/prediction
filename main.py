import sys
import os
import gc
import logging
import warnings

# Configure matplotlib BEFORE any import that may trigger it (e.g. Prophet)
os.environ.setdefault("MPLCONFIGDIR", os.path.join(os.environ.get("TMPDIR", "/tmp"), "matplotlib"))
os.environ.setdefault("MPLBACKEND", "Agg")
import matplotlib
matplotlib.use("Agg")

import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

from dotenv import load_dotenv

load_dotenv()

_IS_RENDER = os.environ.get("RENDER") is not None

# ============================================================
# 1. CONFIGURACAO
# ============================================================

URL_BASE = os.getenv("WOOCOMMERCE_URL", "https://tcche.org/wp-json/wc/v3/")
CONSUMER_KEY = os.getenv("WOOCOMMERCE_KEY", "ck_54336e22a72c18dc35961d611bf0f2b5c5e0142d")
CONSUMER_SECRET = os.getenv("WOOCOMMERCE_SECRET", "cs_196af8ffe0125718a5335d424710add10d5f50a3")

AUTH_PARAMS = {
    "consumer_key": CONSUMER_KEY,
    "consumer_secret": CONSUMER_SECRET,
}

# Apenas prever para produtos com vendas nas ultimas N semanas
ACTIVE_WINDOW_WEEKS = 12
# Dias minimos de dados para usar Prophet (senao usa media)
MIN_DAYS_PROPHET = 14
# Gap maximo (dias) antes de considerar nova fase de vendas
MAX_GAP_DAYS = 42  # 6 semanas
# Dias de previsao
FORECAST_DAYS = 30


# ============================================================
# 2. FUNCOES DE COLETA DE DADOS
# ============================================================

def fetch_all_pages(endpoint: str, extra_params: dict | None = None,
                     max_retries: int = 3, retry_delay: int = 5) -> list:
    """Busca todos os registros de um endpoint paginado da API WooCommerce."""
    import time
    all_data: list = []
    page = 1

    while True:
        params = {**AUTH_PARAMS, "per_page": 100, "page": page}
        if extra_params:
            params.update(extra_params)

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(f"{URL_BASE}{endpoint}", params=params, timeout=60)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    wait = retry_delay * attempt
                    print(f"  [RETRY] Page {page} attempt {attempt} failed ({e}). Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"  [ERROR] Page {page} failed after {max_retries} attempts.")
                    raise

        data = response.json()

        if not data:
            break

        all_data.extend(data)
        print(f"  -> Pagina {page}: {len(data)} registros obtidos")
        page += 1

    return all_data


def _extract_meta(meta_list, key):
    """Extrai um valor de meta_data por chave."""
    if not isinstance(meta_list, list):
        return None
    for m in meta_list:
        if m.get("key") == key:
            return m.get("value")
    return None


def fetch_products() -> pd.DataFrame:
    """Busca todos os produtos da loja, incluindo datas de evento."""
    print("\n[*] Buscando produtos...")
    products = fetch_all_pages("products")
    df = pd.DataFrame(products)

    if df.empty:
        print("  Nenhum produto encontrado.")
        return df

    cols = ["id", "name", "price", "regular_price", "sale_price",
            "total_sales", "stock_quantity", "categories", "status", "meta_data"]
    available_cols = [c for c in cols if c in df.columns]
    df = df[available_cols]

    # Extrair categorias
    if "categories" in df.columns:
        df["category"] = df["categories"].apply(
            lambda cats: "|".join(c["name"] for c in cats)
            if isinstance(cats, list) and len(cats) > 0
            else "Sem categoria"
        )
        df.drop(columns=["categories"], inplace=True)

    # Extrair datas de ticket/evento do meta_data
    if "meta_data" in df.columns:
        df["ticket_start_date"] = df["meta_data"].apply(
            lambda m: _extract_meta(m, "_ticket_start_date")
        )
        df["ticket_end_date"] = df["meta_data"].apply(
            lambda m: _extract_meta(m, "_ticket_end_date")
        )
        df["event_id"] = df["meta_data"].apply(
            lambda m: _extract_meta(m, "_tribe_wooticket_for_event")
        )
        df.drop(columns=["meta_data"], inplace=True)

    n_with_dates = df["ticket_end_date"].notna().sum() if "ticket_end_date" in df.columns else 0
    print(f"  Total: {len(df)} produtos encontrados ({n_with_dates} com data de evento).")
    return df


def fetch_orders(after_date=None) -> list:
    """
    Busca pedidos completed e processing da API.
    Se after_date for fornecido, busca apenas pedidos apos essa data (incremental).
    Retorna lista de dicts brutos da API.
    """
    print("\n[*] Buscando pedidos...")
    all_orders = []

    for status in ["completed", "processing"]:
        extra = {"status": status}
        if after_date:
            # API WooCommerce aceita 'after' como ISO 8601
            iso_date = pd.to_datetime(after_date).isoformat()
            extra["after"] = iso_date
            print(f"  Buscando '{status}' apos {iso_date}...")
        else:
            print(f"  Buscando todos com status '{status}'...")

        orders = fetch_all_pages("orders", extra_params=extra)
        all_orders.extend(orders)

    print(f"  Total: {len(all_orders)} pedidos obtidos.")
    return all_orders


# ============================================================
# 3. PREPARACAO DOS DADOS PARA PROPHET
# ============================================================

def find_active_phase(daily_data, gap_days=MAX_GAP_DAYS):
    """
    Detecta o inicio da fase ativa atual do produto.
    Uma nova fase comeca apos um gap de > gap_days sem nenhuma venda.
    Retorna apenas os dados da fase ativa mais recente.
    """
    sorted_dates = np.sort(daily_data["order_date"].unique())

    if len(sorted_dates) <= 1:
        return daily_data

    for i in range(len(sorted_dates) - 1, 0, -1):
        gap = (sorted_dates[i] - sorted_dates[i - 1]) / np.timedelta64(1, "D")
        if gap > gap_days:
            cutoff = pd.Timestamp(sorted_dates[i])
            return daily_data[daily_data["order_date"] >= cutoff]

    max_date = pd.Timestamp(sorted_dates[-1])
    cutoff = max_date - pd.Timedelta(days=365)
    return daily_data[daily_data["order_date"] >= cutoff]


def prepare_prophet_data(daily_data, today):
    """
    Prepara dados para Prophet:
    - Filtra fase ativa do produto
    - Preenche datas sem vendas com 0
    - Retorna DataFrame com colunas 'ds' e 'y'
    """
    active = find_active_phase(daily_data)

    daily_agg = active.groupby("order_date")["quantity_sold"].sum().reset_index()
    daily_agg.columns = ["ds", "y"]

    if daily_agg.empty:
        return pd.DataFrame(columns=["ds", "y"])

    date_range = pd.date_range(daily_agg["ds"].min(), today)
    full = pd.DataFrame({"ds": date_range})
    full = full.merge(daily_agg, on="ds", how="left")
    full["y"] = full["y"].fillna(0)

    return full


# ============================================================
# 4. TREINAMENTO E PREVISAO COM PROPHET
# ============================================================

def predict_with_prophet(prophet_data, pname, cat, pid, forecast_days, today):
    """
    Treina Prophet e gera previsoes com intervalo de confianca.
    Treina 2 vezes: 1x no split para avaliar, 1x em tudo para prever.
    """
    from prophet import Prophet
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    logging.getLogger("prophet").setLevel(logging.WARNING)
    logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

    n = len(prophet_data)
    has_yearly = n > 365

    split_idx = int(n * 0.8)
    train = prophet_data.iloc[:split_idx]
    test = prophet_data.iloc[split_idx:]

    model_eval = Prophet(
        daily_seasonality=False,
        weekly_seasonality=True,
        yearly_seasonality=has_yearly,
        seasonality_mode="additive",
        changepoint_prior_scale=0.15,
        seasonality_prior_scale=1.0,
        interval_width=0.80,
    )
    model_eval.fit(train)

    if len(test) > 0:
        test_forecast = model_eval.predict(test[["ds"]])
        y_pred = test_forecast["yhat"].clip(lower=0).values
        y_true = test["y"].values
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred) if len(y_true) > 1 else 0
    else:
        mae = rmse = r2 = 0

    del model_eval
    gc.collect()

    model = Prophet(
        daily_seasonality=False,
        weekly_seasonality=True,
        yearly_seasonality=has_yearly,
        seasonality_mode="additive",
        changepoint_prior_scale=0.15,
        seasonality_prior_scale=1.0,
        interval_width=0.80,
    )
    model.fit(prophet_data)

    future = model.make_future_dataframe(periods=forecast_days)
    forecast = model.predict(future)

    future_pred = forecast[forecast["ds"] > today].copy()

    del model, forecast, future
    gc.collect()

    if future_pred.empty:
        return None, None

    result_df = pd.DataFrame({
        "order_date": future_pred["ds"].values,
        "predicted_quantity": future_pred["yhat"].clip(lower=0).round(2).values,
        "yhat_lower": future_pred["yhat_lower"].clip(lower=0).round(2).values,
        "yhat_upper": future_pred["yhat_upper"].clip(lower=0).round(2).values,
        "product_id": pid,
        "product_name": pname,
        "category": cat,
    })

    metrics = {
        "product_id": pid,
        "product_name": pname,
        "category": cat,
        "mae": round(mae, 2),
        "rmse": round(rmse, 2),
        "r2_score": round(r2, 3),
        "train_size": split_idx,
        "test_size": len(test),
        "method": "prophet",
    }

    return result_df, metrics


def predict_simple_average(daily_data, pname, cat, pid, forecast_days, today):
    """Fallback: media ponderada para produtos com poucos dados."""
    recent_cutoff = today - pd.Timedelta(days=28)
    recent = daily_data[daily_data["order_date"] >= recent_cutoff]

    if recent.empty:
        recent = daily_data.tail(7)

    total_qty = recent["quantity_sold"].sum()
    n_days = max((recent["order_date"].max() - recent["order_date"].min()).days, 1)
    daily_avg = total_qty / n_days

    if daily_avg < 0.01:
        return None, None

    future_dates = pd.date_range(today + timedelta(days=1), periods=forecast_days)
    result_df = pd.DataFrame({
        "order_date": future_dates,
        "predicted_quantity": round(daily_avg, 2),
        "yhat_lower": round(max(daily_avg * 0.5, 0), 2),
        "yhat_upper": round(daily_avg * 1.5, 2),
        "product_id": pid,
        "product_name": pname,
        "category": cat,
    })

    metrics = {
        "product_id": pid,
        "product_name": pname,
        "category": cat,
        "mae": 0,
        "rmse": 0,
        "r2_score": 0,
        "train_size": len(daily_data),
        "test_size": 0,
        "method": "weighted_average",
    }

    return result_df, metrics


def train_and_predict(daily_sales: pd.DataFrame, forecast_days: int = FORECAST_DAYS):
    """
    Pipeline completo com Prophet:
    Para cada produto ativo -> preparar dados -> treinar Prophet -> prever.
    """
    today = pd.Timestamp.now().normalize()

    cutoff = today - pd.Timedelta(weeks=ACTIVE_WINDOW_WEEKS)
    active_pids = set(
        daily_sales[daily_sales["order_date"] >= cutoff]["product_id"].unique()
    )
    print(f"\n  Produtos ativos (ultimas {ACTIVE_WINDOW_WEEKS} semanas): {len(active_pids)}")
    print(f"\n[*] Treinando Prophet ({forecast_days} dias a frente)...\n")

    results = []
    model_metrics = []
    total = len(active_pids)

    # Mapa de ticket_end_date por product_id
    end_date_by_pid = {}
    if "ticket_end_date" in daily_sales.columns:
        for pid_val, grp in daily_sales.groupby("product_id"):
            val = grp["ticket_end_date"].dropna().iloc[0] if grp["ticket_end_date"].notna().any() else ""
            if str(val).strip() and str(val).strip() != "NaT":
                end_date_by_pid[pid_val] = str(val).strip()

    for idx, pid in enumerate(sorted(active_pids), 1):
        product_data = daily_sales[daily_sales["product_id"] == pid].copy()
        if product_data.empty:
            continue

        pname = product_data["product_name"].iloc[-1]
        cat = product_data["category"].iloc[-1]
        t_end = end_date_by_pid.get(pid, "")

        prophet_data = prepare_prophet_data(product_data, today)
        n_days = len(prophet_data)
        n_nonzero = int((prophet_data["y"] > 0).sum()) if not prophet_data.empty else 0

        if n_days >= MIN_DAYS_PROPHET and n_nonzero >= 3:
            future_df, metrics = predict_with_prophet(
                prophet_data, pname, cat, pid, forecast_days, today
            )
            if future_df is not None and not future_df.empty:
                future_df["ticket_end_date"] = t_end
                future_df["method"] = "prophet"
                results.append(future_df)
                metrics["ticket_end_date"] = t_end
                model_metrics.append(metrics)
                avg = future_df["predicted_quantity"].mean()
                print(f"  [{idx}/{total}] [Prophet] {pname[:50]}: {avg:.2f}/dia | MAE={metrics['mae']:.2f} | R2={metrics['r2_score']:.3f} | {n_days}d ({n_nonzero} vendas)")
        else:
            future_df, metrics = predict_simple_average(
                product_data, pname, cat, pid, forecast_days, today
            )
            if future_df is not None:
                future_df["ticket_end_date"] = t_end
                future_df["method"] = "weighted_average"
                results.append(future_df)
                metrics["ticket_end_date"] = t_end
                model_metrics.append(metrics)
                avg = future_df["predicted_quantity"].iloc[0]
                print(f"  [{idx}/{total}] [Media]   {pname[:50]}: {avg:.2f}/dia | {n_nonzero} vendas")

    metrics_df = pd.DataFrame(model_metrics)
    predictions_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    return predictions_df, metrics_df


# ============================================================
# 5. VISUALIZACOES
# ============================================================

def plot_predictions(daily_sales: pd.DataFrame, predictions_df: pd.DataFrame, top_n: int = 6):
    """Grafico de vendas historicas + previsoes para os top N produtos."""
    if _IS_RENDER:
        print("  [SKIP] Graficos desativados em producao (economia de memoria).")
        return

    if predictions_df.empty:
        print("  Sem previsoes para plotar.")
        return

    import matplotlib.pyplot as plt

    predicted_pids = predictions_df["product_id"].unique()
    top_products = (
        daily_sales[daily_sales["product_id"].isin(predicted_pids)]
        .groupby(["product_id", "product_name"])["quantity_sold"]
        .sum().nlargest(top_n).reset_index()
    )

    n_cols = 2
    n_rows = (top_n + 1) // 2
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 5 * n_rows))
    axes = axes.flatten()

    for i, (_, row) in enumerate(top_products.iterrows()):
        pid = row["product_id"]
        pname = row["product_name"]
        ax = axes[i]

        hist = daily_sales[daily_sales["product_id"] == pid].sort_values("order_date")
        ax.plot(hist["order_date"], hist["quantity_sold"],
                label="Real", color="#4A90D9", linewidth=1.2, alpha=0.8)

        pred = predictions_df[predictions_df["product_id"] == pid].sort_values("order_date")
        if not pred.empty:
            ax.plot(pred["order_date"], pred["predicted_quantity"],
                    label="Previsao", color="#F5A623", linewidth=2)

            if "yhat_lower" in pred.columns and "yhat_upper" in pred.columns:
                ax.fill_between(pred["order_date"],
                                pred["yhat_lower"], pred["yhat_upper"],
                                alpha=0.2, color="#F5A623", label="Intervalo 80%")

        title = pname if len(pname) <= 40 else pname[:37] + "..."
        ax.set_title(title, fontsize=11, fontweight="bold")
        ax.tick_params(axis="x", rotation=45)
        ax.legend(fontsize=8)

    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    plt.suptitle("Previsao de Vendas por Produto (Prophet)", fontsize=16, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig("previsao_vendas.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  [OK] Grafico salvo: previsao_vendas.png")


def print_forecast_summary(predictions_df: pd.DataFrame, metrics_df: pd.DataFrame):
    """Exibe resumo das previsoes no console."""
    if predictions_df.empty:
        print("\n  Sem previsoes disponiveis.")
        return

    print("\n" + "=" * 90)
    print("  RESUMO DE PREVISAO DE VENDAS - PROPHET (Proximos 30 dias)")
    print("=" * 90)

    summary = (
        predictions_df
        .groupby(["product_id", "product_name"])
        .agg(
            total_previsto=("predicted_quantity", "sum"),
            media_diaria=("predicted_quantity", "mean"),
            max_dia=("predicted_quantity", "max"),
        )
        .reset_index()
        .sort_values("total_previsto", ascending=False)
    )

    summary["total_previsto"] = summary["total_previsto"].round(1)
    summary["media_diaria"] = summary["media_diaria"].round(2)

    summary = summary.merge(
        metrics_df[["product_id", "mae", "r2_score", "method"]],
        on="product_id", how="left"
    )

    print(f"\n{'Produto':<45} {'Total 30d':<10} {'Media/d':<10} {'MAE':<8} {'R2':<8} {'Metodo':<10}")
    print("-" * 91)

    for _, row in summary.iterrows():
        name = row["product_name"]
        if len(name) > 43:
            name = name[:40] + "..."
        method = str(row.get("method", "?"))
        print(
            f"{name:<45} "
            f"{row['total_previsto']:<10.1f} "
            f"{row['media_diaria']:<10.2f} "
            f"{row['mae']:<8.2f} "
            f"{row['r2_score']:<8.3f} "
            f"{method:<10}"
        )

    print("-" * 91)
    print(f"{'TOTAL':<45} {summary['total_previsto'].sum():<10.1f}")
    print("=" * 90)


# ============================================================
# 6. EXECUCAO PRINCIPAL
# ============================================================

def main():
    import db

    full_mode = "--full" in sys.argv

    print("=" * 60)
    print("   SISTEMA DE PREVISAO DE VENDAS - WooCommerce")
    print("   (v4 - Prophet + PostgreSQL)")
    print("=" * 60)

    # --- Conectar ao banco ---
    if not db.test_connection():
        print("\n[ERRO] PostgreSQL nao esta disponivel.")
        print("  Execute: docker compose up -d")
        return

    db.create_tables()

    # --- 1. Sync de produtos (sempre completo - sao poucos) ---
    products_df = fetch_products()
    if not products_df.empty:
        n = db.upsert_products(products_df)
        print(f"  [OK] {n} produtos sincronizados no banco.")

    # --- 2. Sync incremental de pedidos ---
    last_sync = db.get_last_sync_date()
    existing_orders = db.get_order_count()

    if full_mode or last_sync is None or existing_orders == 0:
        if full_mode:
            print("\n[*] Modo --full: buscando TODOS os pedidos...")
        else:
            print("\n[*] Primeira execucao: buscando todos os pedidos...")
        orders_raw = fetch_orders(after_date=None)
    else:
        # Buscar a partir de 2 dias antes do ultimo sync (margem de seguranca)
        after = last_sync - timedelta(days=2)
        print(f"\n[*] Sync incremental (pedidos apos {after})...")
        orders_raw = fetch_orders(after_date=after)

    if orders_raw:
        inserted = db.insert_orders(orders_raw, products_df)
        total = db.get_order_count()
        print(f"  [OK] {inserted} novos itens inseridos. Total no banco: {total}")
    else:
        print("  Nenhum pedido novo encontrado.")

    # --- 3. Geocodificar localizacoes de pedidos ---
    print("\n[*] Geocodificando localizacoes de pedidos...")
    db.geocode_new_orders()

    # --- 4. Reagregar daily_sales ---
    print("\n[*] Atualizando vendas diarias agregadas...")
    db.refresh_daily_sales()

    # --- 5. Carregar dados para treinamento ---
    daily_sales = db.load_daily_sales()

    if daily_sales.empty:
        print("\n[ERRO] Nenhum dado de venda no banco.")
        return

    print(f"\n[*] Dados carregados: {len(daily_sales)} registros, "
          f"{daily_sales['product_id'].nunique()} produtos")

    # --- 6. Treinar Prophet e gerar previsoes ---
    predictions_df, metrics_df = train_and_predict(daily_sales, forecast_days=FORECAST_DAYS)

    # --- 7. Salvar previsoes no banco ---
    run_id = db.generate_run_id()
    print(f"\n[*] Salvando previsoes (run_id: {run_id})...")

    if not predictions_df.empty:
        n_pred = db.save_predictions(predictions_df, run_id)
        print(f"  [OK] {n_pred} previsoes salvas no banco.")

    if not metrics_df.empty:
        n_met = db.save_metrics(metrics_df, run_id)
        print(f"  [OK] {n_met} metricas salvas no banco.")
        prophet_count = (metrics_df["method"] == "prophet").sum()
        avg_count = (metrics_df["method"] == "weighted_average").sum()
        print(f"  Prophet: {prophet_count} | Media Ponderada: {avg_count}")

    # --- 8. Graficos e resumo ---
    print("\n[*] Gerando graficos de previsao...")
    plot_predictions(daily_sales, predictions_df)
    print_forecast_summary(predictions_df, metrics_df)

    # --- 9. Salvar CSVs (apenas local, pular em producao) ---
    if not _IS_RENDER:
        daily_sales.to_csv("vendas_historicas.csv", index=False)
        print("\n[OK] CSV de historico salvo: vendas_historicas.csv")

        if not predictions_df.empty:
            predictions_df.to_csv("previsoes_vendas.csv", index=False)
            print(f"[OK] CSV de previsoes salvo: previsoes_vendas.csv ({len(predictions_df)} registros)")

        if not metrics_df.empty:
            metrics_df.to_csv("metricas_modelos.csv", index=False)
            print(f"[OK] CSV de metricas salvo: metricas_modelos.csv ({len(metrics_df)} modelos)")
    else:
        print("\n[OK] Producao: dados salvos no banco (CSVs desativados).")

    # Liberar memoria
    gc.collect()
    print("\n[OK] Sync finalizado com sucesso.")


if __name__ == "__main__":
    main()
