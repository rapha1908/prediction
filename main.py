import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from datetime import datetime, timedelta
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import logging
import warnings

warnings.filterwarnings("ignore")
logging.getLogger("prophet").setLevel(logging.WARNING)
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

# ============================================================
# 1. CONFIGURACAO DA API WOOCOMMERCE
# ============================================================

URL_BASE = "https://tcche.org/wp-json/wc/v3/"
CONSUMER_KEY = "ck_54336e22a72c18dc35961d611bf0f2b5c5e0142d"
CONSUMER_SECRET = "cs_196af8ffe0125718a5335d424710add10d5f50a3"

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

def fetch_all_pages(endpoint: str, extra_params: dict | None = None) -> list:
    """Busca todos os registros de um endpoint paginado da API WooCommerce."""
    all_data: list = []
    page = 1

    while True:
        params = {**AUTH_PARAMS, "per_page": 100, "page": page}
        if extra_params:
            params.update(extra_params)

        response = requests.get(f"{URL_BASE}{endpoint}", params=params)
        response.raise_for_status()
        data = response.json()

        if not data:
            break

        all_data.extend(data)
        print(f"  -> Pagina {page}: {len(data)} registros obtidos")
        page += 1

    return all_data


def fetch_products() -> pd.DataFrame:
    """Busca todos os produtos da loja."""
    print("\n[*] Buscando produtos...")
    products = fetch_all_pages("products")
    df = pd.DataFrame(products)

    if df.empty:
        print("  Nenhum produto encontrado.")
        return df

    cols = ["id", "name", "price", "regular_price", "sale_price",
            "total_sales", "stock_quantity", "categories", "status"]
    available_cols = [c for c in cols if c in df.columns]
    df = df[available_cols]

    if "categories" in df.columns:
        df["category"] = df["categories"].apply(
            lambda cats: "|".join(c["name"] for c in cats)
            if isinstance(cats, list) and len(cats) > 0
            else "Sem categoria"
        )
        df.drop(columns=["categories"], inplace=True)

    print(f"  Total: {len(df)} produtos encontrados.")
    return df


def fetch_orders() -> pd.DataFrame:
    """Busca pedidos completed e processing (ignora cancelados/reembolsados)."""
    print("\n[*] Buscando pedidos...")
    all_orders = []

    for status in ["completed", "processing"]:
        print(f"  Buscando pedidos com status '{status}'...")
        orders = fetch_all_pages("orders", extra_params={"status": status})
        all_orders.extend(orders)

    df = pd.DataFrame(all_orders)

    if df.empty:
        print("  Nenhum pedido encontrado.")
        return df

    print(f"  Total: {len(df)} pedidos (completed + processing).")
    return df


# ============================================================
# 3. PROCESSAMENTO DOS DADOS DE VENDAS
# ============================================================

def build_sales_dataframe(orders_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Processa pedidos -> DataFrame de vendas diarias.
    Consolida nomes usando o catalogo de produtos.
    """
    print("\n[*] Processando dados de vendas...")

    cat_map = {}
    name_map = {}
    if not products_df.empty:
        if "id" in products_df.columns and "category" in products_df.columns:
            cat_map = dict(zip(products_df["id"], products_df["category"]))
        if "id" in products_df.columns and "name" in products_df.columns:
            name_map = dict(zip(products_df["id"], products_df["name"]))

    rows = []
    for _, order in orders_df.iterrows():
        order_date = pd.to_datetime(order.get("date_created", None))
        if order_date is None:
            continue

        line_items = order.get("line_items", [])
        if not isinstance(line_items, list):
            continue

        for item in line_items:
            pid = item.get("product_id")
            qty = item.get("quantity", 0)
            total = float(item.get("total", 0))

            if qty <= 0:
                continue

            pname = name_map.get(pid, item.get("name", "Desconhecido"))

            rows.append({
                "order_date": order_date.date(),
                "product_id": pid,
                "product_name": pname,
                "category": cat_map.get(pid, "Sem categoria"),
                "quantity": qty,
                "total": total,
            })

    sales_df = pd.DataFrame(rows)

    if sales_df.empty:
        print("  Nenhuma venda encontrada nos pedidos.")
        return sales_df

    sales_df["order_date"] = pd.to_datetime(sales_df["order_date"])

    daily_sales = (
        sales_df
        .groupby(["order_date", "product_id"])
        .agg(
            product_name=("product_name", "first"),
            category=("category", "first"),
            quantity_sold=("quantity", "sum"),
            revenue=("total", "sum"),
        )
        .reset_index()
    )

    print(f"  {len(daily_sales)} registros de vendas diarias processados.")
    print(f"  Produtos unicos com vendas: {daily_sales['product_id'].nunique()}")
    print(f"  Categorias encontradas: {daily_sales['category'].nunique()}")
    return daily_sales


# ============================================================
# 4. PREPARACAO DOS DADOS PARA PROPHET
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

    # Percorrer de tras pra frente e encontrar o primeiro gap grande
    for i in range(len(sorted_dates) - 1, 0, -1):
        gap = (sorted_dates[i] - sorted_dates[i - 1]) / np.timedelta64(1, "D")
        if gap > gap_days:
            cutoff = pd.Timestamp(sorted_dates[i])
            return daily_data[daily_data["order_date"] >= cutoff]

    # Sem gap grande - limitar a 365 dias
    max_date = pd.Timestamp(sorted_dates[-1])
    cutoff = max_date - pd.Timedelta(days=365)
    return daily_data[daily_data["order_date"] >= cutoff]


def prepare_prophet_data(daily_data, today):
    """
    Prepara dados para Prophet:
    - Filtra fase ativa do produto
    - Preenche datas sem vendas com 0 (dia sem venda = 0, nao missing)
    - Retorna DataFrame com colunas 'ds' e 'y'
    """
    active = find_active_phase(daily_data)

    daily_agg = active.groupby("order_date")["quantity_sold"].sum().reset_index()
    daily_agg.columns = ["ds", "y"]

    if daily_agg.empty:
        return pd.DataFrame(columns=["ds", "y"])

    # Preencher datas faltantes com 0 dentro da fase ativa
    date_range = pd.date_range(daily_agg["ds"].min(), today)
    full = pd.DataFrame({"ds": date_range})
    full = full.merge(daily_agg, on="ds", how="left")
    full["y"] = full["y"].fillna(0)

    return full


# ============================================================
# 5. TREINAMENTO E PREVISAO COM PROPHET
# ============================================================

def predict_with_prophet(prophet_data, pname, cat, pid, forecast_days, today):
    """
    Treina Prophet e gera previsoes com intervalo de confianca.
    Treina 2 vezes: 1x no split para avaliar, 1x em tudo para prever.
    """
    n = len(prophet_data)
    has_yearly = n > 365

    # --- 1) Avaliar com split temporal 80/20 ---
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

    # --- 2) Treinar com TODOS os dados para previsao final ---
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

    # --- 3) Prever futuro ---
    future = model.make_future_dataframe(periods=forecast_days)
    forecast = model.predict(future)

    future_pred = forecast[forecast["ds"] > today].copy()

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

    # Filtrar produtos ativos (vendas recentes)
    cutoff = today - pd.Timedelta(weeks=ACTIVE_WINDOW_WEEKS)
    active_pids = set(
        daily_sales[daily_sales["order_date"] >= cutoff]["product_id"].unique()
    )
    print(f"\n  Produtos ativos (ultimas {ACTIVE_WINDOW_WEEKS} semanas): {len(active_pids)}")
    print(f"\n[*] Treinando Prophet ({forecast_days} dias a frente)...\n")

    results = []
    model_metrics = []
    total = len(active_pids)

    for idx, pid in enumerate(sorted(active_pids), 1):
        product_data = daily_sales[daily_sales["product_id"] == pid].copy()
        if product_data.empty:
            continue

        pname = product_data["product_name"].iloc[-1]
        cat = product_data["category"].iloc[-1]

        # Preparar dados para Prophet
        prophet_data = prepare_prophet_data(product_data, today)
        n_days = len(prophet_data)
        n_nonzero = int((prophet_data["y"] > 0).sum()) if not prophet_data.empty else 0

        if n_days >= MIN_DAYS_PROPHET and n_nonzero >= 3:
            # Prophet
            future_df, metrics = predict_with_prophet(
                prophet_data, pname, cat, pid, forecast_days, today
            )
            if future_df is not None and not future_df.empty:
                results.append(future_df)
                model_metrics.append(metrics)
                avg = future_df["predicted_quantity"].mean()
                print(f"  [{idx}/{total}] [Prophet] {pname[:50]}: {avg:.2f}/dia | MAE={metrics['mae']:.2f} | R2={metrics['r2_score']:.3f} | {n_days}d ({n_nonzero} vendas)")
        else:
            # Simple average fallback
            future_df, metrics = predict_simple_average(
                product_data, pname, cat, pid, forecast_days, today
            )
            if future_df is not None:
                results.append(future_df)
                model_metrics.append(metrics)
                avg = future_df["predicted_quantity"].iloc[0]
                print(f"  [{idx}/{total}] [Media]   {pname[:50]}: {avg:.2f}/dia | {n_nonzero} vendas")

    metrics_df = pd.DataFrame(model_metrics)
    predictions_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    return predictions_df, metrics_df


# ============================================================
# 6. VISUALIZACOES
# ============================================================

def plot_sales_overview(daily_sales: pd.DataFrame):
    """Grafico geral de vendas ao longo do tempo."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    total_by_day = daily_sales.groupby("order_date")["quantity_sold"].sum().reset_index()
    axes[0].plot(total_by_day["order_date"], total_by_day["quantity_sold"],
                 color="#2196F3", linewidth=1.5)
    axes[0].fill_between(total_by_day["order_date"], total_by_day["quantity_sold"],
                         alpha=0.15, color="#2196F3")
    axes[0].set_title("Vendas Totais por Dia", fontsize=14, fontweight="bold")
    axes[0].set_xlabel("Data")
    axes[0].set_ylabel("Quantidade Vendida")
    axes[0].tick_params(axis="x", rotation=45)

    top_products = (
        daily_sales.groupby("product_name")["quantity_sold"].sum()
        .nlargest(10).reset_index()
    )
    sns.barplot(data=top_products, y="product_name", x="quantity_sold",
                palette="Blues_d", ax=axes[1])
    axes[1].set_title("Top 10 Produtos por Quantidade Vendida", fontsize=14, fontweight="bold")
    axes[1].set_xlabel("Quantidade Total Vendida")
    axes[1].set_ylabel("")

    plt.tight_layout()
    plt.savefig("vendas_overview.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  [OK] Grafico salvo: vendas_overview.png")


def plot_predictions(daily_sales: pd.DataFrame, predictions_df: pd.DataFrame, top_n: int = 6):
    """Grafico de vendas historicas + previsoes para os top N produtos."""
    if predictions_df.empty:
        print("  Sem previsoes para plotar.")
        return

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
# 7. EXECUCAO PRINCIPAL
# ============================================================

def main():
    print("=" * 60)
    print("   SISTEMA DE PREVISAO DE VENDAS - WooCommerce")
    print("   (v3 - Prophet by Meta)")
    print("=" * 60)

    # 1. Coletar dados
    products_df = fetch_products()
    orders_df = fetch_orders()

    if orders_df.empty:
        print("\n[ERRO] Nao foi possivel obter pedidos. Verifique a API.")
        return

    # 2. Processar vendas
    daily_sales = build_sales_dataframe(orders_df, products_df)

    if daily_sales.empty:
        print("\n[ERRO] Nenhum dado de venda foi processado.")
        return

    # 3. Visualizar dados historicos
    print("\n[*] Gerando visualizacoes de vendas...")
    plot_sales_overview(daily_sales)

    # 4. Treinar Prophet e gerar previsoes
    predictions_df, metrics_df = train_and_predict(daily_sales, forecast_days=FORECAST_DAYS)

    # 5. Estatisticas
    if not metrics_df.empty:
        prophet_count = (metrics_df["method"] == "prophet").sum()
        avg_count = (metrics_df["method"] == "weighted_average").sum()
        print(f"\n[*] Modelos treinados: {len(metrics_df)}")
        print(f"    Prophet: {prophet_count} | Media Ponderada: {avg_count}")

    # 6. Visualizar previsoes
    print("\n[*] Gerando graficos de previsao...")
    plot_predictions(daily_sales, predictions_df)

    # 7. Resumo final
    print_forecast_summary(predictions_df, metrics_df)

    # 8. Salvar CSVs
    daily_sales.to_csv("vendas_historicas.csv", index=False)
    print("\n[OK] Historico salvo em: vendas_historicas.csv")

    if not predictions_df.empty:
        predictions_df.to_csv("previsoes_vendas.csv", index=False)
        print(f"[OK] Previsoes salvas em: previsoes_vendas.csv ({len(predictions_df)} registros)")

    if not metrics_df.empty:
        metrics_df.to_csv("metricas_modelos.csv", index=False)
        print(f"[OK] Metricas salvas em: metricas_modelos.csv ({len(metrics_df)} modelos)")

    print("\n>>> Para abrir a dashboard, execute: py dashboard.py")


if __name__ == "__main__":
    main()
