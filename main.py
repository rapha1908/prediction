import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
import warnings

warnings.filterwarnings("ignore")

# ============================================================
# 1. CONFIGURAÇÃO DA API WOOCOMMERCE
# ============================================================

URL_BASE = "https://tcche.org/wp-json/wc/v3/"
CONSUMER_KEY = "ck_54336e22a72c18dc35961d611bf0f2b5c5e0142d"
CONSUMER_SECRET = "cs_196af8ffe0125718a5335d424710add10d5f50a3"

AUTH_PARAMS = {
    "consumer_key": CONSUMER_KEY,
    "consumer_secret": CONSUMER_SECRET,
}


# ============================================================
# 2. FUNÇÕES DE COLETA DE DADOS
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
        print(f"  -> Página {page}: {len(data)} registros obtidos")
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

    # Extrair TODAS as categorias do produto (pipe-separadas)
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
    """Busca todos os pedidos da loja."""
    print("\n[*] Buscando pedidos...")
    orders = fetch_all_pages("orders", extra_params={"status": "any"})
    df = pd.DataFrame(orders)

    if df.empty:
        print("  Nenhum pedido encontrado.")
        return df

    print(f"  Total: {len(df)} pedidos encontrados.")
    return df


# ============================================================
# 3. PROCESSAMENTO DOS DADOS DE VENDAS
# ============================================================

def build_sales_dataframe(orders_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Processa os pedidos e constrói um DataFrame de vendas por produto por dia.
    Cada linha = (data, produto, categoria, quantidade vendida, receita).
    """
    print("\n[*] Processando dados de vendas...")

    # Mapa product_id -> categoria
    cat_map = {}
    if not products_df.empty and "id" in products_df.columns and "category" in products_df.columns:
        cat_map = dict(zip(products_df["id"], products_df["category"]))

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
            rows.append({
                "order_date": order_date.date(),
                "product_id": pid,
                "product_name": item.get("name", "Desconhecido"),
                "category": cat_map.get(pid, "Sem categoria"),
                "quantity": item.get("quantity", 0),
                "total": float(item.get("total", 0)),
            })

    sales_df = pd.DataFrame(rows)

    if sales_df.empty:
        print("  Nenhuma venda encontrada nos pedidos.")
        return sales_df

    sales_df["order_date"] = pd.to_datetime(sales_df["order_date"])

    # Agregar vendas por produto por dia
    daily_sales = (
        sales_df
        .groupby(["order_date", "product_id", "product_name", "category"])
        .agg(quantity_sold=("quantity", "sum"), revenue=("total", "sum"))
        .reset_index()
    )

    print(f"  {len(daily_sales)} registros de vendas diarias processados.")
    print(f"  Produtos unicos com vendas: {daily_sales['product_id'].nunique()}")
    print(f"  Categorias encontradas: {daily_sales['category'].nunique()}")
    return daily_sales


def create_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria features temporais para o modelo de previsão.
    """
    df = df.copy()
    df["day_of_week"] = df["order_date"].dt.dayofweek
    df["day_of_month"] = df["order_date"].dt.day
    df["month"] = df["order_date"].dt.month
    df["week_of_year"] = df["order_date"].dt.isocalendar().week.astype(int)
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["days_since_start"] = (df["order_date"] - df["order_date"].min()).dt.days
    return df


def fill_missing_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche datas sem vendas com 0, para cada produto ter
    uma série temporal contínua.
    """
    all_frames = []
    date_range = pd.date_range(df["order_date"].min(), df["order_date"].max())

    for (pid, pname), group in df.groupby(["product_id", "product_name"]):
        cat = group["category"].iloc[0] if "category" in group.columns else "Sem categoria"
        idx = pd.DataFrame({"order_date": date_range})
        merged = idx.merge(group, on="order_date", how="left")
        merged["product_id"] = pid
        merged["product_name"] = pname
        merged["category"] = cat
        merged["quantity_sold"] = merged["quantity_sold"].fillna(0)
        merged["revenue"] = merged["revenue"].fillna(0)
        all_frames.append(merged)

    return pd.concat(all_frames, ignore_index=True)


# ============================================================
# 4. TREINAMENTO E PREVISÃO
# ============================================================

def train_and_predict(daily_sales: pd.DataFrame, forecast_days: int = 30) -> pd.DataFrame:
    """
    Treina um modelo RandomForest para cada produto e gera previsões.
    Retorna um DataFrame com previsões futuras para cada produto.
    """
    print(f"\n[*] Treinando modelos de previsao ({forecast_days} dias a frente)...\n")

    # Preencher datas faltantes e criar features
    full_data = fill_missing_dates(daily_sales)
    full_data = create_features(full_data)

    feature_cols = ["day_of_week", "day_of_month", "month",
                    "week_of_year", "is_weekend", "days_since_start"]

    results = []
    model_metrics = []

    products = full_data.groupby(["product_id", "product_name"])

    for (pid, pname), product_data in products:
        product_data = product_data.sort_values("order_date")
        cat = product_data["category"].iloc[0] if "category" in product_data.columns else "Sem categoria"

        # Precisa de pelo menos 10 registros para treinar
        if len(product_data) < 10:
            print(f"  [!] {pname}: dados insuficientes ({len(product_data)} registros), pulando...")
            continue

        X = product_data[feature_cols]
        y = product_data["quantity_sold"]

        # Separar treino/teste (80/20, sem embaralhar pois é serie temporal)
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

        # Treinar modelo
        model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=1,
        )
        model.fit(X_train, y_train)

        # Avaliar
        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred) if len(y_test) > 1 else 0

        model_metrics.append({
            "product_id": pid,
            "product_name": pname,
            "category": cat,
            "mae": round(mae, 2),
            "rmse": round(rmse, 2),
            "r2_score": round(r2, 3),
            "train_size": len(X_train),
            "test_size": len(X_test),
        })

        print(f"  [OK] {pname}: MAE={mae:.2f} | RMSE={rmse:.2f} | R2={r2:.3f}")

        # Gerar previsao futura a partir de HOJE
        data_start = product_data["order_date"].min()
        today = pd.Timestamp(datetime.now().date())

        future_dates = pd.date_range(today + timedelta(days=1), periods=forecast_days)
        future_df = pd.DataFrame({"order_date": future_dates})
        future_df["day_of_week"] = future_df["order_date"].dt.dayofweek
        future_df["day_of_month"] = future_df["order_date"].dt.day
        future_df["month"] = future_df["order_date"].dt.month
        future_df["week_of_year"] = future_df["order_date"].dt.isocalendar().week.astype(int)
        future_df["is_weekend"] = (future_df["day_of_week"] >= 5).astype(int)
        future_df["days_since_start"] = (future_dates - data_start).days

        future_pred = model.predict(future_df[feature_cols])
        future_pred = np.maximum(future_pred, 0)

        future_df["predicted_quantity"] = np.round(future_pred, 1)
        future_df["product_id"] = pid
        future_df["product_name"] = pname
        future_df["category"] = cat
        results.append(future_df)

    metrics_df = pd.DataFrame(model_metrics)
    predictions_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    return predictions_df, metrics_df


# ============================================================
# 5. VISUALIZAÇÕES
# ============================================================

def plot_sales_overview(daily_sales: pd.DataFrame):
    """Gráfico geral de vendas ao longo do tempo."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Vendas totais por dia
    total_by_day = daily_sales.groupby("order_date")["quantity_sold"].sum().reset_index()
    axes[0].plot(total_by_day["order_date"], total_by_day["quantity_sold"],
                 color="#2196F3", linewidth=1.5)
    axes[0].fill_between(total_by_day["order_date"], total_by_day["quantity_sold"],
                         alpha=0.15, color="#2196F3")
    axes[0].set_title("Vendas Totais por Dia", fontsize=14, fontweight="bold")
    axes[0].set_xlabel("Data")
    axes[0].set_ylabel("Quantidade Vendida")
    axes[0].tick_params(axis="x", rotation=45)

    # Top 10 produtos por vendas
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
    """Gráfico de vendas históricas + previsões para os top N produtos."""
    if predictions_df.empty:
        print("  Sem previsões para plotar.")
        return

    # Selecionar os top_n produtos com mais vendas
    top_products = (
        daily_sales.groupby(["product_id", "product_name"])["quantity_sold"]
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

        # Dados históricos
        hist = daily_sales[daily_sales["product_id"] == pid].sort_values("order_date")
        ax.plot(hist["order_date"], hist["quantity_sold"],
                label="Histórico", color="#2196F3", linewidth=1.2, alpha=0.8)

        # Previsões
        pred = predictions_df[predictions_df["product_id"] == pid].sort_values("order_date")
        if not pred.empty:
            ax.plot(pred["order_date"], pred["predicted_quantity"],
                    label="Previsão", color="#FF5722", linewidth=2, linestyle="--")
            ax.fill_between(pred["order_date"], pred["predicted_quantity"],
                            alpha=0.15, color="#FF5722")

            # Linha separadora
            ax.axvline(x=hist["order_date"].max(), color="gray",
                       linestyle=":", alpha=0.7, label="Início previsão")

        # Truncar nome se muito longo
        title = pname if len(pname) <= 40 else pname[:37] + "..."
        ax.set_title(title, fontsize=11, fontweight="bold")
        ax.tick_params(axis="x", rotation=45)
        ax.legend(fontsize=8)

    # Esconder eixos vazios
    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    plt.suptitle("Previsão de Vendas por Produto", fontsize=16, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig("previsao_vendas.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  [OK] Grafico salvo: previsao_vendas.png")


def print_forecast_summary(predictions_df: pd.DataFrame, metrics_df: pd.DataFrame):
    """Exibe um resumo das previsões no console."""
    if predictions_df.empty:
        print("\n  Sem previsões disponíveis.")
        return

    print("\n" + "=" * 70)
    print("RESUMO DE PREVISAO DE VENDAS (Proximos 30 dias)")
    print("=" * 70)

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
    summary["max_dia"] = summary["max_dia"].round(1)

    # Adicionar métricas do modelo
    summary = summary.merge(
        metrics_df[["product_id", "mae", "r2_score"]],
        on="product_id", how="left"
    )

    print(f"\n{'Produto':<35} {'Total Prev.':<12} {'Média/Dia':<10} {'MAE':<8} {'R²':<8}")
    print("-" * 73)

    for _, row in summary.iterrows():
        name = row["product_name"]
        if len(name) > 33:
            name = name[:30] + "..."
        print(
            f"{name:<35} "
            f"{row['total_previsto']:<12.1f} "
            f"{row['media_diaria']:<10.2f} "
            f"{row['mae']:<8.2f} "
            f"{row['r2_score']:<8.3f}"
        )

    print("-" * 73)
    print(f"{'TOTAL':<35} {summary['total_previsto'].sum():<12.1f}")
    print("=" * 70)


# ============================================================
# 6. EXECUÇÃO PRINCIPAL
# ============================================================

def main():
    print("=" * 60)
    print("   SISTEMA DE PREVISÃO DE VENDAS - WooCommerce")
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

    # 3. Visualizar dados históricos
    print("\n[*] Gerando visualizacoes de vendas...")
    plot_sales_overview(daily_sales)

    # 4. Treinar modelos e gerar previsões
    predictions_df, metrics_df = train_and_predict(daily_sales, forecast_days=30)

    # 5. Exibir métricas dos modelos
    if not metrics_df.empty:
        print("\n[*] Metricas dos Modelos Treinados:")
        print(metrics_df.to_string(index=False))

    # 6. Visualizar previsões
    print("\n[*] Gerando graficos de previsao...")
    plot_predictions(daily_sales, predictions_df)

    # 7. Resumo final
    print_forecast_summary(predictions_df, metrics_df)

    # 8. Salvar dados em CSV
    daily_sales.to_csv("vendas_historicas.csv", index=False)
    print("\n[OK] Historico salvo em: vendas_historicas.csv")

    if not predictions_df.empty:
        predictions_df.to_csv("previsoes_vendas.csv", index=False)
        print("[OK] Previsoes salvas em: previsoes_vendas.csv")

    if not metrics_df.empty:
        metrics_df.to_csv("metricas_modelos.csv", index=False)
        print("[OK] Metricas salvas em: metricas_modelos.csv")

    print("\n>>> Para abrir a dashboard, execute: py dashboard.py")


if __name__ == "__main__":
    main()
