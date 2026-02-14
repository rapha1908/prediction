import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from datetime import datetime, timedelta
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
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

# Semanas minimas para usar ML (senao usa media ponderada)
MIN_WEEKS_ML = 12
# Apenas prever para produtos com vendas nas ultimas N semanas
ACTIVE_WINDOW_WEEKS = 12
# Maximo de semanas de gap (inatividade) antes de considerar nova fase
MAX_GAP_WEEKS = 6
# Janela maxima de treino (semanas)
MAX_TRAIN_WINDOW = 52


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
    """Busca pedidos completed e processing da loja (ignora cancelados/reembolsados)."""
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
    Processa os pedidos e constroi um DataFrame de vendas por produto por dia.
    Consolida nomes usando o catalogo de produtos (evita duplicatas por renomeacao).
    """
    print("\n[*] Processando dados de vendas...")

    # Mapa product_id -> categoria e nome (do catalogo de produtos)
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

            # Ignorar itens sem quantidade
            if qty <= 0:
                continue

            # Usar nome do catalogo (mais atual) quando disponivel
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

    # Agregar vendas por produto por dia (chave = product_id, nao product_name)
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
# 4. AGREGACAO SEMANAL E FEATURES
# ============================================================

def create_weekly_data(daily_sales: pd.DataFrame) -> pd.DataFrame:
    """
    Converte vendas diarias em semanais.
    Preenche semanas faltantes APENAS dentro do periodo ativo de cada produto
    (da primeira venda ate a semana atual).
    """
    df = daily_sales.copy()
    df["week_start"] = df["order_date"].dt.to_period("W").apply(lambda r: r.start_time)

    # Agregar por semana e produto
    weekly = (
        df.groupby(["week_start", "product_id"])
        .agg(
            product_name=("product_name", "first"),
            category=("category", "first"),
            quantity_sold=("quantity_sold", "sum"),
            revenue=("revenue", "sum"),
            days_with_sales=("order_date", "nunique"),
        )
        .reset_index()
    )

    # Preencher semanas faltantes por produto (apenas no periodo ativo)
    today_monday = pd.Timestamp.now().normalize()
    today_monday = today_monday - pd.Timedelta(days=today_monday.weekday())

    all_frames = []
    for pid, group in weekly.groupby("product_id"):
        pname = group["product_name"].iloc[-1]
        cat = group["category"].iloc[-1]

        first_week = group["week_start"].min()
        week_range = pd.date_range(first_week, today_monday, freq="W-MON")

        idx = pd.DataFrame({"week_start": week_range})
        merged = idx.merge(group, on="week_start", how="left")
        merged["product_id"] = pid
        merged["product_name"] = pname
        merged["category"] = cat
        merged["quantity_sold"] = merged["quantity_sold"].fillna(0)
        merged["revenue"] = merged["revenue"].fillna(0)
        merged["days_with_sales"] = merged["days_with_sales"].fillna(0)
        all_frames.append(merged)

    result = pd.concat(all_frames, ignore_index=True)
    print(f"  {len(result)} registros semanais para {result['product_id'].nunique()} produtos.")
    return result


def create_weekly_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria features temporais e de lag para dados semanais.
    Inclui: sazonalidade, lags 1-4 semanas, rolling mean/std/max, velocidade.
    """
    df = df.copy().sort_values(["product_id", "week_start"])

    # Features temporais
    df["month"] = df["week_start"].dt.month
    df["week_of_year"] = df["week_start"].dt.isocalendar().week.astype(int)
    df["quarter"] = df["week_start"].dt.quarter

    # Lags por produto (vendas das semanas anteriores)
    for lag in [1, 2, 3, 4]:
        df[f"lag_{lag}w"] = df.groupby("product_id")["quantity_sold"].shift(lag)

    # Rolling statistics (janela de 4 semanas, shift para evitar data leakage)
    g = df.groupby("product_id")["quantity_sold"]
    df["rolling_mean_4w"] = g.transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).mean()
    )
    df["rolling_std_4w"] = g.transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).std().fillna(0)
    )
    df["rolling_max_4w"] = g.transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).max()
    )
    df["rolling_mean_8w"] = g.transform(
        lambda x: x.shift(1).rolling(8, min_periods=1).mean()
    )

    # Vendas acumuladas
    df["cumulative_sales"] = df.groupby("product_id")["quantity_sold"].cumsum()

    # Semanas desde inicio das vendas do produto
    df["weeks_active"] = df.groupby("product_id").cumcount()

    # Velocidade (variacao entre semanas)
    df["sales_velocity"] = df.groupby("product_id")["quantity_sold"].diff().shift(1)

    # Preencher NaN com 0
    fill_cols = [c for c in df.columns if "lag_" in c or "rolling_" in c or c == "sales_velocity"]
    df[fill_cols] = df[fill_cols].fillna(0)

    return df


# Features usadas pelo modelo
FEATURE_COLS = [
    "month", "week_of_year", "quarter",
    "lag_1w", "lag_2w", "lag_3w", "lag_4w",
    "rolling_mean_4w", "rolling_std_4w", "rolling_max_4w",
    "rolling_mean_8w",
    "cumulative_sales", "weeks_active", "sales_velocity",
]


# ============================================================
# 5. TREINAMENTO E PREVISÃO
# ============================================================

def trim_to_active_phase(product_data):
    """
    Remove semanas antigas inativas do inicio dos dados.
    Detecta a 'fase ativa' atual: o periodo de vendas mais recente,
    separado por um gap de > MAX_GAP_WEEKS semanas sem vendas.
    Retorna apenas dados da fase ativa (+ 4 semanas de warmup para lags).
    """
    data = product_data.sort_values("week_start").copy()

    # Indices das semanas com vendas > 0
    sale_idx = data.index[data["quantity_sold"] > 0].tolist()
    all_idx = data.index.tolist()

    if len(sale_idx) <= 1:
        return data.tail(MAX_TRAIN_WINDOW)

    # Percorrer de tras pra frente e encontrar o primeiro gap grande
    active_start_pos = 0
    for i in range(len(sale_idx) - 1, 0, -1):
        pos_current = all_idx.index(sale_idx[i])
        pos_prev = all_idx.index(sale_idx[i - 1])
        gap = pos_current - pos_prev

        if gap > MAX_GAP_WEEKS:
            # Encontrou gap - fase ativa comeca na semana com venda apos o gap
            # Incluir 4 semanas antes para warmup de lags
            active_start_pos = max(0, all_idx.index(sale_idx[i]) - 4)
            break

    trimmed = data.iloc[active_start_pos:]

    # Tambem limitar ao maximo de semanas
    if len(trimmed) > MAX_TRAIN_WINDOW:
        trimmed = trimmed.tail(MAX_TRAIN_WINDOW)

    return trimmed


def predict_smart_average(product_weekly, pname, cat, pid, forecast_days, today):
    """
    Previsao por media ponderada exponencial com ajuste de tendencia.
    Usado para produtos com poucas semanas ou como fallback.
    """
    recent = product_weekly.tail(8)
    sales = recent["quantity_sold"].values

    if len(sales) == 0 or np.sum(sales) == 0:
        return None, None

    # Media ponderada exponencial (mais peso nas semanas recentes)
    n = len(sales)
    weights = np.exp(np.linspace(-1.5, 0, n))
    avg_weekly = float(np.average(sales, weights=weights))

    # Fator de tendencia: comparar primeira e segunda metade
    if n >= 4:
        first_half = np.mean(sales[:n // 2])
        second_half = np.mean(sales[n // 2:])
        if first_half > 0:
            trend = second_half / first_half
            trend = max(0.7, min(1.5, trend))  # limitar entre 0.7x e 1.5x
        else:
            trend = 1.0
    else:
        trend = 1.0

    avg_weekly_adj = avg_weekly * trend
    daily_avg = avg_weekly_adj / 7

    if daily_avg < 0.01:
        return None, None

    future_dates = pd.date_range(today + timedelta(days=1), periods=forecast_days)
    future_df = pd.DataFrame({
        "order_date": future_dates,
        "predicted_quantity": np.round(daily_avg, 2),
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
        "train_size": len(product_weekly),
        "test_size": 0,
        "method": "weighted_average",
    }

    return future_df, metrics


def predict_with_ml(product_data, pname, cat, pid, forecast_days, forecast_weeks, today):
    """
    Previsao com GradientBoosting e forecasting recursivo.
    Usado para produtos com dados suficientes na fase ativa.
    """
    X = product_data[FEATURE_COLS]
    y = product_data["quantity_sold"]

    # Split temporal 80/20
    split_idx = int(len(X) * 0.8)
    split_idx = max(split_idx, 4)
    if split_idx >= len(X):
        split_idx = len(X) - 1

    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    # Sample weights: exponencial, recentes pesam mais
    n_train = len(X_train)
    sample_weight = np.exp(np.linspace(-2, 0, n_train))

    # GradientBoosting com learning rate mais alto para datasets menores
    model = GradientBoostingRegressor(
        n_estimators=300,
        max_depth=3,
        learning_rate=0.1,
        subsample=0.8,
        min_samples_leaf=2,
        random_state=42,
    )
    model.fit(X_train, y_train, sample_weight=sample_weight)

    # Avaliar no conjunto de teste
    if len(X_test) > 0:
        y_pred_test = np.maximum(model.predict(X_test), 0)
        mae = mean_absolute_error(y_test, y_pred_test)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
        r2 = r2_score(y_test, y_pred_test) if len(y_test) > 1 else 0
    else:
        mae = rmse = r2 = 0

    # Cap: previsao nao pode ultrapassar 2x o maximo historico semanal
    hist_max_weekly = product_data["quantity_sold"].max()
    pred_cap = max(hist_max_weekly * 2, 1)

    # --- Forecasting recursivo (semana a semana) ---
    recent_sales = list(product_data["quantity_sold"].tail(8).values)
    cum_sales = float(product_data["cumulative_sales"].iloc[-1])
    weeks_active_val = int(product_data["weeks_active"].iloc[-1])
    last_week = product_data["week_start"].iloc[-1]

    weekly_preds = []
    for w in range(forecast_weeks):
        fw = last_week + pd.Timedelta(weeks=w + 1)

        feat = {
            "month": fw.month,
            "week_of_year": int(fw.isocalendar().week),
            "quarter": (fw.month - 1) // 3 + 1,
            "lag_1w": recent_sales[-1] if len(recent_sales) >= 1 else 0,
            "lag_2w": recent_sales[-2] if len(recent_sales) >= 2 else 0,
            "lag_3w": recent_sales[-3] if len(recent_sales) >= 3 else 0,
            "lag_4w": recent_sales[-4] if len(recent_sales) >= 4 else 0,
            "rolling_mean_4w": float(np.mean(recent_sales[-4:])),
            "rolling_std_4w": float(np.std(recent_sales[-4:])) if len(recent_sales) >= 2 else 0,
            "rolling_max_4w": float(max(recent_sales[-4:])),
            "rolling_mean_8w": float(np.mean(recent_sales[-8:])),
            "cumulative_sales": cum_sales,
            "weeks_active": weeks_active_val + w + 1,
            "sales_velocity": (recent_sales[-1] - recent_sales[-2]) if len(recent_sales) >= 2 else 0,
        }

        pred = float(model.predict(pd.DataFrame([feat]))[0])
        pred = max(min(pred, pred_cap), 0)

        weekly_preds.append({"week_start": fw, "pred_weekly": round(pred, 2)})
        recent_sales.append(pred)
        cum_sales += pred

    # Converter previsoes semanais para diarias
    future_dates = pd.date_range(today + timedelta(days=1), periods=forecast_days)
    daily_preds = []

    for date in future_dates:
        week_monday = date - pd.Timedelta(days=date.weekday())
        # Encontrar a previsao semanal mais proxima
        best = None
        best_dist = 999
        for wp in weekly_preds:
            dist = abs((wp["week_start"] - week_monday).days)
            if dist < best_dist:
                best_dist = dist
                best = wp

        daily_pred = round(best["pred_weekly"] / 7, 2) if best else 0
        daily_preds.append({
            "order_date": date,
            "predicted_quantity": daily_pred,
            "product_id": pid,
            "product_name": pname,
            "category": cat,
        })

    future_df = pd.DataFrame(daily_preds)

    metrics = {
        "product_id": pid,
        "product_name": pname,
        "category": cat,
        "mae": round(mae, 2),
        "rmse": round(rmse, 2),
        "r2_score": round(r2, 3),
        "train_size": int(len(X_train)),
        "test_size": int(len(X_test)),
        "method": "gradient_boosting",
    }

    return future_df, metrics


def train_and_predict(daily_sales: pd.DataFrame, forecast_days: int = 30):
    """
    Pipeline completo: dados semanais -> features -> treino -> previsao.
    Usa GradientBoosting para produtos com dados suficientes,
    media ponderada para produtos com poucos dados.
    """
    forecast_weeks = (forecast_days + 6) // 7
    today = pd.Timestamp.now().normalize()

    # 1) Criar dados semanais com preenchimento inteligente
    print("\n[*] Criando dados semanais...")
    weekly_data = create_weekly_data(daily_sales)
    weekly_data = create_weekly_features(weekly_data)

    # 2) Filtrar produtos ativos (vendas recentes)
    cutoff = today - pd.Timedelta(weeks=ACTIVE_WINDOW_WEEKS)
    active_pids = set(
        daily_sales[daily_sales["order_date"] >= cutoff]["product_id"].unique()
    )
    print(f"\n  Produtos ativos (ultimas {ACTIVE_WINDOW_WEEKS} semanas): {len(active_pids)}")

    # 3) Treinar e prever por produto
    print(f"\n[*] Treinando modelos ({forecast_weeks} semanas / ~{forecast_days} dias)...\n")

    results = []
    model_metrics = []

    for pid in sorted(active_pids):
        product_data = weekly_data[weekly_data["product_id"] == pid].sort_values("week_start")
        if product_data.empty:
            continue

        pname = product_data["product_name"].iloc[-1]
        cat = product_data["category"].iloc[-1]

        # CORRECAO CHAVE: trimmar para fase ativa (remover zeros antigos)
        product_data = trim_to_active_phase(product_data)
        n_active_weeks = len(product_data)
        n_nonzero = (product_data["quantity_sold"] > 0).sum()

        if n_active_weeks >= MIN_WEEKS_ML and n_nonzero >= 4:
            # ML approach
            future_df, metrics = predict_with_ml(
                product_data, pname, cat, pid, forecast_days, forecast_weeks, today
            )
            if future_df is not None:
                results.append(future_df)
                model_metrics.append(metrics)
                avg_pred = future_df["predicted_quantity"].mean()
                print(f"  [ML]  {pname}: pred={avg_pred:.2f}/dia | MAE={metrics['mae']:.2f} | R2={metrics['r2_score']:.3f} | {n_active_weeks}w ({n_nonzero} com vendas)")
        else:
            # Smart average
            future_df, metrics = predict_smart_average(
                product_data, pname, cat, pid, forecast_days, today
            )
            if future_df is not None:
                results.append(future_df)
                model_metrics.append(metrics)
                avg = future_df["predicted_quantity"].iloc[0]
                print(f"  [AVG] {pname}: pred={avg:.2f}/dia | {n_active_weeks}w ({n_nonzero} com vendas)")

    metrics_df = pd.DataFrame(model_metrics)
    predictions_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    return predictions_df, metrics_df


# ============================================================
# 6. VISUALIZAÇÕES
# ============================================================

def plot_sales_overview(daily_sales: pd.DataFrame):
    """Grafico geral de vendas ao longo do tempo."""
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
    """Grafico de vendas historicas + previsoes para os top N produtos."""
    if predictions_df.empty:
        print("  Sem previsoes para plotar.")
        return

    # Selecionar os top_n produtos com mais vendas entre os que tem previsao
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

        # Dados historicos
        hist = daily_sales[daily_sales["product_id"] == pid].sort_values("order_date")
        ax.plot(hist["order_date"], hist["quantity_sold"],
                label="Historico", color="#2196F3", linewidth=1.2, alpha=0.8)

        # Previsoes
        pred = predictions_df[predictions_df["product_id"] == pid].sort_values("order_date")
        if not pred.empty:
            ax.plot(pred["order_date"], pred["predicted_quantity"],
                    label="Previsao", color="#FF5722", linewidth=2, linestyle="--")
            ax.fill_between(pred["order_date"], pred["predicted_quantity"],
                            alpha=0.15, color="#FF5722")

            # Linha separadora
            ax.axvline(x=hist["order_date"].max(), color="gray",
                       linestyle=":", alpha=0.7, label="Inicio previsao")

        title = pname if len(pname) <= 40 else pname[:37] + "..."
        ax.set_title(title, fontsize=11, fontweight="bold")
        ax.tick_params(axis="x", rotation=45)
        ax.legend(fontsize=8)

    # Esconder eixos vazios
    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    plt.suptitle("Previsao de Vendas por Produto", fontsize=16, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig("previsao_vendas.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  [OK] Grafico salvo: previsao_vendas.png")


def print_forecast_summary(predictions_df: pd.DataFrame, metrics_df: pd.DataFrame):
    """Exibe um resumo das previsoes no console."""
    if predictions_df.empty:
        print("\n  Sem previsoes disponiveis.")
        return

    print("\n" + "=" * 80)
    print("  RESUMO DE PREVISAO DE VENDAS (Proximos 30 dias)")
    print("=" * 80)

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

    # Adicionar metricas do modelo
    summary = summary.merge(
        metrics_df[["product_id", "mae", "r2_score", "method"]],
        on="product_id", how="left"
    )

    print(f"\n{'Produto':<40} {'Total':<10} {'Media/d':<10} {'MAE':<8} {'R2':<8} {'Metodo':<18}")
    print("-" * 94)

    for _, row in summary.iterrows():
        name = row["product_name"]
        if len(name) > 38:
            name = name[:35] + "..."
        method = row.get("method", "?")
        print(
            f"{name:<40} "
            f"{row['total_previsto']:<10.1f} "
            f"{row['media_diaria']:<10.2f} "
            f"{row['mae']:<8.2f} "
            f"{row['r2_score']:<8.3f} "
            f"{method:<18}"
        )

    print("-" * 94)
    print(f"{'TOTAL':<40} {summary['total_previsto'].sum():<10.1f}")
    print("=" * 80)


# ============================================================
# 7. EXECUÇÃO PRINCIPAL
# ============================================================

def main():
    print("=" * 60)
    print("   SISTEMA DE PREVISAO DE VENDAS - WooCommerce")
    print("   (v2 - GradientBoosting + Dados Semanais)")
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

    # 4. Treinar modelos e gerar previsoes
    predictions_df, metrics_df = train_and_predict(daily_sales, forecast_days=30)

    # 5. Exibir metricas
    if not metrics_df.empty:
        print(f"\n[*] Modelos treinados: {len(metrics_df)}")
        ml_count = (metrics_df["method"] == "gradient_boosting").sum()
        avg_count = (metrics_df["method"] == "weighted_average").sum()
        print(f"    GradientBoosting: {ml_count} | Media Ponderada: {avg_count}")

    # 6. Visualizar previsoes
    print("\n[*] Gerando graficos de previsao...")
    plot_predictions(daily_sales, predictions_df)

    # 7. Resumo final
    print_forecast_summary(predictions_df, metrics_df)

    # 8. Salvar dados em CSV
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
