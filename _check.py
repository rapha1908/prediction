import pandas as pd

df = pd.read_csv("previsoes_vendas.csv")
print("=== PREVISOES ===")
print(f"Total registros: {len(df)}")
print(f"Produtos: {df['product_id'].nunique()}")
print(f"Pred > 0: {(df['predicted_quantity'] > 0).sum()}")
print(f"Max pred: {df['predicted_quantity'].max()}")
print(f"Mean pred (>0): {df[df['predicted_quantity']>0]['predicted_quantity'].mean():.4f}")
print()

top = (
    df.groupby(["product_id", "product_name"])["predicted_quantity"]
    .agg(["sum", "mean", "max"])
    .sort_values("sum", ascending=False)
    .head(25)
)
print(top.to_string())

print("\n=== HISTORICO RECENTE (ultimos 3 meses) ===")
h = pd.read_csv("vendas_historicas.csv", parse_dates=["order_date"])
cutoff = h["order_date"].max() - pd.Timedelta(days=90)
recent = h[h["order_date"] >= cutoff]
weekly = (
    recent.groupby("product_id")
    .agg(
        name=("product_name", "first"),
        total_qty=("quantity_sold", "sum"),
        n_days=("order_date", "nunique"),
    )
    .reset_index()
)
weekly["avg_daily"] = (weekly["total_qty"] / weekly["n_days"]).round(2)
weekly["avg_weekly"] = (weekly["total_qty"] / (90/7)).round(2)

# Merge with predictions
pred_totals = df.groupby("product_id")["predicted_quantity"].sum().reset_index()
pred_totals.columns = ["product_id", "pred_total_30d"]

comp = weekly.merge(pred_totals, on="product_id", how="left").fillna(0)
comp["pred_daily"] = (comp["pred_total_30d"] / 30).round(2)
comp = comp.sort_values("total_qty", ascending=False).head(20)

print(f"{'Produto':<55} {'Hist avg/d':>10} {'Pred avg/d':>10} {'Ratio':>8}")
print("-" * 88)
for _, r in comp.iterrows():
    name = r["name"][:52]
    ratio = r["pred_daily"] / r["avg_daily"] if r["avg_daily"] > 0 else 0
    print(f"{name:<55} {r['avg_daily']:>10.2f} {r['pred_daily']:>10.2f} {ratio:>8.2f}")
