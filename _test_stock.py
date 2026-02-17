import db, time

print("=== Step 1: Reset test 2 (60482) to stock=3 ===")
ok = db.wc_update_stock(60482, 3)
print(f"  Reset success: {ok}")
live = db.wc_get_stock(60482)
print(f"  Verified: stock={live['stock_quantity']}, sold={live['total_sales']}")

print("\n=== Step 2: Wait 5 seconds ===")
time.sleep(5)

print("\n=== Step 3: Run auto-replenish ===")
actions = db.auto_replenish_stock()
for a in actions:
    s = "OK" if a["success"] else "FAILED"
    print(f"  {a['product_name']}: {a['old_stock']} -> {a['new_stock']} (+{a['added']}) [{s}]")
if not actions:
    print("  No products needed replenishment")

print("\n=== Step 4: Verify from WooCommerce ===")
live2 = db.wc_get_stock(60482)
print(f"  Final stock: {live2['stock_quantity']}, sold={live2['total_sales']}")
print(f"  Expected: stock=6 (3 + 3 replenish)")
