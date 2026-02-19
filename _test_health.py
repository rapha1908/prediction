from order_bumps import health, setup, analytics_summary

print("=== Health Check ===")
h = health()
for k, v in h.items():
    print(f"  {k}: {v}")

print()
if h.get("error"):
    print("Health endpoint not available, trying setup...")
    print()
    s = setup()
    print("=== Setup Result ===")
    for k, v in s.items():
        print(f"  {k}: {v}")
elif not h.get("tables_exist"):
    print("Tables missing! Running setup...")
    print()
    s = setup()
    print("=== Setup Result ===")
    for k, v in s.items():
        print(f"  {k}: {v}")
else:
    print("Tables OK!")

print()
print("=== Analytics (all time) ===")
a = analytics_summary(date_from="2020-01-01", date_to="2026-12-31")
for k, v in a.items():
    print(f"  {k}: {v}")
