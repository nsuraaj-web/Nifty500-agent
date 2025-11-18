from stock_intel_engine import generate_stock_intel

ticker = "TCS"

result = generate_stock_intel(ticker)

print("=== FINAL REPORT ===")
print(result["full_report"])
