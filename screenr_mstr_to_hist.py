import os
from dotenv import load_dotenv
from supabase import create_client, Client
import datetime
import math

load_dotenv(dotenv_path=r"C:\Project\nifty500\.env")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def clean_numeric(value, max_value=999.99):
    try:
        if value is None:
            return None
        val_str = str(value).replace(",", "").replace("%", "").strip()
        num = float(val_str) if val_str != '' else None
        if num is not None:
            if math.isnan(num):
                return None
            if num > max_value:
                num = max_value
            elif num < -max_value:
                num = -max_value
            return round(num, 2)
        return None
    except:
        return None

def fetch_screenr_master():
    response = supabase.from_("screenr_master").select("*").execute()
    return response.data if hasattr(response, 'data') else []

def upsert_screenr_history(data):
    # Corrected the on_conflict parameter to list with separate keys
    supabase.from_("screenr_hist").upsert(data, on_conflict=["ticker,snapshot_date"]).execute()

def main():
    records = fetch_screenr_master()
    today = datetime.date.today().isoformat()
    for rec in records:
        history_row = {
            "ticker": rec.get("ticker"),
            "snapshot_date": today,
            "promoters_pct": clean_numeric(rec.get("promoters_pct")),
            "fii_pct": clean_numeric(rec.get("fii_pct")),
            "dii_pct": clean_numeric(rec.get("dii_pct")),
            "govt_pct": clean_numeric(rec.get("govt_pct")),
            "public_pct": clean_numeric(rec.get("public_pct")),
            "debtor_days": clean_numeric(rec.get("debtor_days")),
            "cash_conversion_cycle": clean_numeric(rec.get("cash_conversion_cycle")),
            "working_capital_days": clean_numeric(rec.get("working_capital_days")),
            "roce_pct": clean_numeric(rec.get("roce_pct")),
            "opm_pct": clean_numeric(rec.get("opm_pct")),
            "net_profit_pct": clean_numeric(rec.get("net_profit_pct")),
            "net_cash_flow_latest": clean_numeric(rec.get("net_cash_flow_latest")),
            "book_value": clean_numeric(rec.get("book_value")),
            "last_update": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        upsert_screenr_history(history_row)
        print(f"screenr_hist updated for {rec.get('ticker')}")

if __name__ == "__main__":
    main()
