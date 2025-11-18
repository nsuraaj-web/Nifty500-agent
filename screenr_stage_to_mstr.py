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
                return None  # convert NaN to None
            # Clamp value
            if num > max_value:
                num = max_value
            elif num < -max_value:
                num = -max_value
            return round(num, 2)
        return None
    except:
        return None



def fetch_screenr_stage():
    response = supabase.from_("screenr_stage").select("*").execute()
    return response.data if hasattr(response, 'data') else []


def upsert_screenr_master(data):
    supabase.from_("screenr_master").upsert(data, on_conflict=["ticker"]).execute()


def main():
    records = fetch_screenr_stage()
    for rec in records:
        master_row = {
            "ticker": rec.get("ticker"),
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
        upsert_screenr_master(master_row)
        print(f"screenr_master updated for {rec.get('ticker')}")


if __name__ == "__main__":
    main()
