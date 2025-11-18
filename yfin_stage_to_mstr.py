import os
from dotenv import load_dotenv
from supabase import create_client, Client
import datetime
import math

load_dotenv(dotenv_path=r"C:\Project\nifty500\.env")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def clean_numeric(value, max_value=9999999999.99):
    try:
        if value is None:
            return None
        val_str = str(value).replace(",", "").replace("%", "").strip()
        num = float(val_str) if val_str != '' else None
        if num is not None:
            if math.isnan(num):
                return None
            # Clamp value
            if num > max_value:
                num = max_value
            elif num < -max_value:
                num = -max_value
            return round(num, 2)
        return None
    except:
        return None

def clean_date(value):
    try:
        if not value or value == "":
            return None
        # Assuming ISO format YYYY-MM-DD, modify if needed for other formats
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
    except:
        return None

def clean_datetime(value):
    try:
        if not value or value == "":
            return None
        # Try parsing ISO datetime strings
        return datetime.datetime.fromisoformat(value)
    except:
        return None

def fetch_yfin_stage():
    response = supabase.from_("yfin_stage").select("*").execute()
    return response.data if hasattr(response, 'data') else []

def upsert_yfin_master(data):
    supabase.from_("yfin_master").upsert(data, on_conflict=["ticker"]).execute()

def main():
    records = fetch_yfin_stage()
    for rec in records:
        # Convert all date/datetime fields to ISO strings or None
        ex_dividend_date_obj = clean_date(rec.get("ex_dividend_date"))
        ex_dividend_date_str = ex_dividend_date_obj.isoformat() if ex_dividend_date_obj else None

        fetched_at_obj = clean_datetime(rec.get("fetched_at"))
        fetched_at_str = fetched_at_obj.isoformat() if fetched_at_obj else None

        created_at_obj = clean_datetime(rec.get("created_at"))
        created_at_str = created_at_obj.isoformat() if created_at_obj else None

        # last_update set to current UTC timestamp always
        last_update_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

        master_row = {
            "ticker": rec.get("ticker"),
            "current_price": clean_numeric(rec.get("current_price")),
            "previous_close": clean_numeric(rec.get("previous_close")),
            "open_price": clean_numeric(rec.get("open_price")),
            "day_range_high": clean_numeric(rec.get("day_range_high")),
            "day_range_low": clean_numeric(rec.get("day_range_low")),
            "price_3m_ago": clean_numeric(rec.get("price_3m_ago")),
            "price_6m_ago": clean_numeric(rec.get("price_6m_ago")),
            "price_12m_ago": clean_numeric(rec.get("price_12m_ago")),
            "fifty_two_week_high": clean_numeric(rec.get("fifty_two_week_high")),
            "fifty_two_week_low": clean_numeric(rec.get("fifty_two_week_low")),
            "market_cap": clean_numeric(rec.get("market_cap")),
            "enterprise_value": clean_numeric(rec.get("enterprise_value")),
            "pe_ratio_trailing": clean_numeric(rec.get("pe_ratio_trailing")),
            "pe_ratio_forward": clean_numeric(rec.get("pe_ratio_forward")),
            "price_to_sale": clean_numeric(rec.get("price_to_sale")),
            "price_to_book": clean_numeric(rec.get("price_to_book")),
            "enterprise_value_ebitda": clean_numeric(rec.get("enterprise_value_ebitda")),
            "dividend_yield": clean_numeric(rec.get("dividend_yield")),
            "ex_dividend_date": ex_dividend_date_str,
            "net_income": clean_numeric(rec.get("net_income")),
            "gross_margin": clean_numeric(rec.get("gross_margin")),
            "operating_margin": clean_numeric(rec.get("operating_margin")),
            "ebitda": clean_numeric(rec.get("ebitda")),
            "earnings_per_share": clean_numeric(rec.get("earnings_per_share")),
            "book_value_per_share": clean_numeric(rec.get("book_value_per_share")),
            "free_cash_flow": clean_numeric(rec.get("free_cash_flow")),
            "total_revenue": clean_numeric(rec.get("total_revenue")),
            "revenue_3m_ago": clean_numeric(rec.get("revenue_3m_ago")),
            "revenue_6m_ago": clean_numeric(rec.get("revenue_6m_ago")),
            "revenue_12m_ago": clean_numeric(rec.get("revenue_12m_ago")),
            "eps_3m_ago": clean_numeric(rec.get("eps_3m_ago")),
            "eps_6m_ago": clean_numeric(rec.get("eps_6m_ago")),
            "eps_12m_ago": clean_numeric(rec.get("eps_12m_ago")),
            "debt_to_equity": clean_numeric(rec.get("debt_to_equity")),
            "operating_cash_flow": clean_numeric(rec.get("operating_cash_flow")),
            "revenue_growth": clean_numeric(rec.get("revenue_growth")),
            "eps_growth": clean_numeric(rec.get("eps_growth")),
            "analyst_target_price": clean_numeric(rec.get("analyst_target_price")),
            "analyst_high_target": clean_numeric(rec.get("analyst_high_target")),
            "analyst_low_target": clean_numeric(rec.get("analyst_low_target")),
            "analyst_number": int(rec.get("analyst_number")) if rec.get("analyst_number") not in [None, ""] else None,
            "analyst_rating": clean_numeric(rec.get("analyst_rating")),
            "beta": clean_numeric(rec.get("beta")),
            "volume": clean_numeric(rec.get("volume")),
            "avg_volume": clean_numeric(rec.get("avg_volume")),
            "insider_holdings": clean_numeric(rec.get("insider_holdings")),
            "institutional_holdings": clean_numeric(rec.get("institutional_holdings")),
            "fetched_at": fetched_at_str,
            "created_at": created_at_str,
            "last_update": last_update_str
        }
        upsert_yfin_master(master_row)
        print(f"yfin_master updated for {rec.get('ticker')}")

if __name__ == "__main__":
    main()
