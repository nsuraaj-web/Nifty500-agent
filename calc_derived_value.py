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
            if num > max_value:
                num = max_value
            elif num < -max_value:
                num = -max_value
            return round(num, 2)
        return None
    except:
        return None

def fetch_consolidated_master():
    response = supabase.from_("consolidated_master").select("*").execute()
    return response.data if hasattr(response, 'data') else []

def upsert_derived_master(data):
    supabase.from_("derived_master").upsert(data, on_conflict=["ticker"]).execute()

def compute_derived(row):
    derived = {}
    cp = clean_numeric(row.get("current_price"))
    pc = clean_numeric(row.get("previous_close"))
    p3m = clean_numeric(row.get("price_3m_ago"))
    p6m = clean_numeric(row.get("price_6m_ago"))
    p12m = clean_numeric(row.get("price_12m_ago"))
    f52w_high = clean_numeric(row.get("fifty_two_week_high"))
    f52w_low = clean_numeric(row.get("fifty_two_week_low"))
    
    # Returns
    derived["price_return_3m_pct"] = round((cp - p3m) / p3m * 100, 2) if cp and p3m else None
    derived["price_return_6m_pct"] = round((cp - p6m) / p6m * 100, 2) if cp and p6m else None
    derived["price_return_12m_pct"] = round((cp - p12m) / p12m * 100, 2) if cp and p12m else None
    derived["position_52w_pct"] = round((cp - f52w_low) / (f52w_high - f52w_low) * 100, 2) if cp and f52w_high and f52w_low and (f52w_high - f52w_low) != 0 else None
    derived["ytd_return_pct"] = round((cp - pc) / pc * 100, 2) if cp and pc else None
    
    # Ratios
    market_cap = clean_numeric(row.get("market_cap"))
    total_revenue = clean_numeric(row.get("total_revenue"))
    derived["market_cap_to_revenue"] = round(market_cap / total_revenue, 2) if market_cap and total_revenue else None
    
    enterprise_value = clean_numeric(row.get("enterprise_value"))
    ebitda = clean_numeric(row.get("ebitda"))
    derived["enterprise_value_to_ebitda"] = round(enterprise_value / ebitda, 2) if enterprise_value and ebitda else None

    pe_ratio_trailing = clean_numeric(row.get("pe_ratio_trailing"))
    sector_pe = None  # You need to supply this value externally
    derived["pe_discount_vs_sector"] = round(pe_ratio_trailing - sector_pe, 2) if pe_ratio_trailing and sector_pe else None

    gross_margin = clean_numeric(row.get("gross_margin"))
    derived["gross_margin_pct"] = gross_margin

    operating_margin = clean_numeric(row.get("operating_margin"))
    derived["operating_margin_pct"] = operating_margin

    net_income = clean_numeric(row.get("net_income"))
    derived["net_margin_pct"] = round(net_income / total_revenue * 100, 2) if net_income and total_revenue else None

    # EPS and Revenue Growth (point-in-time)
    eps = clean_numeric(row.get("earnings_per_share"))
    eps_3m = clean_numeric(row.get("eps_3m_ago"))
    eps_6m = clean_numeric(row.get("eps_6m_ago"))
    eps_12m = clean_numeric(row.get("eps_12m_ago"))
    derived["eps_growth_3m_pct"] = round((eps - eps_3m) / eps_3m * 100, 2) if eps and eps_3m else None
    derived["eps_growth_6m_pct"] = round((eps - eps_6m) / eps_6m * 100, 2) if eps and eps_6m else None
    derived["eps_growth_12m_pct"] = round((eps - eps_12m) / eps_12m * 100, 2) if eps and eps_12m else None

    rev_3m = clean_numeric(row.get("revenue_3m_ago"))
    rev_6m = clean_numeric(row.get("revenue_6m_ago"))
    rev_12m = clean_numeric(row.get("revenue_12m_ago"))
    derived["revenue_growth_3m_pct"] = round((total_revenue - rev_3m) / rev_3m * 100, 2) if total_revenue and rev_3m else None
    derived["revenue_growth_6m_pct"] = round((total_revenue - rev_6m) / rev_6m * 100, 2) if total_revenue and rev_6m else None
    derived["revenue_growth_12m_pct"] = round((total_revenue - rev_12m) / rev_12m * 100, 2) if total_revenue and rev_12m else None

    # Additional ratios
    bvps = clean_numeric(row.get("book_value_per_share"))
    derived["book_to_price_ratio"] = round(bvps / cp, 2) if bvps and cp and cp != 0 else None
    
    eps_growth = clean_numeric(row.get("eps_growth"))
    derived["peg_ratio"] = round(pe_ratio_trailing / eps_growth, 2) if pe_ratio_trailing and eps_growth and eps_growth != 0 else None

    dividend_yield = clean_numeric(row.get("dividend_yield"))
    derived["dividend_yield_pct"] = dividend_yield
    
    beta = clean_numeric(row.get("beta"))
    derived["beta"] = beta
    
    debt_to_equity = clean_numeric(row.get("debt_to_equity"))
    derived["debt_to_equity"] = debt_to_equity
    
    free_cash_flow = clean_numeric(row.get("free_cash_flow"))
    derived["free_cash_flow_margin"] = round(free_cash_flow / total_revenue * 100, 2) if free_cash_flow and total_revenue and total_revenue != 0 else None
    
    operating_cash_flow = clean_numeric(row.get("operating_cash_flow"))
    derived["operating_cash_flow_margin"] = round(operating_cash_flow / total_revenue * 100, 2) if operating_cash_flow and total_revenue and total_revenue != 0 else None
    
    derived["dividend_payout_ratio"] = round(dividend_yield / eps, 2) if dividend_yield and eps and eps != 0 else None
    
    analyst_target = clean_numeric(row.get("analyst_target_price"))
    derived["upside_potential_pct"] = round((analyst_target - cp) / cp * 100, 2) if analyst_target and cp and cp != 0 else None

    return derived

def main():
    rows = fetch_consolidated_master()
    today = datetime.date.today().isoformat()
    for row in rows:
        derived_row = {
            "ticker": row.get("ticker"),
            "snapshot_date": today,
            **compute_derived(row)
        }
        upsert_derived_master(derived_row)
        print(f"derived_master updated for {row.get('ticker')}")

if __name__ == "__main__":
    main()
