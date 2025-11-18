import requests
import re
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
import time
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(dotenv_path=r"C:\Project\nifty500\.env")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_tickers(master_table):
    response = supabase.from_(master_table).select("ticker").execute()
    if hasattr(response, 'data') and response.data:
        return [row["ticker"] for row in response.data if "ticker" in row]
    return []

def fetch_screener_fields(ticker):
    url = f"https://www.screener.in/company/{ticker}/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    html = r.text
    result = {"ticker": ticker.upper()}

    def extract_shareholding(label):
        pattern = rf'{label}.*?([\d\.]+)%'
        m = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
        return m.group(1) if m else None

    result['promoters_pct'] = extract_shareholding('Promoters')
    result['fii_pct'] = extract_shareholding('FIIs')
    result['dii_pct'] = extract_shareholding('DIIs')
    result['govt_pct'] = extract_shareholding('Government')
    result['public_pct'] = extract_shareholding('Public')

    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        tables = []

    def extract_ratio(field_name):
        for t in tables:
            try:
                if t.iloc[:,0].str.contains(field_name, case=False, na=False).any():
                    val = t[t.iloc[:,0].str.contains(field_name, case=False, na=False)].iloc[0,-1]
                    return str(val).replace('%','').strip()
            except Exception:
                continue
        return None

    ratio_fields = [
        ('Debtor Days', 'debtor_days'),
        ('Cash Conversion Cycle', 'cash_conversion_cycle'),
        ('Working Capital Days', 'working_capital_days'),
        ('ROCE', 'roce_pct'),
        ('OPM', 'opm_pct'),
        ('Net Profit', 'net_profit_pct'),
    ]

    for field, key in ratio_fields:
        result[key] = extract_ratio(field)

    def extract_growth(metric, years):
        pattern = rf'{metric}.*?{years} Years:.*?([\d\.\-]+)'
        m = re.search(pattern, html, re.IGNORECASE)
        return m.group(1) if m else None

    def extract_summary_value(label):
        pattern = rf'{label}.*?([\d\.,]+)'
        m = re.search(pattern, html, re.IGNORECASE)
        return m.group(1).replace(',', '') if m else None

    result['book_value'] = extract_summary_value('Book Value')

    result['net_cash_flow_latest'] = None
    for t in tables:
        if 'Net Cash Flow' in t.to_string():
            try:
                cash_flow_val = t[t.iloc[:, 0].str.contains('Net Cash Flow', case=False, na=False)].iloc[0, -1]
                result['net_cash_flow_latest'] = str(cash_flow_val).replace(',', '').strip()
                break
            except Exception:
                continue
    return result

def stringify_data(data):
    import math
    stringified = {}
    for k, v in data.items():
        if v is None:
            stringified[k] = None
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            stringified[k] = None
        else:
            stringified[k] = str(v)
    return stringified

def upsert_screenr(table, data):
    if data:
        data_str = stringify_data(data)
        supabase.from_(table).upsert(data_str, on_conflict=["ticker"]).execute()

def main():
    master_table = "master_universe"
    staging_table = "screenr_stage"
    tickers = fetch_tickers(master_table)
    for ticker in tickers:
        data = fetch_screener_fields(ticker)
        upsert_screenr(staging_table, data)
        print(f"Synced {ticker} to staging.")
        time.sleep(1)  # polite delay

if __name__ == "__main__":
    main()
