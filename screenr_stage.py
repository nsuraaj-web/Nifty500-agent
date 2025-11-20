import os
import time
import math
import re
from io import StringIO

import requests
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client, Client

# Try local Windows .env path (your current setup),
# and also fallback to `.env` in repo root (for CI like GitHub Actions).
load_dotenv(dotenv_path=r"C:\Project\nifty500\.env")
load_dotenv()  # fallback: current directory

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Supabase credentials missing: check SUPABASE_URL & SUPABASE_SERVICE_KEY / SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_tickers(master_table: str):
    """Load list of tickers from master_universe (or other table)."""
    response = supabase.from_(master_table).select("ticker").execute()
    if hasattr(response, "data") and response.data:
        return [row["ticker"] for row in response.data if "ticker" in row]
    return []


# -----------------------------
# Core Screener fetch logic (single attempt)
# -----------------------------
def fetch_screener_fields_raw(ticker: str) -> dict:
    """
    Do ONE attempt to fetch Screener data for a ticker.
    All retry/backoff logic is in fetch_screener_fields().
    """
    url = f"https://www.screener.in/company/{ticker}/"
    headers = {"User-Agent": "Mozilla/5.0"}

    r = requests.get(url, headers=headers, timeout=20)
    # Raise HTTPError for 4xx/5xx so caller can decide on retry
    r.raise_for_status()

    html = r.text
    soup = BeautifulSoup(html, "html.parser")  # kept if you want more parsing later

    result = {"ticker": ticker.upper()}

    # --- Shareholding pattern ---
    def extract_shareholding(label: str):
        pattern = rf"{label}.*?([\d\.]+)%"
        m = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
        return m.group(1) if m else None

    result["promoters_pct"] = extract_shareholding("Promoters")
    result["fii_pct"] = extract_shareholding("FIIs")
    result["dii_pct"] = extract_shareholding("DIIs")
    result["govt_pct"] = extract_shareholding("Government")
    result["public_pct"] = extract_shareholding("Public")

    # --- Parse tables with pandas ---
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        tables = []

    def extract_ratio(field_name: str):
        for t in tables:
            try:
                if t.iloc[:, 0].str.contains(field_name, case=False, na=False).any():
                    val = t[
                        t.iloc[:, 0].str.contains(field_name, case=False, na=False)
                    ].iloc[0, -1]
                    return str(val).replace("%", "").strip()
            except Exception:
                continue
        return None

    ratio_fields = [
        ("Debtor Days", "debtor_days"),
        ("Cash Conversion Cycle", "cash_conversion_cycle"),
        ("Working Capital Days", "working_capital_days"),
        ("ROCE", "roce_pct"),
        ("OPM", "opm_pct"),
        ("Net Profit", "net_profit_pct"),
    ]

    for field, key in ratio_fields:
        result[key] = extract_ratio(field)

    # --- Summary values (like Book Value) ---
    def extract_summary_value(label: str):
        pattern = rf"{label}.*?([\d\.,]+)"
        m = re.search(pattern, html, re.IGNORECASE)
        return m.group(1).replace(",", "") if m else None

    result["book_value"] = extract_summary_value("Book Value")

    # --- Net Cash Flow (latest) ---
    result["net_cash_flow_latest"] = None
    for t in tables:
        try:
            if t.iloc[:, 0].str.contains("Net Cash Flow", case=False, na=False).any():
                cash_flow_val = t[
                    t.iloc[:, 0].str.contains("Net Cash Flow", case=False, na=False)
                ].iloc[0, -1]
                result["net_cash_flow_latest"] = (
                    str(cash_flow_val).replace(",", "").strip()
                )
                break
        except Exception:
            continue

    return result


# -----------------------------
# Retry wrapper with backoff
# -----------------------------
def fetch_screener_fields(ticker: str, max_retries: int = 3, base_delay: float = 3.0):
    """
    Retry wrapper for Screener.in fetch to handle 429 / transient failures.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return fetch_screener_fields_raw(ticker)
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            msg = f"HTTP {status}" if status else str(e)

            # If Too Many Requests, retry with backoff
            if status == 429 and attempt < max_retries:
                wait = base_delay * attempt
                print(
                    f"[WARN] Screener 429 for {ticker} "
                    f"(attempt {attempt}/{max_retries}). Waiting {wait}s..."
                )
                time.sleep(wait)
                continue
            else:
                print(f"[ERROR] Screener HTTP error for {ticker}: {msg}")
                return None
        except Exception as e:
            msg = str(e)
            # Sometimes 429 might appear just as text in an exception message
            if ("429" in msg or "Too Many Requests" in msg) and attempt < max_retries:
                wait = base_delay * attempt
                print(
                    f"[WARN] Screener 429-like error for {ticker} "
                    f"(attempt {attempt}/{max_retries}). Waiting {wait}s..."
                )
                time.sleep(wait)
                continue
            else:
                print(f"[ERROR] Screener error for {ticker}: {msg}")
                return None

    print(f"[ERROR] Screener failed for {ticker} after {max_retries} attempts.")
    return None


def stringify_data(data: dict):
    """Convert all values to strings or None, avoiding NaN and infinite float issues."""
    stringified = {}
    for k, v in data.items():
        if v is None:
            stringified[k] = None
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            stringified[k] = None
        else:
            stringified[k] = str(v)
    return stringified


def upsert_screenr(table: str, data: dict):
    if data:
        data_str = stringify_data(data)
        supabase.from_(table).upsert(data_str, on_conflict=["ticker"]).execute()


def main():
    master_table = "master_universe"
    staging_table = "screenr_stage"

    tickers = fetch_tickers(master_table)

    # Optional limit for CI / GitHub Actions, e.g. SCREENER_MAX_TICKERS=200
    max_tickers = int(os.getenv("SCREENER_MAX_TICKERS", "0"))
    if max_tickers > 0:
        tickers = tickers[:max_tickers]

    print(f"[INFO] Screener: processing {len(tickers)} tickers from {master_table}...")

    for i, ticker in enumerate(tickers, start=1):
        print(f"[INFO] ({i}/{len(tickers)}) Screener fetch {ticker}...")
        data = fetch_screener_fields(ticker)
        if data:
            upsert_screenr(staging_table, data)
            print(f"[OK] Screener synced {ticker} to staging.")
        else:
            print(f"[SKIP] Screener skipped {ticker} (no data).")

        # Polite delay to avoid hammering Screener
        time.sleep(0.5)


if __name__ == "__main__":
    main()
