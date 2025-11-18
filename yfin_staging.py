import os
from supabase import create_client, Client
from dotenv import load_dotenv
import yfinance
import datetime
import math

load_dotenv(dotenv_path=r"C:\Project\nifty500\.env")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def unix_to_iso_utc(unix_ts):
    """
    Convert a UNIX timestamp (seconds since epoch) to an ISO 8601 datetime string (UTC timezone).
    Returns None if input is None or invalid.
    """
    if unix_ts is None:
        return None
    try:
        dt = datetime.datetime.fromtimestamp(unix_ts, tz=datetime.timezone.utc)
        return dt.isoformat()
    except Exception:
        return None

def fetch_tickers(master_table):
    response = supabase.from_(master_table).select("ticker").execute()
    if hasattr(response, 'data') and response.data:
        return [row["ticker"] for row in response.data if "ticker" in row]
    else:
        print("No tickers found or error:", response)
        return []

def get_hist_price(stock, months):
    days = months * 30
    history = stock.history(period=f"{days}d")
    if not history.empty:
        oldest = history.iloc[0]
        return float(oldest["Close"])
    return None

def get_yf_eps_and_revenue(stock):
    try:
        financials = stock.financials
        income_stmt = stock.income_stmt
        revenue_12m_ago = revenue_6m_ago = revenue_3m_ago = None
        eps_12m_ago = eps_6m_ago = eps_3m_ago = None
        if financials is not None and not financials.empty:
            columns = list(financials.columns)
            if len(columns) > 0:
                revenue_12m_ago = financials.loc['Total Revenue', columns[-1]]
            if len(columns) > 1:
                revenue_6m_ago = financials.loc['Total Revenue', columns[-2]]
            if len(columns) > 2:
                revenue_3m_ago = financials.loc['Total Revenue', columns[-3]]
        if income_stmt is not None and not income_stmt.empty:
            columns = list(income_stmt.columns)
            if 'Basic EPS' in income_stmt.index:
                if len(columns) > 0:
                    eps_12m_ago = income_stmt.loc['Basic EPS', columns[-1]]
                if len(columns) > 1:
                    eps_6m_ago = income_stmt.loc['Basic EPS', columns[-2]]
                if len(columns) > 2:
                    eps_3m_ago = income_stmt.loc['Basic EPS', columns[-3]]
        return (eps_3m_ago, eps_6m_ago, eps_12m_ago, revenue_3m_ago, revenue_6m_ago, revenue_12m_ago)
    except Exception as e:
        print("Error fetching EPS/revenue:", e)
        return (None,) * 6

def stringify_data(data):
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

def fetch_yfinance_data(ticker):
    try:
        yf_ticker = ticker if ticker.endswith('.NS') else ticker + '.NS'
        stock = yfinance.Ticker(yf_ticker)
        info = stock.info
        if not info or 'currentPrice' not in info:
            print(f"No info for ticker: {yf_ticker}")
            return None

         # Get financial dataframes for detailed fields
        balance_sheet = stock.balance_sheet
        cashflow = stock.cashflow
        financials = stock.financials

        # Extract values from dataframes if present
        def get_latest_val(df, field):
            if df is not None and not df.empty and field in df.index:
                return df.loc[field].iloc[0]
            return None
        
        price_3m_ago = get_hist_price(stock, 3)
        price_6m_ago = get_hist_price(stock, 6)
        price_12m_ago = get_hist_price(stock, 12)
        eps_3m_ago, eps_6m_ago, eps_12m_ago, revenue_3m_ago, revenue_6m_ago, revenue_12m_ago = get_yf_eps_and_revenue(stock)

        return {
            "ticker": ticker,
            "current_price": info.get("currentPrice"),
            "previous_close": info.get("previousClose"),
            "open_price": info.get("open"),
            "day_range_high": info.get("dayHigh"),
            "day_range_low": info.get("dayLow"),
            "price_3m_ago": price_3m_ago,
            "price_6m_ago": price_6m_ago,
            "price_12m_ago": price_12m_ago,
            "fifty_two_week_high": info.get("fiftyTwoWeekHigh"),
            "fifty_two_week_low": info.get("fiftyTwoWeekLow"),
            "market_cap": info.get("marketCap"),
            "enterprise_value": info.get("enterpriseValue"),
            "pe_ratio_trailing": info.get("trailingPE"),
            "pe_ratio_forward": info.get("forwardPE"),
            "price_to_sale": info.get("priceToSalesTrailing12Months"),
            "price_to_book": info.get("priceToBook"),
            "enterprise_value_ebitda": info.get("enterpriseToEbitda"),
            "dividend_yield": info.get("dividendYield"),
            "ex_dividend_date": unix_to_iso_utc(info.get("exDividendDate")),
            "net_income": info.get("netIncomeToCommon"),
            "gross_margin": info.get("grossMargins"),
            "operating_margin": info.get("operatingMargins"),
            "ebitda": info.get("ebitda"),
            "earnings_per_share": info.get("forwardEps"),
            "book_value_per_share": info.get("bookValue"),
            "free_cash_flow": info.get("freeCashflow"),
            "total_revenue": info.get("totalRevenue"),
            "revenue_3m_ago": revenue_3m_ago,
            "revenue_6m_ago": revenue_6m_ago,
            "revenue_12m_ago": revenue_12m_ago,
            "eps_3m_ago": eps_3m_ago,
            "eps_6m_ago": eps_6m_ago,
            "eps_12m_ago": eps_12m_ago,
            "revenue_growth": info.get("revenueGrowth"),
            "eps_growth": info.get("earningsQuarterlyGrowth"),
            "analyst_target_price": info.get("targetMeanPrice"),
            "analyst_high_target": info.get("targetHighPrice"),
            "analyst_low_target": info.get("targetLowPrice"),
            "analyst_number": info.get("numberOfAnalystOpinions"),
            "analyst_rating": info.get("recommendationMean"),
            "beta": info.get("beta"),
            "volume": info.get("volume"),
            "avg_volume": info.get("averageVolume"),
            "insider_holdings": info.get("heldPercentInsiders"),
            "institutional_holdings": info.get("heldPercentInstitutions"),
            "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None

def upsert_staging(table, data):
    if data:
        data_str = stringify_data(data)
        supabase.from_(table).upsert(data_str, on_conflict=["ticker"]).execute()

def main():
    master_table = "master_universe"
    staging_table = "yfin_stage"
    tickers = fetch_tickers(master_table)
    for ticker in tickers:
        ydata = fetch_yfinance_data(ticker)
        if ydata:
            upsert_staging(staging_table, ydata)
            print(f"Synced {ticker} to staging.")
        else:
            print(f"Skipped {ticker}, no valid Yahoo data.")

if __name__ == "__main__":
    main()
