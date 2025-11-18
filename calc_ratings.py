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

def fetch_derived_master():
    response = supabase.from_("derived_master").select("*").execute()
    return {row['ticker']: row for row in response.data} if hasattr(response, 'data') else {}

def upsert_ratings_master(data):
    supabase.from_("ratings_master").upsert(data, on_conflict=["ticker"]).execute()

def minmax_score(value, min_val, max_val, invert=False):
    """Scale value between min_val and max_val, optionally invert scale."""
    if value is None or min_val is None or max_val is None or max_val == min_val:
        return 0
    score = (value - min_val) / (max_val - min_val)
    score = max(0, min(1, score))  # Clamp to [0, 1]
    return (1 - score) if invert else score

def score_from_row(row, cons_row=None):
    """
    Compute rating factor scores. Adjust weights and bandings as needed.
    All scores are normalized between 0 and 100 for comparability.
    """

    # Define realistic banding for normalization (update as needed for your universe)
    MINMAX = {
        "roc": (-50, 100), 
        "momentum_pos": (0, 100),
        "gross_margin": (0, 70),
        "op_margin": (0, 50),
        "pe": (5, 50),
        "peg": (0, 3),
        "eps_growth": (-50, 50),
        "rev_growth": (-50, 50),
        "cash_flow_margin": (-50, 50),
        "debt_to_equity": (0, 2),
        "beta": (0, 2)
    }

    scores = {}

    # Momentum: blend ROC_12M, price_return_3m_pct, position_52w_pct
    roc = row.get("price_return_12m_pct")
    pos52w = row.get("position_52w_pct")
    pr3m = row.get("price_return_3m_pct")
    scores["momentum_score"] = round(
        100 * (
            0.5 * minmax_score(roc, *MINMAX["roc"]) +
            0.3 * minmax_score(pos52w, *MINMAX["momentum_pos"]) +
            0.2 * minmax_score(pr3m, *MINMAX["roc"])
        ), 2
    )

    # Quality: gross margin, operating margin, net margin
    gm = row.get("gross_margin_pct")
    om = row.get("operating_margin_pct")
    nm = row.get("net_margin_pct")
    scores["quality_score"] = round(
        100 * (
            0.5 * minmax_score(gm, *MINMAX["gross_margin"]) +
            0.3 * minmax_score(om, *MINMAX["op_margin"]) +
            0.2 * minmax_score(nm, 0, 40)
        ), 2
    )

    # Valuation: lower PE better, lower PEG better, higher dividend yield better
    pe = cons_row.get("pe_ratio_trailing") if cons_row else None
    peg = row.get("peg_ratio")
    dy = row.get("dividend_yield_pct")
    scores["valuation_score"] = round(
        100 * (
            0.4 * minmax_score(pe, *MINMAX["pe"], invert=True) +
            0.4 * minmax_score(peg, *MINMAX["peg"], invert=True) +
            0.2 * minmax_score(dy, 0, 10)
        ), 2
    )

    # Growth: EPS growth + revenue growth
    eg = row.get("eps_growth_12m_pct")
    rg = row.get("revenue_growth_12m_pct")
    scores["growth_score"] = round(
        100 * (
            0.6 * minmax_score(eg, *MINMAX["eps_growth"]) +
            0.4 * minmax_score(rg, *MINMAX["rev_growth"])
        ), 2
    )

    # Financial stability: lower debt/equity and lower beta preferred
    de = row.get("debt_to_equity")
    beta = row.get("beta")
    scores["financial_stability_score"] = round(
        100 * (
            0.6 * minmax_score(de, *MINMAX["debt_to_equity"], invert=True) +
            0.4 * minmax_score(beta, *MINMAX["beta"], invert=True)
        ), 2
    )

    # Cash flow: higher margin preferred
    cfm = row.get("free_cash_flow_margin")
    ocm = row.get("operating_cash_flow_margin")
    scores["cash_flow_score"] = round(
        100 * (
            0.5 * minmax_score(cfm, *MINMAX["cash_flow_margin"]) +
            0.5 * minmax_score(ocm, *MINMAX["cash_flow_margin"])
        ), 2
    )

    # Composite score: weighted sum (adjust weights for your preferred strategy)
    scores["composite_score"] = round(
        0.2 * scores["momentum_score"] +
        0.2 * scores["quality_score"] +
        0.2 * scores["valuation_score"] +
        0.2 * scores["growth_score"] +
        0.1 * scores["financial_stability_score"] +
        0.1 * scores["cash_flow_score"], 2
    )

    # Rating/Banding (simple grading example, customize as you like)
    cs = scores["composite_score"]
    if cs >= 80:
        scores['rating'] = "A"
        scores['action'] = "BUY"
    elif cs >= 65:
        scores['rating'] = "B"
        scores['action'] = "ACCUMULATE"
    elif cs >= 50:
        scores['rating'] = "C"
        scores['action'] = "HOLD"
    else:
        scores['rating'] = "D"
        scores['action'] = "AVOID"

    return scores

def main():
    today = datetime.date.today().isoformat()
    cons_master = fetch_consolidated_master()
    derived_master = fetch_derived_master()
    ratings_entries = []
    for rec in cons_master:
        tkr = rec.get("ticker")
        derived = derived_master.get(tkr, {})
        scores = score_from_row(derived, cons_row=rec)
        ratings_row = {
            "ticker": tkr,
            "rating_date": today,
            **scores,
            "rank": None,  # Could be set after all scores calculated
            "ytd_return_pct": derived.get("ytd_return_pct"),
            "upside_potential_pct": derived.get("upside_potential_pct"),
            "beta": derived.get("beta"),
            "debt_to_equity": derived.get("debt_to_equity"),
            "dividend_yield_pct": derived.get("dividend_yield_pct"),
            "target_price_6m": rec.get("analyst_target_price"),
            "analyst_rating": rec.get("analyst_rating"),
        }
        ratings_entries.append(ratings_row)

    # Rank items and update table
    ratings_entries = sorted(ratings_entries, key=lambda x: x["composite_score"] if x["composite_score"] is not None else -999, reverse=True)
    for idx, row in enumerate(ratings_entries, 1):
        row["rank"] = idx
        upsert_ratings_master(row)
        print(f"ratings_master updated for {row['ticker']} with rank {row['rank']}")

if __name__ == "__main__":
    main()
