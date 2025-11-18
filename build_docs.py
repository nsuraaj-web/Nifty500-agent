# build_docs.py
from typing import List, Dict, Any

try:
    # LangChain >= 0.2
    from langchain_core.documents import Document
except ImportError:
    # Older versions
    from langchain.schema import Document


def safe_fmt(value, suffix: str = ""):
    if value is None:
        return "N/A"
    return f"{value}{suffix}"


def merged_row_to_document(row: Dict[str, Any]) -> Document:
    """
    Input row structure from build_merged_stock_data():

    {
      "ticker": "TCS",
      "name": "...",
      "sector": "...",
      "consolidated": {...},
      "derived": {...},
      "ratings": {...},
      "screenr": {...},
    }
    """
    ticker = row.get("ticker")
    name = row.get("name") or ticker
    sector = row.get("sector")

    cons = row.get("consolidated", {}) or {}
    der = row.get("derived", {}) or {}
    rat = row.get("ratings", {}) or {}
    scr = row.get("screenr", {}) or {}

    # --------- Pull out key fields (you can extend later) ----------

    # Price & performance
    current_price = cons.get("current_price")
    prev_close = cons.get("previous_close")
    open_price = cons.get("open_price")
    high_52w = cons.get("fifty_two_week_high")
    low_52w = cons.get("fifty_two_week_low")

    price_3m_ago = cons.get("price_3m_ago")
    price_6m_ago = cons.get("price_6m_ago")
    price_12m_ago = cons.get("price_12m_ago")

    price_return_3m = der.get("price_return_3m_pct")
    price_return_6m = der.get("price_return_6m_pct")
    price_return_12m = der.get("price_return_12m_pct")
    ytd_return_pct = der.get("ytd_return_pct")
    position_52w_pct = der.get("position_52w_pct")

    # Valuation
    pe_trailing = cons.get("pe_ratio_trailing")
    pe_forward = cons.get("pe_ratio_forward")
    ps = cons.get("price_to_sale")
    pb = cons.get("price_to_book")
    ev_ebitda = cons.get("enterprise_value_ebitda")
    market_cap = cons.get("market_cap")
    enterprise_value = cons.get("enterprise_value")

    pe_discount_vs_sector = der.get("pe_discount_vs_sector")
    book_to_price_ratio = der.get("book_to_price_ratio")
    peg_ratio = der.get("peg_ratio")

    analyst_tp = cons.get("analyst_target_price")
    analyst_high = cons.get("analyst_high_target")
    analyst_low = cons.get("analyst_low_target")
    analyst_rating = cons.get("analyst_rating")
    analyst_number = cons.get("analyst_number")

    # Profitability / margins / size
    total_revenue = cons.get("total_revenue")
    net_income = cons.get("net_income")
    ebitda = cons.get("ebitda")
    gross_margin = cons.get("gross_margin")
    operating_margin = cons.get("operating_margin")
    net_margin_pct = cons.get("net_profit_pct") or der.get("net_margin_pct")

    # Growth
    revenue_growth = cons.get("revenue_growth")
    eps_growth = cons.get("eps_growth")
    eps_growth_3m = der.get("eps_growth_3m_pct")
    eps_growth_6m = der.get("eps_growth_6m_pct")
    eps_growth_12m = der.get("eps_growth_12m_pct")
    rev_growth_3m = der.get("revenue_growth_3m_pct")
    rev_growth_6m = der.get("revenue_growth_6m_pct")
    rev_growth_12m = der.get("revenue_growth_12m_pct")

    # Risk & leverage
    debt_to_equity = cons.get("debt_to_equity") or der.get("debt_to_equity")
    beta = cons.get("beta") or der.get("beta")
    volume = cons.get("volume")
    avg_volume = cons.get("avg_volume")

    # Cash flow & efficiency
    free_cash_flow = cons.get("free_cash_flow")
    operating_cash_flow = cons.get("operating_cash_flow")
    debtor_days = scr.get("debtor_days")
    ccc = scr.get("cash_conversion_cycle")
    working_capital_days = scr.get("working_capital_days")

    # Ownership
    promoters_pct = cons.get("promoters_pct") or scr.get("promoters_pct")
    fii_pct = cons.get("fii_pct") or scr.get("fii_pct")
    dii_pct = cons.get("dii_pct") or scr.get("dii_pct")
    govt_pct = cons.get("govt_pct") or scr.get("govt_pct")
    public_pct = cons.get("public_pct") or scr.get("public_pct")

    # Internal rating system
    rating_date = rat.get("rating_date")
    momentum_score = rat.get("momentum_score")
    quality_score = rat.get("quality_score")
    valuation_score = rat.get("valuation_score")
    growth_score = rat.get("growth_score")
    fin_stab_score = rat.get("financial_stability_score")
    cash_flow_score = rat.get("cash_flow_score")
    composite_score = rat.get("composite_score")
    rating = rat.get("rating")
    rank = rat.get("rank")
    action = rat.get("action")
    rating_ytd_return = rat.get("ytd_return_pct")
    rating_upside_potential = rat.get("upside_potential_pct")
    target_price_6m = rat.get("target_price_6m")

    fetched_at = cons.get("last_update") or cons.get("fetched_at")

    # ---------------- Build the text for RAG ----------------

    text = f"""
    STOCK SNAPSHOT
    --------------
    Name: {name}
    Ticker: {ticker}
    Sector: {sector}

    Latest data snapshot date: {fetched_at}
    Rating snapshot date: {rating_date}

    1) Price & Returns
    ------------------
    Current price: {safe_fmt(current_price)}
    Previous close: {safe_fmt(prev_close)}
    Open: {safe_fmt(open_price)}
    52-week range: {safe_fmt(low_52w)} - {safe_fmt(high_52w)}

    Historical prices:
    - 3M ago: {safe_fmt(price_3m_ago)}
    - 6M ago: {safe_fmt(price_6m_ago)}
    - 12M ago: {safe_fmt(price_12m_ago)}

    Returns:
    - 3M return: {safe_fmt(price_return_3m, '%')}
    - 6M return: {safe_fmt(price_return_6m, '%')}
    - 12M return: {safe_fmt(price_return_12m, '%')}
    - YTD return: {safe_fmt(ytd_return_pct, '%')}
    - Position in 52W range: {safe_fmt(position_52w_pct, '%')} (0% = 52W low, 100% = 52W high)

    2) Valuation
    ------------
    Market cap: {safe_fmt(market_cap)}
    Enterprise value: {safe_fmt(enterprise_value)}

    Valuation ratios:
    - P/E (trailing): {safe_fmt(pe_trailing)}
    - P/E (forward): {safe_fmt(pe_forward)}
    - P/S: {safe_fmt(ps)}
    - P/B: {safe_fmt(pb)}
    - EV/EBITDA: {safe_fmt(ev_ebitda)}

    Relative valuation:
    - P/E discount vs sector: {safe_fmt(pe_discount_vs_sector, '%')}
    - Book-to-price ratio: {safe_fmt(book_to_price_ratio)}
    - PEG ratio: {safe_fmt(peg_ratio)}

    Analyst view (external):
    - Consensus target price: {safe_fmt(analyst_tp)}
    - High / Low target: {safe_fmt(analyst_high)} / {safe_fmt(analyst_low)}
    - Analyst rating: {safe_fmt(analyst_rating)}
    - Number of analysts: {safe_fmt(analyst_number)}

    3) Size & Profitability
    -----------------------
    - Total revenue: {safe_fmt(total_revenue)}
    - Net income: {safe_fmt(net_income)}
    - EBITDA: {safe_fmt(ebitda)}

    Margins:
    - Gross margin: {safe_fmt(gross_margin, '%')}
    - Operating margin: {safe_fmt(operating_margin, '%')}
    - Net margin: {safe_fmt(net_margin_pct, '%')}

    4) Growth
    ---------
    High level:
    - Revenue growth (overall): {safe_fmt(revenue_growth, '%')}
    - EPS growth (overall): {safe_fmt(eps_growth, '%')}

    Recent:
    - EPS growth 3M / 6M / 12M: {safe_fmt(eps_growth_3m, '%')} / {safe_fmt(eps_growth_6m, '%')} / {safe_fmt(eps_growth_12m, '%')}
    - Revenue growth 3M / 6M / 12M: {safe_fmt(rev_growth_3m, '%')} / {safe_fmt(rev_growth_6m, '%')} / {safe_fmt(rev_growth_12m, '%')}

    5) Risk & Leverage
    ------------------
    - Debt-to-equity: {safe_fmt(debt_to_equity)}
    - Beta (volatility): {safe_fmt(beta)}
    - Volume (latest): {safe_fmt(volume)}
    - Average volume: {safe_fmt(avg_volume)}

    6) Cash Flow & Efficiency
    -------------------------
    - Free cash flow: {safe_fmt(free_cash_flow)}
    - Operating cash flow: {safe_fmt(operating_cash_flow)}

    Working capital efficiency (from Screenr if available):
    - Debtor days: {safe_fmt(debtor_days)}
    - Cash conversion cycle: {safe_fmt(ccc)}
    - Working capital days: {safe_fmt(working_capital_days)}

    7) Ownership
    ------------
    - Promoters holding: {safe_fmt(promoters_pct, '%')}
    - FII holding: {safe_fmt(fii_pct, '%')}
    - DII holding: {safe_fmt(dii_pct, '%')}
    - Government holding: {safe_fmt(govt_pct, '%')}
    - Public holding: {safe_fmt(public_pct, '%')}

    8) Internal Rating & Scores (Your System)
    ----------------------------------------
    - Composite score: {safe_fmt(composite_score)}
    - Momentum score: {safe_fmt(momentum_score)}
    - Quality score: {safe_fmt(quality_score)}
    - Valuation score: {safe_fmt(valuation_score)}
    - Growth score: {safe_fmt(growth_score)}
    - Financial stability score: {safe_fmt(fin_stab_score)}
    - Cash flow score: {safe_fmt(cash_flow_score)}

    Final recommendation:
    - Rating: {rating}
    - Rank in universe: {safe_fmt(rank)}
    - Action: {action}
    - YTD return (at rating time): {safe_fmt(rating_ytd_return, '%')}
    - Upside potential vs target: {safe_fmt(rating_upside_potential, '%')}
    - 6M target price (internal): {safe_fmt(target_price_6m)}

    Interpretation:
    - Use the internal scores (quality, growth, valuation, stability, cash flow) along with valuation ratios and ownership to
      understand if this is a quality / growth / value / high-risk stock.
    """

    metadata = {
        "ticker": ticker,
        "name": name,
        "sector": sector,
        "rating": rating,
        "rank": rank,
        "action": action,
        "snapshot_date": str(fetched_at),
        "rating_date": str(rating_date),
        "source": "combined_stock_snapshot",
    }

    return Document(page_content=text.strip(), metadata=metadata)


def merged_rows_to_documents(rows: List[Dict[str, Any]]) -> List[Document]:
    docs: List[Document] = []
    for r in rows:
        try:
            doc = merged_row_to_document(r)
            docs.append(doc)
        except Exception as e:
            print(f"Error building doc for {r.get('ticker')}: {e}")
    return docs


if __name__ == "__main__":
    # quick test
    from merge_data import build_merged_stock_data
    merged = build_merged_stock_data(limit=2)
    docs = merged_rows_to_documents(merged)
    for d in docs:
        print("==== DOC ====")
        print(d.metadata)
        print(d.page_content[:1000], "...\n")
