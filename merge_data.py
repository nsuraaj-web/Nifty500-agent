# merge_data.py
from typing import Dict, Any, List

from supabaseclient import (
    fetch_consolidated,
    fetch_derived,
    fetch_ratings,
    fetch_screenr,
)

def index_by_ticker(rows: List[dict], ticker_key: str = "ticker") -> Dict[str, dict]:
    """
    Build a dict like {ticker: row}.
    If multiple rows per ticker exist, last one wins.
    For historical stuff we can refine later.
    """
    out: Dict[str, dict] = {}
    for r in rows:
        t = r.get(ticker_key)
        if not t:
            continue
        out[t] = r
    return out

def build_merged_stock_data(limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Returns a list like:
    [
      {
        "ticker": "TCS",
        "name": "...",
        "sector": "...",
        "consolidated": {...},   # from consolidated_master
        "derived": {...},        # from derived_master
        "ratings": {...},        # from ratings_master
        "screenr": {...},        # from screenr_master
      },
      ...
    ]
    """

    consolidated = index_by_ticker(fetch_consolidated(limit=limit), "ticker")
    derived = index_by_ticker(fetch_derived(limit=limit), "ticker")
    ratings = index_by_ticker(fetch_ratings(limit=limit), "ticker")
    screenr = index_by_ticker(fetch_screenr(limit=limit), "ticker")

    merged: List[Dict[str, Any]] = []

    # Drive off consolidated (fundamentals) as the base universe
    for ticker, cons_row in consolidated.items():
        combined: Dict[str, Any] = {}
        combined["ticker"] = ticker
        combined["name"] = cons_row.get("name")
        combined["sector"] = cons_row.get("sector")

        combined["consolidated"] = cons_row
        combined["derived"] = derived.get(ticker, {})
        combined["ratings"] = ratings.get(ticker, {})
        combined["screenr"] = screenr.get(ticker, {})

        merged.append(combined)

    return merged

if __name__ == "__main__":
    data = build_merged_stock_data(limit=5)
    from pprint import pprint
    for stock in data:
        print("------")
        pprint({
            "ticker": stock["ticker"],
            "name": stock["name"],
            "sector": stock["sector"],
            "has_consolidated": bool(stock["consolidated"]),
            "has_derived": bool(stock["derived"]),
            "has_ratings": bool(stock["ratings"]),
            "has_screenr": bool(stock["screenr"]),
        })
