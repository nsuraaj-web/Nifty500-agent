# finance_source_loader.py
import json
import os

def load_finance_source_templates():
    """
    Loads the generic URL templates.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "finance_sources.json")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"finance_sources.json not found at: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_urls_for_ticker(ticker: str):
    """
    Substitute $ticker$ in templates with the actual ticker symbol.

    Returns:
      {
        "yahoo": "https://finance.yahoo.com/quote/TCS.NS",
        "google": "https://www.google.com/finance/quote/TCS:NSE",
        "screener": "https://www.screener.in/company/TCS/"
      }
    """
    templates = load_finance_source_templates()
    ticker_upper = ticker.upper()

    urls = {}
    for key, template in templates.items():
        url = template.replace("$ticker$", ticker_upper)
        urls[key] = url

    return urls
