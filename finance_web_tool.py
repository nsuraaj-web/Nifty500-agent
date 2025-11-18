# finance_web_tool.py
from typing import List, Dict
import requests
from bs4 import BeautifulSoup

from llm_setup import get_llm
from finance_source_loader import build_urls_for_ticker


# -----------------------------
# STEP 1: Fetch + clean HTML
# -----------------------------
def fetch_and_clean_url(url: str, timeout: int = 10) -> str:
    """
    Fetch a URL and extract readable text.
    Works for Yahoo Finance, Google Finance, Screener etc.
    """
    resp = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    # Remove noisy elements
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator="\n")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    cleaned = "\n".join(lines)

    # Trim if extremely long
    if len(cleaned) > 8000:
        cleaned = cleaned[:8000]

    return cleaned


# -----------------------------
# STEP 2: LLM summariser
# -----------------------------
def summarise_finance_pages(ticker: str, sources: List[Dict[str, str]]) -> str:
    """
    Summarise extracted text from multiple finance pages into a structured report.
    """
    llm = get_llm()

    blocks = []
    for src in sources:
        label = src.get("label", "Source")
        url = src["url"]

        try:
            text = fetch_and_clean_url(url)
        except Exception as e:
            text = f"[ERROR fetching {url}: {e}]"

        blocks.append(
            f"""Source: {label}
URL: {url}

Extracted content:
{text}
"""
        )

    combined_text = "\n\n---\n\n".join(blocks)

    prompt = f"""
You are an equity research assistant.

You are given raw text scraped from finance websites (Yahoo, Google Finance, Screener)
for stock {ticker}.

Raw extracted content:
{combined_text}

Task:
- Produce a concise, structured summary for {ticker} covering:
  1) Business overview
  2) Recent performance
  3) Growth outlook
  4) Valuation commentary (qualitative only)
  5) Key risks / red flags
- Use bullet points.
- Do NOT hallucinate missing numbers.
- Mention if some sections have no info.
"""

    resp = llm.invoke(prompt)
    summary = getattr(resp, "content", str(resp))
    return summary


# -----------------------------
# STEP 3: Auto-build URLs
# -----------------------------
def fetch_and_summarise_ticker_sources_auto(ticker: str):
    """
    Build URLs from generic templates and summarise them.
    """
    urls = build_urls_for_ticker(ticker)

    sources = []
    if "yahoo" in urls:
        sources.append({"label": "Yahoo Finance", "url": urls["yahoo"]})
    if "google" in urls:
        sources.append({"label": "Google Finance", "url": urls["google"]})
    if "screener" in urls:
        sources.append({"label": "Screener", "url": urls["screener"]})

    if not sources:
        return {
            "ticker": ticker,
            "sources": [],
            "summary": f"No source templates found for ticker: {ticker}"
        }

    summary = summarise_finance_pages(ticker, sources)
    return {
        "ticker": ticker,
        "sources": sources,
        "summary": summary
    }


# -----------------------------
# DEBUG / TEST
# -----------------------------
if __name__ == "__main__":
    ticker = "TCS"   # try any NSE ticker in uppercase
    result = fetch_and_summarise_ticker_sources_auto(ticker)

    print("=== SUMMARY ===")
    print(result["summary"])
    print("\n=== SOURCES USED ===")
    for s in result["sources"]:
        print(s["label"], "-", s["url"])
