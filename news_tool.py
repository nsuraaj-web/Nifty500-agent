# news_tool.py
import urllib.parse
from typing import List, Dict

import feedparser

from llm_setup import get_llm

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en"


def fetch_news_for_ticker(ticker: str, max_articles: int = 5) -> List[Dict]:
    """
    Fetch top news items for a given ticker using Google News RSS.
    This is free and lightweight.

    Returns: list of dicts: { 'title', 'link', 'summary' }
    """
    # Query pattern: "TCS stock India", etc.
    query = urllib.parse.quote_plus(f"{ticker} stock India")
    url = GOOGLE_NEWS_RSS.format(query=query)

    feed = feedparser.parse(url)

    articles: List[Dict] = []
    for entry in feed.entries[:max_articles]:
        articles.append(
            {
                "title": entry.get("title"),
                "link": entry.get("link"),
                # RSS often has a short summary / snippet
                "summary": entry.get("summary", ""),
            }
        )

    return articles


def summarise_news(ticker: str, articles: List[Dict]) -> str:
    """
    Use Gemini (via LangChain) to summarise a list of news articles for the ticker.
    Returns a markdown string.
    """
    if not articles:
        return f"No recent major news articles found for {ticker} (via Google News RSS)."

    llm = get_llm()

    # Build a compact context for the model
    news_blocks = []
    for i, art in enumerate(articles, start=1):
        news_blocks.append(
            f"""Article {i}:
Title: {art['title']}
Snippet: {art['summary']}
Link: {art['link']}
"""
        )

    news_text = "\n\n".join(news_blocks)

    prompt = f"""
You are an equity research assistant.

You are given recent news headlines and snippets for stock {ticker}.

News items:
{news_text}

Task:
- Summarise the TOP 5 key developments affecting {ticker} in the last few days/weeks.
- Group them by theme (e.g., results, macro/sector news, management/ESG, regulatory, other).
- For each theme, mention:
  - What happened
  - Whether it seems positive, negative, or mixed for the stock, based on the snippet (if unclear, say 'uncertain').
- Keep the summary short and bullet-pointed.
- At the end, list the article titles and links as 'References'.
"""

    resp = llm.invoke(prompt)
    summary_text = getattr(resp, "content", str(resp))
    return summary_text


def get_top_news_summary(ticker: str, max_articles: int = 5) -> Dict:
    """
    Convenience function:
    1) Fetch top news via RSS
    2) Summarise via Gemini
    Returns:
      {
        "ticker": ...,
        "articles": [...],
        "summary": "...markdown..."
      }
    """
    articles = fetch_news_for_ticker(ticker, max_articles=max_articles)
    summary = summarise_news(ticker, articles)
    return {
        "ticker": ticker,
        "articles": articles,
        "summary": summary,
    }


if __name__ == "__main__":
    example_ticker = "TCS"  # change to any Indian stock ticker symbol you track

    result = get_top_news_summary(example_ticker, max_articles=5)
    print("=== SUMMARY ===")
    print(result["summary"])
    print("\n=== RAW ARTICLES ===")
    for a in result["articles"]:
        print(a["title"], "-", a["link"])
