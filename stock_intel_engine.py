# stock_intel_engine.py
from rag_chain import ask_rag
from news_tool import get_top_news_summary
from finance_web_tool import fetch_and_summarise_ticker_sources_auto


def generate_stock_intel(ticker: str, question: str = None):
    """
    Combines:
      - Internal RAG (DB)
      - Latest news
      - Finance summaries (Yahoo/Google/Screener)
    """

    # 1. Internal RAG
    rag_question = question or f"Provide an investment analysis for {ticker} using internal financial data."
    rag_answer, rag_sources = ask_rag(rag_question, ticker=ticker)

    # 2. News
    news_result = get_top_news_summary(ticker)

    # 3. External finance summary
    finance_result = fetch_and_summarise_ticker_sources_auto(ticker)

    # Prepare sections separately (avoids f-string escaping issues)
    rag_sources_list = "\n".join(str(s.metadata) for s in rag_sources)

    news_sources_list = ""
    for a in news_result["articles"]:
        news_sources_list += f"- {a['title']} ({a['link']})\n"

    finance_sources_list = ""
    for s in finance_result["sources"]:
        finance_sources_list += f"- {s['label']}: {s['url']}\n"

    # Build full report as a normal string (not an f-string)
    report = (
        f"# Stock Intelligence Report: {ticker}\n\n"
        "---------------------------------\n\n"
        "## 📌 Section 1 — Internal Fundamental View (RAG from Database)\n"
        f"{rag_answer}\n\n"
        "**Internal Vector DB Sources:**\n"
        f"{rag_sources_list}\n\n"
        "---------------------------------\n\n"
        "## 📰 Section 2 — Latest 5 News Summary\n"
        f"{news_result['summary']}\n\n"
        "**News Sources:**\n"
        f"{news_sources_list}\n\n"
        "---------------------------------\n\n"
        "## 🌐 Section 3 — External Market View (Yahoo, Google, Screener)\n"
        f"{finance_result['summary']}\n\n"
        "**External Sources:**\n"
        f"{finance_sources_list}\n\n"
        "---------------------------------\n\n"
        "## Final Notes\n"
        "This report blends internal metrics, latest news, and external commentary "
        "into one consolidated research view.\n"
    )

    return {
        "ticker": ticker,
        "rag_answer": rag_answer,
        "news_summary": news_result,
        "finance_summary": finance_result,
        "full_report": report
    }
