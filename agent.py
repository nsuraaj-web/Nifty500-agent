# agent.py
from typing import Any, Dict, List

from langchain_core.tools import tool
from langchain.agents import create_agent

from llm_setup import get_llm
from rag_chain import ask_rag
from news_tool import get_top_news_summary
from finance_web_tool import fetch_and_summarise_ticker_sources_auto


# ---------- TOOLS ----------

@tool
def internal_rag_tool(ticker: str, question: str) -> str:
    """
    Use internal database RAG (Supabase + Chroma) for fundamentals, valuation,
    growth, risk, and internal ratings of the given stock ticker.
    """
    answer, _sources = ask_rag(question, ticker=ticker)
    return answer


@tool
def stock_news_tool(ticker: str) -> str:
    """
    Summarise the latest important news (last few weeks) for the given stock ticker.
    """
    result = get_top_news_summary(ticker)
    return result["summary"]


@tool
def finance_sites_tool(ticker: str) -> str:
    """
    Summarise qualitative information about the stock (business overview, risks, etc.)
    from Yahoo Finance, Google Finance, and Screener using generic URL templates.
    """
    result = fetch_and_summarise_ticker_sources_auto(ticker)
    return result["summary"]


tools = [internal_rag_tool, stock_news_tool, finance_sites_tool]


# ---------- AGENT BUILD / RUN ----------

def build_agent():
    """
    Build a LangChain v1 agent graph using create_agent.
    This agent:
    - uses your Gemini LLM (ChatGoogleGenerativeAI)
    - can call the three tools above
    """
    llm = get_llm()

    SYSTEM_PROMPT = """
You are an equity research assistant for Indian stocks.

You have the following tools:
- internal_rag_tool(ticker, question): use this for fundamentals, valuation, internal scores.
- stock_news_tool(ticker): use this for latest news summary.
- finance_sites_tool(ticker): use this for qualitative info from Yahoo/Google Finance/Screener.

Rules:
- ALWAYS require a ticker to do any stock-specific analysis.
- If the user does not provide a ticker, ask them to specify one.
- For a full deep-dive on a single stock:
  1) Call internal_rag_tool for fundamentals.
  2) Call stock_news_tool for news.
  3) Call finance_sites_tool for external qualitative view.
- Structure your final answer with sections:
  - Internal View
  - News
  - External Market View
  - Conclusion / Overall Take
- Do NOT invent exact live prices or guaranteed returns.
"""

    # This returns a compiled graph (LangGraph under the hood)
    agent = create_agent(
        model=llm,
        tools=tools,
        system_prompt=SYSTEM_PROMPT,
    )

    return agent


def run_agent(query: str) -> Dict[str, Any]:
    """
    Run the agent with a single user query.
    Returns the raw graph output (dict with 'messages', etc.).
    """
    agent = build_agent()

    # Agent expects messages in OpenAI-style format
    result = agent.invoke(
        {
            "messages": [
                {"role": "user", "content": query}
            ]
        }
    )
    return result


def extract_final_text(result: Dict[str, Any]) -> str:
    """
    Helper to pull the final AI message text out of the agent result.
    Handles Gemini-style content where .content can be a list of parts.
    """
    messages: List[Any] = result.get("messages", [])
    if not messages:
        return "No messages returned by agent."

    last = messages[-1]

    # Try attribute-style and dict-style access
    content = getattr(last, "content", None)
    if content is None and isinstance(last, dict):
        content = last.get("content", "")

    # If it's already a string, just return it
    if isinstance(content, str):
        return content

    # Gemini / LangChain often use a list of content parts
    if isinstance(content, list):
        parts: List[str] = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                parts.append(part.get("text", ""))
            else:
                parts.append(str(part))
        return "\n".join(p for p in parts if p)

    # Fallback
    return str(content or "")
