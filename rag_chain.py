# rag_chain.py
from typing import Optional, List

try:
    # LangChain >= 0.2
    from langchain_core.documents import Document
except ImportError:
    from langchain.schema import Document  # fallback

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser

from llm_setup import get_llm
from vectorstore import get_vectorstore

SYSTEM_PROMPT = """
You are an equity research assistant for Indian stocks.

Use ONLY the provided context and your general financial knowledge.
Do NOT hallucinate exact live prices or guaranteed future returns.
If information is missing from context, say so clearly.
Always mention tickers when referencing companies.
"""


def get_retriever(ticker: Optional[str] = None):
    """
    Build a Chroma retriever with optional ticker filter.
    This is pure LangChain (vectorstore.as_retriever).
    """
    vs = get_vectorstore()

    search_kwargs = {"k": 5}
    if ticker:
        # We stored "ticker" in Document.metadata["ticker"]
        search_kwargs["filter"] = {"ticker": ticker}

    retriever = vs.as_retriever(search_kwargs=search_kwargs)
    return retriever


def format_docs(docs: List[Document]) -> str:
    parts = []
    for d in docs:
        meta = d.metadata or {}
        header = f"[Ticker: {meta.get('ticker')}, Name: {meta.get('name')}, Sector: {meta.get('sector')}]"
        parts.append(header + "\n" + d.page_content)
    return "\n\n---\n\n".join(parts)


def get_rag_chain(ticker: Optional[str] = None):
    """
    Build a LangChain RAG pipeline using LCEL:
      question -> retriever -> prompt -> Gemini -> string
    """
    llm = get_llm()
    retriever = get_retriever(ticker=ticker)

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", SYSTEM_PROMPT),
            (
                "human",
                """Context from internal knowledge base:
{context}

User question:
{question}

Instructions:
- Use the context above to answer.
- Be clear and structured (use headings and bullet points where useful).
- Tie your explanation to valuation, growth, risk, ownership and internal scores in the context.
- Explicitly mention if some important data is missing or not available.
- Do not invent precise numbers that are not in the context.
""",
            ),
        ]
    )

    # Build the chain:
    # {"context": <retrieved_docs_as_text>, "question": <original_question>}
    #   -> prompt -> llm -> str
    rag_chain = (
        {
            "context": retriever | format_docs,
            "question": RunnablePassthrough(),
        }
        | prompt
        | llm
        | StrOutputParser()
    )

    return rag_chain, retriever


def ask_rag(question: str, ticker: Optional[str] = None):
    """
    Public helper:
      - runs the RAG chain to get an answer (string)
      - also returns the raw source Documents
    """
    chain, retriever = get_rag_chain(ticker=ticker)

    # 1) run full RAG chain to get answer string
    answer: str = chain.invoke(question)

    # 2) separately fetch source docs via retriever for inspection
    docs: List[Document] = retriever.invoke(question)

    return answer, docs


if __name__ == "__main__":
    # Quick manual test
    example_ticker = "TCS"  # change to a ticker you know exists
    q = "Explain the valuation, growth and risk profile based on our internal data."

    ans, srcs = ask_rag(q, ticker=example_ticker)
    print("=== ANSWER ===")
    print(ans)
    print("\n=== SOURCES ===")
    for s in srcs:
        print(s.metadata)
