# 📈 Stock Intelligence Platform — RAG-Based Equity Research System

A full-stack equity intelligence system that aggregates data from multiple financial sources, computes derived analytics, generates composite ratings, and provides a natural-language research interface powered by RAG (Retrieval-Augmented Generation).

> **Built as a personal project to explore production-grade RAG architectures, multi-source data pipelines, and LLM-powered financial analysis.**

---

## 🔥 Key Features

- **Multi-Source Data Ingestion** — Automated pipelines pulling from Screener.in, Yahoo Finance, and Google News to build unified structured datasets
- **Derived Analytics Engine** — Computes valuation, momentum, growth, profitability, and risk metrics across the NSE 500 universe
- **Composite Rating System** — Configurable Buy/Accumulate/Hold/Avoid rating engine with adjustable weights and scoring methodology
- **RAG-Powered Research Agent** — Natural-language equity research using LangChain + Gemini with hallucination control and source attribution
- **Vector Store Integration** — Chroma-based vectorstore with automated refresh workflows for up-to-date embeddings
- **Full-Stack Deployment** — FastAPI backend + Streamlit UI deployed on Render.com

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Streamlit UI                           │
│  Research Chat  │  Stock Screener  │  Rating Dashboard    │
└────────┬────────────────┬──────────────────┬─────────────┘
         │                │                  │
         ▼                ▼                  ▼
┌──────────────────────────────────────────────────────────┐
│                   FastAPI Backend                         │
│                                                          │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐ │
│  │ RAG Agent    │  │ Analytics    │  │ Rating Engine    │ │
│  │ LangChain +  │  │ Engine       │  │ Composite Score  │ │
│  │ Gemini       │  │              │  │ Calculator       │ │
│  └──────┬───────┘  └──────┬───────┘  └────────┬────────┘ │
└─────────┼─────────────────┼────────────────────┼─────────┘
          │                 │                    │
          ▼                 ▼                    ▼
┌──────────────┐  ┌──────────────────────────────────────┐
│ Chroma       │  │       Data Ingestion Pipelines       │
│ Vector Store │  │                                      │
│ (Embeddings) │  │ Screener.in  Yahoo Finance  Google   │
│              │  │ (Financials) (Price/Volume) (News)   │
└──────────────┘  └──────────────────────────────────────┘
                              │
                              ▼
                  ┌──────────────────────┐
                  │   Supabase (PostgreSQL)│
                  │   Unified Dataset      │
                  └──────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| **Frontend** | Streamlit |
| **Backend API** | FastAPI (async Python) |
| **LLM / RAG** | LangChain + Google Gemini |
| **Vector Database** | Chroma |
| **Primary Database** | Supabase (PostgreSQL) |
| **Data Sources** | Screener.in, Yahoo Finance, Google News |
| **Deployment** | Render.com (free tier) |
| **Language** | Python 3.10+ |

---

## 📊 Analytics & Metrics

The platform computes the following derived metrics across the NSE 500 universe:

| Category | Metrics |
|----------|---------|
| **Valuation** | P/E, P/B, EV/EBITDA, dividend yield |
| **Momentum** | Price change (1M, 3M, 6M, 1Y), relative strength |
| **Growth** | Revenue growth, EPS growth, profit growth |
| **Profitability** | ROE, ROCE, operating margin, net margin |
| **Risk** | Beta, debt-to-equity, promoter holding changes |

### Composite Rating Engine

Stocks are scored using a weighted composite of all metric categories and assigned a rating:

| Rating | Signal |
|--------|--------|
| 🟢 **Buy** | Strong across most metrics |
| 🔵 **Accumulate** | Positive overall, minor concerns |
| 🟡 **Hold** | Mixed signals, wait for clarity |
| 🔴 **Avoid** | Weak fundamentals or high risk |

Weights are fully configurable to match different investment styles (value, growth, momentum).

---

## 🤖 RAG Research Agent

Ask natural-language questions about any stock in the NSE 500 universe:

**Example queries:**
- *"What are the growth prospects for Infosys based on recent financials?"*
- *"Compare the valuation metrics of TCS vs Wipro"*
- *"Which IT stocks have the best momentum in the last 3 months?"*
- *"Should I look at HDFC Bank at current levels?"*

The agent retrieves relevant data from the vectorstore, augments the prompt with factual context, and generates responses with hallucination control — citing data sources and flagging when information may be incomplete.

---

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- Google Gemini API key
- Supabase project (free tier works)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/stock-intelligence-platform.git
cd stock-intelligence-platform

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GOOGLE_API_KEY="your-gemini-key"
export SUPABASE_URL="your-supabase-url"
export SUPABASE_KEY="your-supabase-key"

# Run data ingestion (first time)
python ingest.py

# Start the FastAPI backend
uvicorn main:app --reload

# In a separate terminal, start the Streamlit UI
streamlit run ui.py
```

> **Note:** The hosted version on Render.com uses a free tier and may spin down after periods of inactivity. The code runs fully locally with the above setup.

---

## 💡 How It Works

1. **Ingest** — Pipelines pull financial data from Screener.in, price data from Yahoo Finance, and news from Google News
2. **Transform** — Raw data is cleaned, normalized, and enriched with derived metrics (valuation, momentum, growth, profitability, risk)
3. **Store** — Structured data goes to Supabase; text data is embedded and stored in Chroma vectorstore
4. **Rate** — The composite engine scores each stock and assigns Buy/Accumulate/Hold/Avoid ratings
5. **Query** — Users can browse the screener, view ratings, or ask the RAG agent natural-language research questions
6. **Refresh** — Automated workflows keep the vectorstore and analytics current

---

## 📌 Roadmap

- [ ] Add technical analysis indicators (RSI, MACD, Bollinger Bands)
- [ ] Portfolio tracking and watchlist feature
- [ ] Sector-level comparison dashboards
- [ ] Multi-LLM support (Claude, GPT-4 alongside Gemini)
- [ ] Alerts and notification system for rating changes

---

## ⚠️ Disclaimer

This tool is for **educational and research purposes only**. It is not financial advice. Always do your own research and consult a qualified financial advisor before making investment decisions.

---

## 📄 License

This project is for educational and demonstration purposes.

---

## 🙋‍♂️ Author

**Suraj Nair** — AI Solutions Leader | Presales Architect | GenAI Strategist

- 20+ years in enterprise technology
- Building AI-powered solutions for Fortune 500 clients
- Exploring practical agentic AI and GenAI applications

[LinkedIn](https://linkedin.com/in/surajnair) • [Email](mailto:nsuraaj@gmail.com)
