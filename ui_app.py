from dotenv import load_dotenv
load_dotenv()

import os
import textwrap
from typing import Optional, Dict, Any

import bcrypt
import streamlit as st
import requests
import pandas as pd
from fpdf import FPDF

from supabaseclient import supabase  # your existing Supabase client

# -----------------------------
# Streamlit Config
# -----------------------------
st.set_page_config(
    page_title="Stock Rating & RAG Assistant",
    page_icon="📈",
    layout="wide",
)

# Backend API base (FastAPI + agent)
DEFAULT_API_BASE = os.getenv("DEFAULT_API_BASE", "http://127.0.0.1:8000")

# -----------------------------
# Conceptual Buckets for Internal Data
# -----------------------------
INTERNAL_BUCKETS = {
    "Price & Returns": {
        "consolidated_master": [
            "current_price",
            "previous_close",
            "price_3m_ago",
            "price_6m_ago",
            "price_12m_ago",
            "fifty_two_week_high",
            "fifty_two_week_low",
        ],
        "derived_master": [
            "price_return_3m_pct",
            "price_return_6m_pct",
            "price_return_12m_pct",
            "ytd_return_pct",
            "position_52w_pct",
        ],
        "ratings_master": [
            "ytd_return_pct",
            "upside_potential_pct",
        ],
    },

    "Valuation": {
        "consolidated_master": [
            "market_cap",
            "enterprise_value",
            "pe_ratio_trailing",
            "pe_ratio_forward",
            "price_to_sale",
            "price_to_book",
            "enterprise_value_ebitda",
        ],
        "derived_master": [
            "market_cap_to_revenue",
            "enterprise_value_to_ebitda",
            "pe_discount_vs_sector",
            "book_to_price_ratio",
            "peg_ratio",
        ],
        "ratings_master": [
            "target_price_6m",
        ],
    },

    "Growth": {
        "consolidated_master": [
            "revenue_growth",
            "eps_growth",
        ],
        "derived_master": [
            "revenue_growth_3m_pct",
            "revenue_growth_6m_pct",
            "revenue_growth_12m_pct",
            "eps_growth_3m_pct",
            "eps_growth_6m_pct",
            "eps_growth_12m_pct",
        ],
    },

    "Profitability & Margins": {
        "consolidated_master": [
            "gross_margin",
            "operating_margin",
            "net_profit_pct",
            "roce_pct",
            "opm_pct",
        ],
        "derived_master": [
            "gross_margin_pct",
            "operating_margin_pct",
            "net_margin_pct",
        ],
    },

    "Leverage & Balance Sheet": {
        "consolidated_master": [
            "debt_to_equity",
        ],
        "derived_master": [
            "debt_to_equity",
        ],
        "ratings_master": [
            "debt_to_equity",
        ],
    },

    "Cash Flow": {
        "consolidated_master": [
            "free_cash_flow",
            "operating_cash_flow",
            "net_cash_flow_latest",
        ],
        "derived_master": [
            "free_cash_flow_margin",
            "operating_cash_flow_margin",
        ],
        "ratings_master": [
            "cash_flow_score",
        ],
    },

    "Ownership / Holdings": {
        "consolidated_master": [
            "promoters_pct",
            "fii_pct",
            "dii_pct",
            "govt_pct",
            "public_pct",
            "insider_holdings",
            "institutional_holdings",
        ],
        "ratings_master": [
            "dividend_yield_pct",
        ],
    },

    "Risk & Volatility": {
        "consolidated_master": ["beta"],
        "derived_master": ["beta"],
        "ratings_master": ["beta"],
    },

    "Analyst & Sentiment": {
        "consolidated_master": [
            "analyst_target_price",
            "analyst_high_target",
            "analyst_low_target",
            "analyst_number",
            "analyst_rating",
        ],
        "derived_master": [
            "upside_potential_pct",
        ],
        "ratings_master": [
            "analyst_rating",
        ],
    },

    "Ratings & Scores": {
        "ratings_master": [
            "momentum_score",
            "quality_score",
            "valuation_score",
            "growth_score",
            "financial_stability_score",
            "composite_score",
            "rating",
            "rank",
            "action",
        ],
    },
}

# -----------------------------
# Helper Functions
# -----------------------------


@st.cache_data(show_spinner=False)
def load_tickers() -> Dict[str, str]:
    """Load ticker -> name mapping from Supabase."""
    try:
        res = (
            supabase.table("master_universe")
            .select("ticker,name")
            .order("ticker")
            .execute()
        )
        data = res.data or []
        if not data:
            return {}
        return {row["ticker"]: row.get("name", row["ticker"]) for row in data}
    except Exception as e:
        st.warning(f"Could not load tickers from Supabase: {e}")
        return {}


def get_ticker_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Fetch latest snapshot for a ticker from consolidated_master + ratings_master.
    """
    snapshot: Dict[str, Any] = {}

    try:
        # Price + fundamentals from consolidated_master
        res_price = (
            supabase.table("consolidated_master")
            .select(
                "current_price, previous_close, price_3m_ago, price_6m_ago,"
                "price_12m_ago, market_cap, pe_ratio_trailing, pe_ratio_forward,"
                "price_to_book, dividend_yield, revenue_growth, eps_growth, beta"
            )
            .eq("ticker", ticker)
            .limit(1)
            .execute()
        )
        if res_price.data:
            snapshot["price_block"] = res_price.data[0]

        # Ratings from ratings_master (latest rating_date)
        res_rating = (
            supabase.table("ratings_master")
            .select(
                "momentum_score, quality_score, valuation_score, growth_score,"
                "financial_stability_score, cash_flow_score, composite_score,"
                "rating, rank, action, ytd_return_pct, upside_potential_pct, "
                "beta, debt_to_equity, dividend_yield_pct, target_price_6m"
            )
            .eq("ticker", ticker)
            .order("rating_date", desc=True)
            .limit(1)
            .execute()
        )
        if res_rating.data:
            snapshot["rating_block"] = res_rating.data[0]

    except Exception as e:
        st.error(f"Error fetching snapshot for {ticker}: {e}")

    return snapshot


def get_full_internal_rows(ticker: str) -> Dict[str, Dict[str, Any]]:
    """
    Fetch a single 'full row' for this ticker from key tables:
    consolidated_master, derived_master, ratings_master (latest).
    Returns: {table_name: row_dict}
    """
    result: Dict[str, Dict[str, Any]] = {}

    # consolidated_master
    try:
        res = (
            supabase.table("consolidated_master")
            .select("*")
            .eq("ticker", ticker)
            .limit(1)
            .execute()
        )
        if res.data:
            result["consolidated_master"] = res.data[0]
    except Exception as e:
        st.warning(f"Error loading consolidated_master for {ticker}: {e}")

    # derived_master
    try:
        res = (
            supabase.table("derived_master")
            .select("*")
            .eq("ticker", ticker)
            .order("snapshot_date", desc=True)
            .limit(1)
            .execute()
        )
        if res.data:
            result["derived_master"] = res.data[0]
    except Exception as e:
        st.warning(f"Error loading derived_master for {ticker}: {e}")

    # ratings_master (latest rating_date)
    try:
        res = (
            supabase.table("ratings_master")
            .select("*")
            .eq("ticker", ticker)
            .order("rating_date", desc=True)
            .limit(1)
            .execute()
        )
        if res.data:
            result["ratings_master"] = res.data[0]
    except Exception as e:
        st.warning(f"Error loading ratings_master for {ticker}: {e}")

    return result


def call_agent(api_base: str, query: str) -> Optional[str]:
    try:
        resp = requests.post(
            f"{api_base.rstrip('/')}/agent",
            json={"query": query},
            timeout=60,
        )
        if resp.status_code != 200:
            st.error(f"Agent error: {resp.status_code} - {resp.text}")
            return None
        data = resp.json()
        return data.get("answer", "")
    except Exception as e:
        st.error(f"Failed to call agent: {e}")
        return None


def beautify_number(val: Any) -> str:
    if val is None:
        return "-"
    try:
        f = float(val)
    except Exception:
        return str(val)

    # Large numbers as Cr/Bn style
    if abs(f) >= 1e9:
        return f"{f / 1e9:.2f} B"
    if abs(f) >= 1e7:
        return f"{f / 1e7:.2f} Cr"
    if abs(f) >= 1e5:
        return f"{f:,.0f}"
    return f"{f:.2f}"


def make_report_prompt(ticker: str, company_name: str, template_text: str) -> str:
    base_instruction = textwrap.dedent(
        f"""
        You are an equity research assistant.

        Generate a detailed, investor-friendly report for the stock {ticker} ({company_name}).

        Use the internal fundamentals, derived metrics, and ratings available in the tools/RAG system,
        and augment with external web/news sources when relevant.

        Follow this report template exactly, filling in the sections with well-structured content,
        tables where appropriate (as markdown), and clear conclusions and risks.
        """
    ).strip()

    return base_instruction + "\n\n=== TEMPLATE START ===\n" + template_text + "\n=== TEMPLATE END ==="


def download_text_file(content: str, filename: str = "stock_report.md") -> bytes:
    return content.encode("utf-8")


def generate_pdf_bytes(report_text: str, title: str) -> bytes:
    """
    Very simple text → PDF using fpdf2.
    Handles multi-line text and basic wrapping.
    """
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_title(title)

    pdf.set_font("Arial", size=12)

    # Simple wrapping: break on newline, then multi_cell for each line
    for line in report_text.splitlines():
        if not line.strip():
            pdf.ln(5)
        else:
            pdf.multi_cell(0, 6, line)

    # Return in-memory PDF bytes
    return pdf.output(dest="S").encode("latin-1")

# -----------------------------
# Auth Helpers (Login)
# -----------------------------


def authenticate_user(email: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Check credentials against app_users table in Supabase.
    Returns user dict (without password_hash) if valid, else None.
    """
    try:
        res = (
            supabase.table("app_users")
            .select("id, email, full_name, password_hash")
            .eq("email", email)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        if not rows:
            return None

        user = rows[0]
        stored_hash = user.get("password_hash")
        if not stored_hash:
            return None

        if bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8")):
            # Don't expose hash in session
            user.pop("password_hash", None)
            return user
        return None
    except Exception as e:
        st.error(f"Login error: {e}")
        return None


def require_login():
    """
    Simple login gate. Call early in your app to force login.
    """
    if "user" not in st.session_state:
        st.session_state.user = None

    if st.session_state.user is not None:
        return  # already logged in

    st.title("🔐 Stock Intel Login")

    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

    if submitted:
        user = authenticate_user(email, password)
        if user:
            if user:
                st.session_state.user = user
                st.success(f"Welcome, {user.get('full_name') or user['email']}!")
                st.rerun()

        else:
            st.error("Invalid email or password.")

    # Stop rendering the rest of the app until logged in
    st.stop()


# 🔐 Enforce login before rendering the rest of the app
require_login()

# -----------------------------
# Sidebar
# -----------------------------
st.sidebar.title("⚙️ Settings")

user = st.session_state.user
st.sidebar.write(f"👤 {user.get('full_name') or user['email']}")

if st.sidebar.button("Logout"):
    st.session_state.user = None
    st.rerun()

api_base = st.sidebar.text_input("API Base URL", value=DEFAULT_API_BASE)

tickers_map = load_tickers()
if not tickers_map:
    st.sidebar.error("No tickers loaded from Supabase. Check supabaseclient & env.")
    selected_ticker = None
    selected_name = None
else:
    ticker_options = [f"{t} - {name}" for t, name in tickers_map.items()]
    default_index = 0
    selected_label = st.sidebar.selectbox(
        "Select ticker", ticker_options, index=default_index
    )
    selected_ticker = selected_label.split(" - ")[0]
    selected_name = tickers_map.get(selected_ticker, selected_ticker)

st.sidebar.markdown("---")
st.sidebar.caption("Backend: FastAPI + LangChain RAG + Supabase + Chroma")

# -----------------------------
# Layout: Tabs
# -----------------------------
st.title("📈 Stock Rating & RAG Assistant")

tab_dashboard, tab_chat, tab_internal, tab_reports = st.tabs(
    ["📊 Dashboard", "💬 Chat with Data", "📚 Internal Data", "📄 Reports"]
)

# -----------------------------
# Tab 1: Dashboard
# -----------------------------
with tab_dashboard:
    if not selected_ticker:
        st.info("Select a ticker in the sidebar to view its snapshot.")
    else:
        st.subheader(f"{selected_ticker} — {selected_name}")

        snapshot = get_ticker_snapshot(selected_ticker)

        col1, col2, col3 = st.columns(3)

        price_block = snapshot.get("price_block", {})
        rating_block = snapshot.get("rating_block", {})

        with col1:
            st.markdown("**Price & Valuation**")
            st.metric("Current Price", beautify_number(price_block.get("current_price")))
            st.metric("Prev Close", beautify_number(price_block.get("previous_close")))
            st.metric("P/E (TTM)", beautify_number(price_block.get("pe_ratio_trailing")))
            st.metric("P/B", beautify_number(price_block.get("price_to_book")))
            st.metric(
                "Dividend Yield %",
                beautify_number(price_block.get("dividend_yield")),
            )

        with col2:
            st.markdown("**Returns & Growth**")
            st.metric("Price 3M Ago", beautify_number(price_block.get("price_3m_ago")))
            st.metric("Price 6M Ago", beautify_number(price_block.get("price_6m_ago")))
            st.metric(
                "Price 12M Ago", beautify_number(price_block.get("price_12m_ago"))
            )
            st.metric(
                "Revenue Growth", beautify_number(price_block.get("revenue_growth"))
            )
            st.metric("EPS Growth", beautify_number(price_block.get("eps_growth")))

        with col3:
            st.markdown("**Rating Snapshot**")
            st.metric(
                "Composite Score",
                beautify_number(rating_block.get("composite_score")),
            )
            st.metric("Rating", rating_block.get("rating") or "-")
            st.metric("Rank", rating_block.get("rank") or "-")
            st.metric("Action", rating_block.get("action") or "-")
            st.metric(
                "Upside %",
                beautify_number(rating_block.get("upside_potential_pct")),
            )

        st.markdown("---")

        st.markdown("### Detailed rating breakdown")

        rating_rows = []
        if rating_block:
            rating_rows.append(
                {
                    "Momentum": rating_block.get("momentum_score"),
                    "Quality": rating_block.get("quality_score"),
                    "Valuation": rating_block.get("valuation_score"),
                    "Growth": rating_block.get("growth_score"),
                    "Fin. Stability": rating_block.get("financial_stability_score"),
                    "Cash Flow": rating_block.get("cash_flow_score"),
                }
            )

        if rating_rows:
            st.dataframe(rating_rows, use_container_width=True)
        else:
            st.info("No rating details found for this ticker.")

# -----------------------------
# Tab 2: Chat with Data
# -----------------------------
with tab_chat:
    st.subheader("Ask questions about this stock or your universe")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    placeholder_ticker = selected_ticker or "TCS"
    user_question = st.text_area(
        "Your question",
        value=f"Give me a concise investment thesis for {placeholder_ticker}, including key risks and valuation view.",
        height=100,
    )

    col_chat1, col_chat2 = st.columns([1, 3])

    with col_chat1:
        use_selected = st.checkbox("Anchor on selected ticker", value=True)

    with col_chat2:
        if st.button("Ask Agent", type="primary"):
            if not api_base:
                st.error("Please set API Base URL in sidebar.")
            else:
                query = user_question
                if use_selected and selected_ticker:
                    query = (
                        f"For ticker {selected_ticker} ({selected_name}), "
                        f"use all internal tables and RAG, plus external finance/news sources. "
                        f"Then answer: {user_question}"
                    )

                answer = call_agent(api_base, query)
                if answer:
                    st.session_state.chat_history.append(
                        {"role": "user", "content": query}
                    )
                    st.session_state.chat_history.append(
                        {"role": "assistant", "content": answer}
                    )

    st.markdown("---")
    st.markdown("### Conversation")

    if not st.session_state.chat_history:
        st.info("No messages yet. Ask something above.")
    else:
        for msg in st.session_state.chat_history:
            if msg["role"] == "user":
                st.markdown(f"**🧑 You:** {msg['content']}")
            else:
                st.markdown(f"**🤖 Agent:** {msg['content']}")

# -----------------------------
# Tab 3: Full Internal Data
# -----------------------------
with tab_internal:
    st.subheader("Full Internal Data by Conceptual Buckets")

    if not selected_ticker:
        st.info("Select a ticker in the sidebar to view its internal data.")
    else:
        st.markdown(f"**Current ticker:** `{selected_ticker}` — {selected_name}")

        data_by_table = get_full_internal_rows(selected_ticker)

        if not data_by_table:
            st.warning("No internal rows found for this ticker in the key tables.")
        else:
            # Track which fields we've already shown, to build an 'Other' bucket later
            shown_fields = {tbl: set() for tbl in data_by_table.keys()}

            for bucket_name, tables_spec in INTERNAL_BUCKETS.items():
                records = []
                for table_name, columns in tables_spec.items():
                    row = data_by_table.get(table_name)
                    if not row:
                        continue
                    for col in columns:
                        if col not in row:
                            continue
                        val = row[col]
                        dtype = type(val).__name__
                        display_val = (
                            beautify_number(val)
                            if isinstance(val, (int, float))
                            else val
                        )
                        shown_fields[table_name].add(col)
                        records.append(
                            {
                                "Field": f"{table_name}.{col}",
                                "Value": display_val,
                                "Type": dtype,
                            }
                        )

                if records:
                    st.markdown(f"### {bucket_name}")
                    df = pd.DataFrame(records).sort_values(by=["Field"])
                    st.dataframe(df, use_container_width=True)

            # 'Other' bucket: any columns not mapped above
            other_records = []
            for table_name, row in data_by_table.items():
                for col, val in row.items():
                    if col in shown_fields.get(table_name, set()):
                        continue
                    dtype = type(val).__name__
                    display_val = (
                        beautify_number(val)
                        if isinstance(val, (int, float))
                        else val
                    )
                    other_records.append(
                        {
                            "Field": f"{table_name}.{col}",
                            "Value": display_val,
                            "Type": dtype,
                        }
                    )

            if other_records:
                st.markdown("### Other Fields")
                df_other = pd.DataFrame(other_records).sort_values(by=["Field"])
                st.dataframe(df_other, use_container_width=True)

# -----------------------------
# Tab 4: Reports
# -----------------------------
with tab_reports:
    st.subheader("Generate detailed reports")

    if not selected_ticker:
        st.info("Select a ticker in the sidebar first.")
    else:
        st.markdown(f"**Current ticker:** `{selected_ticker}` — {selected_name}")

        st.markdown("#### 1. Upload or edit a report template")

        uploaded_file = st.file_uploader(
            "Upload template (Markdown / TXT)",
            type=["md", "txt"],
            help="For now we treat everything as text/markdown.",
        )

        default_template = textwrap.dedent(
            f"""
            # {selected_ticker} - {selected_name} Investment Report

            ## 1. Company Overview
            - Business description
            - Key segments
            - Market position

            ## 2. Historical Performance
            - Price performance (3M, 6M, 12M, YTD)
            - Revenue and EPS trends
            - Margin profile

            ## 3. Quality & Financial Strength
            - Return ratios (ROE/ROCE if available)
            - Leverage & coverage
            - Cash flows

            ## 4. Growth & Drivers
            - Key growth drivers
            - Tailwinds & headwinds
            - Management commentary (if any)

            ## 5. Valuation
            - Current valuations vs history and sector
            - Upside / downside vs target / fair value

            ## 6. Risk Factors
            - Business risks
            - Financial risks
            - Regulatory / macro risks

            ## 7. Investment Thesis & Recommendation
            - Summary of thesis
            - Final rating / action
            - Time horizon
            """
        ).strip()

        if uploaded_file is not None:
            template_text = uploaded_file.read().decode("utf-8", errors="ignore")
        else:
            template_text = default_template

        template_text = st.text_area(
            "Template (you can edit this)",
            value=template_text,
            height=400,
        )

        st.markdown("#### 2. Generate report with Agent")

        if st.button("Generate Report", type="primary"):
            if not api_base:
                st.error("Please set API Base URL in sidebar.")
            else:
                prompt = make_report_prompt(
                    ticker=selected_ticker,
                    company_name=selected_name,
                    template_text=template_text,
                )
                with st.spinner("Asking agent to generate report..."):
                    report = call_agent(api_base, prompt)

                if report:
                    st.success("Report generated.")
                    st.markdown("### Preview")
                    st.markdown(report)

                    # Markdown download
                    md_bytes = download_text_file(
                        report,
                        filename=f"{selected_ticker}_report.md",
                    )
                    st.download_button(
                        label="⬇️ Download report as Markdown",
                        data=md_bytes,
                        file_name=f"{selected_ticker}_report.md",
                        mime="text/markdown",
                    )

                    # PDF download
                    pdf_bytes = generate_pdf_bytes(
                        report,
                        title=f"{selected_ticker} - {selected_name} Report",
                    )
                    st.download_button(
                        label="⬇️ Download report as PDF",
                        data=pdf_bytes,
                        file_name=f"{selected_ticker}_report.pdf",
                        mime="application/pdf",
                    )
