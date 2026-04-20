"""
app.py — Family Portfolio Tracker (Streamlit)
Storage : GitHub CSV (encrypted with Fernet)
Auth    : Session-state password gate
LLM     : NVIDIA Llama via OpenAI-compatible API
"""

from datetime import date

import pandas as pd
import streamlit as st
from openai import OpenAI

import engine

st.set_page_config(page_title="Family Portfolio Tracker", layout="wide")

# ===========================================================================
# 0. Secrets
# ===========================================================================

try:
    GITHUB_TOKEN   = st.secrets["github_token"]
    ROIC_API_KEY   = st.secrets["roic_api_key"]
    NVIDIA_API_KEY = st.secrets["nvidia_api_key"]
    APP_PASSWORD   = st.secrets["app_password"]
    CSV_ENC_KEY    = st.secrets["csv_enc_key"]
except (KeyError, FileNotFoundError) as e:
    st.error(
        f"Missing secret: `{e}`\n\n"
        "Create `.streamlit/secrets.toml` — see `.streamlit/secrets.toml.example`."
    )
    st.stop()

# ===========================================================================
# 1. Password gate
# ===========================================================================

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "login_attempts" not in st.session_state:
    st.session_state.login_attempts = 0

if not st.session_state.authenticated:
    st.markdown("## Portfolio Tracker")
    pw = st.text_input("Password", type="password", key="pw_input")
    if st.button("Enter", type="primary"):
        if pw == APP_PASSWORD:
            st.session_state.authenticated = True
            st.session_state.login_attempts = 0
            st.rerun()
        else:
            st.session_state.login_attempts += 1
            attempts = st.session_state.login_attempts
            if attempts >= 5:
                st.error("Too many incorrect attempts. Close the browser tab and try again.")
            else:
                st.error(f"Incorrect password. ({attempts}/5 attempts)")
    st.stop()

# ===========================================================================
# 2. Config & transactions
# ===========================================================================

config = engine.load_config("config.toml")

try:
    txdf = engine.read_transactions(config, GITHUB_TOKEN, CSV_ENC_KEY)
except RuntimeError as e:
    st.error(f"Cannot load transactions from GitHub.\n\n{e}")
    st.stop()

# ===========================================================================
# 3. Cached market data
# ===========================================================================

@st.cache_data(ttl=3600)
def get_prices(tickers_tuple, etf_list_tuple):
    return engine.fetch_prices(list(tickers_tuple), list(etf_list_tuple))

@st.cache_data(ttl=3600)
def get_fx_rates(currencies_tuple, overrides_items):
    return engine.fetch_fx_rates(list(currencies_tuple), dict(overrides_items))

@st.cache_data(ttl=86400)
def get_fundamentals(tickers_tuple, api_key, etf_list_tuple):
    return engine.fetch_fundamentals(list(tickers_tuple), api_key, list(etf_list_tuple))

positions    = engine.compute_positions(txdf)
consolidated = engine.compute_consolidated(positions)
all_tickers  = sorted(positions["ticker"].unique().tolist()) if not positions.empty else []
etf_list     = config["etfs"]["tickers"]
theme_map    = config.get("theme_map", {})
acct_names   = config["accounts"]

ALL_CURRENCIES  = ("CAD", "SGD", "HKD", "JPY", "EUR", "GBP", "AED", "BRL", "KRW", "INR")
fx_overrides    = config.get("fx_overrides", {})
overrides_items = tuple(sorted(fx_overrides.items()))

with st.spinner("Loading market data..."):
    prices_df       = get_prices(tuple(all_tickers), tuple(etf_list))
    fx_rates        = get_fx_rates(ALL_CURRENCIES, overrides_items)
    fundamentals_df = get_fundamentals(tuple(all_tickers), ROIC_API_KEY, tuple(etf_list))

holdings    = engine.build_holdings(
    positions, consolidated, prices_df, fx_rates, fundamentals_df, theme_map, config
)
per_account = engine.build_per_account_holdings(
    positions, prices_df, fx_rates, fundamentals_df, theme_map
)

# ===========================================================================
# 4. Helpers
# ===========================================================================

def _fmt_usd(v):
    return "–" if pd.isna(v) else f"${v:,.0f}"

def _fmt_pct(v, d=1):
    return "–" if pd.isna(v) else f"{v * 100:.{d}f}%"

def _fmt_pct_raw(v, d=1):
    return "–" if pd.isna(v) else f"{v:.{d}f}%"

def _val(v):
    return v if (v is not None and not (isinstance(v, float) and pd.isna(v))) else "–"

# ===========================================================================
# 5. Sidebar
# ===========================================================================

page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Holdings", "Add Transaction", "Risk & Themes",
     "Income & Cash Flow", "AI Assistant"],
)

st.sidebar.markdown("---")
with st.sidebar.expander("FX Rates (USD base)"):
    for ccy, rate in sorted(fx_rates.items()):
        if ccy != "USD":
            st.write(f"1 USD = {rate:.4f} {ccy}")

if st.sidebar.button("Log out"):
    st.session_state.authenticated = False
    st.rerun()

# ===========================================================================
# Page 1 — Overview
# ===========================================================================

if page == "Overview":
    st.title("Portfolio Overview")
    if holdings.empty:
        st.info("No positions found. Run `python setup_github.py` to load opening positions.")
        st.stop()

    tv  = holdings["market_value_usd"].sum()
    tc  = holdings["total_cost_usd"].sum()
    pnl = tv - tc
    pct = pnl / tc if tc > 0 else float("nan")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Value (USD)",    _fmt_usd(tv))
    c2.metric("Total Cost (USD)",     _fmt_usd(tc))
    c3.metric("Unrealised P&L (USD)", _fmt_usd(pnl), delta=_fmt_pct(pct))
    c4.metric("P&L %",                _fmt_pct(pct))

    st.markdown("---")
    st.subheader("Account Summary")
    acct_rows = []
    for aid, adisp in acct_names.items():
        sub = per_account[per_account["account_id"] == aid]
        if sub.empty:
            continue
        mv = sub["market_value_usd"].sum(); tc2 = sub["total_cost_usd"].sum(); p = mv - tc2
        acct_rows.append({
            "Account": adisp, "Market Value": _fmt_usd(mv), "Cost Basis": _fmt_usd(tc2),
            "P&L": _fmt_usd(p), "P&L %": _fmt_pct(p / tc2 if tc2 > 0 else float("nan")),
            "Positions": len(sub), "Weight": _fmt_pct(mv / tv if tv > 0 else float("nan")),
        })
    st.dataframe(pd.DataFrame(acct_rows), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Sector Allocation")
    sec = engine.compute_sector_allocation(holdings)
    if not sec.empty:
        d = sec.copy()
        d["market_value_usd"] = d["market_value_usd"].apply(_fmt_usd)
        d["weight_pct"]       = d["weight_pct"].apply(_fmt_pct_raw)
        d = d.rename(columns={"sector": "Sector", "market_value_usd": "Value (USD)", "weight_pct": "Weight"})
        st.dataframe(d, use_container_width=True, hide_index=True)

# ===========================================================================
# Page 2 — Holdings
# ===========================================================================

elif page == "Holdings":
    st.title("Holdings")
    if holdings.empty:
        st.info("No positions loaded.")
        st.stop()

    view = st.radio("View", ["Consolidated", "Per Account"], horizontal=True)

    st.sidebar.markdown("### Filters")
    sel_accts  = st.sidebar.multiselect("Account", list(acct_names.values()), default=list(acct_names.values()))
    all_themes = sorted(holdings["theme"].dropna().unique().tolist())
    sel_themes = st.sidebar.multiselect("Theme", all_themes, default=all_themes)
    type_f     = st.sidebar.radio("Type", ["All", "Equities Only", "ETFs Only"])
    sort_by    = st.sidebar.selectbox("Sort by", ["Market Value (USD)", "P&L (USD)", "P&L %", "Ticker", "Theme", "Weight"])
    sort_map   = {"Market Value (USD)": "market_value_usd", "P&L (USD)": "unrealised_pnl_usd",
                  "P&L %": "pnl_pct", "Ticker": "ticker", "Theme": "theme", "Weight": "weight"}

    def _apply_filters(df, is_consolidated=True):
        acct_id_sel = [k for k, v in acct_names.items() if v in sel_accts]
        if is_consolidated:
            df = df[df["accounts_held"].apply(lambda ah: any(a in acct_id_sel for a in ah))]
        df = df[df["theme"].isin(sel_themes)]
        if type_f == "Equities Only":
            df = df[~df["ticker"].isin(etf_list)]
        elif type_f == "ETFs Only":
            df = df[df["ticker"].isin(etf_list)]
        sc = sort_map.get(sort_by, "market_value_usd")
        if sc in df.columns:
            df = df.sort_values(sc, ascending=(sc == "ticker"), na_position="last")
        return df

    def _show_drill_down(sel_ticker, sel_row, acct_id_filter=None):
        with st.expander(f"Detail — {sel_ticker}", expanded=True):
            ca, cb = st.columns(2)
            with ca:
                st.markdown("**Per-Account Breakdown**")
                sub = per_account[per_account["ticker"] == sel_ticker]
                if acct_id_filter:
                    sub = sub[sub["account_id"] == acct_id_filter]
                if not sub.empty:
                    d = sub[["account_id","net_shares","avg_cost_local","currency",
                              "market_value_usd","unrealised_pnl_usd"]].copy()
                    d["account_id"] = d["account_id"].map(acct_names)
                    d = d.rename(columns={"account_id": "Account", "net_shares": "Shares",
                                          "avg_cost_local": "Avg Cost", "currency": "CCY",
                                          "market_value_usd": "Value (USD)", "unrealised_pnl_usd": "P&L (USD)"})
                    st.dataframe(d, use_container_width=True, hide_index=True)
            with cb:
                st.markdown("**Fundamentals**")
                for label, key in [("Sector","sector"),("Industry","industry"),("Country","country"),
                                    ("P/E","pe_ratio"),("EV/EBITDA","ev_ebitda"),
                                    ("P/B","pb_ratio"),("Gross Margin","gross_margin"),("Div Yield","dividend_yield")]:
                    st.write(f"**{label}:** {_val(sel_row.get(key))}")
            st.markdown("**Last 5 Transactions**")
            if not txdf.empty:
                q = txdf[txdf["ticker"] == sel_ticker]
                if acct_id_filter:
                    q = q[q["account_id"] == acct_id_filter]
                st.dataframe(q.sort_values("date", ascending=False).head(5),
                              use_container_width=True, hide_index=True)

    if view == "Consolidated":
        df = _apply_filters(holdings.copy())
        disp = pd.DataFrame({
            "Ticker": df["ticker"], "Name": df["company_name"],
            "Held In": df["accounts_display"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x),
            "Shares": df["total_shares"].round(4), "Avg Cost (Local)": df["avg_cost_local"].round(4),
            "CCY": df["currency"], "Avg Cost (USD)": df["avg_cost_usd"].round(4),
            "Price (USD)": df["price_usd"].round(4), "Value (USD)": df["market_value_usd"].round(0),
            "P&L (USD)": df["unrealised_pnl_usd"].round(0), "P&L %": (df["pnl_pct"] * 100).round(2),
            "Weight %": (df["weight"] * 100).round(2), "Theme": df["theme"],
        })
        sel = st.dataframe(disp, use_container_width=True, hide_index=True,
                            selection_mode="single-row", on_select="rerun")
        if sel.selection.rows:
            idx = sel.selection.rows[0]
            _show_drill_down(df.iloc[idx]["ticker"], df.iloc[idx])

    else:
        sel_aid = st.selectbox("Account", list(acct_names.keys()), format_func=lambda k: acct_names[k])
        df      = per_account[per_account["account_id"] == sel_aid].copy()
        df      = _apply_filters(df, is_consolidated=False)
        disp    = pd.DataFrame({
            "Ticker": df["ticker"], "Name": df["company_name"],
            "Shares": df["net_shares"].round(4), "Avg Cost (Local)": df["avg_cost_local"].round(4),
            "CCY": df["currency"], "Avg Cost (USD)": df["avg_cost_usd"].round(4),
            "Price (USD)": df["price_usd"].round(4), "Value (USD)": df["market_value_usd"].round(0),
            "P&L (USD)": df["unrealised_pnl_usd"].round(0), "P&L %": (df["pnl_pct"] * 100).round(2),
            "Theme": df["theme"],
        })
        sel = st.dataframe(disp, use_container_width=True, hide_index=True,
                            selection_mode="single-row", on_select="rerun")
        if sel.selection.rows:
            idx = sel.selection.rows[0]
            _show_drill_down(df.iloc[idx]["ticker"], df.iloc[idx], acct_id_filter=sel_aid)

# ===========================================================================
# Page 3 — Add Transaction
# ===========================================================================

elif page == "Add Transaction":
    st.title("Add Transaction")

    acct_display_to_id = {v: k for k, v in acct_names.items()}
    pence_tickers      = config.get("pence_tickers", {}).get("tickers", [])

    with st.form("add_tx_form"):
        tx_date    = st.date_input("Date", value=date.today())
        tx_account = st.selectbox("Account", list(acct_names.values()))
        tx_ticker  = st.text_input("Ticker").strip().upper()
        tx_action  = st.selectbox("Action", ["BUY","SELL","DIVIDEND","SPLIT","TRANSFER IN","TRANSFER OUT"])
        tx_shares  = st.number_input("Shares", min_value=0.0, step=0.01, format="%.4f")
        tx_price   = st.number_input("Price (local CCY)", min_value=0.0, step=0.01, format="%.4f")
        tx_ccy     = st.selectbox("Currency", ["USD","CAD","SGD","HKD","JPY","EUR","GBP","AED","BRL","KRW","INR"])
        tx_comm    = st.number_input("Commission (USD)", min_value=0.0, value=0.0, step=0.01, format="%.2f")
        tx_notes   = st.text_input("Notes (optional)")
        submitted  = st.form_submit_button("Save Transaction")

    price_confirmed = tx_price
    if tx_ticker in pence_tickers and 0 < tx_price < 10:
        st.warning(f"⚠️ This looks like pence. Did you mean {tx_price:.2f} GBP or {tx_price*100:.2f}p?")
        if st.radio("Confirm unit", ["GBP (as entered)", "Convert from pence to GBP"], key="pence") == "Convert from pence to GBP":
            price_confirmed = tx_price / 100.0
            st.info(f"Will save as {price_confirmed:.4f} GBP.")

    if submitted:
        errors = []
        if not tx_ticker:
            errors.append("Ticker is required.")
        if tx_action != "DIVIDEND" and tx_shares <= 0:
            errors.append("Shares must be > 0.")
        if tx_action == "SELL" and tx_ticker and not errors:
            aid = acct_display_to_id[tx_account]
            cur = positions[(positions["account_id"] == aid) & (positions["ticker"] == tx_ticker)]
            cur_shares = cur["net_shares"].sum() if not cur.empty else 0.0
            if tx_shares > cur_shares:
                st.warning(f"Sell of {tx_shares} exceeds current {cur_shares:.4f} shares in {tx_account}.")
                if not st.checkbox("Proceed anyway", key="short_ok"):
                    errors.append("Confirm above to proceed.")
        if errors:
            for e in errors:
                st.error(e)
        else:
            row = {
                "date": str(tx_date), "account_id": acct_display_to_id[tx_account],
                "ticker": tx_ticker, "action": tx_action, "shares": tx_shares,
                "price_local": price_confirmed, "currency": tx_ccy,
                "commission_usd": tx_comm, "notes": tx_notes,
            }
            try:
                engine.save_transaction(row, config, GITHUB_TOKEN, CSV_ENC_KEY)
                st.success(f"Saved: {tx_action} {tx_shares} {tx_ticker} @ {price_confirmed} {tx_ccy} in {tx_account}")
                st.rerun()
            except RuntimeError as e:
                st.error(f"Could not save to GitHub.\n\n{e}")

    st.markdown("---")
    with st.expander("Recent Transactions (last 10)"):
        if not txdf.empty:
            st.dataframe(txdf.sort_values("date", ascending=False).head(10),
                          use_container_width=True, hide_index=True)
            st.markdown("**Delete last transaction**")
            if "confirm_delete" not in st.session_state:
                st.session_state.confirm_delete = False
            if not st.session_state.confirm_delete:
                if st.button("Delete last transaction"):
                    st.session_state.confirm_delete = True
                    st.rerun()
            else:
                lr = txdf.sort_values("date").iloc[-1]
                st.warning(
                    f"Delete: {str(lr['date'])[:10]} | {lr['account_id']} | {lr['ticker']} | "
                    f"{lr['action']} | {lr['shares']} @ {lr['price_local']} {lr['currency']}"
                )
                c1, c2 = st.columns(2)
                if c1.button("Confirm delete"):
                    try:
                        engine.delete_last_transaction(config, GITHUB_TOKEN, CSV_ENC_KEY)
                        st.session_state.confirm_delete = False
                        st.success("Deleted.")
                        st.rerun()
                    except RuntimeError as e:
                        st.error(str(e))
                if c2.button("Cancel"):
                    st.session_state.confirm_delete = False
                    st.rerun()
        else:
            st.info("No transactions yet.")

# ===========================================================================
# Page 4 — Risk & Themes
# ===========================================================================

elif page == "Risk & Themes":
    st.title("Risk & Themes")
    if holdings.empty:
        st.info("No positions loaded.")
        st.stop()

    st.subheader("Theme Allocation")
    theme_df = engine.compute_theme_allocation(holdings)
    if not theme_df.empty:
        td = theme_df.copy()
        td["market_value_usd"] = td["market_value_usd"].apply(_fmt_usd)
        td["weight_pct"]       = td["weight_pct"].apply(_fmt_pct_raw)
        def _flag(row):
            try:
                return ["background-color: #fff3cd"] * len(row) if float(str(row["weight_pct"]).replace("%","")) > 15 else [""] * len(row)
            except Exception:
                return [""] * len(row)
        td = td.rename(columns={"theme":"Theme","market_value_usd":"Value (USD)","weight_pct":"Weight %","tickers":"Tickers"})
        st.dataframe(td.style.apply(_flag, axis=1), use_container_width=True, hide_index=True)
        st.caption("Amber = theme weight > 15%")

    st.markdown("---")
    st.subheader("Geographic Allocation")
    geo = engine.compute_geo_allocation(holdings)
    if not geo.empty:
        gd = geo.copy()
        gd["market_value_usd"] = gd["market_value_usd"].apply(_fmt_usd)
        gd["weight_pct"]       = gd["weight_pct"].apply(_fmt_pct_raw)
        gd = gd.rename(columns={"region":"Region","market_value_usd":"Value (USD)","weight_pct":"Weight %"})
        st.dataframe(gd, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Cross-Account Overlap")
    ov = engine.compute_overlap(per_account)
    if ov.empty:
        st.info("No tickers held in multiple accounts.")
    else:
        dc = {"ticker": "Ticker", "company_name": "Name"}
        for aid, adisp in acct_names.items():
            if f"shares_{aid}" in ov.columns:
                dc[f"shares_{aid}"] = f"{adisp} Shares"
        dc["total_shares"] = "Total Shares"; dc["total_value_usd"] = "Total Value (USD)"
        od = ov[[c for c in dc if c in ov.columns]].rename(columns=dc)
        if "Total Value (USD)" in od.columns:
            od["Total Value (USD)"] = od["Total Value (USD)"].apply(_fmt_usd)
        st.dataframe(od, use_container_width=True, hide_index=True)

# ===========================================================================
# Page 5 — Income & Cash Flow
# ===========================================================================

elif page == "Income & Cash Flow":
    st.title("Income & Cash Flow")
    income = engine.compute_income(txdf, holdings)

    st.subheader("Projected Annual Dividend Income")
    proj = income["projected_income"]
    if not proj.empty:
        ps = proj.sort_values("projected_annual_income_usd", ascending=False).copy()
        ps["dividend_yield"] = (ps["dividend_yield"] * 100).round(2).astype(str) + "%"
        ps["projected_annual_income_usd"] = ps["projected_annual_income_usd"].apply(_fmt_usd)
        ps["price_usd"] = ps["price_usd"].round(4)
        ps = ps.rename(columns={"ticker":"Ticker","company_name":"Name","total_shares":"Shares",
                                  "price_usd":"Price (USD)","dividend_yield":"Div Yield",
                                  "projected_annual_income_usd":"Proj. Annual Income (USD)"})
        st.dataframe(ps, use_container_width=True, hide_index=True)
        st.metric("Total Projected Annual Income", _fmt_usd(income["projected_income"]["projected_annual_income_usd"].sum()))
    else:
        st.info("No dividend-paying positions found.")

    st.markdown("---")
    st.subheader("Realised Cash Flow Log")

    def _add_usd(df):
        if df.empty: return df
        df = df.copy()
        df["value_usd"] = df["price_local"] * df["shares"] / df["currency"].map(
            lambda c: fx_rates.get(c, 1.0) if c != "USD" else 1.0)
        return df

    divs  = _add_usd(income["dividends_received"])
    sells = _add_usd(income["sale_proceeds"])
    combined = pd.concat([divs, sells], ignore_index=True)
    if not combined.empty:
        combined = combined.sort_values("date", ascending=False)
        combined["account_id"] = combined["account_id"].map(lambda a: acct_names.get(a, a))
        dc = {"date":"Date","account_id":"Account","ticker":"Ticker","action":"Action",
              "shares":"Shares","price_local":"Price","currency":"CCY","value_usd":"Value (USD)","notes":"Notes"}
        cd = combined[[c for c in dc if c in combined.columns]].rename(columns=dc)
        if "Value (USD)" in cd.columns:
            cd["Value (USD)"] = cd["Value (USD)"].apply(_fmt_usd)
        st.dataframe(cd, use_container_width=True, hide_index=True)
        c1, c2, c3 = st.columns(3)
        td = divs["value_usd"].sum()  if not divs.empty  else 0
        ts = sells["value_usd"].sum() if not sells.empty else 0
        c1.metric("Dividends Received", _fmt_usd(td))
        c2.metric("Sale Proceeds",       _fmt_usd(ts))
        c3.metric("Combined",            _fmt_usd(td + ts))
    else:
        st.info("No SELL or DIVIDEND transactions yet.")

# ===========================================================================
# Page 6 — AI Assistant
# ===========================================================================

elif page == "AI Assistant":
    st.title("AI Portfolio Assistant")
    st.caption("Powered by Llama 3.3 70B via NVIDIA API · Context: live portfolio snapshot")

    # Build portfolio context once per session (or when holdings change)
    if "portfolio_context" not in st.session_state or st.button("Refresh portfolio context"):
        with st.spinner("Building portfolio context..."):
            st.session_state.portfolio_context = engine.build_portfolio_context(
                holdings, per_account, txdf, fx_rates, config
            )
        st.success("Context refreshed.")

    # Chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Show chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Input
    if prompt := st.chat_input("Ask anything about your portfolio..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        system_prompt = f"""You are a knowledgeable personal portfolio analyst assistant for a private family equity portfolio.
You have access to the full live portfolio snapshot below. Answer questions accurately and concisely.
When referencing monetary values, default to USD unless asked otherwise.
Be direct — this is a private tool for the portfolio owner only.

{st.session_state.portfolio_context}"""

        client = OpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=NVIDIA_API_KEY,
        )

        with st.chat_message("assistant"):
            try:
                stream = client.chat.completions.create(
                    model="meta/llama-3.3-70b-instruct",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        *st.session_state.messages,
                    ],
                    stream=True,
                    temperature=0.2,
                    max_tokens=1024,
                )
                response = st.write_stream(
                    chunk.choices[0].delta.content or ""
                    for chunk in stream
                    if chunk.choices[0].delta.content
                )
            except Exception as e:
                response = f"Error calling NVIDIA API: {e}"
                st.error(response)

        st.session_state.messages.append({"role": "assistant", "content": response})

    # Clear chat
    if st.session_state.messages:
        if st.button("Clear conversation"):
            st.session_state.messages = []
            st.rerun()
