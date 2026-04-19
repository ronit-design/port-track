"""
app.py — Family Portfolio Tracker (Streamlit)
Single-file UI. All computation delegated to engine.py.
Storage: GitHub CSV via REST API. Token read from .streamlit/secrets.toml.
"""

from datetime import date

import pandas as pd
import streamlit as st

import engine

st.set_page_config(page_title="Family Portfolio Tracker", layout="wide")

# ---------------------------------------------------------------------------
# Secrets & Config
# ---------------------------------------------------------------------------

try:
    github_token = st.secrets["github_token"]
    roic_api_key = st.secrets["roic_api_key"]
except (KeyError, FileNotFoundError) as e:
    st.error(
        f"Secret not found: `{e}`\n\n"
        "Create `.streamlit/secrets.toml` (see `.streamlit/secrets.toml.example`)."
    )
    st.stop()

config = engine.load_config("config.toml")

# ---------------------------------------------------------------------------
# Transactions — always fresh, never cached
# ---------------------------------------------------------------------------

try:
    txdf = engine.read_transactions(config, github_token)
except RuntimeError as e:
    st.error(f"Cannot load transactions from GitHub.\n\n{e}")
    st.stop()

# ---------------------------------------------------------------------------
# Cached market data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def get_prices(tickers_tuple, etf_list_tuple):
    return engine.fetch_prices(list(tickers_tuple), list(etf_list_tuple))


@st.cache_data(ttl=3600)
def get_fx_rates(currencies_tuple, overrides_items):
    return engine.fetch_fx_rates(list(currencies_tuple), dict(overrides_items))


@st.cache_data(ttl=86400)
def get_fundamentals(tickers_tuple, api_key, etf_list_tuple):
    return engine.fetch_fundamentals(list(tickers_tuple), api_key, list(etf_list_tuple))


# ---------------------------------------------------------------------------
# Positions & market data
# ---------------------------------------------------------------------------

positions    = engine.compute_positions(txdf)
consolidated = engine.compute_consolidated(positions)

all_tickers     = sorted(positions["ticker"].unique().tolist()) if not positions.empty else []
etf_list        = config["etfs"]["tickers"]
theme_map       = config.get("theme_map", {})
acct_names      = config["accounts"]

ALL_CURRENCIES  = ("CAD", "SGD", "HKD", "JPY", "EUR", "GBP", "AED", "BRL", "KRW", "INR")
fx_overrides    = config.get("fx_overrides", {})
overrides_items = tuple(sorted(fx_overrides.items()))

with st.spinner("Loading market data..."):
    prices_df       = get_prices(tuple(all_tickers), tuple(etf_list))
    fx_rates        = get_fx_rates(ALL_CURRENCIES, overrides_items)
    fundamentals_df = get_fundamentals(
        tuple(all_tickers), roic_api_key, tuple(etf_list)
    )

holdings    = engine.build_holdings(
    positions, consolidated, prices_df, fx_rates, fundamentals_df, theme_map, config
)
per_account = engine.build_per_account_holdings(
    positions, prices_df, fx_rates, fundamentals_df, theme_map
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_usd(v):
    if pd.isna(v):
        return "–"
    return f"${v:,.0f}"


def _fmt_pct(v, decimals=1):
    if pd.isna(v):
        return "–"
    return f"{v * 100:.{decimals}f}%"


def _fmt_pct_raw(v, decimals=1):
    if pd.isna(v):
        return "–"
    return f"{v:.{decimals}f}%"


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Holdings", "Add Transaction", "Risk & Themes", "Income & Cash Flow"],
)

st.sidebar.markdown("---")
with st.sidebar.expander("FX Rates (USD base)"):
    for ccy, rate in sorted(fx_rates.items()):
        if ccy != "USD":
            st.write(f"1 USD = {rate:.4f} {ccy}")


# ===========================================================================
# Page 1 — Overview
# ===========================================================================

if page == "Overview":
    st.title("Portfolio Overview")

    if holdings.empty:
        st.info("No positions found. Run `python setup_github.py` to load opening positions.")
        st.stop()

    total_value   = holdings["market_value_usd"].sum()
    total_cost    = holdings["total_cost_usd"].sum()
    total_pnl     = total_value - total_cost
    total_pnl_pct = total_pnl / total_cost if total_cost > 0 else float("nan")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Value (USD)",    _fmt_usd(total_value))
    c2.metric("Total Cost (USD)",     _fmt_usd(total_cost))
    c3.metric("Unrealised P&L (USD)", _fmt_usd(total_pnl), delta=_fmt_pct(total_pnl_pct))
    c4.metric("P&L %",                _fmt_pct(total_pnl_pct))

    st.markdown("---")
    st.subheader("Account Summary")

    acct_rows = []
    for acct_id, acct_disp in acct_names.items():
        sub = per_account[per_account["account_id"] == acct_id]
        if sub.empty:
            continue
        mv  = sub["market_value_usd"].sum()
        tc  = sub["total_cost_usd"].sum()
        pnl = mv - tc
        acct_rows.append({
            "Account":        acct_disp,
            "Market Value":   _fmt_usd(mv),
            "Cost Basis":     _fmt_usd(tc),
            "Unrealised P&L": _fmt_usd(pnl),
            "P&L %":          _fmt_pct(pnl / tc if tc > 0 else float("nan")),
            "Positions":      len(sub),
            "Weight":         _fmt_pct(mv / total_value if total_value > 0 else float("nan")),
        })
    st.dataframe(pd.DataFrame(acct_rows), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Sector Allocation")

    sector_df = engine.compute_sector_allocation(holdings)
    if not sector_df.empty:
        disp = sector_df.copy()
        disp["market_value_usd"] = disp["market_value_usd"].apply(_fmt_usd)
        disp["weight_pct"]       = disp["weight_pct"].apply(_fmt_pct_raw)
        disp = disp.rename(columns={"sector": "Sector", "market_value_usd": "Value (USD)", "weight_pct": "Weight"})
        st.dataframe(disp, use_container_width=True, hide_index=True)


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
    all_accts_display = list(acct_names.values())
    sel_accts  = st.sidebar.multiselect("Account", all_accts_display, default=all_accts_display)
    all_themes = sorted(holdings["theme"].dropna().unique().tolist())
    sel_themes = st.sidebar.multiselect("Theme", all_themes, default=all_themes)
    type_filter = st.sidebar.radio("Type", ["All", "Equities Only", "ETFs Only"])
    sort_by = st.sidebar.selectbox(
        "Sort by",
        ["Market Value (USD)", "P&L (USD)", "P&L %", "Ticker", "Theme", "Weight"],
    )
    sort_map = {
        "Market Value (USD)": "market_value_usd",
        "P&L (USD)":          "unrealised_pnl_usd",
        "P&L %":              "pnl_pct",
        "Ticker":             "ticker",
        "Theme":              "theme",
        "Weight":             "weight",
    }

    if view == "Consolidated":
        df = holdings.copy()
        acct_id_sel = [k for k, v in acct_names.items() if v in sel_accts]
        df = df[df["accounts_held"].apply(lambda ah: any(a in acct_id_sel for a in ah))]
        df = df[df["theme"].isin(sel_themes)]
        if type_filter == "Equities Only":
            df = df[~df["ticker"].isin(etf_list)]
        elif type_filter == "ETFs Only":
            df = df[df["ticker"].isin(etf_list)]

        sort_col = sort_map.get(sort_by, "market_value_usd")
        if sort_col in df.columns:
            df = df.sort_values(sort_col, ascending=(sort_col == "ticker"), na_position="last")

        display_df = pd.DataFrame({
            "Ticker":          df["ticker"],
            "Name":            df["company_name"],
            "Held In":         df["accounts_display"].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x)),
            "Shares":          df["total_shares"].round(4),
            "Avg Cost (Local)": df["avg_cost_local"].round(4),
            "CCY":             df["currency"],
            "Avg Cost (USD)":  df["avg_cost_usd"].round(4),
            "Price (USD)":     df["price_usd"].round(4),
            "Value (USD)":     df["market_value_usd"].round(0),
            "P&L (USD)":       df["unrealised_pnl_usd"].round(0),
            "P&L %":           (df["pnl_pct"] * 100).round(2),
            "Weight %":        (df["weight"] * 100).round(2),
            "Theme":           df["theme"],
        })

        selection = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun",
        )

        if selection.selection.rows:
            idx      = selection.selection.rows[0]
            sel_row  = df.iloc[idx]
            sel_ticker = sel_row["ticker"]

            with st.expander(f"Detail — {sel_ticker}", expanded=True):
                col_a, col_b = st.columns(2)

                with col_a:
                    st.markdown("**Per-Account Breakdown**")
                    sub_pa = per_account[per_account["ticker"] == sel_ticker]
                    if not sub_pa.empty:
                        pa_disp = sub_pa[["account_id", "net_shares", "avg_cost_local",
                                          "currency", "market_value_usd", "unrealised_pnl_usd"]].copy()
                        pa_disp["account_id"] = pa_disp["account_id"].map(acct_names)
                        pa_disp = pa_disp.rename(columns={
                            "account_id": "Account", "net_shares": "Shares",
                            "avg_cost_local": "Avg Cost", "currency": "CCY",
                            "market_value_usd": "Value (USD)", "unrealised_pnl_usd": "P&L (USD)",
                        })
                        st.dataframe(pa_disp, use_container_width=True, hide_index=True)

                with col_b:
                    st.markdown("**Fundamentals**")
                    for label, key in [
                        ("Sector", "sector"), ("Industry", "industry"), ("Country", "country"),
                        ("P/E", "pe_ratio"), ("EV/EBITDA", "ev_ebitda"),
                        ("P/B", "pb_ratio"), ("Gross Margin", "gross_margin"),
                        ("Div Yield", "dividend_yield"),
                    ]:
                        v = sel_row.get(key)
                        display_v = v if (v is not None and not (isinstance(v, float) and pd.isna(v))) else "–"
                        st.write(f"**{label}:** {display_v}")

                st.markdown("**Last 5 Transactions**")
                if not txdf.empty:
                    st.dataframe(
                        txdf[txdf["ticker"] == sel_ticker]
                        .sort_values("date", ascending=False)
                        .head(5),
                        use_container_width=True, hide_index=True,
                    )

    else:
        sel_acct_id = st.selectbox(
            "Account", list(acct_names.keys()), format_func=lambda k: acct_names[k]
        )
        df = per_account[per_account["account_id"] == sel_acct_id].copy()
        df = df[df["theme"].isin(sel_themes)]
        if type_filter == "Equities Only":
            df = df[~df["ticker"].isin(etf_list)]
        elif type_filter == "ETFs Only":
            df = df[df["ticker"].isin(etf_list)]

        sort_col = sort_map.get(sort_by, "market_value_usd")
        if sort_col in df.columns:
            df = df.sort_values(sort_col, ascending=(sort_col == "ticker"), na_position="last")

        display_df = pd.DataFrame({
            "Ticker":          df["ticker"],
            "Name":            df["company_name"],
            "Shares":          df["net_shares"].round(4),
            "Avg Cost (Local)": df["avg_cost_local"].round(4),
            "CCY":             df["currency"],
            "Avg Cost (USD)":  df["avg_cost_usd"].round(4),
            "Price (USD)":     df["price_usd"].round(4),
            "Value (USD)":     df["market_value_usd"].round(0),
            "P&L (USD)":       df["unrealised_pnl_usd"].round(0),
            "P&L %":           (df["pnl_pct"] * 100).round(2),
            "Theme":           df["theme"],
        })

        selection = st.dataframe(
            display_df, use_container_width=True, hide_index=True,
            selection_mode="single-row", on_select="rerun",
        )

        if selection.selection.rows:
            idx      = selection.selection.rows[0]
            sel_row  = df.iloc[idx]
            sel_ticker = sel_row["ticker"]

            with st.expander(f"Detail — {sel_ticker}", expanded=True):
                for label, key in [
                    ("Sector", "sector"), ("Industry", "industry"), ("Country", "country"),
                    ("P/E", "pe_ratio"), ("EV/EBITDA", "ev_ebitda"),
                    ("P/B", "pb_ratio"), ("Gross Margin", "gross_margin"),
                    ("Div Yield", "dividend_yield"),
                ]:
                    v = sel_row.get(key)
                    display_v = v if (v is not None and not (isinstance(v, float) and pd.isna(v))) else "–"
                    st.write(f"**{label}:** {display_v}")

                if not txdf.empty:
                    st.markdown("**Last 5 Transactions**")
                    st.dataframe(
                        txdf[(txdf["ticker"] == sel_ticker) & (txdf["account_id"] == sel_acct_id)]
                        .sort_values("date", ascending=False)
                        .head(5),
                        use_container_width=True, hide_index=True,
                    )


# ===========================================================================
# Page 3 — Add Transaction
# ===========================================================================

elif page == "Add Transaction":
    st.title("Add Transaction")

    acct_display_to_id = {v: k for k, v in acct_names.items()}
    acct_options       = list(acct_names.values())
    pence_tickers      = config.get("pence_tickers", {}).get("tickers", [])

    with st.form("add_tx_form"):
        tx_date    = st.date_input("Date", value=date.today())
        tx_account = st.selectbox("Account", acct_options)
        tx_ticker  = st.text_input("Ticker").strip().upper()
        tx_action  = st.selectbox("Action", ["BUY", "SELL", "DIVIDEND", "SPLIT", "TRANSFER IN", "TRANSFER OUT"])
        tx_shares  = st.number_input("Shares", min_value=0.0, step=0.01, format="%.4f")
        tx_price   = st.number_input("Price (local CCY)", min_value=0.0, step=0.01, format="%.4f")
        tx_ccy     = st.selectbox("Currency", ["USD", "CAD", "SGD", "HKD", "JPY", "EUR", "GBP", "AED", "BRL", "KRW", "INR"])
        tx_comm    = st.number_input("Commission (USD)", min_value=0.0, value=0.0, step=0.01, format="%.2f")
        tx_notes   = st.text_input("Notes (optional)")
        submitted  = st.form_submit_button("Save Transaction")

    # Pence warning (outside form for live feedback)
    price_confirmed = tx_price
    if tx_ticker in pence_tickers and tx_price > 0 and tx_price < 10:
        st.warning(
            f"⚠️ London-listed stocks are priced in GBP. "
            f"This looks low — did you mean {tx_price:.2f} GBP or {tx_price * 100:.2f}p (pence)?"
        )
        pence_choice = st.radio(
            "Confirm price unit",
            ["GBP (as entered)", "Convert from pence to GBP"],
            key="pence_choice",
        )
        if pence_choice == "Convert from pence to GBP":
            price_confirmed = tx_price / 100.0
            st.info(f"Price will be saved as {price_confirmed:.4f} GBP.")

    if submitted:
        errors = []
        if not tx_ticker:
            errors.append("Ticker is required.")
        if tx_action != "DIVIDEND" and tx_shares <= 0:
            errors.append("Shares must be > 0 (except DIVIDEND).")

        # Short-sell guard
        short_override = False
        if tx_action == "SELL" and tx_ticker and not errors:
            acct_id = acct_display_to_id[tx_account]
            cur_pos = positions[
                (positions["account_id"] == acct_id) & (positions["ticker"] == tx_ticker)
            ]
            current_shares = cur_pos["net_shares"].sum() if not cur_pos.empty else 0.0
            if tx_shares > current_shares:
                st.warning(
                    f"This sell would result in a short position "
                    f"({tx_ticker} in {tx_account}: current {current_shares:.4f} shares, "
                    f"selling {tx_shares:.4f})."
                )
                short_override = st.checkbox("I understand and want to proceed anyway", key="short_override")
                if not short_override:
                    errors.append("Confirm the short-sell above to proceed.")

        if errors:
            for err in errors:
                st.error(err)
        else:
            row = {
                "date":          str(tx_date),
                "account_id":    acct_display_to_id[tx_account],
                "ticker":        tx_ticker,
                "action":        tx_action,
                "shares":        tx_shares,
                "price_local":   price_confirmed,
                "currency":      tx_ccy,
                "commission_usd": tx_comm,
                "notes":         tx_notes,
            }
            try:
                engine.save_transaction(row, config, github_token)
                st.success(
                    f"Saved: {tx_action} {tx_shares} {tx_ticker} "
                    f"@ {price_confirmed} {tx_ccy} in {tx_account}"
                )
                st.rerun()
            except RuntimeError as e:
                st.error(
                    f"Transaction could not be saved to GitHub.\n\n{e}\n\n"
                    "No data was written. Please try again when connectivity is restored."
                )

    st.markdown("---")
    with st.expander("Recent Transactions (last 10)"):
        if not txdf.empty:
            recent = txdf.sort_values("date", ascending=False).head(10)
            st.dataframe(recent, use_container_width=True, hide_index=True)

            st.markdown("**Delete last transaction**")
            if "confirm_delete" not in st.session_state:
                st.session_state.confirm_delete = False

            if not st.session_state.confirm_delete:
                if st.button("Delete last transaction"):
                    st.session_state.confirm_delete = True
                    st.rerun()
            else:
                last_row = txdf.sort_values("date").iloc[-1]
                st.warning(
                    f"About to delete: {last_row['date'].date()} | "
                    f"{last_row['account_id']} | {last_row['ticker']} | "
                    f"{last_row['action']} | {last_row['shares']} @ "
                    f"{last_row['price_local']} {last_row['currency']}"
                )
                c1, c2 = st.columns(2)
                if c1.button("Confirm delete"):
                    try:
                        engine.delete_last_transaction(config, github_token)
                        st.session_state.confirm_delete = False
                        st.success("Last transaction deleted.")
                        st.rerun()
                    except RuntimeError as e:
                        st.error(f"Cannot delete from GitHub.\n\n{e}")
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
        theme_disp = theme_df.copy()
        theme_disp["market_value_usd"] = theme_disp["market_value_usd"].apply(_fmt_usd)
        theme_disp["weight_pct"]       = theme_disp["weight_pct"].apply(_fmt_pct_raw)

        def _flag_high(row):
            try:
                if float(str(row["weight_pct"]).replace("%", "")) > 15:
                    return ["background-color: #fff3cd"] * len(row)
            except Exception:
                pass
            return [""] * len(row)

        theme_disp = theme_disp.rename(columns={
            "theme": "Theme", "market_value_usd": "Value (USD)",
            "weight_pct": "Weight %", "tickers": "Tickers",
        })
        st.dataframe(
            theme_disp.style.apply(_flag_high, axis=1),
            use_container_width=True, hide_index=True,
        )
        st.caption("Amber = theme weight > 15%")

    st.markdown("---")
    st.subheader("Geographic Allocation")
    geo_df = engine.compute_geo_allocation(holdings)
    if not geo_df.empty:
        geo_disp = geo_df.copy()
        geo_disp["market_value_usd"] = geo_disp["market_value_usd"].apply(_fmt_usd)
        geo_disp["weight_pct"]       = geo_disp["weight_pct"].apply(_fmt_pct_raw)
        geo_disp = geo_disp.rename(columns={
            "region": "Region", "market_value_usd": "Value (USD)", "weight_pct": "Weight %",
        })
        st.dataframe(geo_disp, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Cross-Account Overlap")
    overlap_df = engine.compute_overlap(per_account)
    if overlap_df.empty:
        st.info("No tickers held in multiple accounts.")
    else:
        display_cols = {"ticker": "Ticker", "company_name": "Name"}
        for acct_id, acct_disp in acct_names.items():
            col = f"shares_{acct_id}"
            if col in overlap_df.columns:
                display_cols[col] = f"{acct_disp} Shares"
        display_cols["total_shares"]    = "Total Shares"
        display_cols["total_value_usd"] = "Total Value (USD)"

        overlap_disp = overlap_df[[c for c in display_cols if c in overlap_df.columns]].copy()
        overlap_disp = overlap_disp.rename(columns=display_cols)
        if "Total Value (USD)" in overlap_disp.columns:
            overlap_disp["Total Value (USD)"] = overlap_disp["Total Value (USD)"].apply(_fmt_usd)
        st.dataframe(overlap_disp, use_container_width=True, hide_index=True)


# ===========================================================================
# Page 5 — Income & Cash Flow
# ===========================================================================

elif page == "Income & Cash Flow":
    st.title("Income & Cash Flow")

    income = engine.compute_income(txdf, holdings)

    st.subheader("Projected Annual Dividend Income")
    proj = income["projected_income"]
    if not proj.empty:
        proj_sorted = proj.sort_values("projected_annual_income_usd", ascending=False).copy()
        proj_sorted["dividend_yield"] = (proj_sorted["dividend_yield"] * 100).round(2).astype(str) + "%"
        proj_sorted["projected_annual_income_usd"] = proj_sorted["projected_annual_income_usd"].apply(_fmt_usd)
        proj_sorted["price_usd"] = proj_sorted["price_usd"].round(4)
        proj_sorted = proj_sorted.rename(columns={
            "ticker": "Ticker", "company_name": "Name", "total_shares": "Shares",
            "price_usd": "Price (USD)", "dividend_yield": "Div Yield",
            "projected_annual_income_usd": "Proj. Annual Income (USD)",
        })
        st.dataframe(proj_sorted, use_container_width=True, hide_index=True)
        st.metric(
            "Total Projected Annual Income (USD)",
            _fmt_usd(income["projected_income"]["projected_annual_income_usd"].sum())
        )
    else:
        st.info("No dividend-paying positions found.")

    st.markdown("---")
    st.subheader("Realised Cash Flow Log")

    def _add_usd(df):
        if df.empty:
            return df
        df = df.copy()
        rates = df["currency"].map(lambda c: fx_rates.get(c, 1.0) if c != "USD" else 1.0)
        df["value_usd"] = df["price_local"] * df["shares"] / rates
        return df

    divs  = _add_usd(income["dividends_received"])
    sells = _add_usd(income["sale_proceeds"])
    combined = pd.concat([divs, sells], ignore_index=True)

    if not combined.empty:
        combined = combined.sort_values("date", ascending=False)
        combined["account_id"] = combined["account_id"].map(lambda a: acct_names.get(a, a))
        disp_cols = {
            "date": "Date", "account_id": "Account", "ticker": "Ticker",
            "action": "Action", "shares": "Shares", "price_local": "Price",
            "currency": "CCY", "value_usd": "Value (USD)", "notes": "Notes",
        }
        combined_disp = combined[[c for c in disp_cols if c in combined.columns]].rename(columns=disp_cols)
        if "Value (USD)" in combined_disp.columns:
            combined_disp["Value (USD)"] = combined_disp["Value (USD)"].apply(_fmt_usd)
        st.dataframe(combined_disp, use_container_width=True, hide_index=True)

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Dividends Received", _fmt_usd(divs["value_usd"].sum()  if not divs.empty  else 0))
        c2.metric("Total Sale Proceeds",       _fmt_usd(sells["value_usd"].sum() if not sells.empty else 0))
        c3.metric("Combined",                  _fmt_usd((divs["value_usd"].sum() if not divs.empty else 0)
                                                       + (sells["value_usd"].sum() if not sells.empty else 0)))
    else:
        st.info("No SELL or DIVIDEND transactions yet.")
