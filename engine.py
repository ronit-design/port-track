"""
engine.py — Pure computation module for Family Portfolio Tracker.
No Streamlit imports. No side effects. All functions are pure data transformations.
Storage: GitHub CSV via REST API, encrypted with Fernet symmetric encryption.
"""

import base64
import csv
import io
import logging
import time

import pandas as pd
import requests
import toml
import yfinance as yf
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GITHUB_API = "https://api.github.com"

FX_FALLBACKS: dict = {
    "CAD": 1.3688, "SGD": 1.3200, "HKD": 7.831,  "JPY": 150.0,
    "EUR": 0.9200, "GBP": 0.7900, "AED": 3.6725,  "BRL": 5.05,
    "KRW": 1350.0, "INR": 83.5,   "USD": 1.0,
}

_FX_SYMBOLS: dict = {
    "CAD": ("USDCAD=X", False), "SGD": ("USDSGD=X", False),
    "HKD": ("USDHKD=X", False), "JPY": ("USDJPY=X", False),
    "EUR": ("EURUSD=X",  True),  "GBP": ("GBPUSD=X",  True),
    "AED": ("USDAED=X", False), "BRL": ("USDBRL=X", False),
    "KRW": ("USDKRW=X", False), "INR": ("USDINR=X", False),
}

ETF_NAMES: dict = {
    "MJ": "ETFMG Alternative Harvest ETF", "YOLO": "AdvisorShares Pure Cannabis ETF",
    "DRAM": "Stacked Intelligence ETF",    "SPHCF": "Sprott Physical Copper Trust",
    "PPLT": "abrdn Physical Platinum Shares ETF", "IAU": "iShares Gold Trust",
    "SLV": "iShares Silver Trust",         "EWY": "iShares MSCI South Korea ETF",
    "3115.HK": "Premia CSI Caixin China New Economy ETF",
    "2823.HK": "iShares FTSE A50 China Index ETF",
    "3067.HK": "iShares Hang Seng TECH ETF",
}


def _to_roic_ticker(ticker: str) -> str:
    """
    Translate internal ticker format to ROIC API format.
    HK stocks with 3-digit codes need a leading zero (e.g. 914.HK → 0914.HK).
    All other tickers pass through unchanged.
    """
    if ticker.endswith(".HK"):
        code = ticker[:-3]
        if len(code) == 3:
            return f"0{code}.HK"
    return ticker

ROIC_BASE = "https://api.roic.ai/v2"

TRANSACTION_COLS = [
    "date", "account_id", "ticker", "action",
    "shares", "price_local", "currency", "commission_usd", "notes",
]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return toml.load(f)


# ---------------------------------------------------------------------------
# Encryption helpers
# ---------------------------------------------------------------------------

def _fernet(key: str) -> Fernet:
    return Fernet(key.encode() if isinstance(key, str) else key)


def _encrypt_csv(content: str, enc_key: str) -> bytes:
    """Encrypt plaintext CSV string → encrypted bytes."""
    return _fernet(enc_key).encrypt(content.encode("utf-8"))


def _decrypt_csv(data: bytes, enc_key: str) -> str:
    """Decrypt encrypted bytes → plaintext CSV string."""
    return _fernet(enc_key).decrypt(data).decode("utf-8")


# ---------------------------------------------------------------------------
# GitHub helpers
# ---------------------------------------------------------------------------

def _gh_headers(token: str) -> dict:
    return {
        "Authorization":        f"Bearer {token}",
        "Accept":               "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _gh_get_file(config: dict, token: str, enc_key: str) -> tuple[str, str]:
    """
    Fetch and decrypt the transactions CSV from GitHub.
    Returns (plaintext_csv_str, sha).
    """
    repo   = config["github"]["repo"]
    path   = config["github"]["file_path"]
    branch = config["github"]["branch"]

    url  = f"{GITHUB_API}/repos/{repo}/contents/{path}?ref={branch}"
    resp = requests.get(url, headers=_gh_headers(token), timeout=15)
    resp.raise_for_status()

    data         = resp.json()
    raw_bytes    = base64.b64decode(data["content"].replace("\n", ""))
    plaintext    = _decrypt_csv(raw_bytes, enc_key)
    return plaintext, data["sha"]


def _gh_put_file(
    config: dict, token: str, enc_key: str,
    plaintext: str, sha: str, message: str,
) -> None:
    """Encrypt and commit the CSV back to GitHub."""
    repo   = config["github"]["repo"]
    path   = config["github"]["file_path"]
    branch = config["github"]["branch"]

    encrypted = _encrypt_csv(plaintext, enc_key)

    url     = f"{GITHUB_API}/repos/{repo}/contents/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(encrypted).decode("utf-8"),
        "sha":     sha,
        "branch":  branch,
    }
    resp = requests.put(url, json=payload, headers=_gh_headers(token), timeout=15)
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Transaction I/O
# ---------------------------------------------------------------------------

def read_transactions(config: dict, token: str, enc_key: str) -> pd.DataFrame:
    """Read and decrypt all rows from GitHub CSV. Raises RuntimeError if unavailable."""
    try:
        plaintext, _ = _gh_get_file(config, token, enc_key)
        df = pd.read_csv(io.StringIO(plaintext))

        for col in TRANSACTION_COLS:
            if col not in df.columns:
                df[col] = None
        df = df[TRANSACTION_COLS]

        df["date"]           = pd.to_datetime(df["date"], errors="coerce")
        df["shares"]         = pd.to_numeric(df["shares"],         errors="coerce")
        df["price_local"]    = pd.to_numeric(df["price_local"],    errors="coerce")
        df["commission_usd"] = pd.to_numeric(df["commission_usd"], errors="coerce").fillna(0.0)

        df.to_csv("transactions_backup.csv", index=False)
        return df

    except Exception as e:
        raise RuntimeError(f"Cannot read transactions from GitHub: {e}") from e


def save_transaction(row: dict, config: dict, token: str, enc_key: str) -> None:
    """Append one transaction row, re-encrypt, and commit to GitHub."""
    try:
        plaintext, sha = _gh_get_file(config, token, enc_key)

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            row["date"], row["account_id"], row["ticker"], row["action"],
            float(row["shares"]), float(row["price_local"]), row["currency"],
            float(row["commission_usd"]), row.get("notes", ""),
        ])
        new_plaintext = plaintext.rstrip("\n") + "\n" + buf.getvalue()

        _gh_put_file(
            config, token, enc_key, new_plaintext, sha,
            f"tx: {row['action']} {row['shares']} {row['ticker']} ({row['account_id']})",
        )
    except Exception as e:
        raise RuntimeError(f"Cannot save transaction to GitHub: {e}") from e


def delete_last_transaction(config: dict, token: str, enc_key: str) -> dict:
    """Remove and re-encrypt the last data row. Returns the deleted row as dict."""
    try:
        plaintext, sha = _gh_get_file(config, token, enc_key)

        reader    = csv.DictReader(io.StringIO(plaintext))
        rows      = list(reader)
        if not rows:
            raise ValueError("No transactions to delete.")

        deleted   = rows[-1]
        remaining = rows[:-1]

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=TRANSACTION_COLS)
        writer.writeheader()
        writer.writerows(remaining)

        _gh_put_file(
            config, token, enc_key, buf.getvalue(), sha,
            f"delete last tx: {deleted.get('action')} {deleted.get('ticker')}",
        )
        return deleted
    except Exception as e:
        raise RuntimeError(f"Cannot delete transaction from GitHub: {e}") from e


# ---------------------------------------------------------------------------
# Position engine
# ---------------------------------------------------------------------------

def compute_positions(txdf: pd.DataFrame) -> pd.DataFrame:
    if txdf.empty:
        return pd.DataFrame(columns=[
            "account_id", "ticker", "net_shares",
            "avg_cost_local", "currency", "total_commission_usd",
        ])

    results = []
    for (account_id, ticker), group in txdf.groupby(["account_id", "ticker"]):
        group = group.sort_values("date").reset_index(drop=True)
        net_shares = 0.0; total_cost = 0.0; total_comm = 0.0; currency = "USD"

        for _, txrow in group.iterrows():
            action = str(txrow.get("action", "")).strip().upper()
            shares = float(txrow.get("shares", 0) or 0)
            price  = float(txrow.get("price_local", 0) or 0)
            comm   = float(txrow.get("commission_usd", 0) or 0)
            ccy    = str(txrow.get("currency", "USD") or "USD").strip()

            if action == "BUY":
                total_cost += shares * price; net_shares += shares
                total_comm += comm; currency = ccy
            elif action == "SELL":
                if net_shares > 0:
                    total_cost *= 1.0 - min(shares / net_shares, 1.0)
                net_shares = max(0.0, net_shares - shares)
            elif action == "TRANSFER IN":
                net_shares += shares
            elif action == "TRANSFER OUT":
                net_shares = max(0.0, net_shares - shares)
            elif action == "SPLIT":
                if shares > 0:
                    net_shares = shares

        if net_shares <= 0:
            continue
        results.append({
            "account_id": account_id, "ticker": ticker,
            "net_shares": net_shares,
            "avg_cost_local": total_cost / net_shares if net_shares > 0 else 0.0,
            "currency": currency, "total_commission_usd": total_comm,
        })

    return pd.DataFrame(results) if results else pd.DataFrame(columns=[
        "account_id", "ticker", "net_shares",
        "avg_cost_local", "currency", "total_commission_usd",
    ])


def compute_consolidated(positions: pd.DataFrame) -> pd.DataFrame:
    if positions.empty:
        return pd.DataFrame(columns=[
            "ticker", "total_shares", "avg_cost_local", "primary_currency", "accounts_held",
        ])
    results = []
    for ticker, grp in positions.groupby("ticker"):
        total_shares = grp["net_shares"].sum()
        results.append({
            "ticker": ticker, "total_shares": total_shares,
            "avg_cost_local": (grp["net_shares"] * grp["avg_cost_local"]).sum() / total_shares,
            "primary_currency": grp["currency"].mode().iloc[0],
            "accounts_held": sorted(grp["account_id"].tolist()),
        })
    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------

def _extract_close(raw: pd.DataFrame, tickers: list) -> dict:
    if raw.empty:
        return {t: float("nan") for t in tickers}
    close = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
    prices = {}
    for t in tickers:
        if t in close.columns:
            s = close[t].dropna()
            prices[t] = float(s.iloc[-1]) if not s.empty else float("nan")
        else:
            prices[t] = float("nan")
    return prices


def fetch_prices(tickers: list, etf_list: list) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame(columns=["ticker", "price_local"])
    prices = {}
    try:
        raw    = yf.download(tickers, period="2d", auto_adjust=True, progress=False)
        prices = _extract_close(raw, tickers)
    except Exception as e:
        logger.warning(f"Batch price fetch failed: {e}")
    missing = [t for t in tickers if t not in prices or pd.isna(prices.get(t))]
    for t in missing:
        try:
            raw = yf.download(t, period="2d", auto_adjust=True, progress=False)
            prices[t] = _extract_close(raw, [t]).get(t, float("nan"))
        except Exception as e:
            logger.warning(f"Price fetch failed for {t}: {e}")
            prices[t] = float("nan")
    return pd.DataFrame([{"ticker": t, "price_local": prices.get(t, float("nan"))} for t in tickers])


def fetch_fx_rates(currencies: list, overrides: dict) -> dict:
    rates = dict(FX_FALLBACKS)
    symbols = [_FX_SYMBOLS[c][0] for c in currencies if c in _FX_SYMBOLS]
    sym_map = {_FX_SYMBOLS[c][0]: (c, _FX_SYMBOLS[c][1]) for c in currencies if c in _FX_SYMBOLS}
    try:
        raw     = yf.download(symbols, period="1d", auto_adjust=False, progress=False)
        fetched = _extract_close(raw, symbols)
        for sym, (ccy, inv) in sym_map.items():
            v = fetched.get(sym)
            if v and not pd.isna(v) and v > 0:
                rates[ccy] = (1.0 / v) if inv else v
    except Exception as e:
        logger.warning(f"FX fetch failed: {e}")
    rates.update(overrides)
    return rates


# ---------------------------------------------------------------------------
# Fundamentals
# ---------------------------------------------------------------------------

def fetch_fundamentals(tickers: list, api_key: str, etf_list: list) -> pd.DataFrame:
    rows = []
    for ticker in tickers:
        if ticker in etf_list:
            rows.append({
                "ticker": ticker, "company_name": ETF_NAMES.get(ticker, ticker),
                "sector": "ETF", "industry": "Exchange Traded Fund",
                "country": "", "exchange": "", "dividend_yield": 0.0,
                "ex_dividend_date": None, "pe_ratio": None,
                "ev_ebitda": None, "pb_ratio": None, "gross_margin": None,
            })
            continue
        row = {k: None for k in ["ticker","company_name","sector","industry","country",
                                   "exchange","dividend_yield","ex_dividend_date",
                                   "pe_ratio","ev_ebitda","pb_ratio","gross_margin"]}
        row["ticker"] = ticker; row["company_name"] = ticker
        roic_ticker = _to_roic_ticker(ticker)
        try:
            resp = requests.get(f"{ROIC_BASE}/company/profile/{roic_ticker}?apikey={api_key}", timeout=15)
            resp.raise_for_status(); p = resp.json()
            row.update({
                "company_name": p.get("company_name", ticker) or ticker,
                "sector": p.get("sector"), "industry": p.get("industry"),
                "country": p.get("country"), "exchange": p.get("exchange_short_name"),
                "dividend_yield": p.get("dividend_yield"), "ex_dividend_date": p.get("ex_dividend_date"),
            })
        except Exception as e:
            logger.warning(f"ROIC profile failed for {roic_ticker}: {e}")
        time.sleep(0.1)
        try:
            resp = requests.get(f"{ROIC_BASE}/fundamental/multiples/{roic_ticker}?apikey={api_key}&limit=1", timeout=15)
            resp.raise_for_status(); data = resp.json()
            if data and isinstance(data, list):
                m = data[0]
                row.update({"pe_ratio": m.get("peRatio"), "ev_ebitda": m.get("evEbitda"),
                             "pb_ratio": m.get("pbRatio"), "gross_margin": m.get("grossMargin")})
        except Exception as e:
            logger.warning(f"ROIC multiples failed for {ticker}: {e}")
        time.sleep(0.1)
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Holdings builders
# ---------------------------------------------------------------------------

def _fx(rate_map: dict, ccy: str) -> float:
    return 1.0 if ccy == "USD" else rate_map.get(ccy, 1.0)


def _build_holding_row(ticker, total_shares, avg_cost_loc, currency, accounts_held,
                        price_lkp, fx_rates, fund_lkp, theme_map, acct_names=None):
    rate = _fx(fx_rates, currency)
    pl   = price_lkp.get(ticker, float("nan"))
    pu   = (pl / rate) if not pd.isna(pl) else float("nan")
    mv   = total_shares * pu  if not pd.isna(pu) else float("nan")
    acu  = avg_cost_loc / rate
    tcu  = total_shares * acu
    pnl  = (mv - tcu) if not pd.isna(mv) else float("nan")
    pct  = (pnl / tcu) if (tcu > 0 and not pd.isna(pnl)) else float("nan")
    fund = fund_lkp.get(ticker, {})
    sec  = fund.get("sector") or "Unknown"
    row  = {
        "ticker": ticker, "company_name": fund.get("company_name") or ticker,
        "total_shares": total_shares, "avg_cost_local": avg_cost_loc, "currency": currency,
        "avg_cost_usd": acu, "total_cost_usd": tcu,
        "price_local": pl, "price_usd": pu,
        "market_value_local": total_shares * pl if not pd.isna(pl) else float("nan"),
        "market_value_usd": mv, "unrealised_pnl_usd": pnl, "pnl_pct": pct,
        "weight": float("nan"), "theme": theme_map.get(ticker, sec), "sector": sec,
        "industry": fund.get("industry") or "", "country": fund.get("country") or "",
        "exchange": fund.get("exchange") or "", "dividend_yield": fund.get("dividend_yield") or 0.0,
        "ex_dividend_date": fund.get("ex_dividend_date"), "pe_ratio": fund.get("pe_ratio"),
        "ev_ebitda": fund.get("ev_ebitda"), "pb_ratio": fund.get("pb_ratio"),
        "gross_margin": fund.get("gross_margin"),
    }
    if acct_names is not None:
        row["accounts_held"]   = accounts_held
        row["accounts_display"] = [acct_names.get(a, a) for a in accounts_held]
    return row


def build_holdings(positions, consolidated, prices, fx_rates, fundamentals, theme_map, config):
    if consolidated.empty:
        return pd.DataFrame()
    pl  = prices.set_index("ticker")["price_local"].to_dict() if not prices.empty else {}
    fl  = fundamentals.set_index("ticker").to_dict("index") if not fundamentals.empty else {}
    an  = config["accounts"]
    rows = [
        _build_holding_row(
            r["ticker"], r["total_shares"], r["avg_cost_local"], r["primary_currency"],
            r["accounts_held"], pl, fx_rates, fl, theme_map, an,
        )
        for _, r in consolidated.iterrows()
    ]
    df = pd.DataFrame(rows)
    if not df.empty:
        tv = df["market_value_usd"].sum()
        df["weight"] = df["market_value_usd"] / tv if tv > 0 else 0.0
    return df


def build_per_account_holdings(positions, prices, fx_rates, fundamentals, theme_map):
    if positions.empty:
        return pd.DataFrame()
    pl = prices.set_index("ticker")["price_local"].to_dict() if not prices.empty else {}
    fl = fundamentals.set_index("ticker").to_dict("index") if not fundamentals.empty else {}
    rows = []
    for _, p in positions.iterrows():
        r = _build_holding_row(
            p["ticker"], p["net_shares"], p["avg_cost_local"], p["currency"],
            [p["account_id"]], pl, fx_rates, fl, theme_map,
        )
        r["account_id"] = p["account_id"]
        r["net_shares"] = p["net_shares"]
        rows.append(r)
    df = pd.DataFrame(rows)
    if not df.empty:
        tv = df["market_value_usd"].sum()
        df["weight"] = df["market_value_usd"] / tv if tv > 0 else 0.0
    return df


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

def compute_income(txdf, holdings):
    empty = pd.DataFrame(columns=TRANSACTION_COLS)
    if txdf.empty:
        return {"dividends_received": empty, "sale_proceeds": empty, "projected_income": pd.DataFrame()}
    div_rows  = txdf[txdf["action"] == "DIVIDEND"].copy()
    sell_rows = txdf[txdf["action"] == "SELL"].copy()
    proj = []
    if not holdings.empty:
        for _, h in holdings.iterrows():
            dy = float(h.get("dividend_yield") or 0)
            if dy > 0:
                proj.append({
                    "ticker": h["ticker"], "company_name": h.get("company_name", h["ticker"]),
                    "total_shares": h["total_shares"], "price_usd": h.get("price_usd", 0.0),
                    "dividend_yield": dy,
                    "projected_annual_income_usd": h["total_shares"] * (h.get("price_usd") or 0.0) * dy,
                })
    return {"dividends_received": div_rows, "sale_proceeds": sell_rows, "projected_income": pd.DataFrame(proj)}


def compute_sector_allocation(holdings):
    if holdings.empty:
        return pd.DataFrame(columns=["sector", "market_value_usd", "weight_pct"])
    g = holdings.groupby("sector")["market_value_usd"].sum().reset_index()
    t = g["market_value_usd"].sum()
    g["weight_pct"] = g["market_value_usd"] / t * 100 if t > 0 else 0.0
    return g.sort_values("market_value_usd", ascending=False).reset_index(drop=True)


def compute_theme_allocation(holdings):
    if holdings.empty:
        return pd.DataFrame(columns=["theme", "market_value_usd", "weight_pct", "tickers"])
    g = holdings.groupby("theme").apply(
        lambda x: pd.Series({"market_value_usd": x["market_value_usd"].sum(),
                              "tickers": ", ".join(sorted(x["ticker"].tolist()))})
    ).reset_index()
    t = g["market_value_usd"].sum()
    g["weight_pct"] = g["market_value_usd"] / t * 100 if t > 0 else 0.0
    return g.sort_values("market_value_usd", ascending=False).reset_index(drop=True)


_REGION_MAP = {
    "United States": "North America", "Canada": "North America",
    "China": "Asia Pacific",          "Hong Kong": "Asia Pacific",
    "Japan": "Asia Pacific",          "Singapore": "Asia Pacific",
    "South Korea": "Asia Pacific",    "Taiwan": "Asia Pacific",
    "Australia": "Asia Pacific",      "India": "Asia Pacific",
    "United Kingdom": "Europe",       "Ireland": "Europe",
    "Denmark": "Europe",              "Germany": "Europe",
    "France": "Europe",               "Netherlands": "Europe",
    "Switzerland": "Europe",          "United Arab Emirates": "Middle East",
    "Brazil": "Latin America",
}


def compute_geo_allocation(holdings):
    if holdings.empty:
        return pd.DataFrame(columns=["region", "market_value_usd", "weight_pct"])
    df = holdings.copy()
    df["region"] = df["country"].apply(
        lambda c: _REGION_MAP.get(str(c).strip(), "Other") if c and not pd.isna(c) else "Other"
    )
    g = df.groupby("region")["market_value_usd"].sum().reset_index()
    t = g["market_value_usd"].sum()
    g["weight_pct"] = g["market_value_usd"] / t * 100 if t > 0 else 0.0
    return g.sort_values("market_value_usd", ascending=False).reset_index(drop=True)


def compute_overlap(per_account):
    if per_account.empty:
        return pd.DataFrame()
    counts = per_account.groupby("ticker")["account_id"].nunique()
    tickers = counts[counts > 1].index.tolist()
    if not tickers:
        return pd.DataFrame()
    sub = per_account[per_account["ticker"].isin(tickers)].copy()
    ps  = sub.pivot_table(index=["ticker","company_name"], columns="account_id",
                           values="net_shares", aggfunc="sum").reset_index()
    ps.columns.name = None
    aids = [c for c in ps.columns if c not in ("ticker","company_name")]
    ps   = ps.rename(columns={a: f"shares_{a}" for a in aids})
    pv   = sub.pivot_table(index="ticker", columns="account_id",
                            values="market_value_usd", aggfunc="sum").reset_index()
    pv.columns.name = None
    vids = [c for c in pv.columns if c != "ticker"]
    pv   = pv.rename(columns={a: f"value_usd_{a}" for a in vids})
    m    = ps.merge(pv, on="ticker", how="left")
    sc   = [c for c in m.columns if c.startswith("shares_")]
    vc   = [c for c in m.columns if c.startswith("value_usd_")]
    m["total_shares"]    = m[sc].fillna(0).sum(axis=1)
    m["total_value_usd"] = m[vc].fillna(0).sum(axis=1)
    return m.fillna(0).sort_values("total_value_usd", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# LLM context builder
# ---------------------------------------------------------------------------

def build_portfolio_context(
    holdings: pd.DataFrame,
    per_account: pd.DataFrame,
    txdf: pd.DataFrame,
    fx_rates: dict,
    config: dict,
) -> str:
    """
    Build a structured text summary of the current portfolio state
    to inject as the LLM system context.
    """
    acct_names = config["accounts"]
    lines = ["# FAMILY PORTFOLIO — LIVE SNAPSHOT", ""]

    # --- Portfolio totals ---
    if not holdings.empty:
        tv  = holdings["market_value_usd"].sum()
        tc  = holdings["total_cost_usd"].sum()
        pnl = tv - tc
        pct = pnl / tc * 100 if tc > 0 else 0
        lines += [
            "## Portfolio Totals",
            f"Total Market Value : ${tv:,.0f} USD",
            f"Total Cost Basis   : ${tc:,.0f} USD",
            f"Unrealised P&L     : ${pnl:,.0f} USD ({pct:+.1f}%)",
            f"Number of positions: {len(holdings)}",
            "",
        ]

    # --- Account breakdown ---
    lines.append("## Account Breakdown")
    for acct_id, acct_disp in acct_names.items():
        sub = per_account[per_account["account_id"] == acct_id] if not per_account.empty else pd.DataFrame()
        if sub.empty:
            continue
        mv  = sub["market_value_usd"].sum()
        tc  = sub["total_cost_usd"].sum()
        pnl = mv - tc
        lines.append(f"{acct_disp}: ${mv:,.0f} value | ${tc:,.0f} cost | ${pnl:,.0f} P&L | {len(sub)} positions")
    lines.append("")

    # --- Full holdings table ---
    if not holdings.empty:
        lines.append("## All Holdings (sorted by market value)")
        lines.append("Ticker | Name | Accounts | Shares | Avg Cost (local) | CCY | Avg Cost USD | Price USD | Value USD | P&L USD | P&L% | Weight% | Theme | Sector | Country | P/E | EV/EBITDA | Div Yield")
        for _, r in holdings.sort_values("market_value_usd", ascending=False).iterrows():
            accts = ", ".join([acct_names.get(a, a) for a in r.get("accounts_held", [])])
            pe    = f"{r['pe_ratio']:.1f}" if r.get("pe_ratio") else "–"
            ev    = f"{r['ev_ebitda']:.1f}" if r.get("ev_ebitda") else "–"
            dy    = f"{float(r['dividend_yield'])*100:.2f}%" if r.get("dividend_yield") else "–"
            pct   = f"{r['pnl_pct']*100:+.1f}%" if not pd.isna(r.get("pnl_pct", float("nan"))) else "–"
            wt    = f"{r['weight']*100:.2f}%" if not pd.isna(r.get("weight", float("nan"))) else "–"
            lines.append(
                f"{r['ticker']} | {r.get('company_name','–')} | {accts} | "
                f"{r['total_shares']:.2f} | {r['avg_cost_local']:.4f} | {r['currency']} | "
                f"{r.get('avg_cost_usd',0):.4f} | {r.get('price_usd',0):.4f} | "
                f"${r.get('market_value_usd',0):,.0f} | ${r.get('unrealised_pnl_usd',0):,.0f} | "
                f"{pct} | {wt} | {r.get('theme','–')} | {r.get('sector','–')} | "
                f"{r.get('country','–')} | {pe} | {ev} | {dy}"
            )
        lines.append("")

    # --- Theme allocation ---
    theme_df = compute_theme_allocation(holdings)
    if not theme_df.empty:
        lines.append("## Theme Allocation")
        for _, r in theme_df.iterrows():
            lines.append(f"{r['theme']}: ${r['market_value_usd']:,.0f} ({r['weight_pct']:.1f}%) — {r['tickers']}")
        lines.append("")

    # --- Sector & geo ---
    sec_df = compute_sector_allocation(holdings)
    if not sec_df.empty:
        lines.append("## Sector Allocation")
        for _, r in sec_df.iterrows():
            lines.append(f"{r['sector']}: ${r['market_value_usd']:,.0f} ({r['weight_pct']:.1f}%)")
        lines.append("")

    geo_df = compute_geo_allocation(holdings)
    if not geo_df.empty:
        lines.append("## Geographic Allocation")
        for _, r in geo_df.iterrows():
            lines.append(f"{r['region']}: ${r['market_value_usd']:,.0f} ({r['weight_pct']:.1f}%)")
        lines.append("")

    # --- FX rates ---
    lines.append("## Current FX Rates (units of foreign CCY per 1 USD)")
    for ccy, rate in sorted(fx_rates.items()):
        if ccy != "USD":
            lines.append(f"1 USD = {rate:.4f} {ccy}")
    lines.append("")

    # --- Recent transactions ---
    if not txdf.empty:
        lines.append("## Last 30 Transactions (most recent first)")
        lines.append("Date | Account | Ticker | Action | Shares | Price | CCY | Commission | Notes")
        recent = txdf.sort_values("date", ascending=False).head(30)
        for _, r in recent.iterrows():
            acct_disp = acct_names.get(str(r.get("account_id", "")), str(r.get("account_id", "")))
            lines.append(
                f"{str(r['date'])[:10]} | {acct_disp} | {r['ticker']} | {r['action']} | "
                f"{r['shares']} | {r['price_local']} | {r['currency']} | "
                f"{r['commission_usd']} | {r.get('notes','')}"
            )
        lines.append("")

    return "\n".join(lines)
