"""
engine.py — Pure computation module for Family Portfolio Tracker.
No Streamlit imports. No side effects. All functions are pure data transformations.
Storage backend: GitHub CSV via REST API.
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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GITHUB_API = "https://api.github.com"

FX_FALLBACKS: dict = {
    "CAD": 1.3688,
    "SGD": 1.3200,
    "HKD": 7.831,
    "JPY": 150.0,
    "EUR": 0.9200,
    "GBP": 0.7900,
    "AED": 3.6725,
    "BRL": 5.05,
    "KRW": 1350.0,
    "INR": 83.5,
    "USD": 1.0,
}

# (yfinance symbol, needs_inversion)
# EUR/GBP: fetch EURUSD=X / GBPUSD=X then invert to get foreign-per-USD
_FX_SYMBOLS: dict = {
    "CAD": ("USDCAD=X", False),
    "SGD": ("USDSGD=X", False),
    "HKD": ("USDHKD=X", False),
    "JPY": ("USDJPY=X", False),
    "EUR": ("EURUSD=X",  True),
    "GBP": ("GBPUSD=X",  True),
    "AED": ("USDAED=X", False),
    "BRL": ("USDBRL=X", False),
    "KRW": ("USDKRW=X", False),
    "INR": ("USDINR=X", False),
}

ETF_NAMES: dict = {
    "MJ":      "ETFMG Alternative Harvest ETF",
    "YOLO":    "AdvisorShares Pure Cannabis ETF",
    "DRAM":    "Stacked Intelligence ETF",
    "COPRF":   "Sprott Physical Copper Trust",
    "PPLT":    "abrdn Physical Platinum Shares ETF",
    "IAU":     "iShares Gold Trust",
    "SLV":     "iShares Silver Trust",
    "EWY":     "iShares MSCI South Korea ETF",
    "3115.HK": "Premia CSI Caixin China New Economy ETF",
    "2823.HK": "iShares FTSE A50 China Index ETF",
    "3067.HK": "iShares Hang Seng TECH ETF",
}

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
# GitHub CSV helpers
# ---------------------------------------------------------------------------

def _gh_headers(token: str) -> dict:
    return {
        "Authorization":        f"Bearer {token}",
        "Accept":               "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _gh_get_file(config: dict, token: str) -> tuple[str, str]:
    """
    Fetch the transactions CSV from GitHub.
    Returns (decoded_content_str, sha).
    Raises RuntimeError on any failure.
    """
    repo   = config["github"]["repo"]
    path   = config["github"]["file_path"]
    branch = config["github"]["branch"]

    url  = f"{GITHUB_API}/repos/{repo}/contents/{path}?ref={branch}"
    resp = requests.get(url, headers=_gh_headers(token), timeout=15)
    resp.raise_for_status()

    data    = resp.json()
    content = base64.b64decode(data["content"].replace("\n", "")).decode("utf-8")
    return content, data["sha"]


def _gh_put_file(
    config: dict, token: str, content: str, sha: str, message: str
) -> None:
    """Commit updated file content back to GitHub."""
    repo   = config["github"]["repo"]
    path   = config["github"]["file_path"]
    branch = config["github"]["branch"]

    url     = f"{GITHUB_API}/repos/{repo}/contents/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "sha":     sha,
        "branch":  branch,
    }
    resp = requests.put(url, json=payload, headers=_gh_headers(token), timeout=15)
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Transaction I/O
# ---------------------------------------------------------------------------

def read_transactions(config: dict, token: str) -> pd.DataFrame:
    """
    Read all rows from the GitHub CSV. Raises RuntimeError if unavailable.
    Writes a passive local backup on every successful read.
    """
    try:
        content, _ = _gh_get_file(config, token)
        df = pd.read_csv(io.StringIO(content))

        # Ensure all expected columns are present
        for col in TRANSACTION_COLS:
            if col not in df.columns:
                df[col] = None

        df = df[TRANSACTION_COLS]  # enforce column order

        df["date"]           = pd.to_datetime(df["date"], errors="coerce")
        df["shares"]         = pd.to_numeric(df["shares"],         errors="coerce")
        df["price_local"]    = pd.to_numeric(df["price_local"],    errors="coerce")
        df["commission_usd"] = pd.to_numeric(df["commission_usd"], errors="coerce").fillna(0.0)

        df.to_csv("transactions_backup.csv", index=False)
        return df

    except Exception as e:
        raise RuntimeError(f"Cannot read transactions from GitHub: {e}") from e


def save_transaction(row: dict, config: dict, token: str) -> None:
    """
    Append one transaction row to the GitHub CSV.
    Raises RuntimeError on any failure.
    """
    try:
        content, sha = _gh_get_file(config, token)

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            row["date"],
            row["account_id"],
            row["ticker"],
            row["action"],
            float(row["shares"]),
            float(row["price_local"]),
            row["currency"],
            float(row["commission_usd"]),
            row.get("notes", ""),
        ])
        new_line = buf.getvalue()

        new_content = content.rstrip("\n") + "\n" + new_line

        _gh_put_file(
            config, token, new_content, sha,
            f"tx: {row['action']} {row['shares']} {row['ticker']} ({row['account_id']})",
        )

    except Exception as e:
        raise RuntimeError(f"Cannot save transaction to GitHub: {e}") from e


def delete_last_transaction(config: dict, token: str) -> dict:
    """
    Remove the last data row from the GitHub CSV.
    Returns the deleted row as a dict. Raises RuntimeError on failure.
    """
    try:
        content, sha = _gh_get_file(config, token)

        reader = csv.DictReader(io.StringIO(content))
        rows   = list(reader)

        if not rows:
            raise ValueError("No transactions to delete.")

        deleted   = rows[-1]
        remaining = rows[:-1]

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=TRANSACTION_COLS)
        writer.writeheader()
        writer.writerows(remaining)
        new_content = buf.getvalue()

        _gh_put_file(
            config, token, new_content, sha,
            f"delete last tx: {deleted.get('action')} {deleted.get('ticker')}",
        )
        return deleted

    except Exception as e:
        raise RuntimeError(f"Cannot delete transaction from GitHub: {e}") from e


# ---------------------------------------------------------------------------
# Position engine
# ---------------------------------------------------------------------------

def compute_positions(txdf: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-(account, ticker) net positions from the transaction ledger.
    Returns: account_id, ticker, net_shares, avg_cost_local, currency, total_commission_usd
    Closed positions (net_shares <= 0) are excluded.
    """
    if txdf.empty:
        return pd.DataFrame(
            columns=["account_id", "ticker", "net_shares",
                     "avg_cost_local", "currency", "total_commission_usd"]
        )

    results = []

    for (account_id, ticker), group in txdf.groupby(["account_id", "ticker"]):
        group = group.sort_values("date").reset_index(drop=True)

        net_shares         = 0.0
        total_cost_local   = 0.0
        total_commission   = 0.0
        currency           = "USD"

        for _, txrow in group.iterrows():
            action     = str(txrow.get("action", "")).strip().upper()
            shares     = float(txrow.get("shares", 0) or 0)
            price      = float(txrow.get("price_local", 0) or 0)
            commission = float(txrow.get("commission_usd", 0) or 0)
            row_ccy    = str(txrow.get("currency", "USD") or "USD").strip()

            if action == "BUY":
                total_cost_local += shares * price
                net_shares       += shares
                total_commission += commission
                currency          = row_ccy

            elif action == "SELL":
                if net_shares > 0:
                    fraction_sold     = min(shares / net_shares, 1.0)
                    total_cost_local *= 1.0 - fraction_sold
                net_shares = max(0.0, net_shares - shares)

            elif action == "TRANSFER IN":
                net_shares += shares

            elif action == "TRANSFER OUT":
                net_shares = max(0.0, net_shares - shares)

            elif action == "SPLIT":
                if shares > 0:
                    net_shares = shares  # total_cost unchanged → avg cost falls proportionally

            # DIVIDEND: no effect on positions

        if net_shares <= 0:
            continue

        results.append({
            "account_id":         account_id,
            "ticker":             ticker,
            "net_shares":         net_shares,
            "avg_cost_local":     total_cost_local / net_shares if net_shares > 0 else 0.0,
            "currency":           currency,
            "total_commission_usd": total_commission,
        })

    if not results:
        return pd.DataFrame(
            columns=["account_id", "ticker", "net_shares",
                     "avg_cost_local", "currency", "total_commission_usd"]
        )
    return pd.DataFrame(results)


def compute_consolidated(positions: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-account positions into one row per ticker."""
    if positions.empty:
        return pd.DataFrame(
            columns=["ticker", "total_shares", "avg_cost_local",
                     "primary_currency", "accounts_held"]
        )

    results = []
    for ticker, grp in positions.groupby("ticker"):
        total_shares      = grp["net_shares"].sum()
        primary_currency  = grp["currency"].mode().iloc[0]
        weighted_cost     = (grp["net_shares"] * grp["avg_cost_local"]).sum()
        avg_cost_local    = weighted_cost / total_shares if total_shares > 0 else 0.0
        accounts_held     = sorted(grp["account_id"].tolist())

        results.append({
            "ticker":           ticker,
            "total_shares":     total_shares,
            "avg_cost_local":   avg_cost_local,
            "primary_currency": primary_currency,
            "accounts_held":    accounts_held,
        })

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------

def _extract_close(raw: pd.DataFrame, tickers: list) -> dict:
    if raw.empty:
        return {t: float("nan") for t in tickers}

    if isinstance(raw.columns, pd.MultiIndex):
        try:
            close = raw["Close"]
        except KeyError:
            return {t: float("nan") for t in tickers}
    else:
        close = raw

    prices = {}
    for ticker in tickers:
        if ticker in close.columns:
            series = close[ticker].dropna()
            prices[ticker] = float(series.iloc[-1]) if not series.empty else float("nan")
        else:
            prices[ticker] = float("nan")
    return prices


def fetch_prices(tickers: list, etf_list: list) -> pd.DataFrame:
    """
    Fetch latest close prices for all tickers via yfinance.
    Returns DataFrame: ticker, price_local. Never raises.
    """
    if not tickers:
        return pd.DataFrame(columns=["ticker", "price_local"])

    prices = {}

    try:
        raw    = yf.download(tickers, period="2d", auto_adjust=True, progress=False)
        prices = _extract_close(raw, tickers)
    except Exception as e:
        logger.warning(f"Batch price fetch failed: {e}; trying individually.")

    missing = [t for t in tickers if t not in prices or pd.isna(prices.get(t))]
    for ticker in missing:
        try:
            raw = yf.download(ticker, period="2d", auto_adjust=True, progress=False)
            fetched = _extract_close(raw, [ticker])
            prices[ticker] = fetched.get(ticker, float("nan"))
        except Exception as e:
            logger.warning(f"Price fetch failed for {ticker}: {e}")
            prices[ticker] = float("nan")

    return pd.DataFrame([
        {"ticker": t, "price_local": prices.get(t, float("nan"))} for t in tickers
    ])


def fetch_fx_rates(currencies: list, overrides: dict) -> dict:
    """
    Fetch live FX rates. Convention: units of foreign CCY per 1 USD.
    Falls back to FX_FALLBACKS on failure. Applies overrides last.
    """
    rates = dict(FX_FALLBACKS)
    rates["USD"] = 1.0

    symbols_to_fetch = [_FX_SYMBOLS[c][0] for c in currencies if c in _FX_SYMBOLS]
    symbol_map       = {_FX_SYMBOLS[c][0]: (c, _FX_SYMBOLS[c][1]) for c in currencies if c in _FX_SYMBOLS}

    try:
        raw     = yf.download(symbols_to_fetch, period="1d", auto_adjust=False, progress=False)
        fetched = _extract_close(raw, symbols_to_fetch)

        for symbol, (ccy, needs_inversion) in symbol_map.items():
            val = fetched.get(symbol)
            if val and not pd.isna(val) and val > 0:
                rates[ccy] = (1.0 / val) if needs_inversion else val
    except Exception as e:
        logger.warning(f"FX batch fetch failed: {e}; using fallbacks.")

    rates.update(overrides)
    return rates


# ---------------------------------------------------------------------------
# Fundamentals
# ---------------------------------------------------------------------------

def fetch_fundamentals(tickers: list, api_key: str, etf_list: list) -> pd.DataFrame:
    """
    Fetch company profiles + valuation multiples from ROIC API.
    ETF tickers get hardcoded names. Never raises.
    """
    rows = []

    for ticker in tickers:
        if ticker in etf_list:
            rows.append({
                "ticker":          ticker,
                "company_name":    ETF_NAMES.get(ticker, ticker),
                "sector":          "ETF",
                "industry":        "Exchange Traded Fund",
                "country":         "",
                "exchange":        "",
                "dividend_yield":  0.0,
                "ex_dividend_date": None,
                "pe_ratio":        None,
                "ev_ebitda":       None,
                "pb_ratio":        None,
                "gross_margin":    None,
            })
            continue

        row: dict = {
            "ticker": ticker, "company_name": ticker,
            "sector": None, "industry": None, "country": None, "exchange": None,
            "dividend_yield": None, "ex_dividend_date": None,
            "pe_ratio": None, "ev_ebitda": None, "pb_ratio": None, "gross_margin": None,
        }

        try:
            url  = f"{ROIC_BASE}/company/profile/{ticker}?apikey={api_key}"
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            p = resp.json()
            row.update({
                "company_name":    p.get("company_name", ticker) or ticker,
                "sector":          p.get("sector"),
                "industry":        p.get("industry"),
                "country":         p.get("country"),
                "exchange":        p.get("exchange_short_name"),
                "dividend_yield":  p.get("dividend_yield"),
                "ex_dividend_date": p.get("ex_dividend_date"),
            })
        except Exception as e:
            logger.warning(f"ROIC profile failed for {ticker}: {e}")

        time.sleep(0.1)

        try:
            url  = f"{ROIC_BASE}/fundamental/multiples/{ticker}?apikey={api_key}&limit=1"
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data and isinstance(data, list):
                m = data[0]
                row.update({
                    "pe_ratio":    m.get("peRatio"),
                    "ev_ebitda":   m.get("evEbitda"),
                    "pb_ratio":    m.get("pbRatio"),
                    "gross_margin": m.get("grossMargin"),
                })
        except Exception as e:
            logger.warning(f"ROIC multiples failed for {ticker}: {e}")

        time.sleep(0.1)
        rows.append(row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Holdings builders
# ---------------------------------------------------------------------------

def _fx(rate_map: dict, currency: str) -> float:
    if currency == "USD":
        return 1.0
    return rate_map.get(currency, 1.0)


def build_holdings(
    positions, consolidated, prices, fx_rates, fundamentals, theme_map, config
) -> pd.DataFrame:
    """Master aggregation: one row per ticker (consolidated across accounts)."""
    if consolidated.empty:
        return pd.DataFrame()

    price_lkp  = prices.set_index("ticker")["price_local"].to_dict() if not prices.empty else {}
    fund_lkp   = fundamentals.set_index("ticker").to_dict("index") if not fundamentals.empty else {}
    acct_names = config["accounts"]

    rows = []
    for _, crow in consolidated.iterrows():
        ticker       = crow["ticker"]
        total_shares = crow["total_shares"]
        avg_cost_loc = crow["avg_cost_local"]
        currency     = crow["primary_currency"]
        accounts_held = crow["accounts_held"]

        rate = _fx(fx_rates, currency)

        price_loc   = price_lkp.get(ticker, float("nan"))
        price_usd   = (price_loc / rate) if not pd.isna(price_loc) else float("nan")
        mkt_val_usd = total_shares * price_usd if not pd.isna(price_usd) else float("nan")
        avg_cost_usd  = avg_cost_loc / rate
        total_cost_usd = total_shares * avg_cost_usd
        pnl_usd       = (mkt_val_usd - total_cost_usd) if not pd.isna(mkt_val_usd) else float("nan")
        pnl_pct       = (pnl_usd / total_cost_usd) if (total_cost_usd > 0 and not pd.isna(pnl_usd)) else float("nan")

        fund   = fund_lkp.get(ticker, {})
        sector = fund.get("sector") or "Unknown"

        rows.append({
            "ticker":             ticker,
            "company_name":       fund.get("company_name") or ticker,
            "accounts_held":      accounts_held,
            "accounts_display":   [acct_names.get(a, a) for a in accounts_held],
            "total_shares":       total_shares,
            "avg_cost_local":     avg_cost_loc,
            "currency":           currency,
            "avg_cost_usd":       avg_cost_usd,
            "total_cost_usd":     total_cost_usd,
            "price_local":        price_loc,
            "price_usd":          price_usd,
            "market_value_local": total_shares * price_loc if not pd.isna(price_loc) else float("nan"),
            "market_value_usd":   mkt_val_usd,
            "unrealised_pnl_usd": pnl_usd,
            "pnl_pct":            pnl_pct,
            "weight":             float("nan"),
            "theme":              theme_map.get(ticker, sector),
            "sector":             sector,
            "industry":           fund.get("industry") or "",
            "country":            fund.get("country") or "",
            "exchange":           fund.get("exchange") or "",
            "dividend_yield":     fund.get("dividend_yield") or 0.0,
            "ex_dividend_date":   fund.get("ex_dividend_date"),
            "pe_ratio":           fund.get("pe_ratio"),
            "ev_ebitda":          fund.get("ev_ebitda"),
            "pb_ratio":           fund.get("pb_ratio"),
            "gross_margin":       fund.get("gross_margin"),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        total_val = df["market_value_usd"].sum()
        df["weight"] = df["market_value_usd"] / total_val if total_val > 0 else 0.0
    return df


def build_per_account_holdings(
    positions, prices, fx_rates, fundamentals, theme_map
) -> pd.DataFrame:
    """One row per (account_id, ticker)."""
    if positions.empty:
        return pd.DataFrame()

    price_lkp = prices.set_index("ticker")["price_local"].to_dict() if not prices.empty else {}
    fund_lkp  = fundamentals.set_index("ticker").to_dict("index") if not fundamentals.empty else {}

    rows = []
    for _, prow in positions.iterrows():
        ticker       = prow["ticker"]
        account_id   = prow["account_id"]
        net_shares   = prow["net_shares"]
        avg_cost_loc = prow["avg_cost_local"]
        currency     = prow["currency"]

        rate = _fx(fx_rates, currency)

        price_loc    = price_lkp.get(ticker, float("nan"))
        price_usd    = (price_loc / rate) if not pd.isna(price_loc) else float("nan")
        mkt_val_usd  = net_shares * price_usd if not pd.isna(price_usd) else float("nan")
        avg_cost_usd = avg_cost_loc / rate
        total_cost_usd = net_shares * avg_cost_usd
        pnl_usd      = (mkt_val_usd - total_cost_usd) if not pd.isna(mkt_val_usd) else float("nan")
        pnl_pct      = (pnl_usd / total_cost_usd) if (total_cost_usd > 0 and not pd.isna(pnl_usd)) else float("nan")

        fund   = fund_lkp.get(ticker, {})
        sector = fund.get("sector") or "Unknown"

        rows.append({
            "account_id":         account_id,
            "ticker":             ticker,
            "company_name":       fund.get("company_name") or ticker,
            "net_shares":         net_shares,
            "avg_cost_local":     avg_cost_loc,
            "currency":           currency,
            "avg_cost_usd":       avg_cost_usd,
            "total_cost_usd":     total_cost_usd,
            "price_local":        price_loc,
            "price_usd":          price_usd,
            "market_value_local": net_shares * price_loc if not pd.isna(price_loc) else float("nan"),
            "market_value_usd":   mkt_val_usd,
            "unrealised_pnl_usd": pnl_usd,
            "pnl_pct":            pnl_pct,
            "theme":              theme_map.get(ticker, sector),
            "sector":             sector,
            "industry":           fund.get("industry") or "",
            "country":            fund.get("country") or "",
            "dividend_yield":     fund.get("dividend_yield") or 0.0,
            "pe_ratio":           fund.get("pe_ratio"),
            "ev_ebitda":          fund.get("ev_ebitda"),
            "pb_ratio":           fund.get("pb_ratio"),
            "gross_margin":       fund.get("gross_margin"),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        total_val = df["market_value_usd"].sum()
        df["weight"] = df["market_value_usd"] / total_val if total_val > 0 else 0.0
    return df


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

def compute_income(txdf: pd.DataFrame, holdings: pd.DataFrame) -> dict:
    empty = pd.DataFrame(columns=TRANSACTION_COLS)
    if txdf.empty:
        return {"dividends_received": empty, "sale_proceeds": empty, "projected_income": pd.DataFrame()}

    div_rows  = txdf[txdf["action"] == "DIVIDEND"].copy()
    sell_rows = txdf[txdf["action"] == "SELL"].copy()

    proj_rows = []
    if not holdings.empty:
        for _, hrow in holdings.iterrows():
            dy = hrow.get("dividend_yield") or 0.0
            try:
                dy = float(dy)
            except (ValueError, TypeError):
                dy = 0.0
            if dy > 0:
                proj_rows.append({
                    "ticker":                    hrow["ticker"],
                    "company_name":              hrow.get("company_name", hrow["ticker"]),
                    "total_shares":              hrow["total_shares"],
                    "price_usd":                 hrow.get("price_usd", 0.0),
                    "dividend_yield":            dy,
                    "projected_annual_income_usd": hrow["total_shares"] * (hrow.get("price_usd") or 0.0) * dy,
                })

    return {
        "dividends_received": div_rows,
        "sale_proceeds":      sell_rows,
        "projected_income":   pd.DataFrame(proj_rows),
    }


def compute_sector_allocation(holdings: pd.DataFrame) -> pd.DataFrame:
    if holdings.empty:
        return pd.DataFrame(columns=["sector", "market_value_usd", "weight_pct"])
    grp   = holdings.groupby("sector")["market_value_usd"].sum().reset_index()
    total = grp["market_value_usd"].sum()
    grp["weight_pct"] = (grp["market_value_usd"] / total * 100) if total > 0 else 0.0
    return grp.sort_values("market_value_usd", ascending=False).reset_index(drop=True)


def compute_theme_allocation(holdings: pd.DataFrame) -> pd.DataFrame:
    if holdings.empty:
        return pd.DataFrame(columns=["theme", "market_value_usd", "weight_pct", "tickers"])

    def _agg(grp):
        return pd.Series({
            "market_value_usd": grp["market_value_usd"].sum(),
            "tickers":          ", ".join(sorted(grp["ticker"].tolist())),
        })

    grp   = holdings.groupby("theme").apply(_agg).reset_index()
    total = grp["market_value_usd"].sum()
    grp["weight_pct"] = (grp["market_value_usd"] / total * 100) if total > 0 else 0.0
    return grp.sort_values("market_value_usd", ascending=False).reset_index(drop=True)


_REGION_MAP: dict = {
    "United States": "North America", "Canada": "North America",
    "China": "Asia Pacific", "Hong Kong": "Asia Pacific",
    "Japan": "Asia Pacific", "Singapore": "Asia Pacific",
    "South Korea": "Asia Pacific", "Taiwan": "Asia Pacific",
    "Australia": "Asia Pacific", "India": "Asia Pacific",
    "United Kingdom": "Europe", "Ireland": "Europe",
    "Denmark": "Europe", "Germany": "Europe", "France": "Europe",
    "Netherlands": "Europe", "Switzerland": "Europe",
    "United Arab Emirates": "Middle East",
    "Brazil": "Latin America",
}


def compute_geo_allocation(holdings: pd.DataFrame) -> pd.DataFrame:
    if holdings.empty:
        return pd.DataFrame(columns=["region", "market_value_usd", "weight_pct"])
    df = holdings.copy()
    df["region"] = df["country"].apply(
        lambda c: _REGION_MAP.get(str(c).strip(), "Other") if c and not pd.isna(c) else "Other"
    )
    grp   = df.groupby("region")["market_value_usd"].sum().reset_index()
    total = grp["market_value_usd"].sum()
    grp["weight_pct"] = (grp["market_value_usd"] / total * 100) if total > 0 else 0.0
    return grp.sort_values("market_value_usd", ascending=False).reset_index(drop=True)


def compute_overlap(per_account: pd.DataFrame) -> pd.DataFrame:
    """Tickers held in more than one account."""
    if per_account.empty:
        return pd.DataFrame()

    counts = per_account.groupby("ticker")["account_id"].nunique()
    overlap_tickers = counts[counts > 1].index.tolist()
    if not overlap_tickers:
        return pd.DataFrame()

    sub = per_account[per_account["ticker"].isin(overlap_tickers)].copy()

    pivot_shares = sub.pivot_table(
        index=["ticker", "company_name"], columns="account_id",
        values="net_shares", aggfunc="sum",
    ).reset_index()
    pivot_shares.columns.name = None
    account_ids = [c for c in pivot_shares.columns if c not in ("ticker", "company_name")]
    pivot_shares = pivot_shares.rename(columns={a: f"shares_{a}" for a in account_ids})

    pivot_val = sub.pivot_table(
        index="ticker", columns="account_id",
        values="market_value_usd", aggfunc="sum",
    ).reset_index()
    pivot_val.columns.name = None
    val_ids = [c for c in pivot_val.columns if c != "ticker"]
    pivot_val = pivot_val.rename(columns={a: f"value_usd_{a}" for a in val_ids})

    merged     = pivot_shares.merge(pivot_val, on="ticker", how="left")
    share_cols = [c for c in merged.columns if c.startswith("shares_")]
    val_cols   = [c for c in merged.columns if c.startswith("value_usd_")]
    merged["total_shares"]    = merged[share_cols].fillna(0).sum(axis=1)
    merged["total_value_usd"] = merged[val_cols].fillna(0).sum(axis=1)

    return merged.fillna(0).sort_values("total_value_usd", ascending=False).reset_index(drop=True)
