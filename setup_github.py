"""
setup_github.py — One-time script to create transactions.csv in the GitHub repo
with 72 opening positions.

Run once:
    python setup_github.py

Reads config from config.toml and token from .streamlit/secrets.toml.
Refuses to run if transactions.csv already has data rows.
"""

import base64
import csv
import io
import sys
import requests
import toml

HEADERS = [
    "date", "account_id", "ticker", "action",
    "shares", "price_local", "currency", "commission_usd", "notes",
]

OPENING_DATE  = "2026-04-19"
OPENING_NOTES = "Opening position"

OPENING_POSITIONS = [
    # --- Account R (RBC) — CAD ---
    ("R", "AAPL",  50,      182.0427,  "CAD"),
    ("R", "AMD",   30,      110.0083,  "CAD"),
    ("R", "AMZN",  20,      238.0857,  "CAD"),
    ("R", "BHP",   100,     110.6856,  "CAD"),
    ("R", "BLDP",  100,     21.3254,   "CAD"),
    ("R", "ENB",   800,     47.2997,   "CAD"),
    ("R", "EXPI",  100,     56.7956,   "CAD"),
    ("R", "LCID",  6,       321.9073,  "CAD"),
    ("R", "META",  10,      453.6371,  "CAD"),
    ("R", "NIO",   200,     54.7955,   "CAD"),
    ("R", "NVDA",  80,      17.048,    "CAD"),
    ("R", "QCOM",  200,     184.4865,  "CAD"),
    ("R", "TLRY",  25,      251.1133,  "CAD"),
    ("R", "VZ",    700,     74.7172,   "CAD"),
    ("R", "WDS",   36,      34.4101,   "CAD"),
    ("R", "MJ",    8,       61.558,    "CAD"),
    ("R", "YOLO",  100,     38.6319,   "CAD"),

    # --- Account D (DBS) — mixed ---
    ("D", "FDM.L",   155.62,  226.0,     "GBP"),
    ("D", "WISE.L",  48.0,    941.0,     "GBP"),
    ("D", "3115.HK", 1000,    73.9771,   "HKD"),
    ("D", "2823.HK", 15000,   13.2051,   "HKD"),
    ("D", "3067.HK", 45500,   11.2157,   "HKD"),
    ("D", "914.HK",  4000,    23.1098,   "HKD"),
    ("D", "9888.HK", 2700,    120.1294,  "HKD"),
    ("D", "1876.HK", 5100,    7.4038,    "HKD"),
    ("D", "6690.HK", 1000,    26.9093,   "HKD"),
    ("D", "9618.HK", 2100,    123.6765,  "HKD"),
    ("D", "857.HK",  36000,   5.8128,    "HKD"),
    ("D", "1070.HK", 2000,    6.257,     "HKD"),
    ("D", "4686.T",  1400,    3441.0,    "JPY"),
    ("D", "7839.T",  1800,    1789.0,    "JPY"),
    ("D", "BHP",     200,     63.9563,   "USD"),
    ("D", "BIDU",    300,     124.8972,  "USD"),
    ("D", "NVO",     150,     67.4236,   "USD"),
    ("D", "RIO",     500,     65.7189,   "USD"),
    ("D", "RYAAY",   250,     44.2508,   "USD"),
    ("D", "TSM",     35,      342.1543,  "USD"),
    ("D", "WDS",     72,      21.4751,   "USD"),
    ("D", "MJ",      92,      248.6368,  "USD"),
    ("D", "DRAM",    1000,    27.6304,   "USD"),
    ("D", "COPRF",   10400,   11.6883,   "USD"),
    ("D", "PPLT",    370,     137.1514,  "USD"),
    ("D", "IAU",     4200,    70.2022,   "USD"),
    ("D", "SLV",     4250,    40.1519,   "USD"),
    ("D", "AMR",     100,     162.7437,  "USD"),
    ("D", "GOOG",    150,     144.2827,  "USD"),
    ("D", "AMZN",    80,      195.1923,  "USD"),
    ("D", "BBAI",    810,     6.2122,    "USD"),
    ("D", "ENB",     100,     37.37,     "USD"),
    ("D", "FTNT",    140,     71.6786,   "USD"),
    ("D", "FCX",     1500,    52.9092,   "USD"),
    ("D", "INTC",    720,     34.9723,   "USD"),
    ("D", "IBM",     20,      206.0975,  "USD"),
    ("D", "JPM",     25,      209.96,    "USD"),
    ("D", "LULU",    60,      219.5287,  "USD"),
    ("D", "META",    1,       328.4,     "USD"),
    ("D", "MU",      50,      90.3524,   "USD"),
    ("D", "MSFT",    40,      441.5813,  "USD"),
    ("D", "OXY",     800,     35.9425,   "USD"),
    ("D", "PM",      50,      118.92,    "USD"),
    ("D", "TLRY",    50,      179.36,    "USD"),
    ("D", "VZ",      300,     41.0765,   "USD"),

    # --- Account Citi (CITI) — USD ---
    ("Citi", "ANET",  200,   87.1,     "USD"),
    ("Citi", "BABA",  100,   160.17,   "USD"),
    ("Citi", "ENB",   300,   39.007,   "USD"),
    ("Citi", "EWY",   250,   107.4,    "USD"),
    ("Citi", "FCX",   1000,  51.928,   "USD"),
    ("Citi", "GOOG",  300,   167.38,   "USD"),
    ("Citi", "IAU",   1600,  80.333,   "USD"),
    ("Citi", "ISRG",  30,    489.5,    "USD"),
    ("Citi", "SLV",   1320,  37.46,    "USD"),
    ("Citi", "VZ",    700,   41.008,   "USD"),
]


def _gh_headers(token):
    return {
        "Authorization":        f"Bearer {token}",
        "Accept":               "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def main():
    print("Family Portfolio Tracker — GitHub Setup")
    print("=" * 50)

    # Load config
    try:
        config = toml.load("config.toml")
    except FileNotFoundError:
        print("ERROR: config.toml not found. Run from the portfolio/ directory.")
        sys.exit(1)

    # Load token from .streamlit/secrets.toml
    try:
        secrets = toml.load(".streamlit/secrets.toml")
        token   = secrets["github_token"]
    except FileNotFoundError:
        print("ERROR: .streamlit/secrets.toml not found.")
        print("Create it with:  github_token = \"ghp_...\"")
        sys.exit(1)
    except KeyError:
        print("ERROR: github_token key missing from .streamlit/secrets.toml")
        sys.exit(1)

    gh_config = config.get("github", {})
    repo      = gh_config.get("repo", "")
    branch    = gh_config.get("branch", "main")
    file_path = gh_config.get("file_path", "transactions.csv")

    if "GITHUB_USERNAME" in repo:
        print("ERROR: Update the repo field in config.toml first (e.g. ronit/portfolio-tracker).")
        sys.exit(1)

    print(f"Target: {repo}/{file_path} on branch {branch}")

    # Check if file already exists
    url  = f"https://api.github.com/repos/{repo}/contents/{file_path}?ref={branch}"
    resp = requests.get(url, headers=_gh_headers(token), timeout=15)

    existing_sha = None
    if resp.status_code == 200:
        data    = resp.json()
        content = base64.b64decode(data["content"].replace("\n", "")).decode("utf-8")
        existing_sha = data["sha"]

        # Check if there are already data rows
        rows = list(csv.DictReader(io.StringIO(content)))
        if rows:
            print(
                f"WARNING: {file_path} already has {len(rows)} data row(s).\n"
                "Refusing to write to avoid double-population.\n"
                "Delete the file on GitHub manually if you need to re-run setup."
            )
            sys.exit(0)
        print("File exists but is empty — will write header + positions.")

    elif resp.status_code == 404:
        print("File does not exist — will create it.")
    else:
        print(f"ERROR: GitHub API returned {resp.status_code}: {resp.text}")
        sys.exit(1)

    # Build CSV content
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(HEADERS)
    for acct_id, ticker, shares, price_local, currency in OPENING_POSITIONS:
        writer.writerow([
            OPENING_DATE, acct_id, ticker, "BUY",
            float(shares), float(price_local), currency, 0.0, OPENING_NOTES,
        ])
    csv_content = buf.getvalue()

    # Commit to GitHub
    print(f"Writing {len(OPENING_POSITIONS)} opening positions...")
    put_url = f"https://api.github.com/repos/{repo}/contents/{file_path}"
    payload = {
        "message": "init: add 72 opening positions (2026-04-19)",
        "content": base64.b64encode(csv_content.encode("utf-8")).decode("utf-8"),
        "branch":  branch,
    }
    if existing_sha:
        payload["sha"] = existing_sha

    resp = requests.put(put_url, json=payload, headers=_gh_headers(token), timeout=15)
    if resp.status_code in (200, 201):
        print(f"\nSetup complete. {len(OPENING_POSITIONS)} opening positions written to {repo}/{file_path}.")
    else:
        print(f"ERROR: GitHub API returned {resp.status_code}: {resp.text}")
        sys.exit(1)


if __name__ == "__main__":
    main()
