# scripts/vix.py
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import pandas as pd

# ---------- helpers ----------
def ytd_epochs():
    now = datetime.now(timezone.utc)
    jan1 = datetime(year=now.year, month=1, day=1, tzinfo=timezone.utc)
    return int(jan1.timestamp()), int(now.timestamp())

def _history_url_vix(period1=None, period2=None, interval="1d"):
    if period1 is None or period2 is None:
        period1, period2 = ytd_epochs()
    return (
        "https://finance.yahoo.com/quote/%5EVIX/history/"
        f"?period1={period1}&period2={period2}&interval={interval}&filter=history&frequency=1d"
    )

def _to_float(s):
    s = (s or "").strip().replace(",", "")
    return None if (s in ("", "-", "—")) else float(s)

# ---------- public API ----------
def fetch_vix_ohlc(period1=None, period2=None, interval="1d"):
    """
    Returns a DataFrame with columns: Date, Open, High, Low, Close
    period1/period2 are unix seconds; if None, uses YTD (UTC).
    """
    url = _history_url_vix(period1, period2, interval)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded")

        # best-effort consent popups
        for sel in [
            'button[name="agree"]',
            'button[aria-label="Agree"]',
            'button:has-text("Accept all")',
            'button:has-text("Agree")',
        ]:
            try:
                btn = page.query_selector(sel)
                if btn:
                    btn.click()
                    break
            except Exception:
                pass

        page.wait_for_selector("table")
        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close"])

    rows = table.find_all("tr")

    out = {"Date": [], "Open": [], "High": [], "Low": [], "Close": []}
    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) != 7:
            continue  # skip dividends/splits rows
        try:
            date = pd.to_datetime(cols[0].text.strip())
            op = _to_float(cols[1].text)
            hi = _to_float(cols[2].text)
            lo = _to_float(cols[3].text)
            cl = _to_float(cols[4].text)
            if None in (op, hi, lo, cl):
                continue
            out["Date"].append(date)
            out["Open"].append(op)
            out["High"].append(hi)
            out["Low"].append(lo)
            out["Close"].append(cl)
        except Exception:
            continue

    df = pd.DataFrame(out).sort_values("Date").reset_index(drop=True)
    return df
