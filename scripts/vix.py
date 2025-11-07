import asyncio
import sys
import time
from datetime import datetime, timezone
import pandas as pd
import os
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Fix for Windows subprocess issues with Playwright
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# ---- Helpers ---------------------------------------------------------------

def ytd_epochs():
    """Return (period1, period2) as Unix seconds for Jan 1 of current year -> now."""
    now = datetime.now(timezone.utc)
    jan1 = datetime(year=now.year, month=1, day=1, tzinfo=timezone.utc)
    return int(jan1.timestamp()), int(now.timestamp())

def history_url_for_vix():
    p1, p2 = ytd_epochs()
    # Daily history. (frequency=1d + interval=1d are both used by Yahoo)
    return (
        "https://finance.yahoo.com/quote/%5EVIX/history/"
        f"?period1={p1}&period2={p2}&interval=1d&filter=history&frequency=1d"
    )

# ---- Scraper ---------------------------------------------------------------

async def fetch_vix_ohlc(playwright):
    browser = await playwright.chromium.launch(headless=True)
    page = await browser.new_page()

    url = history_url_for_vix()
    await page.goto(url, wait_until="domcontentloaded")

    # Try to clear any consent popups if present (varies by region/session)
    for sel in [
        'button[name="agree"]',
        'button[aria-label="Agree"]',
        'button:has-text("Accept all")',
        'button:has-text("Agree")',
    ]:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click()
                break
        except:
            pass

    # Wait for the historical prices table to render
    await page.wait_for_selector("table")

    html = await page.content()
    await browser.close()

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        print("[^VIX] No table found")
        return None

    rows = table.find_all("tr")

    dates, opens, highs, lows, closes = [], [], [], [], []

    for row in rows[1:]:
        cols = row.find_all("td")
        # Historical price rows have 7 tds; dividend/split rows usually don't
        if len(cols) != 7:
            continue
        try:
            date = pd.to_datetime(cols[0].text.strip())
            op = cols[1].text.strip().replace(",", "")
            hi = cols[2].text.strip().replace(",", "")
            lo = cols[3].text.strip().replace(",", "")
            cl = cols[4].text.strip().replace(",", "")

            # Skip rows with missing/placeholder values (e.g., '-')
            if any(v in ("-", "") for v in (op, hi, lo, cl)):
                continue

            dates.append(date)
            opens.append(float(op))
            highs.append(float(hi))
            lows.append(float(lo))
            closes.append(float(cl))
        except:
            # silently skip non-parsable rows (splits/dividends/etc.)
            continue

    df = pd.DataFrame(
        {"Date": dates, "Open": opens, "High": highs, "Low": lows, "Close": closes}
    ).sort_values("Date").reset_index(drop=True)

    print(f"[^VIX] {len(df)} daily rows (YTD)")
    return df

async def run_vix_scraper():
    start = time.time()
    async with async_playwright() as p:
        df = await fetch_vix_ohlc(p)
    print(f"\nTotal time: {time.time() - start:.2f} seconds")
    return df

if __name__ == "__main__":
    df = asyncio.run(run_vix_scraper())
    if df is not None and not df.empty:
        # Save in data/raw/ regardless of where script is run from
        out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "vix_ytd_ohlc.csv")

        df.to_csv(out_path, index=False)
        print(f"Saved to {out_path}")
    else:
        print("No data scraped.")