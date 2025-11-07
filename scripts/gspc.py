# scripts/gspc.py
import asyncio
import sys
import time
import os
from datetime import datetime, timezone
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Fix for Windows event loop
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


def sp500_url():
    # Use your provided link values
    return (
        "https://finance.yahoo.com/quote/%5EGSPC/history/"
        "?period1=1735746040&period2=1762443547"
        "&interval=1d&filter=history&frequency=1d"
    )


async def fetch_sp500_data(playwright):
    browser = await playwright.chromium.launch(headless=True)
    page = await browser.new_page()

    url = sp500_url()
    await page.goto(url, wait_until="domcontentloaded")

    # handle cookie banners if they appear
    for sel in [
        'button[name="agree"]',
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

    await page.wait_for_selector("table")

    html = await page.content()
    await browser.close()

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        print("[^GSPC] No table found")
        return None

    rows = table.find_all("tr")

    dates, opens, highs, lows, closes, adjcloses, vols = [], [], [], [], [], [], []

    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) != 7:
            continue
        try:
            date = pd.to_datetime(cols[0].text.strip())
            op = cols[1].text.strip().replace(",", "")
            hi = cols[2].text.strip().replace(",", "")
            lo = cols[3].text.strip().replace(",", "")
            cl = cols[4].text.strip().replace(",", "")
            adj = cols[5].text.strip().replace(",", "")
            vol = cols[6].text.strip().replace(",", "")

            # skip non-numeric (dividends/splits)
            if any(v in ("-", "") for v in (op, hi, lo, cl, adj, vol)):
                continue

            dates.append(date)
            opens.append(float(op))
            highs.append(float(hi))
            lows.append(float(lo))
            closes.append(float(cl))
            adjcloses.append(float(adj))
            vols.append(float(vol))
        except:
            continue

    df = pd.DataFrame({
        "Date": dates,
        "Open": opens,
        "High": highs,
        "Low": lows,
        "Close": closes,
        "Adj Close": adjcloses,
        "Volume": vols
    }).sort_values("Date").reset_index(drop=True)

    print(f"[^GSPC] {len(df)} daily rows")
    return df


async def run_sp500_scraper():
    start = time.time()
    async with async_playwright() as p:
        df = await fetch_sp500_data(p)
    print(f"\nTotal time: {time.time() - start:.2f} seconds")
    return df


if __name__ == "__main__":
    df = asyncio.run(run_sp500_scraper())
    if df is not None and not df.empty:
        # --- Save to data/raw folder (one level above scripts) ---
        out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "sp500_ytd_ohlcv.csv")

        df.to_csv(out_path, index=False)
        print(f"Saved to {out_path}")
    else:
        print("No data scraped.")
