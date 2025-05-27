import asyncio
import os
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, PatternFill
from openpyxl.formatting.rule import CellIsRule
import yagmail

load_dotenv()

# ── Email settings ────────────────────────────────────────────────────────────
EMAIL_SENDER     = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD   = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENT  = os.getenv("EMAIL_RECIPIENT", "").strip()
EMAIL_RECIPIENTS = [
    addr.strip()
    for addr in os.getenv("EMAIL_RECIPIENTS", "").split(",")
    if addr.strip()
]
TO_ADDRS = EMAIL_RECIPIENTS or ([EMAIL_RECIPIENT] if EMAIL_RECIPIENT else [])
if not TO_ADDRS:
    raise RuntimeError("No email recipient configured! Set EMAIL_RECIPIENT or EMAIL_RECIPIENTS in .env")

# ── Build accounts from .env ─────────────────────────────────────────────────
accounts = []
i = 1
while os.getenv(f"ACCOUNT{i}_PHONE"):
    accounts.append({
        "phone":    os.getenv(f"ACCOUNT{i}_PHONE"),
        "password": os.getenv(f"ACCOUNT{i}_PASS"),
        "type":     os.getenv(f"ACCOUNT{i}_TYPE", "Internet"),
        "store":    os.getenv(f"ACCOUNT{i}_NAME", f"Account {i}")
    })
    i += 1

# ── Scraper ──────────────────────────────────────────────────────────────────
async def fetch_usage(account):
    """
    Logs in, scrapes Balance/Remaining/Used + Renewal Cost/Date.
    Retries each locator up to 3×, always returns defaults on failure.
    """
    p = None
    browser = None
    try:
        p = await async_playwright().start()
        browser = await p.chromium.launch(headless=True)
        page    = await browser.new_page()

        # 1) LOGIN
        await page.goto("https://my.te.eg/echannel/#/login", timeout=60000)
        await page.fill('input[placeholder="Service number"]', account["phone"])
        await page.click(".ant-select-selector")
        await page.click(f'.ant-select-item-option >> text={account["type"]}')
        await page.fill('input[placeholder="Password"]', account["password"])
        await page.click('button:has-text("Login")')

        # 2) WAIT FOR DASHBOARD
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000)

        # 3) SCRAPE BALANCE (numeric only)
        balance = "0"
        bal_loc = page.locator(
            '//span[normalize-space(text())="Current Balance"]'
            '/parent::div//div[contains(@style,"font-size")]'
        ).first
        for _ in range(3):
            try:
                await bal_loc.wait_for(timeout=5000)
                txt = (await bal_loc.text_content() or "").strip().split()[0]
                if txt and txt != "0":
                    balance = txt
                    break
            except:
                await page.wait_for_timeout(1000)

        # 4) SCRAPE Remaining
        remaining = 0.0
        rem_loc = page.locator(
            '//span[contains(.,"Remaining")]/preceding-sibling::span[1]'
        ).first
        for _ in range(3):
            try:
                await rem_loc.wait_for(timeout=5000)
                remaining = float((await rem_loc.text_content() or "0").strip())
                break
            except:
                await page.wait_for_timeout(1000)

        # 5) SCRAPE Used
        used = 0.0
        used_loc = page.locator(
            '//span[contains(.,"Used")]/preceding-sibling::span[1]'
        ).first
        for _ in range(3):
            try:
                await used_loc.wait_for(timeout=5000)
                used = float((await used_loc.text_content() or "0").strip())
                break
            except:
                await page.wait_for_timeout(1000)

        # 6) CLICK "More Details"
        try:
            more_details = page.locator('//span[text()="More Details"]').first
            await more_details.wait_for(timeout=5000)
            await more_details.click()
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(2000)
        except:
            pass

        # 7) SCRAPE Renewal Cost
        renewal_cost = "0"
        cost_loc = page.locator(
            '//span[contains(text(),"Renewal Cost")]/following-sibling::span//div[1]'
        ).first
        for _ in range(3):
            try:
                await cost_loc.wait_for(timeout=5000)
                val = (await cost_loc.text_content() or "").strip()
                if val and val != "0":
                    renewal_cost = val
                    break
            except:
                await page.wait_for_timeout(1000)

        # 8) SCRAPE Renewal Date
        renewal_date = ""
        date_loc = page.locator('//span[contains(.,"Renewal Date")]').first
        try:
            await date_loc.wait_for(timeout=5000)
            full = (await date_loc.text_content() or "")
            # take text after ":" and before ","
            renewal_date = full.split(":",1)[1].split(",",1)[0].strip()
        except:
            pass

        return {
            "Store":         account["store"],
            "Number":        account["phone"],
            "Balance":       f"{balance} EGP",
            "Remaining":     remaining,
            "Used":          used,
            "Renewal Cost":  f"{renewal_cost} EGP",
            "Renewal Date":  renewal_date
        }

    except Exception as e:
        print(f"⚠️ Error for {account['phone']}: {e}")
        return {
            "Store":         account["store"],
            "Number":        account["phone"],
            "Balance":       "0 EGP",
            "Remaining":     0.0,
            "Used":          0.0,
            "Renewal Cost":  "0 EGP",
            "Renewal Date":  ""
        }

    finally:
        if browser:
            try: await browser.close()
            except: pass
        if p:
            try: await p.stop()
            except: pass

# ── Main ─────────────────────────────────────────────────────────────────────
async def main():
    # 1) Scrape all accounts in parallel
    rows = await asyncio.gather(*(fetch_usage(ac) for ac in accounts))

    # 2) Build DataFrame
    df = pd.DataFrame(rows)

    # 3) Compute Main Quota = Remaining + Used
    df["Main Quota"] = df["Remaining"] + df["Used"]

    # 4) Clean Balance to integer + " EGP"
    df["Balance"] = (
        df["Balance"]
        .str.replace(r"[^\d\.]+", "", regex=True)
        .astype(float)
        .astype(int)
        .astype(str)
        + " EGP"
    )

    # 5) Reorder columns
    df = df[[
        "Store", "Number", "Balance",
        "Main Quota", "Remaining",
        "Renewal Cost", "Renewal Date"
    ]]

    # 6) Export to Excel
    excel_path = "usage_report.xlsx"
    df.to_excel(excel_path, index=False, sheet_name="Usage")

    # 7) Style with openpyxl
    wb = load_workbook(excel_path)
    ws = wb.active

    # Center all cells
    center = Alignment(horizontal="center", vertical="center")
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row,
                             min_col=1, max_col=ws.max_column):
        for cell in row:
            cell.alignment = center

    # Conditional formatting on Remaining (column E)
    last = ws.max_row
    yellow = PatternFill("solid", fgColor="FFFF00")
    red    = PatternFill("solid", fgColor="FF0000")

    ws.conditional_formatting.add(
        f"E2:E{last}",
        CellIsRule(operator="lessThan", formula=["80"], fill=yellow)
    )
    ws.conditional_formatting.add(
        f"E2:E{last}",
        CellIsRule(operator="lessThan", formula=["20"], fill=red)
    )

    wb.save(excel_path)

    # 8) Email the final report
    yag = yagmail.SMTP(EMAIL_SENDER, EMAIL_PASSWORD)
    yag.send(
        to=TO_ADDRS,
        subject="📊 Usage & Balance Report",
        contents="Please find today’s usage report attached.",
        attachments=[excel_path]
    )
    print("✅ Sent:", excel_path)

if __name__ == "__main__":
    asyncio.run(main())
