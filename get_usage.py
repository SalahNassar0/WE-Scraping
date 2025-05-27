import asyncio
import os
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError
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
# Combine into one list (plural preferred, then singular)
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
        "store":    os.getenv(f"ACCOUNT{i}_NAME")
    })
    i += 1


# ── Scraper ──────────────────────────────────────────────────────────────────
async def fetch_usage(account):
    """
    Logs in, scrapes balance + remaining/used, returns a dict with floats/strings.
    Never raises—on any error returns zeros.
    """
    try:
        p = await async_playwright().start()
        browser = await p.chromium.launch(headless=True)
        page    = await browser.new_page()

        # 1) LOGIN
        await page.goto("https://my.te.eg/echannel/#/login", timeout=60000)
        await page.fill('input[placeholder="Service number"]', account["phone"])
        await page.click(".ant-select-selector")
        await page.click(f".ant-select-item-option >> text={account['type']}")
        await page.fill('input[placeholder="Password"]', account["password"])
        await page.click('button:has-text("Login")')

        # 2) WAIT FOR NETWORK IDLE + RENDER
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(3000)

        # 3) SCRAPE BALANCE (numeric only)
        balance_locator = page.locator(
            'xpath=//span[normalize-space(text())="Current Balance"]'
            '/parent::div/div[2]/div[1]'
        )
        raw_balance = "0"
        for _ in range(3):
            try:
                await balance_locator.wait_for(state="visible", timeout=5000)
                txt = (await balance_locator.text_content() or "").strip()
                if txt and txt != "0":
                    raw_balance = txt.split()[0]
                    break
            except:
                pass
            await page.wait_for_timeout(1000)

        # 4) SCRAPE Remaining & Used (preceding-sibling spans)
        try:
            await page.wait_for_selector(
                'xpath=//span[contains(.,"Remaining")]', timeout=10000
            )
            rem_txt = (await page.locator(
                'xpath=//span[contains(.,"Remaining")]/preceding-sibling::span[1]'
            ).first.text_content() or "").strip()
            remaining = float(rem_txt)
        except:
            remaining = 0.0

        try:
            await page.wait_for_selector(
                'xpath=//span[contains(.,"Used")]', timeout=10000
            )
            used_txt = (await page.locator(
                'xpath=//span[contains(.,"Used")]/preceding-sibling::span[1]'
            ).first.text_content() or "").strip()
            used = float(used_txt)
        except:
            used = 0.0

        return {
            "Store":     account["store"],
            "Number":    account["phone"],
            "Balance":   raw_balance,
            "Remaining": remaining,
            "Used":      used
        }

    except Exception as e:
        print(f"⚠️ Error for {account['phone']}: {e}")
        return {
            "Store":     account["store"],
            "Number":    account["phone"],
            "Balance":   "0",
            "Remaining": 0.0,
            "Used":      0.0
        }

    finally:
        try:
            await browser.close()
            await p.stop()
        except:
            pass


# ── Main ─────────────────────────────────────────────────────────────────────
async def main():
    # 1) Scrape all lines in parallel
    rows = await asyncio.gather(*(fetch_usage(ac) for ac in accounts))

    # 2) Build DataFrame
    df = pd.DataFrame(rows)

    # 3) Compute Main Quota = Remaining + Used, drop raw Used
    df["Main Quota"] = df["Remaining"] + df["Used"]
    df.drop(columns=["Used"], inplace=True)

    # 4) Clean up Balance: integer only + " L.E"
    df["Balance"] = (
        df["Balance"]
        .fillna("0")
        .astype(float)
        .astype(int)
        .astype(str)
        + " L.E"
    )

    # 5) Reorder columns
    df = df[["Store", "Number", "Balance", "Main Quota","Remaining"]]

    # 6) Export to Excel
    excel_path = "usage_report.xlsx"
    df.to_excel(excel_path, index=False, sheet_name="Usage")

    # 7) Style with openpyxl
    wb = load_workbook(excel_path)
    ws = wb.active

    # Center all cells
    center = Alignment(horizontal="center", vertical="center")
    for row in ws.iter_rows(
        min_row=1, max_row=ws.max_row,
        min_col=1, max_col=ws.max_column
    ):
        for cell in row:
            cell.alignment = center

    # Conditional formatting on Remaining (col D)
    yellow = PatternFill("solid", fgColor="FFFF00")
    red    = PatternFill("solid", fgColor="FF0000")
    last = ws.max_row

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
