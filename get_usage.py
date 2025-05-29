import asyncio
import os
import re
import sys
import logging
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, PatternFill
from openpyxl.formatting.rule import CellIsRule
import yagmail

# ── Configure Logging ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log", encoding='utf-8'), # Ensure file handler uses UTF-8
        logging.StreamHandler(sys.stdout)                     # Explicitly set stream to sys.stdout
    ]
)

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

# ── Conditional Formatting Thresholds ────────────────────────────────────────
# Default to 80 and 20 GB if not set in .env
LOW_REMAINING_YELLOW_GB = float(os.getenv("LOW_REMAINING_YELLOW_GB", "80"))
LOW_REMAINING_RED_GB    = float(os.getenv("LOW_REMAINING_RED_GB", "20"))


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
if not accounts:
    logging.warning("No accounts configured! Please set ACCOUNT1_PHONE, ACCOUNT1_PASS, etc., in .env")

# ── Scraper ──────────────────────────────────────────────────────────────────
async def fetch_usage(account):
    """
    Logs in, scrapes Balance/Remaining/Used + Renewal Cost/Date.
    Retries each locator up to 3×, always returns defaults on failure.
    """
    p = None
    browser = None
    page = None # Initialize page
    try:
        logging.info(f"Attempting to fetch usage for {account['store']} ({account['phone']})")
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
        logging.info(f"Login initiated for {account['phone']}")

        # 2) WAIT FOR DASHBOARD
        # Wait for the network to be idle, then an additional buffer
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000) # Give elements time to fully render
        logging.info(f"Dashboard loaded for {account['phone']}")

        # 3) SCRAPE BALANCE (numeric only)
        balance = "0"
        bal_loc = page.locator(
            '//span[normalize-space(text())="Current Balance"]'
            '/parent::div//div[contains(@style,"font-size")]'
        ).first
        for attempt in range(1, 4):
            try:
                await bal_loc.wait_for(timeout=5000)
                txt = (await bal_loc.text_content() or "").strip().split()[0]
                if txt and txt != "0":
                    balance = txt
                    logging.info(f"Balance found for {account['phone']}: {balance}")
                    break
            except Exception as e:
                logging.warning(f"Attempt {attempt}/3 to get Balance for {account['phone']} failed: {e}")
                await page.wait_for_timeout(1000)
        if balance == "0":
            logging.warning(f"Could not reliably scrape Balance for {account['phone']}. Defaulting to 0.")


        # 4) SCRAPE Remaining
        remaining = 0.0
        rem_loc = page.locator(
            '//span[contains(.,"Remaining")]/preceding-sibling::span[1]'
        ).first
        for attempt in range(1, 4):
            try:
                await rem_loc.wait_for(timeout=5000)
                # Ensure the text content is convertible to float
                val_str = (await rem_loc.text_content() or "0").strip()
                remaining = float(re.sub(r'[^\d.]', '', val_str)) # Clean non-numeric
                logging.info(f"Remaining found for {account['phone']}: {remaining}")
                break
            except Exception as e:
                logging.warning(f"Attempt {attempt}/3 to get Remaining for {account['phone']} failed: {e}")
                await page.wait_for_timeout(1000)
        if remaining == 0.0:
            logging.warning(f"Could not reliably scrape Remaining for {account['phone']}. Defaulting to 0.0.")


        # 5) SCRAPE Used
        used = 0.0
        used_loc = page.locator(
            '//span[contains(.,"Used")]/preceding-sibling::span[1]'
        ).first
        for attempt in range(1, 4):
            try:
                await used_loc.wait_for(timeout=5000)
                # Ensure the text content is convertible to float
                val_str = (await used_loc.text_content() or "0").strip()
                used = float(re.sub(r'[^\d.]', '', val_str)) # Clean non-numeric
                logging.info(f"Used found for {account['phone']}: {used}")
                break
            except Exception as e:
                logging.warning(f"Attempt {attempt}/3 to get Used for {account['phone']} failed: {e}")
                await page.wait_for_timeout(1000)
        if used == 0.0:
            logging.warning(f"Could not reliably scrape Used for {account['phone']}. Defaulting to 0.0.")


        # 6) CLICK "More Details"
        try:
            more_details = page.locator('//span[text()="More Details"]').first
            await more_details.wait_for(state='visible', timeout=5000)
            await more_details.click()
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(2000)
            logging.info(f"Clicked 'More Details' for {account['phone']}")
        except Exception as e:
            logging.warning(f"Could not click 'More Details' for {account['phone']}: {e}. Skipping renewal info.")
            # Set defaults if unable to click "More Details"
            renewal_cost = "0"
            renewal_date = ""
            return {
                "Store":        account["store"],
                "Number":       account["phone"],
                "Balance":      f"{balance} EGP",
                "Remaining":    remaining,
                "Used":         used,
                "Renewal Cost": f"{re.sub(r'[^\d.]', '', renewal_cost)} EGP",
                "Renewal Date": renewal_date
            }


        # 7) SCRAPE Renewal Cost
        renewal_cost = "0"
        cost_loc = page.locator(
            '//span[contains(text(),"Renewal Cost")]/following-sibling::span//div[1]'
        ).first
        for attempt in range(1, 4):
            try:
                await cost_loc.wait_for(timeout=5000)
                val = (await cost_loc.text_content() or "").strip()
                if val and val != "0":
                    renewal_cost = val
                    logging.info(f"Renewal Cost found for {account['phone']}: {renewal_cost}")
                    break
            except Exception as e:
                logging.warning(f"Attempt {attempt}/3 to get Renewal Cost for {account['phone']} failed: {e}")
                await page.wait_for_timeout(1000)
        if renewal_cost == "0":
            logging.warning(f"Could not reliably scrape Renewal Cost for {account['phone']}. Defaulting to 0.")


        # 8) SCRAPE Renewal Date
        renewal_date = ""
        date_loc = page.locator('//span[contains(.,"Renewal Date")]').first
        try:
            await date_loc.wait_for(timeout=5000)
            full = (await date_loc.text_content() or "")
            # take text after ":" and before ","
            renewal_date = full.split(":",1)[1].split(",",1)[0].strip()
            logging.info(f"Renewal Date found for {account['phone']}: {renewal_date}")
        except Exception as e:
            logging.warning(f"Could not scrape Renewal Date for {account['phone']}: {e}. Defaulting to empty string.")

        return {
            "Store":        account["store"],
            "Number":       account["phone"],
            "Balance":      f"{balance} EGP",
            "Remaining":    remaining,
            "Used":         used,
            "Renewal Cost": f"{re.sub(r'[^\d.]', '', renewal_cost)} EGP",
            "Renewal Date": renewal_date
        }

    except Exception as e:
        logging.error(f"Critical error fetching usage for {account['store']} ({account['phone']}): {e}", exc_info=True)
        return {
            "Store":        account["store"],
            "Number":       account["phone"],
            "Balance":      "0 EGP",
            "Remaining":    0.0,
            "Used":         0.0,
            "Renewal Cost": "0 EGP",
            "Renewal Date": ""
        }

    finally:
        if browser:
            try: await browser.close()
            except Exception as e: logging.error(f"Error closing browser for {account['phone']}: {e}")
        if p:
            try: await p.stop()
            except Exception as e: logging.error(f"Error stopping Playwright for {account['phone']}: {e}")
        logging.info(f"Finished processing {account['phone']}.")

# ── Main ─────────────────────────────────────────────────────────────────────
async def main():
    logging.info("Starting web scraping process...")

    # 1) Scrape all accounts in parallel
    rows = await asyncio.gather(*(fetch_usage(ac) for ac in accounts))
    logging.info(f"Finished scraping {len(rows)} accounts.")

    # 2) Build DataFrame
    df = pd.DataFrame(rows)

    # 3) Compute Main Quota = Remaining + Used
    # Ensure both are float before addition for accurate calculation
    df["Main Quota"] = df["Remaining"].astype(float) + df["Used"].astype(float)

    # 4) Clean Balance to integer + " EGP"
    df["Balance"] = (
        df["Balance"]
        .str.replace(r"[^\d\.]+", "", regex=True) # Remove non-numeric except dot
        .astype(float) # Convert to float to handle potential decimals during cleaning
        .astype(int)   # Convert to integer
        .astype(str)
        + " EGP"
    )

    # 5) Reorder columns
    df = df[[
        "Store", "Number", "Balance",
        "Main Quota", "Remaining",
        "Renewal Cost", "Renewal Date"
    ]]
    logging.info("DataFrame prepared.")

    # 6) Export to Excel
    excel_path = "usage_report.xlsx"
    df.to_excel(excel_path, index=False, sheet_name="Usage")
    logging.info(f"Data exported to {excel_path}.")

    # 7) Style with openpyxl
    wb = load_workbook(excel_path)
    ws = wb.active

    # Center all cells
    center = Alignment(horizontal="center", vertical="center")
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row,
                             min_col=1, max_col=ws.max_column):
        for cell in row:
            cell.alignment = center

    # Find the column index for Main Quota and Remaining
    main_quota_col = None
    remaining_col = None
    for idx, cell in enumerate(ws[1], 1):
        if cell.value == "Main Quota":
            main_quota_col = idx
        if cell.value == "Remaining":
            remaining_col = idx

    # Apply custom format to Main Quota and Remaining columns
    # '0.00" GB"' for two decimal places, or '0" GB"' for whole numbers
    for col_idx in [main_quota_col, remaining_col]:
        if col_idx:
            for row in ws.iter_rows(min_row=2, min_col=col_idx, max_col=col_idx, max_row=ws.max_row):
                for cell in row:
                    cell.number_format = '0" GB"' 

    # Conditional formatting on Remaining (column E)
    last_row = ws.max_row
    yellow = PatternFill("solid", fgColor="FFFF00") # Hex code for yellow
    red    = PatternFill("solid", fgColor="FF0000") # Hex code for red

    # Conditional formatting rules use formulas as strings
    ws.conditional_formatting.add(
        f"E2:E{last_row}",
        CellIsRule(operator="lessThan", formula=[str(LOW_REMAINING_YELLOW_GB)], fill=yellow)
    )
    ws.conditional_formatting.add(
        f"E2:E{last_row}",
        CellIsRule(operator="lessThan", formula=[str(LOW_REMAINING_RED_GB)], fill=red)
    )
    logging.info("Excel file styled with conditional formatting.")
    wb.save(excel_path)

    # 8) Email the final report
    try:
        yag = yagmail.SMTP(EMAIL_SENDER, EMAIL_PASSWORD)
        yag.send(
            to=TO_ADDRS,
            subject="📊 Usage & Balance Report",
            contents="Please find today’s usage report attached.",
            attachments=[excel_path]
        )
        logging.info(f"✅ Email sent to {TO_ADDRS} with {excel_path} attached.")
    except Exception as e:
        logging.error(f"Failed to send email: {e}", exc_info=True)
    finally:
        yag.close()


if __name__ == "__main__":
    asyncio.run(main())