import asyncio
import os
import re
import sys
import logging
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, PatternFill, Font
from openpyxl.styles.differential import DifferentialStyle
from openpyxl.formatting.rule import Rule
import yagmail

# ── Configure Logging ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
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
    p = None
    browser = None
    page = None
    try:
        logging.info(f"Attempting to fetch usage for {account['store']} ({account['phone']})")
        p = await async_playwright().start()
        browser = await p.chromium.launch(headless=True)
        page    = await browser.new_page()

        await page.goto("https://my.te.eg/echannel/#/login", timeout=60000)
        await page.fill('input[placeholder="Service number"]', account["phone"])
        await page.click(".ant-select-selector")
        await page.click(f'.ant-select-item-option >> text={account["type"]}')
        await page.fill('input[placeholder="Password"]', account["password"])
        await page.click('button:has-text("Login")')
        logging.info(f"Login initiated for {account['phone']}")

        await page.wait_for_load_state("networkidle", timeout=60000)
        await page.wait_for_timeout(3000)
        logging.info(f"Dashboard loaded for {account['phone']}")

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
            logging.warning(f"Could not reliably scrape Balance for {account['phone']} or it is '0'. Defaulting to 0.")
            balance = "0"

        remaining = 0.0
        rem_loc = page.locator(
            '//span[contains(.,"Remaining")]/preceding-sibling::span[1]'
        ).first
        for attempt in range(1, 4):
            try:
                await rem_loc.wait_for(timeout=5000)
                val_str = (await rem_loc.text_content() or "0").strip()
                remaining = float(re.sub(r'[^\d.]', '', val_str))
                logging.info(f"Remaining found for {account['phone']}: {remaining}")
                break
            except Exception as e:
                logging.warning(f"Attempt {attempt}/3 to get Remaining for {account['phone']} failed: {e}")
                await page.wait_for_timeout(1000)
        if remaining == 0.0:
            logging.warning(f"Could not reliably scrape Remaining for {account['phone']}. Defaulting to 0.0.")

        used = 0.0
        used_loc = page.locator(
            '//span[contains(.,"Used")]/preceding-sibling::span[1]'
        ).first
        for attempt in range(1, 4):
            try:
                await used_loc.wait_for(timeout=5000)
                val_str = (await used_loc.text_content() or "0").strip()
                used = float(re.sub(r'[^\d.]', '', val_str))
                logging.info(f"Used found for {account['phone']}: {used}")
                break
            except Exception as e:
                logging.warning(f"Attempt {attempt}/3 to get Used for {account['phone']} failed: {e}")
                await page.wait_for_timeout(1000)
        if used == 0.0:
            logging.warning(f"Could not reliably scrape Used for {account['phone']}. Defaulting to 0.0.")

        renewal_cost = "0"
        renewal_date = ""
        addon_names_str = "N/A"
        addon_prices_str = "N/A" 

        try:
            more_details_locators = [
                page.locator('//span[text()="More Details"]').first,
                page.locator('//a[.//span[contains(text(),"details")]] | //button[.//span[contains(text(),"details")]]').first
            ]

            more_details_clicked = False
            for md_loc in more_details_locators:
                try:
                    await md_loc.wait_for(state='visible', timeout=5000)
                    await md_loc.click()
                    more_details_clicked = True
                    logging.info(f"Clicked 'More Details' (or similar) for {account['phone']}")
                    break
                except Exception:
                    logging.debug(f"More details locator variant failed for {account['phone']}")

            if not more_details_clicked:
                raise Exception("All 'More Details' locator variants failed or timed out.")

            await page.wait_for_load_state("networkidle", timeout=30000)
            await page.wait_for_timeout(3000)

            addon_names_list = []
            addon_prices_list_scraped = []

            not_subscribed_locator = page.locator('//span[contains(normalize-space(.), "You are not subscribed to any bundles currently")]')
            try:
                await not_subscribed_locator.wait_for(timeout=3000, state="visible")
                logging.info(f"Message 'You are not subscribed to any bundles currently' found for {account['phone']}.")
            except Exception:
                logging.info(f"'Not subscribed' message NOT found for {account['phone']}. Looking for add-on cards.")
                addon_card_selector = (
                    '//div[contains(@class, "slick-slide") and @aria-hidden="false"]'
                    '//div[contains(@style, "border-style: solid") and contains(@style, "border-color: var(--ec-brand-primary)")]'
                )
                addon_cards = page.locator(addon_card_selector)

                num_addon_cards = await addon_cards.count()
                logging.info(f"Found {num_addon_cards} potential add-on card(s) for {account['phone']}.")

                for i in range(num_addon_cards):
                    card = addon_cards.nth(i)
                    name_text_cleaned = "N/A"
                    price_text_raw = "N/A" 

                    try:
                        name_loc = card.locator(
                            'xpath=.//div[contains(@style, "font-size: var(--ec-title-h7)") and contains(@style, "font-weight: bold;")]'
                        ).first
                        await name_loc.wait_for(timeout=3000, state="visible")
                        name_text_original = (await name_loc.text_content() or "").strip()
                        name_text_cleaned = name_text_original if name_text_original else "N/A"
                    except Exception as e:
                        logging.warning(f"Could not get add-on name for card {i+1} for {account['phone']}: {e}")

                    try:
                        price_locator_xpath = './/span[contains(normalize-space(.), "Price:")]'
                        price_elements = card.locator(f"xpath={price_locator_xpath}")
                        count = await price_elements.count()

                        if count > 0:
                            price_loc_instance = price_elements.first
                            actual_price_text = (await price_loc_instance.text_content(timeout=3000) or "").strip()
                            if "Price:" in actual_price_text:
                                parsed_price_val = actual_price_text.split("Price:", 1)[-1].strip()
                                if not parsed_price_val: 
                                    price_text_raw = "N/A"
                                elif parsed_price_val: 
                                    price_text_raw = parsed_price_val 
                                else: 
                                     price_text_raw = "N/A"
                            else: 
                                price_text_raw = "N/A"
                        else: 
                            price_text_raw = "N/A"
                    except Exception as e:
                        price_text_raw = "N/A" 

                    if name_text_cleaned != "N/A" and price_text_raw != "N/A" and "EGP" in price_text_raw:
                        price_numeric_part_for_name_cleaning = re.sub(r'[^\d.]', '', price_text_raw.split("EGP")[0])
                        if price_numeric_part_for_name_cleaning and price_numeric_part_for_name_cleaning in name_text_cleaned:
                            patterns_to_remove = [
                                r'-\s*' + re.escape(price_numeric_part_for_name_cleaning) + r'\s*EGP\s*/\s*month',
                                r'-\s*' + re.escape(price_numeric_part_for_name_cleaning) + r'\s*EGP'
                            ]
                            temp_name = name_text_cleaned
                            for pat in patterns_to_remove:
                                temp_name = re.sub(pat, '', temp_name, flags=re.IGNORECASE).strip()
                            name_text_cleaned = temp_name.rstrip('- /').strip()

                    if name_text_cleaned != "N/A" or price_text_raw != "N/A":
                        addon_names_list.append(name_text_cleaned if name_text_cleaned != "N/A" else "Unknown Add-on")
                        addon_prices_list_scraped.append(price_text_raw if price_text_raw != "N/A" else "0 EGP")

                if addon_names_list:
                    addon_names_str = "; ".join(addon_names_list)
                    addon_prices_str = "; ".join(addon_prices_list_scraped)
                
            if addon_names_str != "N/A" :
                 logging.info(f"Add-ons scraped for {account['phone']}: Names='{addon_names_str}', Prices='{addon_prices_str}'")

            cost_loc = page.locator(
                '//span[contains(text(),"Renewal Cost")]/following-sibling::span//div[1]'
            ).first
            for attempt in range(1, 4):
                try:
                    await cost_loc.wait_for(timeout=5000)
                    val = (await cost_loc.text_content() or "").strip()
                    if val and val != "0":
                        renewal_cost = val
                        break
                except Exception:
                    pass 
            if renewal_cost == "0":
                logging.warning(f"Could not reliably scrape Renewal Cost for {account['phone']}. Defaulting to 0.")
                renewal_cost = "0"

            date_loc = page.locator('//span[contains(text(),"Renewal Date:")]').first
            try:
                await date_loc.wait_for(timeout=5000)
                full_text_content = await date_loc.text_content() or ""
                match = re.search(r"Renewal Date:\s*([\d-]+)", full_text_content)
                if match:
                    renewal_date = match.group(1).strip()
                else:
                    parts = full_text_content.split("Renewal Date:", 1)
                    if len(parts) > 1:
                        renewal_date = parts[1].split(",")[0].strip()
                    else:
                        renewal_date = ""
            except Exception:
                renewal_date = ""

        except Exception as e:
            logging.warning(f"Could not click 'More Details' or process subsequent info for {account['phone']}: {e}")

        return {
            "Store":        account["store"],
            "Number":       account["phone"],
            "Balance":      f"{balance} EGP",
            "Remaining":    remaining,
            "Used":         used,
            "Add-ons":      addon_names_str,
            "Add-ons Price": addon_prices_str,
            "Renewal Cost": f"{renewal_cost} EGP",
            "Renewal Date": renewal_date
        }

    except Exception as e:
        logging.error(f"Critical error fetching usage for {account['store']} ({account['phone']}): {e}", exc_info=True)
        return {
            "Store": account["store"], "Number": account["phone"], "Balance": "0 EGP",
            "Remaining": 0.0, "Used": 0.0, "Add-ons": "N/A", "Add-ons Price": "N/A",
            "Renewal Cost": "0 EGP", "Renewal Date": ""
        }
    finally:
        if browser:
            try: await browser.close()
            except Exception: pass
        if p:
            try: await p.stop()
            except Exception: pass
        logging.info(f"Finished processing {account['phone']}.")


def parse_egp_string(cost_str):
    if not isinstance(cost_str, str) or cost_str.lower() == "n/a":
        return 0.0
    total_value = 0.0
    parts = cost_str.split(';')
    for part in parts:
        numeric_part = re.sub(r"[^\d\.]", "", part.strip())
        if numeric_part:
            try:
                total_value += float(numeric_part)
            except ValueError:
                logging.warning(f"Could not parse numeric part '{numeric_part}' from '{part}' to float.")
    return total_value

async def main():
    logging.info("Starting web scraping process...")

    if not accounts:
        logging.warning("No accounts found in .env file. Exiting.")
        return

    rows = await asyncio.gather(*(fetch_usage(ac) for ac in accounts))
    logging.info(f"Finished scraping {len(rows)} accounts.")

    df = pd.DataFrame(rows)

    if df.empty:
        logging.warning("No data scraped. DataFrame is empty. Email will not be sent.")
        return

    # Create numeric columns for calculation
    df["Balance Numeric"] = df["Balance"].apply(parse_egp_string)
    df["Renewal Cost Numeric"] = df["Renewal Cost"].apply(parse_egp_string)
    df["Add-ons Price Numeric"] = df["Add-ons Price"].apply(parse_egp_string)
    df["Total Cost Numeric"] = df["Renewal Cost Numeric"] + df["Add-ons Price Numeric"]

    # Overwrite original columns with numeric data for Excel export
    # The " EGP" suffix will be applied by openpyxl's number formatting
    df["Balance"] = df["Balance Numeric"]
    df["Renewal Cost"] = df["Renewal Cost Numeric"]
    df["Add-ons Price"] = df["Add-ons Price Numeric"]
    df["Total Cost"] = df["Total Cost Numeric"]

    df["Remaining"] = pd.to_numeric(df["Remaining"], errors='coerce').fillna(0.0)
    df["Used"] = pd.to_numeric(df["Used"], errors='coerce').fillna(0.0)
    df["Main Quota"] = df["Remaining"] + df["Used"]

    # Define final column order (no helper columns needed for this approach)
    final_columns = [
        "Store",
        "Number",
        "Balance",
        "Total Cost",       # Moved up
        "Renewal Cost",
        "Add-ons Price",
        "Add-ons",
        "Main Quota",
        "Remaining",
        "Renewal Date"
    ]
    df = df[final_columns]
    logging.info("DataFrame prepared.")

    excel_path = "usage_report.xlsx"
    df.to_excel(excel_path, index=False, sheet_name="Usage")
    logging.info(f"Data exported to {excel_path}.")

    wb = load_workbook(excel_path)
    ws = wb.active

    center = Alignment(horizontal="center", vertical="center")
    for row_cells in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        for cell in row_cells:
            cell.alignment = center

    header_row = ws[1]
    col_letters = {}
    for cell in header_row:
        if cell.value:
            col_letters[cell.value] = cell.column_letter

    gb_format = '0" GB"'
    egp_format = '#,##0.00" EGP"'

    if "Main Quota" in col_letters:
        for cell in ws[col_letters["Main Quota"]][1:]:
            cell.number_format = gb_format
    if "Remaining" in col_letters:
        for cell in ws[col_letters["Remaining"]][1:]:
            cell.number_format = gb_format

    currency_cols_for_formatting = ["Balance", "Add-ons Price", "Renewal Cost", "Total Cost"]
    for col_name in currency_cols_for_formatting:
        if col_name in col_letters:
            for cell in ws[col_letters[col_name]][1:]:
                cell.number_format = egp_format
        else:
            logging.warning(f"Column header '{col_name}' not found for EGP number formatting.")
            
    # --- Direct Cell Styling for Balance Column ---
    red_fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
    yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    
    if "Balance" in col_letters and "Total Cost" in col_letters:
        balance_col_letter_in_excel = col_letters["Balance"]
        
        # Iterate through the DataFrame rows to get numeric values for comparison
        for df_idx, row_data in df.iterrows(): # df.iterrows() gives (index, Series)
            excel_row_num = df_idx + 2 # +1 for header, +1 because Excel is 1-indexed

            try:
                # Use the numeric values directly from the DataFrame (which were already processed)
                balance_val = row_data['Balance'] # This is now numeric
                total_cost_val = row_data['Total Cost'] # This is now numeric
                
                balance_cell_in_excel = ws[f"{balance_col_letter_in_excel}{excel_row_num}"]

                # Round for precise comparison of currency values
                # Using a small epsilon for float equality comparison
                epsilon = 1e-9 
                if balance_val < (total_cost_val - epsilon): # balance_val < total_cost_val
                    balance_cell_in_excel.fill = red_fill
                elif abs(balance_val - total_cost_val) < epsilon: # balance_val == total_cost_val
                    balance_cell_in_excel.fill = yellow_fill
                # Else: no fill, default background
            except KeyError as e:
                logging.error(f"KeyError accessing DataFrame for styling at df_idx {df_idx}, Excel row {excel_row_num}: {e}")
            except Exception as e:
                logging.error(f"Error applying direct style at df_idx {df_idx}, Excel row {excel_row_num}: {e}")
        logging.info("Applied direct cell styling for Balance column based on Python logic.")
    else:
        logging.warning("Could not apply direct styling for Balance: 'Balance' or 'Total Cost' column header not found in col_letters.")


    # Conditional formatting for Remaining GB (this part was working)
    if "Remaining" in col_letters:
        remaining_col_letter = col_letters["Remaining"]
        last_row = ws.max_row
        
        dxf_red_gb = DifferentialStyle(fill=PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid"))
        dxf_yellow_gb = DifferentialStyle(fill=PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid"))

        rule_red_gb = Rule(type="cellIs", operator="lessThan", formula=[str(LOW_REMAINING_RED_GB)], dxf=dxf_red_gb)
        rule_red_gb.stopIfTrue = True
        ws.conditional_formatting.add(f"{remaining_col_letter}2:{remaining_col_letter}{last_row}", rule_red_gb)

        rule_yellow_gb = Rule(type="cellIs", operator="lessThan", formula=[str(LOW_REMAINING_YELLOW_GB)], dxf=dxf_yellow_gb)
        rule_yellow_gb.stopIfTrue = True
        ws.conditional_formatting.add(f"{remaining_col_letter}2:{remaining_col_letter}{last_row}", rule_yellow_gb)

    logging.info("Excel file styled.")
    wb.save(excel_path)

    yag = None
    try:
        if not TO_ADDRS:
            logging.warning("No email recipients configured. Skipping email.")
            return

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
        if yag:
            try:
                yag.close()
            except Exception as e:
                logging.error(f"Error closing yagmail connection: {e}")

if __name__ == "__main__":
    asyncio.run(main())