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
from datetime import datetime, timedelta
import yagmail
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

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

# ── Slack settings ───────────────────────────────────────────────────────────
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

if not (SLACK_BOT_TOKEN and SLACK_CHANNEL_ID):
    logging.warning("Slack Bot Token or Channel ID not found in .env. Slack alerts will be disabled.")

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

def send_slack_message(message_text):
    """Sends a message to the configured Slack channel."""
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL_ID:
        logging.warning("Slack token or channel ID is missing. Cannot send message.")
        return False

    try:
        client = WebClient(token=SLACK_BOT_TOKEN)
        response = client.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=message_text
        )
        logging.info(f"Message sent to Slack channel {SLACK_CHANNEL_ID}: {message_text}")
        return True
    except SlackApiError as e:
        logging.error(f"Error sending message to Slack: {e.response['error']}")
        return False

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

    # --- Process DataFrame for Calculations and Alerts ---
    df["Balance Numeric"] = df["Balance"].apply(parse_egp_string)
    df["Renewal Cost Numeric"] = df["Renewal Cost"].apply(parse_egp_string)
    df["Add-ons Price Numeric"] = df["Add-ons Price"].apply(parse_egp_string)
    df["Total Cost Numeric"] = df["Renewal Cost Numeric"] + df["Add-ons Price Numeric"]
    df["Renewal Date DT"] = pd.to_datetime(df["Renewal Date"], format='%d-%m-%Y', errors='coerce')
    df["Remaining"] = pd.to_numeric(df["Remaining"], errors='coerce').fillna(0.0)
    if 'Used' in df.columns:
        df["Used"] = pd.to_numeric(df["Used"], errors='coerce').fillna(0.0)
    else:
        df["Used"] = 0.0
        logging.warning("'Used' column not found in fetched data, defaulting to 0 for Main Quota calculation.")
    df["Main Quota"] = df["Remaining"] + df["Used"]
    logging.info("DataFrame processed for alert logic.")

    # --- SLACK ALERTS AND REPORTING LOGIC ---
    individual_alerts_to_send = []
    low_gb_alert_count = 0
    renewal_low_balance_alert_count = 0

    if SLACK_BOT_TOKEN and SLACK_CHANNEL_ID:
        # STAGE 1: Always collect potential individual alerts and their counts
        logging.info("Gathering data for all potential alerts...")
        for index, row in df.iterrows():
            if pd.notna(row['Remaining']) and row['Remaining'] < LOW_REMAINING_RED_GB:
                message = (
                    f":warning: *Low GB Alert!* Account: *{row['Store']}* ({row['Number']})\n"
                    f"Remaining Data: *{row['Remaining']:.2f} GB* (Threshold: < {LOW_REMAINING_RED_GB} GB)"
                )
                individual_alerts_to_send.append(message)
                low_gb_alert_count += 1

        current_date_for_alerts = pd.Timestamp.now().normalize()
        for index, row in df.iterrows():
            renewal_date_dt = row['Renewal Date DT']
            balance_numeric = row['Balance Numeric']
            total_cost_numeric = row['Total Cost Numeric']
            if pd.notna(renewal_date_dt):
                days_to_renewal = (renewal_date_dt - current_date_for_alerts).days
                if days_to_renewal <= 5 and balance_numeric < total_cost_numeric:
                    renewal_date_str = row['Renewal Date']
                    message = (
                        f":alarm_clock: *Upcoming Renewal & Low Balance!* Account: *{row['Store']}* ({row['Number']})\n"
                        f"Renews on: *{renewal_date_str}* (in {days_to_renewal} day(s))\n"
                        f"Current Balance: *{balance_numeric:.2f} EGP*\n"
                        f"Estimated Total Cost: *{total_cost_numeric:.2f} EGP*"
                    )
                    individual_alerts_to_send.append(message)
                    renewal_low_balance_alert_count += 1

        # STAGE 2: Decide what to send based on the current time
        now = datetime.now()
        target_summary_time = now.replace(hour=12, minute=0, second=0, microsecond=0)
        interval_minutes = 10
        summary_window_start = target_summary_time - timedelta(minutes=interval_minutes)
        summary_window_end = target_summary_time + timedelta(minutes=interval_minutes)

        # For testing time logic:
        # now = datetime.now().replace(hour=12, minute=5) # Test summary window
        # now = datetime.now().replace(hour=13, minute=0) # Test individual/all-clear window

        if summary_window_start <= now <= summary_window_end: # If in the 12 PM window for the summary report
            logging.info(f"Current time {now.strftime('%H:%M')} is within the summary window. Sending daily summary Slack report ONLY.")
            summary_report_parts = [f"*📊 Daily Usage Report Summary ({pd.Timestamp.now().strftime('%Y-%m-%d %I:%M %p')})*"]
            summary_report_parts.append(f"Total accounts processed: *{len(df)}*")
            if low_gb_alert_count > 0:
                summary_report_parts.append(f":warning: *{low_gb_alert_count} account(s)* currently have Low GB (< {LOW_REMAINING_RED_GB} GB).")
            else:
                summary_report_parts.append(f":white_check_mark: All accounts currently OK for GB usage.")
            if renewal_low_balance_alert_count > 0:
                summary_report_parts.append(f":alarm_clock: *{renewal_low_balance_alert_count} account(s)* currently require attention for renewal and low balance.")
            else:
                summary_report_parts.append(f":white_check_mark: No immediate renewal/balance concerns at this time.")
            # Add note about the email report if email is configured
            if TO_ADDRS and EMAIL_SENDER and EMAIL_PASSWORD: # Check if email sending is configured
                summary_report_parts.append(f"📧 _The detailed Excel report, containing full data for all accounts, has also been sent via email._")
            else:
                summary_report_parts.append(f"📧 _Email reporting is not configured._")
            #summary_report_parts.append(f"\n_This is the consolidated daily summary. Check individual alerts sent at other times if issues were present._")
            daily_summary_message = "\n".join(summary_report_parts)
            send_slack_message(daily_summary_message)
            logging.info("Sent daily summary report to Slack.")
        
        else: # Outside the 12 PM summary window
            logging.info(f"Current time {now.strftime('%H:%M')} is outside the summary window. Checking for individual alerts or sending 'All Clear'.")
            if individual_alerts_to_send:
                logging.info(f"Sending {len(individual_alerts_to_send)} individual alert(s) to Slack...")
                unique_individual_alerts = sorted(list(set(individual_alerts_to_send)))
                for alert_msg in unique_individual_alerts:
                    send_slack_message(alert_msg)
                    await asyncio.sleep(1)
            else: # No individual alerts to send, and it's not summary report time
                logging.info("No individual data-driven alerts. Sending 'All Clear' message to Slack.")
                all_clear_message = f"🎉 Good news! Your friendly WE Usage Watchdog just completed its {now.strftime('%I:%M %p')} rounds, and all accounts are A-OK! :dog2::shield: Time to relax and enjoy the peace of mind! ✨"
                send_slack_message(all_clear_message)
    # --- END: SLACK ALERTS AND REPORTING LOGIC ---

    # --- Prepare DataFrame for Excel Output ---
    df_for_excel = df.copy()
    df_for_excel["Balance"] = df["Balance Numeric"]
    df_for_excel["Renewal Cost"] = df["Renewal Cost Numeric"]
    df_for_excel["Add-ons Price"] = df["Add-ons Price Numeric"]
    df_for_excel["Total Cost"] = df["Total Cost Numeric"]
    final_columns = [
        "Store", "Number", "Balance", "Main Quota", "Remaining",
        "Add-ons", "Add-ons Price", "Renewal Cost", "Total Cost", "Renewal Date"
    ]
    df_for_excel = df_for_excel[final_columns]
    logging.info("DataFrame prepared for Excel export.")

    excel_path = "usage_report.xlsx"
    df_for_excel.to_excel(excel_path, index=False, sheet_name="Usage")
    logging.info(f"Data exported to {excel_path}.")

    # --- Styling with openpyxl ---
    wb = load_workbook(excel_path)
    ws = wb.active
    center = Alignment(horizontal="center", vertical="center")
    for row_cells in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        for cell in row_cells:
            cell.alignment = center
    header_row = ws[1]
    col_letters = {}
    for cell in header_row:
        if cell.value: col_letters[cell.value] = cell.column_letter
    gb_format = '0" GB"'; egp_format = '#,##0.00" EGP"'
    if "Main Quota" in col_letters:
        for cell in ws[col_letters["Main Quota"]][1:]: cell.number_format = gb_format
    if "Remaining" in col_letters:
        for cell in ws[col_letters["Remaining"]][1:]: cell.number_format = gb_format
    currency_cols_for_formatting = ["Balance", "Add-ons Price", "Renewal Cost", "Total Cost"]
    for col_name in currency_cols_for_formatting:
        if col_name in col_letters:
            for cell in ws[col_letters[col_name]][1:]: cell.number_format = egp_format
        else: logging.warning(f"Column header '{col_name}' not found for EGP number formatting.")
    red_fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
    yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    if "Balance" in col_letters:
        balance_col_letter_in_excel = col_letters["Balance"]
        for df_idx in range(len(df)):
            excel_row_num = df_idx + 2
            try:
                balance_val = df.loc[df_idx, 'Balance Numeric']
                total_cost_val = df.loc[df_idx, 'Total Cost Numeric']
                balance_cell_in_excel = ws[f"{balance_col_letter_in_excel}{excel_row_num}"]
                epsilon = 1e-9
                if balance_val < (total_cost_val - epsilon): balance_cell_in_excel.fill = red_fill
                elif abs(balance_val - total_cost_val) < epsilon: balance_cell_in_excel.fill = yellow_fill
            except KeyError as e: logging.error(f"KeyError accessing DataFrame for direct styling at df_idx {df_idx}, Excel row {excel_row_num}: {e}")
            except Exception as e: logging.error(f"Error applying direct style at df_idx {df_idx}, Excel row {excel_row_num}: {e}")
        logging.info("Applied direct cell styling for Balance column based on Python logic.")
    else: logging.warning("Could not apply direct styling for Balance: 'Balance' column header not found.")
    if "Remaining" in col_letters:
        remaining_col_letter = col_letters["Remaining"]; last_row = ws.max_row
        dxf_red_gb = DifferentialStyle(fill=PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid"))
        dxf_yellow_gb = DifferentialStyle(fill=PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid"))
        rule_red_gb = Rule(type="cellIs", operator="lessThan", formula=[str(LOW_REMAINING_RED_GB)], dxf=dxf_red_gb); rule_red_gb.stopIfTrue = True
        ws.conditional_formatting.add(f"{remaining_col_letter}2:{remaining_col_letter}{last_row}", rule_red_gb)
        rule_yellow_gb = Rule(type="cellIs", operator="lessThan", formula=[str(LOW_REMAINING_YELLOW_GB)], dxf=dxf_yellow_gb); rule_yellow_gb.stopIfTrue = True
        ws.conditional_formatting.add(f"{remaining_col_letter}2:{remaining_col_letter}{last_row}", rule_yellow_gb)
    logging.info("Excel file styled.")
    wb.save(excel_path)

    # --- Time-Restricted Email Sending ---
    now_for_email = datetime.now()
    target_email_time = now_for_email.replace(hour=12, minute=0, second=0, microsecond=0)
    interval_minutes_email = 10 
    email_window_start = target_email_time - timedelta(minutes=interval_minutes_email)
    email_window_end = target_email_time + timedelta(minutes=interval_minutes_email)

    # For testing:
    # now_for_email = datetime.now().replace(hour=11, minute=55)

    if email_window_start <= now_for_email <= email_window_end:
        logging.info(f"Current time {now_for_email.strftime('%H:%M')} is within the email window. Attempting to send email report.")
        yag = None
        try:
            if not TO_ADDRS: logging.warning("No email recipients configured. Email will not be sent.")
            elif not EMAIL_SENDER or not EMAIL_PASSWORD: logging.warning("Email sender or password not configured. Email will not be sent.")
            else:
                yag = yagmail.SMTP(EMAIL_SENDER, EMAIL_PASSWORD)
                yag.send(to=TO_ADDRS, subject=f"📊 Daily Usage & Balance Report - {now_for_email.strftime('%Y-%m-%d')}",
                         contents=f"Please find today’s usage report attached (Generated around {now_for_email.strftime('%I:%M %p')}).",
                         attachments=[excel_path])
                logging.info(f"✅ Email sent to {TO_ADDRS} with {excel_path} attached.")
        except Exception as e: logging.error(f"Failed to send email: {e}", exc_info=True)
        finally:
            if yag:
                try: yag.close()
                except Exception as e: logging.error(f"Error closing yagmail connection: {e}")
    else:
        logging.info(f"Current time {now_for_email.strftime('%H:%M')} is outside the designated email window. Email will not be sent.")

# Ensure parse_egp_string and send_slack_message are defined globally or imported
# from datetime import datetime, timedelta # Make sure this is at the top of your script

if __name__ == "__main__":
    asyncio.run(main())
