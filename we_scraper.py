# we_scraper.py
import os, sys, json, asyncio
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

async def fetch_usage(account):
    result = {
        "Store":        account["store"],
        "Number":       account["phone"],
        "Balance":      0.0,
        "Remaining":    0.0,
        "Used":         0.0,
        "Main Quota":   0.0,
        "Renewal Cost": 0.0,
        "Renewal Date": "",
    }
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            # LOGIN
            await page.goto("https://my.te.eg/echannel/#/login", timeout=60000)
            await page.fill('input[placeholder="Service number"]', account["phone"])
            await page.click(".ant-select-selector")
            await page.click(f'.ant-select-item-option >> text="{account["type"]}"')
            await page.fill('input[placeholder="Password"]', account["password"])
            await page.click('button:has-text("Login")')
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(1)

            # Remaining
            try:
                txt = await page.locator(
                    '//span[contains(.,"Remaining")]/preceding-sibling::span[1]'
                ).text_content()
                result["Remaining"] = float(txt.replace(",", "").strip())
            except: pass

            # Used
            try:
                txt = await page.locator(
                    '//span[contains(.,"Used")]/preceding-sibling::span[1]'
                ).text_content()
                result["Used"] = float(txt.replace(",", "").strip())
            except: pass

            # Balance
            try:
                txt = await page.locator(
                    '//span[normalize-space(text())="Current Balance"]'
                    '/following-sibling::div//div[1]'
                ).text_content()
                result["Balance"] = float(txt.split()[0].replace(",", ""))
            except: pass

            # More Details → Renewal Cost & Date
            try:
                await page.click('button:has-text("More Details")')
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(0.5)
            except: pass

            # Renewal Cost
            try:
                txt = await page.locator(
                    '//span[normalize-space(text())="Renewal Cost:"]'
                    '/following-sibling::span//div/div[1]'
                ).text_content()
                result["Renewal Cost"] = float(txt.replace(",", "").strip())
            except: pass

            # Renewal Date (pick the comma form first)
            try:
                spans = await page.locator('//span[contains(text(),"Renewal Date")]').all_text_contents()
                cand = next((s for s in spans if "," in s), spans[0] if spans else "")
                date = cand.split("Renewal Date:")[-1].split(",")[0].strip()
                result["Renewal Date"] = date
            except: pass

            await browser.close()
            result["Main Quota"] = result["Remaining"] + result["Used"]

    except Exception as e:
        print(f"⚠️ Error for {account['phone']}: {e}", file=sys.stderr)

    return result

async def fetch_all():
    # read accounts from .env
    accts = []
    i = 1
    while os.getenv(f"ACCOUNT{i}_PHONE"):
        accts.append({
            "phone": os.getenv(f"ACCOUNT{i}_PHONE"),
            "password": os.getenv(f"ACCOUNT{i}_PASS",""),
            "type": os.getenv(f"ACCOUNT{i}_TYPE","Internet"),
            "store": os.getenv(f"ACCOUNT{i}_NAME",f"Acct{i}")
        })
        i += 1

    tasks = [fetch_usage(a) for a in accts]
    return await asyncio.gather(*tasks)

def main():
    # on Windows
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    data = asyncio.run(fetch_all())
    print(json.dumps(data, ensure_ascii=False))

if __name__=="__main__":
    main()
