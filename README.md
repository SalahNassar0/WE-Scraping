# WE-Scraping

## Project Overview
WE-Scraping is a Playwright based tool that logs into one or more WE internet accounts,
scrapes balance and usage information and produces an Excel report. The script can also
send notifications to Slack and email a copy of the report.

## Installation
1. Ensure Python 3.8+ is installed and create a virtual environment.
2. Install the dependencies listed in `requirements.txt`:
   ```bash
   pip install -r requirements.txt
   playwright install
   ```

## Configuration
The script reads its settings from environment variables. Create a `.env` file or set
the variables in your shell. Important variables include:

- `ACCOUNT1_PHONE` / `ACCOUNT1_PASS` – credentials for the first account.
  Repeat with `ACCOUNT2_...` etc. `ACCOUNTn_TYPE` and `ACCOUNTn_NAME` can also be
  provided to override the default account type (`Internet`) or give a friendly name.
- `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` – enable Slack alerts when provided.
- `EMAIL_SENDER` and `EMAIL_PASSWORD` – credentials used for sending email reports.
  Use `EMAIL_RECIPIENT` for a single address or `EMAIL_RECIPIENTS` for a
  comma‑separated list.
- `LOW_REMAINING_YELLOW_GB` and `LOW_REMAINING_RED_GB` – thresholds for low data
  warnings (optional).

Copy `.env.example` to `.env` for a template of all variables.

## Running
Execute the scraper with:
```bash
python get_usage.py
```
The command will create `usage_report.xlsx` in the project directory and, if Slack
or email settings are present, send notifications accordingly.
