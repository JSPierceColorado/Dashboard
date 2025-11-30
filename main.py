"""
Account dashboard bot for Railway.

What it does (v1):
- Pulls account info from Alpaca, Kraken, and OANDA
- Writes to the 'Dashboard Control tab' sheet of 'Active-Investing'
- Layout starting at row 5:
    A: label for datapoint
    B: value
    C: updated_at in Mountain time (America/Denver)
- Leaves:
    - Row 1 (headers) untouched
    - Rows 2–4 untouched
    - Columns D+ untouched

Configuration (env vars):

GOOGLE_SERVICE_ACCOUNT_JSON  = raw JSON for your Google service account
GOOGLE_SHEET_NAME           = Active-Investing (default)
GOOGLE_WORKSHEET_NAME       = Dashboard Control tab (default)

ALPACA_API_KEY
ALPACA_API_SECRET
ALPACA_PAPER                = true/false (default: true)

KRAKEN_API_KEY
KRAKEN_API_SECRET
KRAKEN_BASE_ASSET           = ZUSD by default (base currency for TradeBalance)
KRAKEN_EARN_CONVERTED_ASSET = optional; currency (e.g. USD, EUR) to express staked/Earn value.
                              If not set, Kraken's default is used (typically USD).

OANDA_API_KEY
OANDA_ACCOUNT_ID
OANDA_ENV                   = practice | live (default: practice)

UPDATE_INTERVAL_SECONDS     = optional; if set, the bot loops with that interval.
                              If not set, it runs once and exits.
"""

import os
import json
import logging
import time
from datetime import datetime

import pytz
import gspread
from google.oauth2.service_account import Credentials

from alpaca.trading.client import TradingClient
from kraken.spot import User as KrakenUser
try:
    # Earn client may not exist in older python-kraken-sdk versions; handle gracefully.
    from kraken.spot import Earn as KrakenEarn  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    KrakenEarn = None  # type: ignore[assignment]

try:
    # For catching the specific "invalid arguments" error from Earn.
    from kraken.exceptions import KrakenInvalidArgumentsError  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    KrakenInvalidArgumentsError = Exception  # type: ignore[assignment]

import oandapyV20
import oandapyV20.endpoints.accounts as oanda_accounts


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("account-dashboard-bot")


# ---------- Time helpers ----------

def get_mountain_timestamp() -> str:
    """Return current time in America/Denver as a nice string."""
    tz = pytz.timezone("America/Denver")
    now = datetime.now(tz)
    return now.strftime("%Y-%m-%d %H:%M:%S %Z")


# ---------- Google Sheets helpers ----------

def get_gspread_client():
    """Create an authenticated gspread client using a service account JSON in env."""
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON env var is not set. "
            "Put your service account JSON contents in this variable."
        )

    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc


def get_dashboard_worksheet(gc):
    """Open the target sheet + worksheet."""
    sheet_name = os.getenv("GOOGLE_SHEET_NAME", "Active-Investing")
    worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "Dashboard Control tab")

    sh = gc.open(sheet_name)
    ws = sh.worksheet(worksheet_name)
    return ws


# ---------- Broker API helpers ----------

def get_alpaca_snapshot():
    """Return account value + available funds for Alpaca, or None if not configured.

    - account_value   -> account.equity
    - available_funds -> account.buying_power
    """
    api_key = os.getenv("ALPACA_API_KEY")
    api_secret = os.getenv("ALPACA_API_SECRET")

    if not api_key or not api_secret:
        logger.info("Skipping Alpaca: missing ALPACA_API_KEY / ALPACA_API_SECRET")
        return None

    paper_flag = os.getenv("ALPACA_PAPER", "true").lower() in {"1", "true", "yes"}
    trading_client = TradingClient(api_key, api_secret, paper=paper_flag)
    account = trading_client.get_account()

    equity = float(account.equity)
    buying_power = float(account.buying_power)
    currency = getattr(account, "currency", "USD")

    return {
        "name": "Alpaca",
        "currency": currency,
        "account_value": equity,
        "available_funds": buying_power,
    }


def get_kraken_snapshot():
    """Return account value + available funds (+ staked/Earn value) for Kraken, or None.

    Uses python-kraken-sdk's User.get_trade_balance(), which wraps Kraken's
    TradeBalance REST endpoint, and (optionally) Earn.list_earn_allocations()
    for staked funds.

    - account_value    -> 'eb' (equivalent balance, combined spot/margin balance)
    - available_funds  -> 'mf' (free margin) if present, else fall back to 'eb'
    - staked_value     -> total Earn allocations (staked assets) converted into
                          KRAKEN_EARN_CONVERTED_ASSET if set, otherwise Kraken's
                          default converted asset (typically USD).
    """
    api_key = os.getenv("KRAKEN_API_KEY")
    api_secret = os.getenv("KRAKEN_API_SECRET")

    if not api_key or not api_secret:
        logger.info("Skipping Kraken: missing KRAKEN_API_KEY / KRAKEN_API_SECRET")
        return None

    base_asset = os.getenv("KRAKEN_BASE_ASSET", "ZUSD")

    # --- Spot trade balance (existing behavior) ---
    user = KrakenUser(key=api_key, secret=api_secret)
    tb = user.get_trade_balance(asset=base_asset)

    equivalent_balance = float(tb.get("eb", 0.0))
    free_margin = float(tb.get("mf", equivalent_balance))

    snapshot = {
        "name": "Kraken",
        "currency": base_asset,
        "account_value": equivalent_balance,
        "available_funds": free_margin,
    }

    # --- Earn / staked balance (new behavior) ---
    if KrakenEarn is None:
        # Older python-kraken-sdk version without Earn client.
        logger.info(
            "Kraken Earn client not available in python-kraken-sdk; "
            "upgrade the package to include staked value."
        )
        return snapshot

    staked_value = None
    staked_currency = None

    try:
        earn_client = KrakenEarn(key=api_key, secret=api_secret)

        # Optional: let user choose converted asset; normalize a few common "Z*" codes.
        earn_converted_asset = os.getenv("KRAKEN_EARN_CONVERTED_ASSET")
        kwargs: dict = {}

        if earn_converted_asset:
            normalized = earn_converted_asset.upper()
            alias_map = {
                "ZUSD": "USD",
                "ZEUR": "EUR",
                "ZGBP": "GBP",
                "ZCAD": "CAD",
                "ZAUD": "AUD",
                "ZNZD": "NZD",
                "ZJPY": "JPY",
            }
            normalized = alias_map.get(normalized, normalized)
            kwargs["converted_asset"] = normalized

        # You might want to hide old zero-balance allocations; we can try this,
        # but if Kraken rejects the arguments, we fall back to a bare call.
        kwargs["hide_zero_allocations"] = True

        try:
            allocations = earn_client.list_earn_allocations(**kwargs)
        except KrakenInvalidArgumentsError:
            logger.warning(
                "Kraken Earn allocations call failed due to invalid arguments; "
                "retrying without parameters."
            )
            allocations = earn_client.list_earn_allocations()

        total_allocated = allocations.get("total_allocated")

        if total_allocated is not None:
            staked_value = float(total_allocated)
            staked_currency = allocations.get(
                "converted_asset",
                earn_converted_asset or base_asset,
            )

    except Exception:
        # Don't kill the whole update if the Earn endpoint is flaky or
        # permissions are missing/misconfigured.
        logger.exception("Error fetching Kraken Earn (staked) allocations")

    if staked_value is not None:
        snapshot["staked_value"] = staked_value
        snapshot["staked_currency"] = staked_currency or base_asset

    return snapshot


def get_oanda_snapshot():
    """Return account value + available funds for OANDA, or None if not configured.

    Uses oandapyV20 AccountSummary endpoint.

    - account_value   -> 'NAV' (Net Asset Value)
    - available_funds -> 'marginAvailable'
    """
    api_key = os.getenv("OANDA_API_KEY")
    account_id = os.getenv("OANDA_ACCOUNT_ID")
    environment = os.getenv("OANDA_ENV", "practice")  # 'practice' or 'live'

    if not api_key or not account_id:
        logger.info("Skipping OANDA: missing OANDA_API_KEY / OANDA_ACCOUNT_ID")
        return None

    client = oandapyV20.API(access_token=api_key, environment=environment)
    r = oanda_accounts.AccountSummary(account_id)
    client.request(r)
    account = r.response["account"]

    nav = float(account["NAV"])
    margin_available = float(account["marginAvailable"])
    currency = account.get("currency", "USD")

    return {
        "name": "OANDA",
        "currency": currency,
        "account_value": nav,
        "available_funds": margin_available,
    }


# ---------- Sheet writing ----------

def build_rows(snapshots, timestamp_str):
    """Convert snapshots into rows for the sheet starting at row 5.

    Column A: label of datapoint
    Column B: numeric value
    Column C: updated_at (Mountain time)
    """
    rows = []
    for snap in snapshots:
        if not snap:
            continue

        name = snap["name"]
        currency = snap["currency"]
        acct_val = snap["account_value"]
        avail = snap["available_funds"]

        label_value = f"{name}: Account Value ({currency})"
        label_available = f"{name}: Available Funds ({currency})"

        rows.append([label_value, acct_val, timestamp_str])
        rows.append([label_available, avail, timestamp_str])

        # Optional: staked / Earn value (currently only Kraken sets this)
        staked_val = snap.get("staked_value")
        if staked_val is not None:
            staked_currency = snap.get("staked_currency", currency)
            label_staked = f"{name}: Staked / Earn Value ({staked_currency})"
            rows.append([label_staked, staked_val, timestamp_str])

    return rows


def update_sheet_once():
    """Fetch all snapshots + update the Google Sheet once."""
    gc = get_gspread_client()
    ws = get_dashboard_worksheet(gc)

    timestamp_str = get_mountain_timestamp()

    snapshots = [
        get_alpaca_snapshot(),
        get_kraken_snapshot(),
        get_oanda_snapshot(),
    ]

    rows = build_rows(snapshots, timestamp_str)

    if not rows:
        logger.warning("No rows to write (no accounts configured?)")
        return

    # Start at A5 downward; do not touch rows 1-4, and do not touch cols D+
    start_row = 5
    end_row = start_row + len(rows) - 1
    cell_range = f"A{start_row}:C{end_row}"

    logger.info("Updating range %s with %d rows", cell_range, len(rows))
    ws.update(
        range_name=cell_range,
        values=rows,
        value_input_option="USER_ENTERED",
    )


# ---------- Main loop ----------

def main():
    """Entry-point for Railway.

    If UPDATE_INTERVAL_SECONDS is set, run in a loop with that interval.
    Otherwise, perform a single update and exit.
    """
    interval_str = os.getenv("UPDATE_INTERVAL_SECONDS")
    if not interval_str:
        logger.info("Running single update...")
        update_sheet_once()
        logger.info("Done.")
        return

    try:
        interval = int(interval_str)
    except ValueError:
        raise RuntimeError("UPDATE_INTERVAL_SECONDS must be an integer (seconds)")

    logger.info("Running in loop every %s seconds", interval)
    while True:
        try:
            update_sheet_once()
        except Exception:
            logger.exception("Error while updating sheet")
        time.sleep(interval)


if __name__ == "__main__":
    main()
