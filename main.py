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
from kraken.spot import User as KrakenUser, Market as KrakenMarket
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

    - account_value  -> account.equity
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


def _kraken_base_alt_name(base_asset: str) -> str:
    """Map Kraken internal fiat codes (ZUSD, ZEUR, ...) to their alt 'human' codes."""
    mapping = {
        "ZUSD": "USD",
        "ZEUR": "EUR",
        "ZGBP": "GBP",
        "ZCAD": "CAD",
        "ZAUD": "AUD",
        "ZJPY": "JPY",
        "ZCHF": "CHF",
    }
    return mapping.get(base_asset, base_asset)


def get_kraken_earn_wallet_value(user: KrakenUser, base_asset: str) -> float:
    """Return the total value (in base_asset) of Kraken *Earn wallet* balances.

    Strategy:
    - Query balances from Kraken (Balance or BalanceEx).
    - Treat assets ending with:
        - '.B' -> new yield-bearing products (Earn)
        - '.S' -> staked balances
        - '.M' -> opt-in rewards
      as part of the Earn / staking wallet.
    - EXCLUDE '.F' (balances earning automatically in Kraken Rewards / Auto Earn).
    - Convert each such asset into base_asset using spot tickers.
    """
    try:
        # Try several balance methods so we work across python-kraken-sdk versions.
        try:
            balances = user.get_account_balance()  # type: ignore[attr-defined]
        except AttributeError:
            try:
                balances = user.get_balance()  # type: ignore[attr-defined]
            except AttributeError:
                try:
                    balances = user.get_balances()  # type: ignore[attr-defined]
                except AttributeError:
                    logger.warning(
                        "Kraken User client has no balance method we recognize; "
                        "Earn wallet value will be 0."
                    )
                    return 0.0
    except Exception:
        logger.exception("Error fetching Kraken balances for Earn wallet calculation")
        return 0.0

    # Normalize balances into {asset: float_amount}
    earn_suffixes = (".B", ".S", ".M")
    earn_balances = {}

    for asset, entry in balances.items():
        # Skip Auto Earn (.F) explicitly.
        if isinstance(asset, str) and asset.endswith(".F"):
            continue

        if not isinstance(asset, str):
            continue

        if not any(asset.endswith(sfx) for sfx in earn_suffixes):
            continue

        # Entry may be a simple string (Balance) or a dict (BalanceEx).
        if isinstance(entry, dict):
            amount_str = entry.get("balance", "0")
        else:
            amount_str = entry

        try:
            amount = float(amount_str)
        except (TypeError, ValueError):
            continue

        if amount <= 0:
            continue

        earn_balances[asset] = amount

    if not earn_balances:
        return 0.0

    # Convert these Earn balances into base_asset using Market tickers.
    market = KrakenMarket()  # unauthenticated is fine for public price data
    base_alt = _kraken_base_alt_name(base_asset)

    total_value = 0.0
    price_cache = {}

    for asset_with_suffix, amount in earn_balances.items():
        # Strip suffix: 'DOT.B' -> 'DOT'
        underlying = asset_with_suffix.split(".", 1)[0]

        # If underlying is already the base asset (ZUSD) or its alt (USD),
        # we can just add the amount directly.
        if underlying == base_asset or underlying == base_alt:
            total_value += amount
            continue

        # Cache prices per underlying to avoid repeated ticker calls.
        if underlying in price_cache:
            price = price_cache[underlying]
        else:
            pair_alt = f"{underlying}{base_alt}"  # e.g. 'DOTUSD', 'XBTUSD'
            try:
                ticker = market.get_ticker(pair=pair_alt)
            except Exception:
                logger.exception(
                    "Error fetching Kraken price for %s/%s when valuing Earn wallet",
                    underlying,
                    base_asset,
                )
                continue

            if not ticker:
                logger.warning(
                    "No ticker data returned for Kraken pair %s; skipping from Earn wallet",
                    pair_alt,
                )
                continue

            # get_ticker returns dict like {'XXBTZUSD': {...}}
            try:
                first_key = next(iter(ticker))
                last_trade_close = ticker[first_key]["c"][0]
                price = float(last_trade_close)
            except Exception:
                logger.exception(
                    "Unexpected ticker format for pair %s when valuing Earn wallet",
                    pair_alt,
                )
                continue

            price_cache[underlying] = price

        total_value += amount * price

    return total_value


def get_kraken_snapshot():
    """Return account value + available funds for Kraken, plus Earn wallet value.

    Uses python-kraken-sdk's User.get_trade_balance() for:
    - account_value   -> 'eb' (equivalent balance, combined)
    - available_funds -> 'mf' (free margin) if present, else fall back to 'eb'

    And computes Earn wallet value separately (excluding Auto Earn) using balances.
    """
    api_key = os.getenv("KRAKEN_API_KEY")
    api_secret = os.getenv("KRAKEN_API_SECRET")

    if not api_key or not api_secret:
        logger.info("Skipping Kraken: missing KRAKEN_API_KEY / KRAKEN_API_SECRET")
        return None

    base_asset = os.getenv("KRAKEN_BASE_ASSET", "ZUSD")

    user = KrakenUser(key=api_key, secret=api_secret)
    tb = user.get_trade_balance(asset=base_asset)

    equivalent_balance = float(tb.get("eb", 0.0))
    free_margin = float(tb.get("mf", equivalent_balance))

    earn_wallet_value = 0.0
    try:
        earn_wallet_value = get_kraken_earn_wallet_value(user, base_asset)
    except Exception:
        logger.exception("Error computing Kraken Earn wallet value")
        earn_wallet_value = 0.0

    snapshot = {
        "name": "Kraken",
        "currency": base_asset,
        "account_value": equivalent_balance,
        "available_funds": free_margin,
    }

    # Only include Earn wallet value if it's positive (non-trivial).
    if earn_wallet_value > 0:
        snapshot["earn_wallet_value"] = earn_wallet_value

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

        # Optional: Kraken Earn wallet value (excluding Auto Earn)
        earn_wallet_value = snap.get("earn_wallet_value")
        if earn_wallet_value is not None:
            label_earn = f"{name}: Earn Wallet Value ({currency})"
            rows.append([label_earn, earn_wallet_value, timestamp_str])

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
    # Use named arguments to avoid the DeprecationWarning from gspread.
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
