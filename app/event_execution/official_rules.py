"""Verified public Binance Event Contract product rules.

These are UI/product facts, NOT a supported programmatic API contract.
Source:
https://www.binance.com/en-NG/support/faq/detail/172247f1c4b8401ab106fb63900f3825
Updated 2026-10-30.
Terms:
https://bin.bnbstatic.com/static/cms/cg08ou2ak0tn7mcplvfg/file/ff1ed8c9a32e4a61aa0aa66ccd618f76bbc88cd420e210d7721001e235cd9acd.pdf
Effective 2026-01-05.

Do not use these facts to invent private-Web request enums or enable order submission.
"""
from decimal import Decimal

FAQ_URL = (
    "https://www.binance.com/en-NG/support/faq/detail/"
    "172247f1c4b8401ab106fb63900f3825"
)
TERMS_URL = (
    "https://bin.bnbstatic.com/static/cms/cg08ou2ak0tn7mcplvfg/file/"
    "ff1ed8c9a32e4a61aa0aa66ccd618f76bbc88cd420e210d7721001e235cd9acd.pdf"
)
FAQ_UPDATED = "2026-10-30"
TERMS_EFFECTIVE = "2026-01-05"

# Publicly documented product UI choices. They do not establish API enums.
UI_HORIZONS_MINUTES = frozenset((10, 30, 60, 1440))
OBSERVED_PRIVATE_WEB_EXAMPLE_ENUM = "TEN_MINUTE"  # one sample only; not official API support

MINIMUM_PREMIUM_USDT = Decimal("5")
PAYOUT_RATIO_DYNAMIC = True
PAYOUT_FIXED_AT_ACCEPTANCE = True
EQUAL_INDEX_AT_EXPIRY_REFUNDS_PREMIUM = True
EARLY_CLOSE_ALLOWED = False
OPEN_TIME_AFTER_SUBMISSION_MS = 1000
STATUS_DELAY_UTC_HOURS = (0, 8, 16)

# Binance's FAQ explicitly states Event Contracts are only available via
# Android/iOS apps, not through an API. The private web endpoint in the
# local evidence snapshot is reverse-engineered and unsupported.
OFFICIAL_PROGRAMMATIC_API_AVAILABLE = False


class UnsupportedOfficialApi(RuntimeError):
    pass


def require_official_api_support():
    """Fail closed: there is no officially supported event-contract API."""
    raise UnsupportedOfficialApi(
        "Binance documents Event Contracts as app-only; no official API adapter exists."
    )


def proxy_equal_result(entry_index_price, expiry_index_price):
    """Classify equality under the published rule, without claiming real settlement.

    The caller must still label this as proxy-only: its entry index tick may
    differ from the actual next-second exchange strike, and no order exists.
    """
    if not EQUAL_INDEX_AT_EXPIRY_REFUNDS_PREMIUM:
        raise RuntimeError("official tie rule unavailable")
    try:
        entry = Decimal(str(entry_index_price))
        expiry = Decimal(str(expiry_index_price))
    except Exception:
        raise ValueError("invalid index-price proxy") from None
    if not entry.is_finite() or not expiry.is_finite() or entry <= 0 or expiry <= 0:
        raise ValueError("invalid index-price proxy")
    return entry == expiry
