"""
phone_validator.py

Validates and normalises phone numbers scraped from real estate listings.
Returns a clean E.164 formatted number, or None if the input is garbage.

Supported regions: Luxembourg (LU), France (FR), Germany (DE), Belgium (BE)

Usage in scraper:
    from phone_validator import clean_phone

    phone = clean_phone(raw_value, hint="LU")  # hint is optional
"""

import re
import logging
from typing import Optional

import phonenumbers
from phonenumbers import NumberParseException, PhoneNumberType

log = logging.getLogger(__name__)

# Regions to try when no country code is present in the number.
# Order matters — try Luxembourg first since that's the primary market.
DEFAULT_REGIONS = ["LU", "FR", "DE", "BE"]

# Phone number types we accept. Rejects pagers, premium rate, etc.
ACCEPTED_TYPES = {
    PhoneNumberType.MOBILE,
    PhoneNumberType.FIXED_LINE,
    PhoneNumberType.FIXED_LINE_OR_MOBILE,
    PhoneNumberType.VOIP,
}

# Patterns that indicate a scraper artifact rather than a real phone number.
# Checked before attempting phonenumbers parsing.
GARBAGE_PATTERNS = [
    r"^20\d{2}[01]\d[0-3]\d",   # date-like: 20240315..., 202301...
    r"^[0-9]{1,6}$",             # too short (listing refs, codes)
    r"^[0-9]{16,}$",             # too long
    r"^(0+)$",                   # all zeros
    r"^(\d)\1{6,}$",             # repeated digit: 1111111, 2222222
    r"^(12345|00000|11111)$",    # obvious test numbers
]

_GARBAGE_RE = [re.compile(p) for p in GARBAGE_PATTERNS]


def _looks_like_garbage(raw: str) -> bool:
    digits = re.sub(r"\D", "", raw)
    return any(p.match(digits) for p in _GARBAGE_RE)


def clean_phone(raw: Optional[str], hint: Optional[str] = None) -> Optional[str]:
    """
    Parse, validate and normalise a raw phone number string.

    Args:
        raw:  The raw string from the scraper (may include spaces, dashes, etc.)
        hint: ISO 3166-1 alpha-2 country code hint, e.g. "LU", "FR", "DE".
              Used as the default region when the number has no country prefix.
              If None, tries DEFAULT_REGIONS in order.

    Returns:
        E.164 formatted string (e.g. "+352621123456") if valid, else None.
    """
    if not raw:
        return None

    raw = str(raw).strip()

    if _looks_like_garbage(raw):
        log.debug("Rejected garbage phone: %s", raw)
        return None

    regions_to_try = [hint] if hint else DEFAULT_REGIONS

    # Build a list of formats to try for each region:
    # 1. Raw as-is (handles full international like +352621555555)
    # 2. Prefixed with + (handles 352621555555 without plus)
    # 3. Raw with region hint (handles bare local digits like 621555555)
    digits_only = re.sub(r"\D", "", raw)

    candidates = [raw]
    if not raw.startswith("+"):
        candidates.append("+" + digits_only)

    for region in regions_to_try:
        for candidate in candidates:
            try:
                parsed = phonenumbers.parse(candidate, region)
            except NumberParseException:
                continue

            if not phonenumbers.is_valid_number(parsed):
                continue

            number_type = phonenumbers.number_type(parsed)
            if number_type not in ACCEPTED_TYPES:
                log.debug("Rejected unsupported number type %s: %s", number_type, raw)
                continue

            formatted = phonenumbers.format_number(
                parsed, phonenumbers.PhoneNumberFormat.E164
            )
            log.debug("Cleaned phone: %s → %s", raw, formatted)
            return formatted

    log.debug("Could not validate phone number: %s", raw)
    return None


def get_country(e164_number: str) -> Optional[str]:
    """
    Returns the ISO country code for a validated E.164 number.
    e.g. "+352621123456" → "LU"
    """
    try:
        parsed = phonenumbers.parse(e164_number)
        region = phonenumbers.region_code_for_number(parsed)
        return region
    except NumberParseException:
        return None
