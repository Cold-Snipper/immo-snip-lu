"""
athome.lu Scraper  —  v4
========================
Targets:
  https://www.athome.lu/vente?sort=date_desc
  https://www.athome.lu/location?sort=date_desc

Strategy
--------
  • Scrape the index pages (sorted newest-first) with Selenium.
  • For each listing ref found on the index:
      – If ref is NOT in the DB → scrape the detail page fully, insert.
      – If ref IS in DB but title changed → re-scrape and update (price
        drop, description rewrite, etc.).
      – If ref IS in DB and title matches → STOP early for this index URL.
        (Since the list is newest-first, once we see a known-unchanged ad
         we've caught up. Move to next index URL.)
  • Two pages max per run (configurable). After a few days the DB is complete.

Phone logic (two passes)
  1. Regex scan of the description text.
  2. Selenium click on "Afficher le numéro" / "Show phone number",
     then read the <a href="tel:…"> injected into the DOM.

DB schema  (SQLite, listings.db)
  listing_ref      TEXT  PRIMARY KEY   — from "Réf atHome XXXXXXXX" / URL
  agency_ref       TEXT
  transaction_type TEXT
  listing_url      TEXT
  title            TEXT
  location         TEXT
  description      TEXT
  sale_price       REAL
  rent_price       REAL
  monthly_charges  REAL
  deposit          REAL
  commission       TEXT
  availability     TEXT
  surface_m2       REAL
  floor            INTEGER
  rooms            INTEGER
  bedrooms         INTEGER
  year_of_construction INTEGER
  fitted_kitchen   INTEGER  (0/1)
  open_kitchen     INTEGER
  shower_rooms     INTEGER
  bathrooms        INTEGER
  separate_toilets INTEGER
  furnished        INTEGER
  balcony          INTEGER
  terrace          INTEGER
  garden           INTEGER
  parking_spaces   INTEGER
  energy_class     TEXT
  thermal_insulation_class TEXT
  gas_heating      INTEGER
  electric_heating INTEGER
  heat_pump        INTEGER
  district_heating INTEGER
  pellet_heating   INTEGER
  oil_heating      INTEGER
  solar_heating    INTEGER
  basement         INTEGER
  laundry_room     INTEGER
  elevator         INTEGER
  storage          INTEGER
  pets_allowed     INTEGER
  phone_number     TEXT
  phone_source     TEXT
  agency_name      TEXT
  agency_url       TEXT
  agent_name       TEXT
  agency_logo_url  TEXT
  image_urls       TEXT   (JSON array)
  images_dir       TEXT
  first_seen       TEXT   (ISO datetime)
  last_updated     TEXT   (ISO datetime)
  title_history    TEXT   (JSON array of previous titles with timestamps)
"""

import re
import sys
import time
import json
import sqlite3
import logging
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple

from bs4 import BeautifulSoup, Tag

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException,
        ElementClickInterceptedException,
    )
    SELENIUM_OK = True
except ImportError:
    SELENIUM_OK = False

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
DB_PATH     = Path("listings.db")
IMAGES_ROOT = Path("images")
BASE_URL    = "https://www.athome.lu"
USER_AGENT  = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("athome_scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("athome")

# ─────────────────────────────────────────────────────────────
# Characteristics map  FR / EN / DE → (field, type)
# ─────────────────────────────────────────────────────────────
CHAR_MAP = [
    # Sale
    ("sale_price",               ["Sale price","Prix de vente","Kaufpreis"],                 "price"),
    ("commission",               ["Commission paid by","Commission payée par","Provision"],  "str"),
    ("availability",             ["Availability","Disponibilité","Verfügbarkeit"],            "str"),
    # Rental
    ("rent_price",               ["Rent","Loyer","Miete"],                                   "price"),
    ("monthly_charges",          ["Monthly charges","Charges mensuelles","Nebenkosten"],     "price"),
    ("deposit",                  ["Deposit","Caution","Kaution"],                             "price"),
    # General
    ("surface_m2",               ["Livable surface","Surface habitable","Wohnfläche"],       "float"),
    ("floor",                    ["Property's floor","Etage du bien","Stockwerk"],           "int"),
    ("rooms",                    ["Number of rooms","Nombre de pièces","Zimmeranzahl"],       "int"),
    ("bedrooms",                 ["Number of bedrooms","Nombre de chambres","Schlafzimmer"], "int"),
    ("year_of_construction",     ["Year of construction","Année de construction","Baujahr"], "int"),
    # Indoor
    ("fitted_kitchen",           ["Fitted kitchen","Cuisine équipée","Einbauküche"],         "bool"),
    ("open_kitchen",             ["Open kitchen","Cuisine ouverte","Offene Küche"],          "bool"),
    ("shower_rooms",             ["Shower rooms","Salles de douche","Duschen"],              "int"),
    ("bathrooms",                ["Bathrooms","Salles de bain","Badezimmer"],                "int"),
    ("separate_toilets",         ["Separate toilets","Toilettes séparées","Separate WC"],   "int"),
    ("furnished",                ["Furnished","Meublé","Möbliert"],                          "bool"),
    # Outdoor
    ("balcony",                  ["Balcony","Balcon","Balkon"],                              "bool"),
    ("balcony_m2",               ["Balcony","Balcon","Balkon"],                              "float_area"),
    ("terrace_m2",               ["Terrace","Terrasse"],                                     "float_area"),
    ("garden",                   ["Garden","Jardin","Garten"],                               "bool"),
    ("parking_spaces",           ["Parking spaces","Places de parking","Parkplätze"],       "int"),
    # Energy
    ("energy_class",             ["Energy class","Classe énergétique","Energieklasse"],      "energy"),
    ("thermal_insulation_class", ["Thermal insulation class","Classe d'isolation thermique","Wärmedämmklasse"], "energy"),
    ("gas_heating",              ["Gas heating","Chauffage gaz","Gasheizung"],               "bool"),
    ("electric_heating",         ["Electric heating","Chauffage électrique","Elektroheizung"],"bool"),
    ("heat_pump",                ["Heat pump","Pompe à chaleur","Wärmepumpe"],               "bool"),
    ("district_heating",         ["District heating","Chauffage urbain","Fernheizung"],      "bool"),
    ("pellet_heating",           ["Pellet heating","Chauffage aux pellets","Pelletheizung"], "bool"),
    ("oil_heating",              ["Oil heating","Chauffage mazout","Ölheizung"],             "bool"),
    ("solar_heating",            ["Solar heating","Chauffage solaire","Solarheizung"],       "bool"),
    # Others
    ("basement",                 ["Basement","Cave","Keller"],                               "bool"),
    ("laundry_room",             ["Laundry room","Buanderie","Waschküche"],                  "bool"),
    ("elevator",                 ["Elevator","Ascenseur","Aufzug"],                          "bool"),
    ("storage",                  ["Storage","Local de stockage","Lagerraum"],               "bool"),
    ("pets_allowed",             ["Pets allowed","Animaux acceptés","Haustiere erlaubt"],   "bool"),
]

ALL_FIELDS = [
    "listing_ref","agency_ref","transaction_type","listing_url","source","title",
    "location","description",
    "sale_price","rent_price","monthly_charges","deposit","commission","availability",
    "surface_m2","floor","rooms","bedrooms","year_of_construction",
    "fitted_kitchen","open_kitchen","shower_rooms","bathrooms","separate_toilets","furnished",
    "balcony","balcony_m2","terrace_m2","garden","parking_spaces",
    "energy_class","thermal_insulation_class",
    "gas_heating","electric_heating","heat_pump","district_heating",
    "pellet_heating","oil_heating","solar_heating",
    "basement","laundry_room","elevator","storage","pets_allowed",
    "phone_number","phone_source",
    "agency_name","agency_url","agent_name","agency_logo_url",
    "image_urls","images_dir",
    "first_seen","last_updated","title_history",
]

# ─────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def db_init():
    """Create tables if they don't exist yet."""
    col_defs = "\n".join(
        f"  {f}  {'TEXT' if f not in ('floor','rooms','bedrooms','year_of_construction','shower_rooms','bathrooms','separate_toilets','parking_spaces') else 'INTEGER'},"
        for f in ALL_FIELDS
        if f not in ("listing_ref",)
    )
    # All boolean-like fields as INTEGER (0/1/NULL), prices as REAL
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS listings (
      listing_ref TEXT PRIMARY KEY,
      agency_ref TEXT, transaction_type TEXT, listing_url TEXT,
      source TEXT,
      title TEXT, location TEXT, description TEXT,
      sale_price REAL, rent_price REAL, monthly_charges REAL,
      deposit REAL, commission TEXT, availability TEXT,
      surface_m2 REAL, floor INTEGER, rooms INTEGER,
      bedrooms INTEGER, year_of_construction INTEGER,
      fitted_kitchen INTEGER, open_kitchen INTEGER,
      shower_rooms INTEGER, bathrooms INTEGER,
      separate_toilets INTEGER, furnished INTEGER,
      balcony INTEGER, balcony_m2 REAL, terrace_m2 REAL,
      garden INTEGER, parking_spaces INTEGER,
      energy_class TEXT, thermal_insulation_class TEXT,
      gas_heating INTEGER, electric_heating INTEGER,
      heat_pump INTEGER, district_heating INTEGER,
      pellet_heating INTEGER, oil_heating INTEGER,
      solar_heating INTEGER,
      basement INTEGER, laundry_room INTEGER,
      elevator INTEGER, storage INTEGER, pets_allowed INTEGER,
      phone_number TEXT, phone_source TEXT,
      agency_name TEXT, agency_url TEXT,
      agent_name TEXT, agency_logo_url TEXT,
      image_urls TEXT, images_dir TEXT,
      first_seen TEXT, last_updated TEXT,
      title_history TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_first_seen ON listings(first_seen);
    CREATE INDEX IF NOT EXISTS idx_transaction ON listings(transaction_type);
    CREATE INDEX IF NOT EXISTS idx_location ON listings(location);
    """
    with db_connect() as conn:
        conn.executescript(create_sql)
    log.info(f"DB ready: {DB_PATH}")


def db_get(ref: str) -> Optional[Dict]:
    """Return the stored row for a listing_ref, or None."""
    with db_connect() as conn:
        row = conn.execute(
            "SELECT * FROM listings WHERE listing_ref = ?", (ref,)
        ).fetchone()
    return dict(row) if row else None


def db_upsert(data: Dict, is_update: bool = False) -> str:
    """
    Insert a new listing or update an existing one.
    Returns 'inserted' | 'updated' | 'unchanged'.
    """
    ref = data.get("listing_ref")
    if not ref:
        return "skipped"

    now = datetime.now(timezone.utc).isoformat()

    if is_update:
        # Preserve first_seen; update last_updated
        data["last_updated"] = now
        existing = db_get(ref) or {}
        data.setdefault("first_seen", existing.get("first_seen", now))

        # Append old title to history if it changed
        old_title = existing.get("title", "")
        new_title = data.get("title", "")
        if old_title and old_title != new_title:
            history = json.loads(existing.get("title_history") or "[]")
            history.append({"title": old_title, "changed_at": now})
            data["title_history"] = json.dumps(history)
        else:
            data["title_history"] = existing.get("title_history", "[]")

        cols  = [k for k in data if k in ALL_FIELDS and k != "listing_ref"]
        sets  = ", ".join(f"{c} = ?" for c in cols)
        vals  = [data[c] for c in cols] + [ref]
        with db_connect() as conn:
            conn.execute(f"UPDATE listings SET {sets} WHERE listing_ref = ?", vals)
        return "updated"
    else:
        data["first_seen"]    = now
        data["last_updated"]  = now
        data["title_history"] = "[]"
        cols = [k for k in ALL_FIELDS if k in data]
        ph   = ", ".join("?" for _ in cols)
        vals = [data[k] for k in cols]
        with db_connect() as conn:
            conn.execute(
                f"INSERT OR IGNORE INTO listings ({', '.join(cols)}) VALUES ({ph})",
                vals,
            )
        return "inserted"


def db_stats() -> Dict:
    with db_connect() as conn:
        total    = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        buy      = conn.execute("SELECT COUNT(*) FROM listings WHERE transaction_type='buy'").fetchone()[0]
        rent     = conn.execute("SELECT COUNT(*) FROM listings WHERE transaction_type='rent'").fetchone()[0]
        w_phone  = conn.execute("SELECT COUNT(*) FROM listings WHERE phone_number IS NOT NULL").fetchone()[0]
        updated  = conn.execute("SELECT COUNT(*) FROM listings WHERE title_history != '[]'").fetchone()[0]
    return {"total": total, "buy": buy, "rent": rent,
            "with_phone": w_phone, "title_changed": updated}


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _clean(t: Any) -> str:
    return re.sub(r"\s+", " ", str(t or "")).strip()

def _parse_price(raw: str) -> Optional[float]:
    """
    Handle both French and English price formats:
      French : 1 308 251,98 €  (space=thousands, comma=decimal)
              "1 308 252 €"    (space=thousands, no decimal)
      English: 714,725         (comma=thousands)
               489,000
    
    Key rule: if a comma is followed by exactly 2 digits at the end → decimal.
    Otherwise comma is a thousands separator and gets stripped.
    """
    s = (raw or "").replace("\u202f","").replace("\xa0","").replace(" ","")
    # Remove currency symbols and trailing noise
    s = re.sub(r"[€$£\s]", "", s)
    if not s:
        return None

    # French decimal: comma followed by exactly 1-2 digits at end of number
    # e.g. "1308251,98" → 1308251.98
    if re.search(r",\d{1,2}$", s):
        s = s.replace(",", ".")
    else:
        # English thousands or no decimal — just strip commas
        s = s.replace(",", "")

    m = re.search(r"\d+(?:\.\d+)?", s)
    try:    return float(m.group()) if m else None
    except: return None

def _parse_bool(raw: str) -> Optional[int]:
    """Store booleans as 0/1/None for SQLite."""
    v = (raw or "").lower().strip()
    if v in ("yes","oui","ja","true","1","✓"):  return 1
    if v in ("no","non","nein","false","0"):     return 0
    return None

def _parse_int(raw: str) -> Optional[int]:
    m = re.search(r"\d+", raw or "")
    return int(m.group()) if m else None

def _parse_float(raw: str) -> Optional[float]:
    raw = (raw or "").replace(",",".").replace("\u202f","").replace("\xa0","")
    m   = re.search(r"\d+(?:\.\d+)?", raw)
    try:    return float(m.group()) if m else None
    except: return None

def _parse_energy(raw: str) -> Optional[str]:
    v = _clean(raw)
    if not v or v.lower() in ("blank","vide","leer","-",""):
        return None
    m = re.search(r"[A-G](?:\+{1,3})?", v)
    return m.group() if m else v

def _extract_phone(text: str) -> Optional[str]:
    text = (text or "").replace("\xa0"," ").replace("\u202f"," ")
    pat  = re.compile(
        r"""(?:(?:\+|00)352[\s\-./]?)?
            ((?:6[0-9]{2}|2[0-9]{1,2}|27[0-9]|28[0-9]|4[2-9][0-9]|5[0-9]{2})
             [\s\-./]?\d{2,3}[\s\-./]?\d{2,3}(?:[\s\-./]?\d{2,3})?)""",
        re.VERBOSE,
    )
    for m in pat.finditer(text):
        digits = re.sub(r"\D","",m.group())
        if digits.startswith("352") and len(digits) > 9:
            digits = digits[3:]
        if 6 <= len(digits) <= 12:
            return digits
    return None

def _parse_characteristics(soup: BeautifulSoup) -> Dict:
    """
    athome.lu renders the Caractéristiques block as flat concatenated text:

        Prix de vente1 308 251,98 €
        Surface habitable111,24 m²
        Nombre de chambres3
        Chauffage au gazOui

    The label and value are consecutive text nodes (or inline spans) inside
    the same parent element — there is NO sibling tag between them.

    Strategy:
      1. Isolate the ## Caractéristiques section of the page.
      2. Build a flat list of (parent_element, full_text) pairs.
      3. For each CHAR_MAP entry, search for a parent whose text STARTS WITH
         a known label and extract everything after the label as the value.
      4. Fallback: scan the entire soup the same way (for edge cases where
         the section heading has a different name).
    """
    # ── Strip all script/style noise first ───────────────────
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()

    result: Dict = {}

    # ── Isolate the Caractéristiques section ─────────────────
    # Find the ## Caractéristiques / ## Characteristics heading
    char_section_start = soup.find(
        lambda t: t.name in ("h2","h3","h4") and
                  re.search(r"caract[eé]ristiques|characteristics|eigenschaften",
                             t.get_text(), re.I)
    )
    # Find where the section ends (next h2 at same level)
    char_section_end = None
    if char_section_start:
        for sib in char_section_start.find_next_siblings():
            if sib.name in ("h2","h3") and sib != char_section_start:
                char_section_end = sib
                break

    # Collect all leaf-ish elements inside the section
    # We want elements whose direct text (not children) carries the label+value
    def _section_elements():
        """Yield elements that are inside the Caractéristiques block."""
        if not char_section_start:
            # Fallback: whole page
            yield from soup.find_all(True)
            return
        in_section = False
        for el in soup.find_all(True):
            if el == char_section_start:
                in_section = True
                continue
            if char_section_end and el == char_section_end:
                in_section = False
            if in_section:
                yield el

    # Build a de-duplicated list of (element, cleaned_text) for matching
    seen_ids: set = set()
    elements: List[Tuple[Tag, str]] = []
    for el in _section_elements():
        eid = id(el)
        if eid in seen_ids:
            continue
        seen_ids.add(eid)
        # Only leaf-ish elements (not containers holding many children)
        # Heuristic: if the element has > 4 direct-child tags, skip it
        direct_tags = [c for c in el.children if hasattr(c, 'name') and c.name]
        if len(direct_tags) > 4:
            continue
        txt = _clean(el.get_text(" "))
        if not txt or len(txt) < 2 or len(txt) > 300:
            continue
        elements.append((el, txt))

    def _extract_value(el_text: str, label: str) -> str:
        """
        Given the full text of an element and the label string,
        return everything after the label (stripped).
        e.g. "Prix de vente1 308 251,98 €"  label="Prix de vente"  → "1 308 251,98 €"
        """
        pat = re.compile(re.escape(label) + r"\s*:?\s*", re.I)
        m   = pat.search(el_text)
        if m:
            return el_text[m.end():].strip()
        return ""

    # ── Match each CHAR_MAP field ─────────────────────────────
    for field, labels, vtype in CHAR_MAP:
        if field in result:
            continue  # already found
        for label in labels:
            # First pass: strict — element text starts with (or equals) the label
            for el, txt in elements:
                if re.match(re.escape(label) + r"\s*:?\s*", txt, re.I):
                    raw = _extract_value(txt, label)
                    if not raw:
                        # Value might be in the very next sibling element
                        nxt = el.find_next_sibling()
                        if nxt:
                            raw = _clean(nxt.get_text(" "))
                    if raw:
                        break
            else:
                # Second pass: looser — label appears anywhere in the text
                raw = ""
                for el, txt in elements:
                    if re.search(re.escape(label), txt, re.I):
                        raw = _extract_value(txt, label)
                        if not raw:
                            nxt = el.find_next_sibling()
                            if nxt:
                                raw = _clean(nxt.get_text(" "))
                        if raw:
                            break

            if not raw:
                continue

            # Sanity check: value must not look like JS or another label
            if len(raw) > 200 or "window." in raw or "{" in raw:
                continue

            if   vtype == "price":      result[field] = _parse_price(raw)
            elif vtype == "int":        result[field] = _parse_int(raw)
            elif vtype == "float":      result[field] = _parse_float(raw)
            elif vtype == "float_area": result[field] = _parse_float(raw)  # e.g. "51,69 m²"
            elif vtype == "bool":       result[field] = _parse_bool(raw)
            elif vtype == "energy":     result[field] = _parse_energy(raw)
            else:                       result[field] = raw
            break  # found this label, move to next field

    return result

# ─────────────────────────────────────────────────────────────
# Selenium helpers
# ─────────────────────────────────────────────────────────────

_PHONE_BTN_TEXTS = [
    "show phone number","afficher le numéro","voir les coordonnées",
    "voir le numéro","afficher numéro","numéro de téléphone",
    "afficher","appeler","show phone","voir coordonnées",
]

def _make_driver(headless: bool = True) -> "webdriver.Chrome":
    if not SELENIUM_OK:
        raise RuntimeError("Install selenium:  pip install selenium")
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(f"user-agent={USER_AGENT}")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("useAutomationExtension", False)
    drv = webdriver.Chrome(options=opts)
    drv.execute_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    )
    return drv

def _dismiss_cookies(driver: "webdriver.Chrome") -> None:
    for sel in [
        "button#didomi-notice-agree-button","#didomi-notice-agree-button",
        "[id*='onetrust-accept']","button[aria-label*='accept' i]",
        "button[aria-label*='accepter' i]",
    ]:
        try:
            btn = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, sel))
            )
            btn.click(); time.sleep(0.1); return
        except TimeoutException:
            continue

# ─────────────────────────────────────────────────────────────
# Description expander  ("Voir tout" / "See all" / "Mehr anzeigen")
# ─────────────────────────────────────────────────────────────

_EXPAND_BTN_TEXTS = [
    "voir tout", "voir plus", "see all", "see more", "read more",
    "mehr anzeigen", "afficher tout", "afficher la suite",
    "lire la suite", "show more", "tout afficher",
]

# These are navigation/promo links that appear near the description but must
# NEVER be clicked — clicking them navigates away from the listing page.
_EXPAND_BTN_BLACKLIST = [
    "en savoir plus", "learn more", "mehr erfahren", "go for it",
    "j'y vais", "get in touch", "request a quote", "demander",
    "demandez", "kontakt", "contact", "financer", "assurer",
    "publier", "estimer", "louer", "acheter", "vendre",
]

def _expand_description(driver: "webdriver.Chrome") -> None:
    """
    Click the 'Voir tout' / 'See all' button to expand a truncated description.

    IMPORTANT: Only <button> elements are clicked (never <a> tags, which
    navigate away). We also verify the element is positioned within the top
    half of the page (description section), not in footers/promo blocks.
    """
    page_height = driver.execute_script("return document.body.scrollHeight") or 2000

    def _is_safe_expand_el(el) -> bool:
        """Return True only if this element looks like a real expand toggle."""
        try:
            tag = el.tag_name.lower()
            # Never click <a> — it navigates. Only <button> or role=button.
            if tag == "a":
                href = (el.get_attribute("href") or "").strip()
                # Only allow <a> with no href or href="#" (pure JS toggle)
                if href and href != "#" and not href.startswith("javascript"):
                    return False
            txt = (el.text or "").lower().strip()
            if not txt or len(txt) > 30:
                return False
            # Must match expand keywords
            if not any(kw in txt for kw in _EXPAND_BTN_TEXTS):
                return False
            # Must NOT match navigation/promo blacklist
            if any(kw in txt for kw in _EXPAND_BTN_BLACKLIST):
                return False
            # Must be in the upper ~70% of page (description area)
            loc = el.location
            if loc.get("y", 0) > page_height * 0.70:
                return False
            return True
        except Exception:
            return False

    # Pass A: CSS selectors scoped to button tags only
    for sel in [
        "button[data-testid*='show-more']", "button[data-testid*='read-more']",
        "button[data-action*='expand']",     "button[class*='show-more']",
        "button[class*='read-more']",        "button[class*='voir-tout']",
        "button[class*='expand']",           "button[aria-expanded='false']",
    ]:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            if not _is_safe_expand_el(el):
                continue
            driver.execute_script("arguments[0].scrollIntoView(true);", el)
            time.sleep(0.1)
            try:    el.click()
            except ElementClickInterceptedException:
                driver.execute_script("arguments[0].click();", el)
            time.sleep(0.3)
            log.debug(f"  Description expanded via: {sel}")
            return
        except (NoSuchElementException, Exception):
            continue

    # Pass B: scan only <button> elements by text
    try:
        for el in driver.find_elements(By.TAG_NAME, "button"):
            if _is_safe_expand_el(el):
                driver.execute_script("arguments[0].scrollIntoView(true);", el)
                time.sleep(0.1)
                try:    el.click()
                except ElementClickInterceptedException:
                    driver.execute_script("arguments[0].click();", el)
                time.sleep(0.3)
                log.debug(f"  Description expanded via button text: '{el.text.strip()}'")
                return
    except Exception:
        pass
    # Nothing clicked → description not truncated, or no expand button present


def _read_tel_link(driver: "webdriver.Chrome") -> Optional[str]:
    for _ in range(3):
        for lnk in driver.find_elements(By.CSS_SELECTOR, "a[href^='tel:']"):
            raw    = lnk.get_attribute("href").replace("tel:","").strip()
            digits = re.sub(r"\D","",raw)
            if digits.startswith("352") and len(digits) > 9:
                digits = digits[3:]
            if len(digits) >= 6:
                return digits
        time.sleep(0.2)
    return _extract_phone(driver.page_source)

def _click_phone_button(driver: "webdriver.Chrome") -> Optional[str]:
    # Pass A: CSS selectors
    for sel in [
        "a[href^='tel:']","[data-testid*='phone']","[data-action*='phone']",
        "[aria-label*='numéro' i]","[aria-label*='number' i]",
        "[class*='phone']","[class*='reveal-phone']","[class*='show-phone']",
    ]:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            if sel == "a[href^='tel:']":
                return _read_tel_link(driver)
            driver.execute_script("arguments[0].scrollIntoView(true);", el)
            time.sleep(0.1)
            try:    el.click()
            except ElementClickInterceptedException:
                driver.execute_script("arguments[0].click();", el)
            time.sleep(0.5)
            ph = _read_tel_link(driver)
            if ph: return ph
        except (NoSuchElementException, Exception):
            continue
    # Pass B: text scan
    for tag in ("button","a","span","div"):
        try:
            for el in driver.find_elements(By.TAG_NAME, tag):
                try:
                    txt = (el.text or "").lower().strip()
                    if not txt or len(txt) > 80: continue
                    if any(kw in txt for kw in _PHONE_BTN_TEXTS):
                        driver.execute_script("arguments[0].scrollIntoView(true);", el)
                        time.sleep(0.1)
                        try:    el.click()
                        except ElementClickInterceptedException:
                            driver.execute_script("arguments[0].click();", el)
                        time.sleep(0.5)
                        ph = _read_tel_link(driver)
                        if ph: return ph
                except Exception: continue
        except Exception: continue
    return None

# ─────────────────────────────────────────────────────────────
# Image downloader
# ─────────────────────────────────────────────────────────────

def _download_images(urls: List[str], listing_ref: str) -> Path:
    folder = IMAGES_ROOT / listing_ref
    folder.mkdir(parents=True, exist_ok=True)
    sess = requests.Session()
    sess.headers["User-Agent"] = USER_AGENT
    saved = 0
    for i, url in enumerate(urls, 1):
        if not url or url.startswith("data:"): continue
        try:
            r   = sess.get(url, timeout=12); r.raise_for_status()
            ct  = r.headers.get("content-type","image/jpeg")
            ext = ct.split("/")[-1].split(";")[0].strip().replace("jpeg","jpg") or "jpg"
            (folder / f"{i:03d}.{ext}").write_bytes(r.content)
            saved += 1
        except Exception as e:
            log.debug(f"  Image {i} failed: {e}")
    log.info(f"  {saved}/{len(urls)} images saved → {folder}/")
    return folder

# ─────────────────────────────────────────────────────────────
# STEP 1 — collect listing refs from the index page
# ─────────────────────────────────────────────────────────────

def get_index_refs(
    driver: "webdriver.Chrome",
    index_url: str,
    max_pages: int = 2,
) -> List[Tuple[str, str]]:
    """
    Returns list of (listing_ref, listing_url) tuples, newest-first,
    collected from up to `max_pages` pages of the index.
    """
    results: List[Tuple[str, str]] = []
    current = index_url
    wait    = WebDriverWait(driver, 20)

    for page in range(1, max_pages + 1):
        log.info(f"  Index page {page}: {current}")
        driver.get(current)
        if page == 1:
            _dismiss_cookies(driver)

        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "a[href*='/id-']")
            ))
        except TimeoutException:
            log.warning(f"  No listing links on page {page} — stopping.")
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.3)

        soup = BeautifulSoup(driver.page_source, "lxml")
        new_cnt = 0
        for a in soup.find_all("a", href=re.compile(r"/id-(\d+)\.html")):
            href = a["href"]
            m    = re.search(r"/id-(\d+)\.html", href)
            if not m: continue
            ref  = m.group(1)
            full = href if href.startswith("http") else BASE_URL + href
            if not any(r[0] == ref for r in results):
                results.append((ref, full))
                new_cnt += 1

        log.info(f"    +{new_cnt} refs on page {page}  (total: {len(results)})")
        if new_cnt == 0:
            break

        if "page=" in current:
            current = re.sub(r"page=\d+", f"page={page+1}", current)
        else:
            sep     = "&" if "?" in current else "?"
            current = f"{current}{sep}page={page+1}"

    log.info(f"  Collected {len(results)} refs from index.")
    return results

# ─────────────────────────────────────────────────────────────
# STEP 2 — scrape one detail page
# ─────────────────────────────────────────────────────────────

def scrape_detail(
    driver: "webdriver.Chrome",
    url: str,
    transaction_type: str,
    save_images: bool = True,
) -> Dict:
    driver.get(url)
    time.sleep(0.5)
    _dismiss_cookies(driver)
    
    # Wait for h1 title to load (React app takes time to render)
    # Use try-except with fallback to avoid ChromeDriver crashes
    h1_loaded = False
    try:
        wait = WebDriverWait(driver, 5)  # Reduced to 5s to fail faster
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1")))
        h1_loaded = True
        log.debug(f"  h1 loaded for {url}")
    except TimeoutException:
        # h1 didn't load in time, but might still be in page - continue anyway
        log.debug(f"  h1 wait timeout for {url}, continuing anyway")
    except Exception as e:
        # ChromeDriver crash or other issue - use simple sleep fallback
        log.warning(f"  h1 wait failed ({type(e).__name__}), using sleep fallback")
        time.sleep(2)  # Give page more time to render
    
    # Always re-parse the page source (whether h1 loaded or not)
    soup = BeautifulSoup(driver.page_source, "lxml")
    
    # ── Source (athome vs immotop) ──────────────────────────
    source = "athome" if "athome.lu" in url else "immotop" if "immotop.lu" in url else "unknown"
    
    data: Dict = {
        "listing_url":      url,
        "source":           source,
        "transaction_type": transaction_type,
        "phone_number":     None,
        "phone_source":     None,
    }

    # ── Listing ref — three sources (URL is most reliable) ──
    ref_from_url = re.search(r"/id-(\d+)", url)
    if ref_from_url:
        data["listing_ref"] = ref_from_url.group(1)

    # "Réf atHome 8983182" in the page body (confirms the URL ref)
    page_ref = soup.find(string=re.compile(
        r"R[ée]f\.?\s+(?:atHome|athome)?\s*:?\s*(\d{5,})", re.I
    ))
    if page_ref:
        m = re.search(r"(\d{5,})", page_ref)
        if m and not data.get("listing_ref"):
            data["listing_ref"] = m.group(1)

    # "Réf Agence …"
    agency_ref_node = soup.find(string=re.compile(
        r"R[ée]f\.?\s+[Aa]gence|[Aa]gency\s+[Rr]ef", re.I
    ))
    if agency_ref_node:
        parent = agency_ref_node.find_parent()
        if parent:
            nxt = parent.find_next_sibling()
            if nxt:
                data["agency_ref"] = _clean(nxt.get_text())

    # ── Expand truncated description ("Voir tout" / "See all" / "Mehr anzeigen")
    _expand_description(driver)
    # Re-parse after potential expansion
    soup = BeautifulSoup(driver.page_source, "lxml")

    # ── Title ────────────────────────────────────────────────
    h1 = soup.find("h1")
    if h1:
        data["title"] = _clean(h1.get_text())
    else:
        # Fallback 1: Try page title (often contains listing title)
        page_title = soup.find("title")
        if page_title:
            title_text = _clean(page_title.get_text())
            # athome format: "Acheter Immeuble de rapport 221 m² – 950 000 € |Ettelbruck"
            # Extract the meaningful part (before the pipe or dash)
            title_parts = re.split(r'\s*[|–]\s*', title_text)
            if title_parts:
                # Remove "Acheter" / "Louer" prefix
                clean_title = re.sub(r'^(Acheter|Louer|Buy|Rent|Vente|Location)\s+', '', title_parts[0], flags=re.I)
                data["title"] = _clean(clean_title)
                log.debug(f"  Title from <title> tag: {data['title']}")
        
        # Fallback 2: Try og:title meta tag
        if not data.get("title"):
            og_title = soup.find("meta", property="og:title")
            if og_title and og_title.get("content"):
                data["title"] = _clean(og_title["content"])
                log.debug(f"  Title from og:title: {data['title']}")
        
        if not data.get("title"):
            log.warning(f"  No title found for {url}")

    # ── Location (last meaningful breadcrumb) ────────────────
    skip = {"accueil","acheter","louer","vente","location","home","buy","rent",
            "sell","appartement","maison","apartment","house",
            "en savoir plus","learn more","mehr erfahren","voir plus",
            "see more","read more","lire la suite","voir tout"}
    location_found = False
    for crumb in reversed(soup.select(
        "ol li, ol li a, nav[aria-label] a, .breadcrumb a, .breadcrumb span"
    )):
        txt = _clean(crumb.get_text())
        if txt and txt.lower() not in skip and len(txt) > 2 \
                and not re.match(r"R[ée]f", txt):
            # Additional check: must not be a promotional link text
            # Real locations: "Luxembourg-Gare", "Schuttrange", "Bonnevoie"
            # Spam: "En savoir plus", "J'y vais", "Demander"
            if any(kw in txt.lower() for kw in ["savoir","learn","mehr","voir","see","read","lire","demander","request","j'y vais","go for"]):
                continue
            data["location"] = txt
            location_found = True
            break
    
    # Fallback: extract from the <h1> title if breadcrumb failed
    # e.g. "Appartement 3 chambres à Schuttrange" → "Schuttrange"
    if not location_found and data.get("title"):
        m = re.search(r"(?:à|in|in)\s+([A-Z][a-zé\-]+(?:\-[A-Z][a-zé\-]+)*)", data["title"])
        if m:
            data["location"] = m.group(1)

    # ── Description ──────────────────────────────────────────
    # Strategy: find the ## Description heading, collect all sibling text
    # until we hit "Demander plus d'infos" / "Réf atHome" / next <h2>.
    # Always strip <script> and <style> tags before reading text.
    desc_h = soup.find(
        lambda t: t.name in ("h2","h3","h4") and
                  re.search(r"^description$|^beschreibung$", t.get_text().strip(), re.I)
    )
    if desc_h:
        parts = []
        for sib in desc_h.find_next_siblings():
            # Stop at the next section heading
            if sib.name in ("h2","h3","h4"):
                break
            # Stop at "Demander plus d'infos" / refs / ask-for-info blocks
            sib_txt = sib.get_text(" ", strip=True)
            if re.search(
                r"demander plus d.infos|ask for more|mehr informationen|"
                r"r[eé]f\s+(?:atHome|agence)|ref\s+agency",
                sib_txt, re.I
            ):
                break
            # Remove script / style noise before extracting text
            for tag in sib.find_all(["script","style"]):
                tag.decompose()
            txt = sib.get_text("\n", strip=True)
            if txt:
                parts.append(txt)
        if parts:
            data["description"] = _clean("\n".join(parts))
    
    # Fallback: largest text block that isn't in nav/header/footer/script
    if not data.get("description"):
        for tag in soup.find_all(["script","style"]):
            tag.decompose()
        candidates = [
            t for t in soup.find_all(["p","div"])
            if 120 < len(t.get_text()) < 8000  # cap at 8k to avoid JS blobs
            and not any(p.name in ("nav","header","footer") for p in t.parents)
            and "window." not in t.get_text()   # hard exclude JS globals
            and "AT_HOME_APP" not in t.get_text()
        ]
        if candidates:
            data["description"] = _clean(
                max(candidates, key=lambda t: len(t.get_text())).get_text("\n")
            )

    # ── Pass 1: phone from description ───────────────────────
    if data.get("description"):
        ph = _extract_phone(data["description"])
        if ph:
            data["phone_number"] = ph
            data["phone_source"] = "description"

    # ── All characteristics ───────────────────────────────────
    data.update(_parse_characteristics(soup))

    # ── Agency block ─────────────────────────────────────────
    # Strategy 1: Find the "Annonce publiée par" / "Listing published by" section
    agency_section = soup.find(
        lambda t: t.name in ("section","div","aside")
                  and re.search(
                      r"published by|publiée par|veröffentlicht von|annonce publiée",
                      t.get_text() or "", re.I
                  )
    )
    
    if agency_section:
        # Try link to agency profile page
        a_link = agency_section.find("a", href=re.compile(r"/agence|/realestate-agency|/immobilier"))
        if a_link:
            data["agency_name"] = _clean(a_link.get_text())
            href = a_link["href"]
            data["agency_url"] = href if href.startswith("http") else BASE_URL + href
        
        # If no link found, try bold/strong text (often the agency name)
        if not data.get("agency_name"):
            for tag in agency_section.find_all(["strong","b","h3","h4"]):
                txt = _clean(tag.get_text())
                if txt and len(txt) > 3 and len(txt) < 100:
                    # Exclude generic labels
                    if not re.search(r"published|publiée|contact|annonce|listing|téléphone|phone", txt, re.I):
                        data["agency_name"] = txt
                        break
        
        # Logo
        logo = agency_section.find("img")
        if logo:
            src = logo.get("src","")
            data["agency_logo_url"] = src if src.startswith("http") else BASE_URL + src
        
        # Agent name (often in img alt text or a caption)
        for img in agency_section.find_all("img")[1:]:
            alt = (img.get("alt") or "").strip()
            if alt and len(alt) > 3 and len(alt) < 60:
                # Likely a person's name
                if re.search(r"[A-Z][a-z]+\s+[A-Z]", alt):  # "Mathieu SCHERRER"
                    data["agent_name"] = alt
                    break
    
    # Strategy 2: Fallback — scan whole page for agency metadata or schema.org
    if not data.get("agency_name"):
        # Try schema.org structured data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.string or "{}")
                if isinstance(ld, dict):
                    seller = ld.get("seller", {})
                    if isinstance(seller, dict) and seller.get("name"):
                        data["agency_name"] = seller["name"]
                        break
                    provider = ld.get("provider", {})
                    if isinstance(provider, dict) and provider.get("name"):
                        data["agency_name"] = provider["name"]
                        break
            except (json.JSONDecodeError, Exception):
                continue
    
    # Strategy 3: Look for agency name near a logo image with recognizable agency URL
    if not data.get("agency_name"):
        for img in soup.find_all("img"):
            src = img.get("src", "")
            if "logoagences" in src or "logo" in src.lower():
                parent = img.find_parent()
                if parent:
                    txt = _clean(parent.get_text())
                    # Extract first capitalized phrase (likely agency name)
                    m = re.search(r"([A-Z][A-Z\s&]+(?:SARL|SA|SPRL|IMMOBILIER|IMMO|REAL ESTATE)?)", txt)
                    if m and len(m.group(1)) < 60:
                        data["agency_name"] = m.group(1).strip()
                        break

    # ── Images ───────────────────────────────────────────────
    image_urls: List[str] = []
    for img in soup.find_all("img"):
        for attr in ("src","data-src","data-lazy-src","data-original"):
            src = (img.get(attr) or "").strip()
            if (src and "static.athome.eu" in src and "/annonces" in src
                    and not src.startswith("data:") and src not in image_urls):
                image_urls.append(src); break
    data["image_urls"] = json.dumps(image_urls)

    # ── Pass 2: phone from reveal button ─────────────────────
    if not data["phone_number"]:
        ph = _click_phone_button(driver)
        if ph:
            data["phone_number"] = ph
            data["phone_source"] = "button"

    # ── Download images ───────────────────────────────────────
    if save_images and image_urls and data.get("listing_ref"):
        folder = _download_images(image_urls, data["listing_ref"])
        data["images_dir"] = str(folder)

    log.info(
        f"  ✓ ref={data.get('listing_ref')} | "
        f"€{data.get('sale_price') or data.get('rent_price','?')} | "
        f"{data.get('location','?')} | "
        f"phone={data.get('phone_number','—')}[{data.get('phone_source','—')}] | "
        f"{len(image_urls)} imgs"
    )
    return data

# ─────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────

def run(
    index_configs:       List[Dict],
    max_pages_per_index: int   = 2,
    save_images:         bool  = True,
    delay_seconds:       float = 0,      # ← no delay
    headless:            bool  = True,
) -> Dict[str, int]:
    """
    For each index URL:
      - Collect (ref, url) pairs newest-first.
      - For each pair:
          • NEW ref       → scrape fully, insert into DB.
          • KNOWN ref, title CHANGED → re-scrape, update DB, keep history.
          • KNOWN ref, title UNCHANGED → STOP (we've caught up).
    Returns counters dict.
    """
    db_init()
    if not SELENIUM_OK:
        log.error("Selenium not installed. Run: pip install selenium")
        return {}

    driver = _make_driver(headless=headless)
    counters = {"inserted": 0, "updated": 0, "skipped": 0, "stopped_early": 0}

    try:
        for cfg in index_configs:
            idx_url = cfg["url"]
            t_type  = cfg.get("type", "buy")
            log.info(f"\n{'='*60}")
            log.info(f"INDEX  {idx_url}  [{t_type}]")
            log.info("="*60)

            ref_pairs = get_index_refs(driver, idx_url, max_pages=max_pages_per_index)

            for i, (ref, lurl) in enumerate(ref_pairs, 1):
                existing = db_get(ref)

                if existing is None:
                    # ── Brand new listing ─────────────────────────────
                    log.info(f"[{i}] NEW  {ref}  {lurl}")
                    d = scrape_detail(driver, lurl, t_type, save_images=save_images)
                    if d:
                        db_upsert(d, is_update=False)
                        counters["inserted"] += 1
                    time.sleep(delay_seconds)

                else:
                    # ── Known ref — check title via lightweight fetch ──
                    # We only fetch the detail page if the title changed;
                    # to get the title cheaply we use requests (no Selenium cost)
                    try:
                        resp = requests.get(
                            lurl, headers={"User-Agent": USER_AGENT}, timeout=10
                        )
                        soup_light = BeautifulSoup(resp.text, "lxml")
                        h1 = soup_light.find("h1")
                        current_title = _clean(h1.get_text()) if h1 else ""
                    except Exception:
                        current_title = existing.get("title", "")

                    old_title = existing.get("title", "")

                    if current_title and current_title != old_title:
                        # Title changed → full re-scrape + update
                        log.info(
                            f"[{i}] UPDATED  {ref}\n"
                            f"      old: {old_title}\n"
                            f"      new: {current_title}"
                        )
                        d = scrape_detail(driver, lurl, t_type, save_images=save_images)
                        if d:
                            db_upsert(d, is_update=True)
                            counters["updated"] += 1
                        time.sleep(delay_seconds)
                    else:
                        # Unchanged known listing → stop early for this index
                        log.info(
                            f"[{i}] STOP — hit known listing {ref} (unchanged). "
                            f"All newer listings have been processed."
                        )
                        counters["stopped_early"] += 1
                        break   # ← early exit for this index_url

    finally:
        driver.quit()

    stats = db_stats()
    log.info(
        f"\n{'='*60}\n"
        f"Run complete.\n"
        f"  inserted: {counters['inserted']}\n"
        f"  updated:  {counters['updated']}\n"
        f"  stopped early: {counters['stopped_early']}\n"
        f"DB totals → {stats}"
    )
    return counters

# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    INDEX_URLS = [
        # athome.lu
        {"url": "https://www.athome.lu/vente?sort=date_desc",    "type": "buy"},
        {"url": "https://www.athome.lu/location?sort=date_desc", "type": "rent"},
        # immotop.lu (add these when ready to scrape both sites)
        # {"url": "https://www.immotop.lu/vente/?sort=date_desc",    "type": "buy"},
        # {"url": "https://www.immotop.lu/location/?sort=date_desc", "type": "rent"},
    ]

    run(
        index_configs        = INDEX_URLS,
        max_pages_per_index  = 2,      # 2 pages × ~20 listings = ~40 per run
        save_images          = True,
        delay_seconds        = 0,      # ← no delay
        headless             = True,   # False = watch the browser
    )

    # Pretty-print DB stats
    s = db_stats()
    print(f"\n{'─'*45}")
    print(f" DB: {DB_PATH}")
    print(f" Total listings  : {s['total']}")
    print(f" For sale        : {s['buy']}")
    print(f" For rent        : {s['rent']}")
    print(f" With phone      : {s['with_phone']}")
    print(f" Title changed   : {s['title_changed']}")
    print(f"{'─'*45}")