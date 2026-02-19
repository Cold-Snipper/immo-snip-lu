"""
immotop.lu Scraper
==================
Same architecture as athome_scraper.py but adapted for immotop.lu structure.

Uses the SAME SQLite DB as athome — listings.db with source='immotop'.

Key differences from athome.lu:
  - URL pattern: /annonces/XXXXXXX/ instead of /id-XXXXXXX.html
  - Cookie consent: "Tout refuser" then "Tout accepter" two-step flow
  - Listing cards selector: a[href*='/annonces/']
  - Different characteristic labels (but we map to same DB fields)
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
from phone_validator import clean_phone

# Import shared DB functions from athome_scraper
# (We'll create a shared db.py module, but for now just duplicate the essentials)

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
DB_PATH     = Path("listings.db")  # SAME DB as athome
IMAGES_ROOT = Path("images")
BASE_URL    = "https://www.immotop.lu"
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
        logging.FileHandler("immotop_scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("immotop")

# Import all field schemas and DB functions from athome_scraper
# For now we'll just duplicate the minimal DB interface here
# TODO: refactor shared code into db_utils.py

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

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def db_init() -> None:
    """Create the listings table if it doesn't exist (same schema as athome)."""
    with db_connect() as conn:
        conn.execute("""
    CREATE TABLE IF NOT EXISTS listings (
      listing_ref TEXT PRIMARY KEY,
      agency_ref TEXT, transaction_type TEXT, listing_url TEXT,
      source TEXT,
      title TEXT, location TEXT, description TEXT,
      sale_price REAL, rent_price REAL,
      monthly_charges REAL, deposit REAL,
      commission TEXT, availability TEXT,
      surface_m2 REAL, floor INTEGER,
      rooms INTEGER, bedrooms INTEGER,
      year_of_construction INTEGER,
      fitted_kitchen INTEGER, open_kitchen INTEGER,
      shower_rooms INTEGER, bathrooms INTEGER,
      separate_toilets INTEGER, furnished INTEGER,
      balcony INTEGER, balcony_m2 REAL, terrace_m2 REAL,
      garden INTEGER, parking_spaces INTEGER,
      energy_class TEXT, thermal_insulation_class TEXT,
      gas_heating INTEGER, electric_heating INTEGER,
      heat_pump INTEGER, district_heating INTEGER,
      pellet_heating INTEGER, oil_heating INTEGER, solar_heating INTEGER,
      basement INTEGER, laundry_room INTEGER,
      elevator INTEGER, storage INTEGER, pets_allowed INTEGER,
      phone_number TEXT, phone_source TEXT,
      agency_name TEXT, agency_url TEXT,
      agent_name TEXT, agency_logo_url TEXT,
      image_urls TEXT, images_dir TEXT,
      first_seen TEXT, last_updated TEXT,
      title_history TEXT
    )
        """)
    log.info(f"DB initialized: {DB_PATH}")

def db_get(ref: str) -> Optional[Dict]:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT * FROM listings WHERE listing_ref = ?", (ref,)
        ).fetchone()
    return dict(row) if row else None

def db_upsert(data: Dict, is_update: bool = False) -> str:
    ref = data.get("listing_ref")
    if not ref:
        return "skipped"
    now = datetime.now(timezone.utc).isoformat()
    if is_update:
        data["last_updated"] = now
        existing = db_get(ref) or {}
        data.setdefault("first_seen", existing.get("first_seen", now))
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

# ─────────────────────────────────────────────────────────────
# Parsing helpers (same as athome)
# ─────────────────────────────────────────────────────────────

def _clean(t: Any) -> str:
    return re.sub(r"\s+", " ", str(t or "")).strip()

def _parse_price(raw: str) -> Optional[float]:
    s = (raw or "").replace("\u202f","").replace("\xa0","").replace(" ","")
    s = re.sub(r"[€$£\s]", "", s)
    if not s: return None
    if re.search(r",\d{1,2}$", s):
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    m = re.search(r"\d+(?:\.\d+)?", s)
    try:    return float(m.group()) if m else None
    except: return None

def _parse_int(raw: str) -> Optional[int]:
    m = re.search(r"\d+", raw or "")
    return int(m.group()) if m else None

def _parse_float(raw: str) -> Optional[float]:
    raw = (raw or "").replace(",",".").replace("\u202f","").replace("\xa0","")
    m   = re.search(r"\d+(?:\.\d+)?", raw)
    try:    return float(m.group()) if m else None
    except: return None

def _parse_bool(raw: str) -> Optional[int]:
    v = (raw or "").lower().strip()
    if v in ("yes","oui","ja","true","1","✓","yes"):  return 1
    if v in ("no","non","nein","false","0"):          return 0
    return None

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

# ─────────────────────────────────────────────────────────────
# Selenium driver
# ─────────────────────────────────────────────────────────────

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
    """
    immotop cookie strategy (from uploaded code):
      1. Try "Tout refuser" / "Refuser" first
      2. If that fails, try "Tout accepter" / "Accepter"
    """
    reject_texts = ["Tout refuser", "Refuser", "Seulement les essentiels",
                    "Only necessary", "Necessary only"]
    accept_texts = ["Tout accepter", "Accepter", "Accept all", "OK"]
    
    # Try reject first
    for text in reject_texts:
        for sel in [
            f"button:contains('{text}')",
            f"a:contains('{text}')",
            f"[role='button']:contains('{text}')",
        ]:
            # Selenium doesn't have :contains, use xpath
            xpath = f"//button[contains(text(), '{text}')] | //a[contains(text(), '{text}')]"
            try:
                btn = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                btn.click()
                time.sleep(0.2)
                log.debug(f"Cookie reject: {text}")
                return
            except TimeoutException:
                continue
    
    # Fallback: accept
    for text in accept_texts:
        xpath = f"//button[contains(text(), '{text}')] | //a[contains(text(), '{text}')]"
        try:
            btn = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            btn.click()
            time.sleep(0.2)
            log.debug(f"Cookie accept: {text}")
            return
        except TimeoutException:
            continue

# ─────────────────────────────────────────────────────────────
# Index page → collect listing URLs
# ─────────────────────────────────────────────────────────────

def get_index_refs(
    driver: "webdriver.Chrome",
    index_url: str,
    max_pages: int = 2,
) -> List[Tuple[str, str]]:
    """
    immotop URL pattern: /annonces/XXXXXXX/
    Returns list of (listing_ref, listing_url) tuples.
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
                (By.CSS_SELECTOR, "a[href*='/annonces/']")
            ))
        except TimeoutException:
            log.warning(f"  No listing links on page {page} — stopping.")
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.3)

        soup = BeautifulSoup(driver.page_source, "lxml")
        new_cnt = 0
        for a in soup.find_all("a", href=re.compile(r"/annonces/(\d+)")):
            href = a["href"]
            m    = re.search(r"/annonces/(\d+)", href)
            if not m: continue
            ref  = m.group(1)
            full = href if href.startswith("http") else BASE_URL + href
            if not any(r[0] == ref for r in results):
                results.append((ref, full))
                new_cnt += 1

        log.info(f"    +{new_cnt} refs on page {page}  (total: {len(results)})")
        if new_cnt == 0:
            break

        # Pagination — immotop uses ?pag=N
        if "pag=" in current:
            current = re.sub(r"pag=\d+", f"pag={page+1}", current)
        else:
            sep     = "&" if "?" in current else "?"
            current = f"{current}{sep}pag={page+1}"

    log.info(f"  Collected {len(results)} refs from index.")
    return results

# ─────────────────────────────────────────────────────────────
# Detail page scraper
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

    soup = BeautifulSoup(driver.page_source, "lxml")
    
    data: Dict = {
        "listing_url":      url,
        "source":           "immotop",
        "transaction_type": transaction_type,
        "phone_number":     None,
        "phone_source":     None,
    }

    # ── Reference IDs ────────────────────────────────────────
    ref_m = re.search(r"/annonces/(\d+)", url)
    if ref_m:
        data["listing_ref"] = ref_m.group(1)
    
    # Also try to find "référence:" in the description
    ref_text = soup.find(string=re.compile(r"référence\s*:", re.I))
    if ref_text:
        parent_text = ref_text.parent.get_text() if ref_text.parent else ref_text
        agency_ref_m = re.search(r"référence\s*:\s*(\S+)", parent_text, re.I)
        if agency_ref_m:
            data["agency_ref"] = _clean(agency_ref_m.group(1))

    # ── Title ────────────────────────────────────────────────
    h1 = soup.find("h1")
    if h1:
        data["title"] = _clean(h1.get_text())

    # ── Location ─────────────────────────────────────────────
    # immotop shows location under the h1, or in breadcrumb
    # Try the subtitle under h1 first
    if h1:
        next_elem = h1.find_next_sibling()
        if next_elem and len(_clean(next_elem.get_text())) < 100:
            data["location"] = _clean(next_elem.get_text())
    
    # Fallback: breadcrumb
    if not data.get("location"):
        skip = {"accueil","vente","location","annonces","immotop","appartement","maison"}
        for crumb in reversed(soup.select("ol li, .breadcrumb a, .breadcrumb span")):
            txt = _clean(crumb.get_text())
            if txt and txt.lower() not in skip and len(txt) > 2:
                data["location"] = txt
                break

    # ── Description ──────────────────────────────────────────
    # immotop has Description section with "référence:" at start
    desc_section = soup.find(lambda t: t.name in ("div","section") and
                             re.search(r"description", (t.get("class") or [""])[0] if hasattr(t.get("class"), '__iter__') else "", re.I))
    if not desc_section:
        desc_h = soup.find(lambda t: t.name in ("h2","h3") and
                           re.search(r"description", t.get_text(), re.I))
        if desc_h:
            desc_section = desc_h.parent
    
    if desc_section:
        # Remove script/style noise
        for tag in desc_section.find_all(["script","style"]):
            tag.decompose()
        data["description"] = _clean(desc_section.get_text("\n"))
    
    # ── Phone from description ───────────────────────────────
    # Validated with clean_phone() — if garbage, stays None so button click fires.
    if data.get("description"):
        ph = _extract_phone(data["description"])
        if ph:
            ph = clean_phone(ph, hint="LU")
            if ph:
                data["phone_number"] = ph
                data["phone_source"] = "description"
    
    # ── Phone from button click ──────────────────────────────
    # Only runs if Pass 1 found nothing valid.
    if not data.get("phone_number"):
        # immotop uses "Afficher le téléphone" button
        try:
            xpath = "//button[contains(text(), 'Afficher le téléphone')] | //a[contains(text(), 'Afficher le téléphone')]"
            btn = driver.find_element(By.XPATH, xpath)
            driver.execute_script("arguments[0].scrollIntoView(true);", btn)
            time.sleep(0.1)
            btn.click()
            time.sleep(0.5)
            
            # Phone appears in text or as a link
            soup = BeautifulSoup(driver.page_source, "lxml")
            for link in soup.find_all("a", href=re.compile(r"^tel:")):
                raw = link.get("href").replace("tel:","").strip()
                ph = clean_phone(raw, hint="LU")
                if ph:
                    data["phone_number"] = ph
                    data["phone_source"] = "button"
                    break
            
            # Fallback: scan for phone in visible text near button
            if not data.get("phone_number"):
                page_text = soup.get_text()
                ph = _extract_phone(page_text)
                if ph:
                    ph = clean_phone(ph, hint="LU")
                    if ph:
                        data["phone_number"] = ph
                        data["phone_source"] = "button"
        except Exception as e:
            log.debug(f"  Phone button not found or click failed: {e}")

    # ── Characteristics ──────────────────────────────────────
    # immotop can use several formats:
    # 1. Definition lists: <dt>Label</dt> <dd>Value</dd>
    # 2. Divs with label: value pattern
    # 3. Icon + text in flex containers
    
    # Strategy: collect all text that looks like "Label: Value" or "Label Value"
    # anywhere on the page, then map to our fields
    
    page_text = soup.get_text(separator="\n")
    
    # Parse key characteristics from the page summary (top of page near price)
    # immotop shows: price, bedrooms (🛏), surface (m²), etc. with icons
    
    # Price - look near € symbol and h1
    price_section = soup.find(lambda t: "€" in t.get_text() and 
                              len(t.get_text()) < 100)
    if price_section:
        price_text = price_section.get_text()
        if transaction_type == "buy":
            data["sale_price"] = _parse_price(price_text)
        else:
            data["rent_price"] = _parse_price(price_text)
    
    # Look for structured characteristics section
    # Try multiple selectors
    char_section = None
    for selector in [
        lambda t: t.name == "section" and "caractéristiques" in t.get_text().lower()[:200],
        lambda t: t.name == "div" and t.get("class") and "characteristics" in str(t.get("class")),
        lambda t: t.name in ("h2","h3") and "caractéristiques" in t.get_text().lower()
    ]:
        char_section = soup.find(selector)
        if char_section and char_section.name in ("h2","h3"):
            char_section = char_section.find_parent()
        if char_section:
            break
    
    # Parse all label-value pairs from characteristics section (or whole page if not found)
    search_area = char_section if char_section else soup
    
    # Method 1: dt/dd pairs
    for dt in search_area.find_all("dt"):
        label = _clean(dt.get_text()).lower()
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        value = _clean(dd.get_text())
        _map_characteristic(label, value, data, transaction_type)
    
    # Method 2: Divs/spans with ":" separator
    for elem in search_area.find_all(["div","p","li"]):
        text = _clean(elem.get_text())
        if ":" not in text or len(text) > 150:
            continue
        parts = text.split(":", 1)
        if len(parts) == 2:
            label = parts[0].lower().strip()
            value = parts[1].strip()
            _map_characteristic(label, value, data, transaction_type)
    
    # Method 3: Look for numeric + unit patterns in icon-based displays
    # e.g. "3" next to a bed icon, "135 m²" next to area icon
    for elem in soup.find_all(["span","div"], class_=True):
        text = _clean(elem.get_text())
        # Bedrooms: just a number (1-20 range)
        if re.match(r"^\d{1,2}$", text) and not data.get("bedrooms"):
            num = int(text)
            if 1 <= num <= 20:
                # Check if near bedroom-related text
                context = _clean(elem.parent.get_text()) if elem.parent else ""
                if "chambre" in context.lower() or "bedroom" in context.lower():
                    data["bedrooms"] = num
        
        # Surface: number + m²
        if re.search(r"\d+\s*m²", text) and not data.get("surface_m2"):
            data["surface_m2"] = _parse_float(text)
    
    # Energy classes - look for specific patterns
    for section in [soup.find(string=re.compile(r"efficacité énergétique|energy efficiency", re.I)),
                    soup.find(string=re.compile(r"consommation|isolation", re.I))]:
        if not section:
            continue
        parent = section.parent
        if not parent:
            continue
        # Energy class usually in a sibling or nearby element with just "A", "B", etc.
        for neighbor in list(parent.next_siblings)[:5] + list(parent.find_all())[:10]:
            if not hasattr(neighbor, 'get_text'):
                continue
            text = _clean(neighbor.get_text())
            if re.match(r"^[A-G](\+{1,3})?$", text):
                if "isolation" in _clean(parent.get_text()).lower():
                    data["thermal_insulation_class"] = text
                else:
                    data["energy_class"] = text

    # ── Agency ───────────────────────────────────────────────
    # immotop shows agency in "Annonceur" section
    agency_section = soup.find(lambda t: t.name in ("section","div") and
                               re.search(r"annonceur|advertiser", t.get_text() or "", re.I))
    if agency_section:
        # Agency name - usually in an <a> tag or bold text
        a_link = agency_section.find("a", href=re.compile(r"/agences-immobilieres/"))
        if a_link:
            data["agency_name"] = _clean(a_link.get_text())
            href = a_link["href"]
            data["agency_url"] = href if href.startswith("http") else BASE_URL + href
        
        # Agent name - often in an alt tag or near a profile photo
        agent_elem = agency_section.find(lambda t: t.name in ("div","span","p") and
                                         len(_clean(t.get_text())) < 60 and
                                         re.search(r"[A-Z][a-z]+ [A-Z]", t.get_text()))
        if agent_elem:
            data["agent_name"] = _clean(agent_elem.get_text())
        
        # Logo
        logo = agency_section.find("img")
        if logo and logo.get("src"):
            src = logo["src"]
            data["agency_logo_url"] = src if src.startswith("http") else BASE_URL + src

    # ── Images ───────────────────────────────────────────────
    image_urls: List[str] = []
    for img in soup.find_all("img"):
        for attr in ("src","data-src","data-lazy-src"):
            src = (img.get(attr) or "").strip()
            if src and not src.startswith("data:") and "logo" not in src and src not in image_urls:
                if not src.startswith("http"):
                    src = BASE_URL + src if not src.startswith("/") else "https:" + src
                # Only include actual property photos (pic.immotop.lu domain)
                if "pic.immotop.lu" in src:
                    image_urls.append(src)
                    break
    data["image_urls"] = json.dumps(image_urls[:20])  # Limit to first 20

    log.info(
        f"  ✓ ref={data.get('listing_ref')} | "
        f"€{data.get('sale_price') or data.get('rent_price','?')} | "
        f"{data.get('bedrooms','?')}bed | "
        f"{data.get('surface_m2','?')}m² | "
        f"{data.get('location','?')} | "
        f"phone={data.get('phone_number','—')}"
    )
    return data


def _map_characteristic(label: str, value: str, data: Dict, transaction_type: str) -> None:
    """Map an immotop label-value pair to our DB fields."""
    label = label.lower().strip()
    value = value.strip()
    
    # Price
    if "prix" in label and "m²" not in label:
        if transaction_type == "buy":
            data["sale_price"] = _parse_price(value)
        else:
            data["rent_price"] = _parse_price(value)
    elif "charges" in label or "monthly" in label:
        data["monthly_charges"] = _parse_price(value)
    elif "caution" in label or "deposit" in label or "garantie" in label:
        data["deposit"] = _parse_price(value)
    
    # Dimensions
    elif "superficie" in label or "surface habitable" in label or "surface" in label:
        data["surface_m2"] = _parse_float(value)
    elif "étage" in label or "floor" in label:
        data["floor"] = _parse_int(value)
    
    # Rooms
    elif label == "chambres" or label == "bedrooms" or "chambres à coucher" in label:
        data["bedrooms"] = _parse_int(value)
    elif "chambres/pièces" in label or label == "pièces" or label == "rooms":
        data["rooms"] = _parse_int(value)
    
    # Bathrooms
    elif "bains/douches" in label or "salles de bain" in label or "bathrooms" in label:
        count = _parse_int(value)
        if count:
            # immotop combines them - split equally or put all in showers
            data["shower_rooms"] = count
    elif "douche" in label and "salle" in label:
        data["shower_rooms"] = _parse_int(value)
    elif "salle de bain" in label:
        data["bathrooms"] = _parse_int(value)
    elif "wc" in label or "toilette" in label:
        data["separate_toilets"] = _parse_int(value)
    
    # Outdoor
    elif "terrasse" in label:
        if re.search(r"\d", value):
            data["terrace_m2"] = _parse_float(value)
        else:
            data["balcony"] = _parse_bool(value)  # Generic
    elif "balcon" in label:
        if re.search(r"\d", value):
            data["balcony_m2"] = _parse_float(value)
        else:
            data["balcony"] = _parse_bool(value)
    elif "jardin" in label or "garden" in label:
        data["garden"] = _parse_bool(value)
    
    # Facilities
    elif "ascenseur" in label or "elevator" in label or "lift" in label:
        data["elevator"] = _parse_bool(value)
    elif "cave" in label or "basement" in label or "cellar" in label:
        data["basement"] = _parse_bool(value)
    elif "parking" in label or "garage" in label or "box" in label or "stationnement" in label:
        data["parking_spaces"] = _parse_int(value)
    elif "buanderie" in label or "laundry" in label:
        data["laundry_room"] = _parse_bool(value)
    
    # Kitchen
    elif "cuisine" in label:
        if "équipée" in value.lower() or "equipped" in value.lower():
            data["fitted_kitchen"] = 1
        elif "ouverte" in value.lower() or "open" in value.lower():
            data["open_kitchen"] = 1
    
    # Other
    elif "meublé" in label or "furnished" in label:
        data["furnished"] = _parse_bool(value)
    elif "année" in label or "construction" in label or "built" in label:
        data["year_of_construction"] = _parse_int(value)
    elif "disponibilité" in label or "availability" in label or "libre" in label:
        data["availability"] = value


# ─────────────────────────────────────────────────────────────
# Main run function
# ─────────────────────────────────────────────────────────────

def run(
    index_configs:       List[Dict],
    max_pages_per_index: int   = 2,
    save_images:         bool  = False,  # TODO: implement
    delay_seconds:       float = 0,
    headless:            bool  = True,
) -> Dict[str, int]:
    if not SELENIUM_OK:
        log.error("Selenium not installed.")
        return {}

    # Ensure DB exists (create schema if needed)
    db_init()

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
                    log.info(f"[{i}] NEW  {ref}  {lurl}")
                    d = scrape_detail(driver, lurl, t_type, save_images=save_images)
                    if d:
                        db_upsert(d, is_update=False)
                        counters["inserted"] += 1
                    time.sleep(delay_seconds)
                else:
                    # Check title change via lightweight fetch
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
                        log.info(f"[{i}] UPDATED  {ref}")
                        d = scrape_detail(driver, lurl, t_type, save_images=save_images)
                        if d:
                            db_upsert(d, is_update=True)
                            counters["updated"] += 1
                        time.sleep(delay_seconds)
                    else:
                        log.info(f"[{i}] STOP — hit known listing {ref}")
                        counters["stopped_early"] += 1
                        break

    finally:
        driver.quit()

    log.info(
        f"\nRun complete.\n"
        f"  inserted: {counters['inserted']}\n"
        f"  updated:  {counters['updated']}\n"
        f"  stopped early: {counters['stopped_early']}"
    )
    return counters

# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # immotop index pages sorted newest-first
    INDEX_URLS = [
        {"url": "https://www.immotop.lu/vente-maisons-appartements/luxembourg-pays/?criterio=automatico", "type": "buy"},
        {"url": "https://www.immotop.lu/location-maisons-appartements/luxembourg-pays/?criterio=automatico", "type": "rent"},
    ]

    run(
        index_configs        = INDEX_URLS,
        max_pages_per_index  = 2,
        save_images          = False,  # TODO
        delay_seconds        = 0,
        headless             = True,
    )