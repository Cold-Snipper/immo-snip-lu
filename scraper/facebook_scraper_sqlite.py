"""
Facebook Scraper - SQLite Version
===================================
Standalone scraper for Facebook Marketplace + Groups
Stores data in local SQLite database (facebook_listings.db)

No dependencies on athome/immotop system - completely independent.

Usage:
    python facebook_scraper_sqlite.py
"""

import re
import sys
import time
import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

try:
    from playwright.sync_api import sync_playwright, Page
    PLAYWRIGHT_OK = True
except ImportError:
    PLAYWRIGHT_OK = False

try:
    from facebook_parser import parse_facebook_post
    PARSER_OK = True
except ImportError:
    PARSER_OK = False

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("facebook_scraper")

DB_PATH = Path("facebook_listings.db")

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

MARKETPLACE_CARD_SELECTOR = '[data-testid="marketplace_feed_card"]'
GROUP_POST_SELECTOR = 'div[role="article"]'

# ─────────────────────────────────────────────────────────────
# Database Setup
# ─────────────────────────────────────────────────────────────

def db_init():
    """Create SQLite database and tables"""
    conn = sqlite3.connect(DB_PATH)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            listing_ref TEXT PRIMARY KEY,
            listing_url TEXT,
            source TEXT,
            transaction_type TEXT,
            title TEXT,
            description TEXT,
            sale_price REAL,
            rent_price REAL,
            bedrooms INTEGER,
            surface_m2 REAL,
            location TEXT,
            phone_number TEXT,
            phone_source TEXT,
            image_urls TEXT,
            first_seen TEXT,
            last_updated TEXT,
            title_history TEXT
        )
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_source ON listings(source)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_type ON listings(transaction_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_location ON listings(location)")
    
    conn.commit()
    conn.close()
    
    log.info(f"✓ Database ready: {DB_PATH}")

def db_get(listing_ref: str) -> Optional[Dict]:
    """Get listing by ref"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    cursor = conn.execute(
        "SELECT * FROM listings WHERE listing_ref = ?",
        (listing_ref,)
    )
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return dict(row)
    return None

def db_upsert(data: Dict, is_update: bool = False):
    """Insert or update listing"""
    conn = sqlite3.connect(DB_PATH)
    
    if is_update:
        # Update existing
        old = db_get(data["listing_ref"])
        
        # Track title history
        title_history = json.loads(old.get("title_history", "[]"))
        if old.get("title") != data.get("title"):
            title_history.append({
                "title": old.get("title"),
                "changed_at": datetime.now(timezone.utc).isoformat()
            })
            data["title_history"] = json.dumps(title_history)
        
        conn.execute("""
            UPDATE listings SET
                title = ?,
                description = ?,
                sale_price = ?,
                rent_price = ?,
                bedrooms = ?,
                surface_m2 = ?,
                location = ?,
                phone_number = ?,
                phone_source = ?,
                image_urls = ?,
                last_updated = ?,
                title_history = ?
            WHERE listing_ref = ?
        """, (
            data.get("title"),
            data.get("description"),
            data.get("sale_price"),
            data.get("rent_price"),
            data.get("bedrooms"),
            data.get("surface_m2"),
            data.get("location"),
            data.get("phone_number"),
            data.get("phone_source"),
            data.get("image_urls"),
            data.get("last_updated"),
            data.get("title_history", "[]"),
            data.get("listing_ref")
        ))
    else:
        # Insert new
        conn.execute("""
            INSERT INTO listings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            data.get("listing_ref"),
            data.get("listing_url"),
            data.get("source"),
            data.get("transaction_type"),
            data.get("title"),
            data.get("description"),
            data.get("sale_price"),
            data.get("rent_price"),
            data.get("bedrooms"),
            data.get("surface_m2"),
            data.get("location"),
            data.get("phone_number"),
            data.get("phone_source"),
            data.get("image_urls"),
            data.get("first_seen"),
            data.get("last_updated"),
            "[]"  # title_history
        ))
    
    conn.commit()
    conn.close()

def db_stats() -> Dict:
    """Get database stats"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN transaction_type='buy' THEN 1 ELSE 0 END) as buy,
            SUM(CASE WHEN transaction_type='rent' THEN 1 ELSE 0 END) as rent,
            SUM(CASE WHEN phone_number IS NOT NULL THEN 1 ELSE 0 END) as with_phone
        FROM listings
    """)
    row = cursor.fetchone()
    conn.close()
    
    return {
        "total": row[0],
        "buy": row[1],
        "rent": row[2],
        "with_phone": row[3]
    }

# ─────────────────────────────────────────────────────────────
# Ref Extraction
# ─────────────────────────────────────────────────────────────

def extract_ref_from_url(url: str) -> Optional[str]:
    """Extract listing ref from Facebook URL"""
    # Marketplace item ID
    match = re.search(r'/marketplace/item/(\d+)', url)
    if match:
        return f"fb_mp_{match.group(1)}"
    
    # Group post ID
    match = re.search(r'/posts/(\d+)', url)
    if match:
        return f"fb_grp_{match.group(1)}"
    
    # Permalink
    match = re.search(r'/permalink/(\d+)', url)
    if match:
        return f"fb_perm_{match.group(1)}"
    
    return None

# ─────────────────────────────────────────────────────────────
# Browser
# ─────────────────────────────────────────────────────────────

def make_browser(headless: bool = True, storage_state: Optional[str] = None):
    """Create Playwright browser"""
    if not PLAYWRIGHT_OK:
        raise RuntimeError("Playwright not installed. Run: pip install playwright && playwright install chromium")
    
    p = sync_playwright().start()
    browser = p.chromium.launch(
        headless=headless,
        args=['--no-sandbox', '--disable-dev-shm-usage']
    )
    
    context_opts = {
        'viewport': {'width': 1280, 'height': 800},
        'user_agent': USER_AGENT,
    }
    
    if storage_state and Path(storage_state).exists():
        context_opts['storage_state'] = storage_state
        log.info(f"Using storage state: {storage_state}")
    
    context = browser.new_context(**context_opts)
    page = context.new_page()
    page.set_default_timeout(60000)
    
    return p, browser, context, page

def close_browser(p, browser, context):
    """Clean up"""
    try:
        context.close()
        browser.close()
        p.stop()
    except:
        pass

# ─────────────────────────────────────────────────────────────
# Scraping
# ─────────────────────────────────────────────────────────────

def get_feed_listings(page: Page, feed_url: str, selector: str, max_scrolls: int = 5) -> List[Tuple[str, str]]:
    """Scrape feed and extract listing URLs"""
    log.info(f"Loading feed: {feed_url}")
    
    try:
        page.goto(feed_url, wait_until='domcontentloaded')
        time.sleep(3)
    except Exception as e:
        log.error(f"Failed to load {feed_url}: {e}")
        return []
    
    # Scroll to load more
    for i in range(max_scrolls):
        try:
            page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            time.sleep(2)
        except:
            break
    
    # Extract URLs
    listings = []
    try:
        elements = page.query_selector_all(selector)
        log.info(f"Found {len(elements)} potential listings")
        
        for elem in elements:
            try:
                link_elem = elem.query_selector('a[href*="/marketplace/item/"], a[href*="/posts/"], a[href*="/permalink/"]')
                if not link_elem:
                    continue
                
                url = link_elem.get_attribute('href')
                if not url:
                    continue
                
                url = url.split('?')[0]  # Remove query params
                
                ref = extract_ref_from_url(url)
                if not ref:
                    continue
                
                if not url.startswith('http'):
                    url = f"https://www.facebook.com{url}"
                
                listings.append((ref, url))
            except:
                continue
    except Exception as e:
        log.error(f"Failed to extract listings: {e}")
    
    log.info(f"Extracted {len(listings)} listing URLs")
    return listings

def scrape_detail(page: Page, url: str, listing_ref: str, transaction_type: str, source: str) -> Optional[Dict]:
    """Scrape listing detail page"""
    log.info(f"Scraping: {url}")
    
    try:
        page.goto(url, wait_until='domcontentloaded')
        time.sleep(2)
    except Exception as e:
        log.error(f"Failed to load {url}: {e}")
        return None
    
    # Extract text
    post_text = ""
    try:
        post_text = page.inner_text('body')
    except:
        try:
            post_text = page.content()
        except:
            pass
    
    # Extract images
    image_urls = []
    try:
        img_elems = page.query_selector_all('img[src*="scontent"]')
        for img in img_elems[:10]:
            src = img.get_attribute('src')
            if src and 'http' in src:
                image_urls.append(src)
    except:
        pass
    
    # Parse with regex parser
    if PARSER_OK:
        data = parse_facebook_post(post_text, url, image_urls)
    else:
        data = {
            "listing_url": url,
            "description": post_text[:1000],
            "image_urls": json.dumps(image_urls),
            "transaction_type": transaction_type,
        }
    
    # Add metadata
    data["listing_ref"] = listing_ref
    data["source"] = source
    data["first_seen"] = datetime.now(timezone.utc).isoformat()
    data["last_updated"] = datetime.now(timezone.utc).isoformat()
    
    if "transaction_type" not in data:
        data["transaction_type"] = transaction_type
    
    log.info(
        f"✓ {data.get('title', 'No title')} - "
        f"€{data.get('sale_price') or data.get('rent_price') or '?'} - "
        f"{data.get('location', '?')}"
    )
    
    return data

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def run(
    feed_configs: List[Dict],
    max_scrolls: int = 5,
    headless: bool = True,
    storage_state: Optional[str] = None
) -> Dict:
    """
    Run Facebook scraper
    
    Args:
        feed_configs: List of {url, type, source}
        max_scrolls: How many times to scroll
        headless: Run headless
        storage_state: Path to storage_state.json
    
    Returns:
        Counters dict
    """
    db_init()
    
    counters = {"inserted": 0, "updated": 0, "stopped_early": 0}
    
    p, browser, context, page = make_browser(headless=headless, storage_state=storage_state)
    
    try:
        for config in feed_configs:
            feed_url = config["url"]
            t_type = config.get("type", "rent")
            source = config.get("source", "facebook_marketplace")
            
            selector = MARKETPLACE_CARD_SELECTOR if "marketplace" in source else GROUP_POST_SELECTOR
            
            log.info(f"\n{'='*60}")
            log.info(f"FEED: {feed_url} [{t_type}]")
            log.info("="*60)
            
            listings = get_feed_listings(page, feed_url, selector, max_scrolls)
            
            for i, (ref, url) in enumerate(listings, 1):
                existing = db_get(ref)
                
                if existing is None:
                    log.info(f"[{i}] NEW {ref}")
                    data = scrape_detail(page, url, ref, t_type, source)
                    if data:
                        db_upsert(data, is_update=False)
                        counters["inserted"] += 1
                    time.sleep(2)
                else:
                    log.info(f"[{i}] KNOWN {ref}")
                    data = scrape_detail(page, url, ref, t_type, source)
                    
                    if data:
                        old_title = existing.get("title", "")
                        new_title = data.get("title", "")
                        
                        if new_title and new_title != old_title:
                            log.info(f"  Title changed → updating")
                            db_upsert(data, is_update=True)
                            counters["updated"] += 1
                        else:
                            log.info(f"  Unchanged → STOPPING")
                            counters["stopped_early"] += 1
                            break
                    
                    time.sleep(2)
    finally:
        close_browser(p, browser, context)
    
    stats = db_stats()
    log.info(
        f"\n{'='*60}\n"
        f"Complete: new={counters['inserted']} updated={counters['updated']}\n"
        f"DB total: {stats['total']} listings ({stats['buy']} buy / {stats['rent']} rent)\n"
        f"{'='*60}"
    )
    
    return counters

# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Facebook Marketplace & Groups Scraper")
    parser.add_argument("--storage-state", help="Path to storage_state.json (required for Facebook login)")
    parser.add_argument("--headless", action="store_true", help="Run headless")
    parser.add_argument("--scrolls", type=int, default=5, help="Number of scrolls per feed")
    args = parser.parse_args()
    
    # Feed configurations
    feed_configs = [
        {
            "url": "https://www.facebook.com/marketplace/luxembourg/propertyrentals",
            "type": "rent",
            "source": "facebook_marketplace"
        },
        {
            "url": "https://www.facebook.com/groups/317172835476483",  # Rent in Luxembourg
            "type": "rent",
            "source": "facebook_group"
        }
    ]
    
    if not args.storage_state:
        print("\n⚠️  WARNING: No storage_state.json provided!")
        print("Facebook requires login. Create storage_state.json first:")
        print("  python -c 'from playwright.sync_api import sync_playwright; ...'")
        print("\nSee FACEBOOK_SETUP.md for instructions.\n")
    
    run(
        feed_configs=feed_configs,
        max_scrolls=args.scrolls,
        headless=args.headless,
        storage_state=args.storage_state
    )
