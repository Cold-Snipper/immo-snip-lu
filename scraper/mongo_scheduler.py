"""
MongoDB Scheduler - Working Version
====================================
This version monkey-patches the database functions BEFORE importing scrapers.
"""

import logging
import sys
import time
import os
from datetime import datetime
from pathlib import Path

# Setup path
sys.path.insert(0, str(Path(__file__).parent))

# ─────────────────────────────────────────────────────────────
# STEP 1: Import and setup MongoDB FIRST
# ─────────────────────────────────────────────────────────────

# Check MONGO_URI
if not os.getenv("MONGO_URI"):
    print("\n⚠️  ERROR: MONGO_URI environment variable not set!")
    print("Set it like this:")
    print('  export MONGO_URI="mongodb+srv://user:pass@cluster.mongodb.net/realestate"')
    sys.exit(1)

import mongo_db

# ─────────────────────────────────────────────────────────────
# STEP 2: Monkey-patch sys.modules to replace db functions
# ─────────────────────────────────────────────────────────────

# Create a fake module that redirects all DB calls to MongoDB
class MongoDBAdapter:
    db_init = staticmethod(mongo_db.db_init)
    db_get = staticmethod(mongo_db.db_get)
    db_upsert = staticmethod(mongo_db.db_upsert)
    db_stats = staticmethod(mongo_db.db_stats)
    db_connect = staticmethod(lambda: None)  # Not needed for MongoDB
    DB_PATH = Path("UNUSED_MONGODB")  # Dummy path

# Inject this BEFORE the scrapers import
sys.modules['__main__'].db_init = mongo_db.db_init
sys.modules['__main__'].db_get = mongo_db.db_get
sys.modules['__main__'].db_upsert = mongo_db.db_upsert
sys.modules['__main__'].db_stats = mongo_db.db_stats

# ─────────────────────────────────────────────────────────────
# STEP 3: NOW import the scrapers (they'll use MongoDB functions)
# ─────────────────────────────────────────────────────────────

import athome_scraper
import immotop_scraper

# Replace their db functions with MongoDB versions
athome_scraper.db_init = mongo_db.db_init
athome_scraper.db_get = mongo_db.db_get
athome_scraper.db_upsert = mongo_db.db_upsert
athome_scraper.db_stats = mongo_db.db_stats

immotop_scraper.db_init = mongo_db.db_init
immotop_scraper.db_get = mongo_db.db_get
immotop_scraper.db_upsert = mongo_db.db_upsert
immotop_scraper.db_stats = mongo_db.db_stats

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

MAX_PAGES_PER_INDEX = 1
SAVE_IMAGES = True
HEADLESS = True
DELAY_SECONDS = 0
SCAN_EVERY_MINUTES = 5

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("mongo_scheduler.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("scheduler")

# ─────────────────────────────────────────────────────────────
# Main scan function
# ─────────────────────────────────────────────────────────────

def scan() -> None:
    """Run both scrapers sequentially."""
    log.info(f"\n{'='*60}")
    log.info(f"Scan started  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("="*60)

    # Ensure DB/indexes exist
    mongo_db.db_init()
    log.info("Using MongoDB Atlas for storage")

    # ── Run athome.lu scraper ────────────────────────────────
    log.info("\n### ATHOME.LU ###")
    athome_configs = [
        {"url": "https://www.athome.lu/vente?sort=date_desc", "type": "buy"},
        {"url": "https://www.athome.lu/location?sort=date_desc", "type": "rent"},
    ]
    athome_counters = athome_scraper.run(
        index_configs=athome_configs,
        max_pages_per_index=MAX_PAGES_PER_INDEX,
        save_images=SAVE_IMAGES,
        delay_seconds=DELAY_SECONDS,
        headless=HEADLESS,
    )

    # ── Run immotop.lu scraper ───────────────────────────────
    log.info("\n### IMMOTOP.LU ###")
    immotop_configs = [
        {"url": "https://www.immotop.lu/vente-maisons-appartements/luxembourg-pays/?criterio=automatico", "type": "buy"},
        {"url": "https://www.immotop.lu/location-maisons-appartements/luxembourg-pays/?criterio=automatico", "type": "rent"},
    ]
    immotop_counters = immotop_scraper.run(
        index_configs=immotop_configs,
        max_pages_per_index=MAX_PAGES_PER_INDEX,
        save_images=SAVE_IMAGES,
        delay_seconds=DELAY_SECONDS,
        headless=HEADLESS,
    )

    # ── Print summary ────────────────────────────────────────
    stats = mongo_db.db_stats()
    log.info(
        f"\n{'='*60}\n"
        f"Scan complete.\n"
        f"  athome   → new:{athome_counters.get('inserted',0)}  "
        f"updated:{athome_counters.get('updated',0)}  "
        f"stopped_early:{athome_counters.get('stopped_early',0)}\n"
        f"  immotop  → new:{immotop_counters.get('inserted',0)}  "
        f"updated:{immotop_counters.get('updated',0)}  "
        f"stopped_early:{immotop_counters.get('stopped_early',0)}\n"
        f"\n"
        f"  MongoDB total: {stats['total']} listings "
        f"({stats['buy']} sale / {stats['rent']} rent / "
        f"{stats['with_phone']} with phone)\n"
        f"{'='*60}"
    )

# ─────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("="*60)
    log.info("Luxembourg Real Estate Scraper — MongoDB Atlas")
    log.info(f"Scanning every {SCAN_EVERY_MINUTES} minutes")
    log.info("="*60)

    # Verify MongoDB connection
    try:
        mongo_db.db_init()
        stats = mongo_db.db_stats()
        log.info(f"✓ Connected to MongoDB Atlas")
        log.info(f"✓ Current database: {stats['total']} listings")
    except Exception as e:
        log.error(f"MongoDB connection failed: {e}")
        sys.exit(1)

    # Run immediately on startup
    try:
        scan()
    except Exception as e:
        log.error(f"First scan failed: {e}", exc_info=True)

    # Then run every N minutes forever
    while True:
        wait_seconds = SCAN_EVERY_MINUTES * 60
        log.info(f"\nWaiting {SCAN_EVERY_MINUTES} minutes until next scan...")
        time.sleep(wait_seconds)

        try:
            scan()
        except KeyboardInterrupt:
            log.info("\nScheduler stopped by user (Ctrl+C)")
            mongo_db.db_close()
            break
        except Exception as e:
            log.error(f"Scan failed: {e}", exc_info=True)
            log.info("Continuing despite error...")