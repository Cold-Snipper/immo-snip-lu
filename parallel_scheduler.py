"""
Parallel Scheduler — athome.lu + immotop.lu
============================================
Each scraper runs on its own thread with its own timer.

Features:
- Scrapers run in parallel (faster overall)
- Each has its own 5-minute interval
- Prevents concurrent runs of the SAME scraper (waits for previous to finish)
- Allows athome and immotop to run simultaneously

Usage:
    export MONGO_URI="mongodb+srv://user:pass@cluster.mongodb.net/realestate"
    python parallel_scheduler.py
"""

import logging
import sys
import time
import os
import threading
from datetime import datetime
from pathlib import Path

# Setup path
sys.path.insert(0, str(Path(__file__).parent))

# Check MONGO_URI
if not os.getenv("MONGO_URI"):
    print("\n⚠️  ERROR: MONGO_URI environment variable not set!")
    print("Set it like this:")
    print('  export MONGO_URI="mongodb+srv://user:pass@cluster.mongodb.net/realestate"')
    sys.exit(1)

import mongo_db

# Monkey-patch the scrapers to use MongoDB
import athome_scraper
import immotop_scraper

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

# Separate intervals for each scraper
ATHOME_INTERVAL_MINUTES = 5
IMMOTOP_INTERVAL_MINUTES = 5

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("parallel_scheduler.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("scheduler")

# ─────────────────────────────────────────────────────────────
# Scraper Runner Class
# ─────────────────────────────────────────────────────────────

class ScraperRunner:
    """
    Manages a single scraper with its own timer and lock.
    Prevents concurrent runs of the same scraper.
    """
    
    def __init__(self, name: str, scraper_module, configs: list, interval_minutes: int):
        self.name = name
        self.scraper = scraper_module
        self.configs = configs
        self.interval = interval_minutes * 60  # Convert to seconds
        self.lock = threading.Lock()  # Prevents concurrent runs
        self.running = False
        self.last_run = None
        self.total_runs = 0
        self.total_inserted = 0
        self.total_updated = 0
        
    def run_once(self):
        """Run the scraper once, thread-safe."""
        # Try to acquire lock (non-blocking)
        acquired = self.lock.acquire(blocking=False)
        
        if not acquired:
            log.warning(f"{self.name}: Previous run still in progress, skipping this cycle")
            return
        
        try:
            self.running = True
            start_time = time.time()
            
            log.info(f"\n{'='*60}")
            log.info(f"{self.name.upper()} — Scan started")
            log.info("="*60)
            
            counters = self.scraper.run(
                index_configs=self.configs,
                max_pages_per_index=MAX_PAGES_PER_INDEX,
                save_images=SAVE_IMAGES,
                delay_seconds=DELAY_SECONDS,
                headless=HEADLESS,
            )
            
            # Update stats
            self.total_runs += 1
            self.total_inserted += counters.get('inserted', 0)
            self.total_updated += counters.get('updated', 0)
            self.last_run = datetime.now()
            
            elapsed = time.time() - start_time
            
            log.info(
                f"\n{self.name.upper()} complete in {elapsed:.1f}s\n"
                f"  This run: new={counters.get('inserted',0)} "
                f"updated={counters.get('updated',0)} "
                f"stopped_early={counters.get('stopped_early',0)}\n"
                f"  Lifetime: runs={self.total_runs} "
                f"total_new={self.total_inserted} "
                f"total_updated={self.total_updated}"
            )
            
        except Exception as e:
            log.error(f"{self.name} failed: {e}", exc_info=True)
        finally:
            self.running = False
            self.lock.release()
    
    def run_forever(self):
        """Run on a timer forever (blocking, run in thread)."""
        log.info(f"{self.name} thread started (interval: {self.interval//60} minutes)")
        
        # Run immediately on startup
        self.run_once()
        
        # Then run every N minutes
        while True:
            time.sleep(self.interval)
            self.run_once()

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    log.info("="*60)
    log.info("Parallel Scheduler — athome.lu + immotop.lu")
    log.info("="*60)
    
    # Initialize MongoDB
    try:
        mongo_db.db_init()
        stats = mongo_db.db_stats()
        log.info(f"✓ Connected to MongoDB Atlas")
        log.info(f"✓ Current database: {stats['total']} listings")
    except Exception as e:
        log.error(f"MongoDB connection failed: {e}")
        sys.exit(1)
    
    # Create scraper runners
    athome_runner = ScraperRunner(
        name="athome",
        scraper_module=athome_scraper,
        configs=[
            {"url": "https://www.athome.lu/vente?sort=date_desc", "type": "buy"},
            {"url": "https://www.athome.lu/location?sort=date_desc", "type": "rent"},
        ],
        interval_minutes=ATHOME_INTERVAL_MINUTES
    )
    
    immotop_runner = ScraperRunner(
        name="immotop",
        scraper_module=immotop_scraper,
        configs=[
            {"url": "https://www.immotop.lu/vente-maisons-appartements/luxembourg-pays/?criterio=automatico", "type": "buy"},
            {"url": "https://www.immotop.lu/location-maisons-appartements/luxembourg-pays/?criterio=automatico", "type": "rent"},
        ],
        interval_minutes=IMMOTOP_INTERVAL_MINUTES
    )
    
    # Create threads
    athome_thread = threading.Thread(
        target=athome_runner.run_forever,
        name="athome-thread",
        daemon=True  # Dies when main thread dies
    )
    
    immotop_thread = threading.Thread(
        target=immotop_runner.run_forever,
        name="immotop-thread",
        daemon=True
    )
    
    # Start threads
    athome_thread.start()
    immotop_thread.start()
    
    log.info("✓ Both scrapers running in parallel")
    log.info(f"  athome  interval: {ATHOME_INTERVAL_MINUTES} min")
    log.info(f"  immotop interval: {IMMOTOP_INTERVAL_MINUTES} min")
    log.info("\nPress Ctrl+C to stop\n")
    
    # Status reporter thread - prints every 5 minutes
    def print_status():
        # Wait a bit before first status (let initial runs complete)
        time.sleep(60)  # Wait 1 minute before first status
        
        while True:
            stats = mongo_db.db_stats()
            
            # Calculate time since last run
            athome_last = f"{int((datetime.now() - athome_runner.last_run).total_seconds() / 60)}m ago" if athome_runner.last_run else "never"
            immotop_last = f"{int((datetime.now() - immotop_runner.last_run).total_seconds() / 60)}m ago" if immotop_runner.last_run else "never"
            
            log.info(
                f"\n{'='*60}\n"
                f"STATUS REPORT\n"
                f"{'='*60}\n"
                f"athome.lu:\n"
                f"  • Total runs:    {athome_runner.total_runs}\n"
                f"  • New listings:  {athome_runner.total_inserted}\n"
                f"  • Updated:       {athome_runner.total_updated}\n"
                f"  • Last run:      {athome_last}\n"
                f"  • Currently:     {'🟢 Running' if athome_runner.running else '⏸  Waiting'}\n"
                f"\n"
                f"immotop.lu:\n"
                f"  • Total runs:    {immotop_runner.total_runs}\n"
                f"  • New listings:  {immotop_runner.total_inserted}\n"
                f"  • Updated:       {immotop_runner.total_updated}\n"
                f"  • Last run:      {immotop_last}\n"
                f"  • Currently:     {'🟢 Running' if immotop_runner.running else '⏸  Waiting'}\n"
                f"\n"
                f"MongoDB Database:\n"
                f"  • Total:         {stats['total']} listings\n"
                f"  • For sale:      {stats['buy']}\n"
                f"  • For rent:      {stats['rent']}\n"
                f"  • With phone:    {stats['with_phone']}\n"
                f"{'='*60}\n"
            )
            
            time.sleep(300)  # Print status every 5 minutes
    
    status_thread = threading.Thread(target=print_status, daemon=True)
    status_thread.start()
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("\n✓ Scheduler stopped by user (Ctrl+C)")
        mongo_db.db_close()
        sys.exit(0)


if __name__ == "__main__":
    main()