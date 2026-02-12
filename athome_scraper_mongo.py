"""
athome_scraper_mongo.py
=======================
MongoDB-compatible version of athome_scraper.py

This is a thin wrapper that imports athome_scraper.py and replaces
its database functions with MongoDB equivalents.
"""

import sys
from pathlib import Path

# Ensure we can import from the same directory
sys.path.insert(0, str(Path(__file__).parent))

# Import everything from the original scraper
from athome_scraper import *

# Replace database functions with MongoDB versions
import mongo_db
db_init = mongo_db.db_init
db_get = mongo_db.db_get
db_upsert = mongo_db.db_upsert
db_stats = mongo_db.db_stats

# The rest of athome_scraper.py works unchanged!
# All the scraping logic stays the same, just the DB backend changed.