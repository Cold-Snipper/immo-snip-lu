"""
MongoDB Atlas Database Adapter
===============================
Drop-in replacement for SQLite database functions.

Setup:
1. Create free MongoDB Atlas account: https://www.mongodb.com/cloud/atlas/register
2. Create cluster (free M0 tier)
3. Get connection string: mongodb+srv://username:password@cluster.mongodb.net/
4. Set environment variable: export MONGO_URI="your_connection_string"

Usage:
    import mongo_db as db
    
    db.db_init()
    db.db_upsert(listing_data)
    listing = db.db_get("8983200")
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List

try:
    from pymongo import MongoClient, ASCENDING
    from pymongo.errors import DuplicateKeyError, PyMongoError
    PYMONGO_OK = True
except ImportError:
    PYMONGO_OK = False
    print("⚠️  pymongo not installed. Run: pip install pymongo")

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB_NAME", "realestate")
COLLECTION_NAME = "listings"

_client = None
_db = None
_collection = None

# ─────────────────────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────────────────────

def _get_collection():
    """Get or create MongoDB collection (lazy initialization)."""
    global _client, _db, _collection
    
    if _collection is not None:
        return _collection
    
    if not PYMONGO_OK:
        raise RuntimeError("pymongo not installed. Run: pip install pymongo")
    
    try:
        _client = MongoClient(MONGO_URI)
        _db = _client[DB_NAME]
        _collection = _db[COLLECTION_NAME]
        
        # Test connection
        _client.server_info()
        log.info(f"✓ Connected to MongoDB Atlas: {DB_NAME}.{COLLECTION_NAME}")
        
        return _collection
    except Exception as e:
        log.error(f"MongoDB connection failed: {e}")
        raise

def db_init() -> None:
    """
    Initialize database - create indexes.
    MongoDB creates collections automatically, we just set up indexes.
    """
    collection = _get_collection()
    
    # Create indexes for fast lookups
    collection.create_index("listing_ref", unique=True)
    collection.create_index("source")
    collection.create_index("transaction_type")
    collection.create_index([("first_seen", ASCENDING)])
    collection.create_index([("last_updated", ASCENDING)])
    
    log.info("✓ MongoDB indexes created")

# ─────────────────────────────────────────────────────────────
# CRUD Operations
# ─────────────────────────────────────────────────────────────

def db_get(ref: str) -> Optional[Dict]:
    """Get a listing by reference ID."""
    collection = _get_collection()
    doc = collection.find_one({"listing_ref": ref})
    
    if doc:
        # Remove MongoDB's _id field (not needed)
        doc.pop("_id", None)
    
    return doc

def db_upsert(data: Dict, is_update: bool = False) -> str:
    """
    Insert or update a listing.
    
    Returns:
        "inserted" | "updated" | "skipped"
    """
    collection = _get_collection()
    ref = data.get("listing_ref")
    
    if not ref:
        log.warning("Skipping listing without listing_ref")
        return "skipped"
    
    now = datetime.now(timezone.utc).isoformat()
    
    if is_update:
        # Update existing listing
        existing = db_get(ref)
        if not existing:
            # Doesn't exist, insert instead
            return db_upsert(data, is_update=False)
        
        data["last_updated"] = now
        data.setdefault("first_seen", existing.get("first_seen", now))
        
        # Handle title history
        old_title = existing.get("title", "")
        new_title = data.get("title", "")
        
        if old_title and old_title != new_title:
            # Title changed - append to history
            history = existing.get("title_history", [])
            if isinstance(history, str):
                history = json.loads(history) if history else []
            history.append({"title": old_title, "changed_at": now})
            data["title_history"] = history
        else:
            data["title_history"] = existing.get("title_history", [])
        
        # Convert JSON strings to lists (MongoDB native)
        _normalize_json_fields(data)
        
        # Update
        collection.replace_one(
            {"listing_ref": ref},
            data,
            upsert=True
        )
        return "updated"
    
    else:
        # Insert new listing
        data["first_seen"] = now
        data["last_updated"] = now
        data["title_history"] = []
        
        # Convert JSON strings to lists (MongoDB native)
        _normalize_json_fields(data)
        
        try:
            collection.insert_one(data)
            return "inserted"
        except DuplicateKeyError:
            # Already exists, skip
            return "skipped"

def _normalize_json_fields(data: Dict) -> None:
    """
    Convert JSON string fields to native Python lists/dicts for MongoDB.
    Modifies data in-place.
    """
    # Fields that are stored as JSON strings in SQLite but should be lists in MongoDB
    json_fields = ["image_urls", "title_history"]
    
    for field in json_fields:
        if field in data and isinstance(data[field], str):
            try:
                data[field] = json.loads(data[field])
            except (json.JSONDecodeError, TypeError):
                data[field] = []

def db_stats() -> Dict:
    """Get database statistics."""
    collection = _get_collection()
    
    total = collection.count_documents({})
    buy = collection.count_documents({"transaction_type": "buy"})
    rent = collection.count_documents({"transaction_type": "rent"})
    with_phone = collection.count_documents({"phone_number": {"$ne": None}})
    
    return {
        "total": total,
        "buy": buy,
        "rent": rent,
        "with_phone": with_phone,
    }

def db_get_all_refs() -> List[str]:
    """Get all listing_refs in the database."""
    collection = _get_collection()
    return [doc["listing_ref"] for doc in collection.find({}, {"listing_ref": 1})]

def db_close() -> None:
    """Close MongoDB connection (optional, connections auto-close)."""
    global _client
    if _client:
        _client.close()
        log.info("MongoDB connection closed")

# ─────────────────────────────────────────────────────────────
# Query Helpers
# ─────────────────────────────────────────────────────────────

def find_new_since(timestamp: str) -> List[Dict]:
    """
    Find all listings created since a given timestamp.
    
    Args:
        timestamp: ISO format datetime string
    
    Returns:
        List of listing dicts
    """
    collection = _get_collection()
    cursor = collection.find({"first_seen": {"$gte": timestamp}})
    
    results = []
    for doc in cursor:
        doc.pop("_id", None)
        results.append(doc)
    
    return results

def find_updated_since(timestamp: str) -> List[Dict]:
    """Find all listings updated since a given timestamp."""
    collection = _get_collection()
    cursor = collection.find({
        "last_updated": {"$gte": timestamp},
        "title_history": {"$ne": []}  # Only those with title changes
    })
    
    results = []
    for doc in cursor:
        doc.pop("_id", None)
        results.append(doc)
    
    return results

def find_by_filter(filters: Dict) -> List[Dict]:
    """
    Find listings matching filters.
    
    Example:
        find_by_filter({
            "transaction_type": "rent",
            "rent_price": {"$lte": 2500},
            "bedrooms": {"$gte": 2},
            "source": "athome"
        })
    """
    collection = _get_collection()
    cursor = collection.find(filters)
    
    results = []
    for doc in cursor:
        doc.pop("_id", None)
        results.append(doc)
    
    return results

# ─────────────────────────────────────────────────────────────
# Example Usage
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Test connection
    logging.basicConfig(level=logging.INFO)
    
    print("Testing MongoDB connection...")
    db_init()
    
    # Test insert
    test_listing = {
        "listing_ref": "TEST123",
        "source": "test",
        "transaction_type": "buy",
        "title": "Test Apartment",
        "sale_price": 500000.0,
        "bedrooms": 2,
        "image_urls": ["http://example.com/img1.jpg"],
    }
    
    result = db_upsert(test_listing)
    print(f"Insert result: {result}")
    
    # Test get
    retrieved = db_get("TEST123")
    print(f"Retrieved: {retrieved}")
    
    # Test stats
    stats = db_stats()
    print(f"Stats: {stats}")
    
    # Cleanup
    _get_collection().delete_one({"listing_ref": "TEST123"})
    print("Test document deleted")
