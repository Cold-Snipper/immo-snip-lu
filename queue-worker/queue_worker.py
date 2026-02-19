"""
queue_worker.py

Watches the MongoDB listings collection via Change Streams and publishes
new/updated listing events to a Redis Stream for downstream consumers
(B2C notifier, B2B AI scorer, etc.).

Run:
    python queue_worker.py

PM2:
    pm2 start queue_worker.py --interpreter python3 --name queue-worker
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

import redis.asyncio as aioredis
from bson import ObjectId
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MONGO_URI             = os.environ["MONGO_URI"]
MONGO_DB              = os.environ["MONGO_DB"]
MONGO_COLLECTION      = os.environ.get("MONGO_COLLECTION", "listings")
CHECKPOINT_COLLECTION = os.environ.get("CHECKPOINT_COLLECTION", "stream_checkpoints")

REDIS_HOST            = os.environ["REDIS_HOST"]
REDIS_PORT            = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD        = os.environ.get("REDIS_PASSWORD")
REDIS_USERNAME        = os.environ.get("REDIS_USERNAME", "default")
STREAM_NAME           = os.environ.get("STREAM_NAME", "listing:events")
DEAD_LETTER_STREAM    = os.environ.get("DEAD_LETTER_STREAM", "listing:events:dead")

# Fields that, if changed alone, should NOT trigger a downstream event.
# Avoids noise from scraper touching internal bookkeeping fields.
IGNORED_UPDATE_FIELDS = {
    "last_updated",
    "images_dir",
    "image_urls",
    "title_history",
}

# Fields that matter for scoring / notifications — a change here always triggers.
SIGNIFICANT_FIELDS = {
    "sale_price", "rent_price", "location", "bedrooms", "bathrooms",
    "surface_m2", "floor", "elevator", "furnished", "balcony", "parking_spaces",
    "description", "energy_class", "availability", "transaction_type",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def serialize(obj):
    """JSON-serialise MongoDB documents (handles ObjectId, datetime, etc.)"""
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serialisable")


def is_significant_update(updated_fields: dict) -> bool:
    """
    Return True if any of the changed fields are worth notifying about.
    Pure scraper-metadata updates (last_updated, image_urls, etc.) are skipped.
    """
    changed = set(updated_fields.keys())
    # If only ignored fields changed → skip
    if changed.issubset(IGNORED_UPDATE_FIELDS):
        log.debug("Skipping update — only ignored fields changed: %s", changed)
        return False
    return True


def build_event(operation: str, listing: dict, updated_fields: dict | None = None) -> dict:
    """
    Build the payload that gets published to the Redis Stream.
    Consumers receive this and can act without needing to query MongoDB again.
    """
    return {
        "operation":      operation,                          # insert | update
        "listing_id":     str(listing["_id"]),
        "source":         listing.get("source", "unknown"),
        "transaction_type": listing.get("transaction_type"),
        "location":       listing.get("location"),
        "sale_price":     listing.get("sale_price"),
        "rent_price":     listing.get("rent_price"),
        "bedrooms":       listing.get("bedrooms"),
        "bathrooms":      listing.get("bathrooms"),
        "surface_m2":     listing.get("surface_m2"),
        "floor":          listing.get("floor"),
        "elevator":       listing.get("elevator"),
        "furnished":      listing.get("furnished"),
        "balcony":        listing.get("balcony"),
        "balcony_m2":     listing.get("balcony_m2"),
        "terrace_m2":     listing.get("terrace_m2"),
        "parking_spaces": listing.get("parking_spaces"),
        "energy_class":   listing.get("energy_class"),
        "description":    listing.get("description"),
        "listing_url":    listing.get("listing_url"),
        "image_urls":     listing.get("image_urls", [])[:3],  # first 3 for preview
        "agency_name":    listing.get("agency_name"),
        "agent_name":     listing.get("agent_name"),
        "phone_number":   listing.get("phone_number"),
        "availability":   listing.get("availability"),
        "first_seen":     listing.get("first_seen"),
        "updated_fields": list(updated_fields.keys()) if updated_fields else None,
        "published_at":   datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Resume token (checkpoint) — persisted in MongoDB so restarts are seamless
# ---------------------------------------------------------------------------

async def load_resume_token(db) -> dict | None:
    doc = await db[CHECKPOINT_COLLECTION].find_one({"_id": "change_stream_token"})
    if doc and "token" in doc:
        log.info("Resuming Change Stream from saved token.")
        return doc["token"]
    log.info("No saved token found — starting Change Stream from now.")
    return None


async def save_resume_token(db, token: dict):
    await db[CHECKPOINT_COLLECTION].update_one(
        {"_id": "change_stream_token"},
        {"$set": {"token": token, "saved_at": datetime.now(timezone.utc).isoformat()}},
        upsert=True,
    )


# ---------------------------------------------------------------------------
# Redis publishing
# ---------------------------------------------------------------------------

async def publish_to_stream(redis: aioredis.Redis, event: dict):
    """Publish a single event dict to the Redis Stream."""
    # Redis XADD requires flat string key/value pairs
    payload = {k: json.dumps(v, default=serialize) for k, v in event.items() if v is not None}
    msg_id = await redis.xadd(STREAM_NAME, payload)
    log.info("Published to stream [%s] id=%s listing_id=%s op=%s",
             STREAM_NAME, msg_id, event["listing_id"], event["operation"])


async def publish_to_dead_letter(redis: aioredis.Redis, raw_change: dict, error: str):
    """Send a failed/unparseable event to the dead-letter stream for inspection."""
    payload = {
        "error":      error,
        "raw":        json.dumps(raw_change, default=serialize),
        "failed_at":  datetime.now(timezone.utc).isoformat(),
    }
    await redis.xadd(DEAD_LETTER_STREAM, payload)
    log.warning("Sent to dead-letter stream: %s", error)


# ---------------------------------------------------------------------------
# Change Stream processing
# ---------------------------------------------------------------------------

async def process_change(change: dict, mongo_db, redis: aioredis.Redis):
    """
    Handle a single change event from MongoDB.
    Fetches the full document (for updates), builds the event payload,
    and publishes to Redis Stream.
    """
    op = change["operationType"]

    if op == "insert":
        listing = change["fullDocument"]
        event = build_event("insert", listing)
        await publish_to_stream(redis, event)

    elif op == "update":
        updated_fields = change.get("updateDescription", {}).get("updatedFields", {})

        if not is_significant_update(updated_fields):
            return  # skip — not worth notifying downstream

        # Fetch the full document so consumers have complete data
        doc_id = change["documentKey"]["_id"]
        listing = await mongo_db[MONGO_COLLECTION].find_one({"_id": doc_id})
        if not listing:
            log.warning("Updated document not found: %s — skipping.", doc_id)
            return

        event = build_event("update", listing, updated_fields)
        await publish_to_stream(redis, event)

    else:
        # delete, replace, drop, etc. — ignored for now
        log.debug("Ignoring operation type: %s", op)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run():
    log.info("Connecting to MongoDB Atlas...")
    mongo = AsyncIOMotorClient(MONGO_URI)
    db = mongo[MONGO_DB]

    log.info("Connecting to Redis...")
    redis = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        username=REDIS_USERNAME,
        decode_responses=True,
    )
    await redis.ping()
    log.info("Redis connected.")

    # Ensure the Redis Stream exists (XADD with a dummy then delete, or just let first publish create it)
    # Atlas Change Streams work on the full collection
    pipeline = [
        {"$match": {"operationType": {"$in": ["insert", "update"]}}}
    ]

    resume_token = await load_resume_token(db)
    stream_options = {"full_document": "updateLookup"}
    if resume_token:
        stream_options["resume_after"] = resume_token

    log.info("Opening Change Stream on '%s.%s'...", MONGO_DB, MONGO_COLLECTION)

    async with db[MONGO_COLLECTION].watch(pipeline, **stream_options) as stream:
        log.info("Change Stream open. Waiting for events...")

        async for change in stream:
            try:
                await process_change(change, db, redis)
                # Save checkpoint AFTER successful publish
                await save_resume_token(db, stream.resume_token)

            except Exception as e:
                log.error("Failed to process change: %s", e, exc_info=True)
                try:
                    await publish_to_dead_letter(redis, change, str(e))
                except Exception as redis_err:
                    log.error("Failed to write to dead-letter stream: %s", redis_err)
                # Continue — don't crash the worker on a single bad event


# ---------------------------------------------------------------------------
# Entry point + graceful shutdown
# ---------------------------------------------------------------------------

async def main():
    loop = asyncio.get_running_loop()

    stop = asyncio.Event()

    def _shutdown(sig):
        log.info("Received %s — shutting down gracefully...", sig.name)
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    worker = asyncio.create_task(run())
    await stop.wait()
    worker.cancel()
    try:
        await worker
    except asyncio.CancelledError:
        log.info("Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
