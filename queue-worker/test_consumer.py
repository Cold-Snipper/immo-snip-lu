"""
test_consumer.py

A simple script to verify the Redis Stream is receiving events.
Run this in a second terminal while queue_worker.py is running,
then trigger a MongoDB insert/update to see the event flow end-to-end.

Usage:
    python test_consumer.py
"""

import asyncio
import json
import os

import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST         = os.environ["REDIS_HOST"]
REDIS_PORT         = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD     = os.environ.get("REDIS_PASSWORD")
REDIS_USERNAME     = os.environ.get("REDIS_USERNAME", "default")
STREAM_NAME        = os.environ.get("STREAM_NAME", "listing:events")
DEAD_LETTER_STREAM = os.environ.get("DEAD_LETTER_STREAM", "listing:events:dead")


async def main():
    redis = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        username=REDIS_USERNAME,
        decode_responses=True,
    )

    print(f"\nListening on stream: {STREAM_NAME}")
    print("Waiting for events... (Ctrl+C to stop)\n")

    last_id = "$"  # only new messages from this point forward

    while True:
        results = await redis.xread({STREAM_NAME: last_id}, block=5000, count=10)

        if not results:
            print("  (no events yet...)")
            continue

        for stream_name, messages in results:
            for msg_id, fields in messages:
                last_id = msg_id
                print(f"─── Event [{msg_id}] ───────────────────────────────")
                print(f"  operation    : {fields.get('operation')}")
                print(f"  listing_id   : {fields.get('listing_id')}")
                print(f"  source       : {fields.get('source')}")
                print(f"  location     : {fields.get('location')}")
                print(f"  sale_price   : {fields.get('sale_price')}")
                print(f"  rent_price   : {fields.get('rent_price')}")
                print(f"  bedrooms     : {fields.get('bedrooms')}")
                print(f"  surface_m2   : {fields.get('surface_m2')}")

                if fields.get("updated_fields"):
                    changed = json.loads(fields["updated_fields"])
                    print(f"  changed      : {changed}")

                print(f"  published_at : {fields.get('published_at')}")
                print()

        # Also check dead-letter stream
        dead = await redis.xlen(DEAD_LETTER_STREAM)
        if dead > 0:
            print(f"  ⚠️  Dead-letter stream has {dead} unprocessed message(s).")


if __name__ == "__main__":
    asyncio.run(main())
