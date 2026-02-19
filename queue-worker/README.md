# Queue Worker

Watches the MongoDB `listings` collection via Change Streams and publishes events
to a Redis Stream. All downstream services (B2C notifier, B2B AI scorer) consume
from the stream independently.

```
MongoDB Atlas (Change Stream)
        │
        ▼
  queue_worker.py       ← this service
        │
        ▼
  Redis Stream: listing:events
        │
   ┌────┴────┐
   ▼         ▼
B2C       B2B scorer
notifier  + outreach
```

---

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# fill in your MONGO_URI, MONGO_DB, REDIS_URL, REDIS_PASSWORD
```

---

## Run

**Directly:**
```bash
python queue_worker.py
```

**With PM2:**
```bash
pm2 start queue_worker.py --interpreter python3 --name queue-worker
pm2 save
pm2 startup   # auto-restart on reboot
```


## How it works

### Change Stream
- Opens a `watch()` on the `listings` collection filtered to `insert` and `update` operations
- Uses `full_document: updateLookup` so updates include the complete document, not just the diff
- Ignores updates where only internal scraper fields changed (`last_updated`, `image_urls`, etc.)

### Resume token
- After each successfully published event, the Change Stream resume token is saved to a
  `stream_checkpoints` collection in MongoDB
- On restart, the worker resumes from the last saved token — no missed events, no duplicates

### Dead-letter stream
- If processing a change event fails (e.g. malformed document), the raw event is written
  to `listing:events:dead` in Redis for manual inspection
- The worker continues after a failed event — one bad document won't stop the stream

### Redis Stream
Each event published to `listing:events` contains:

| Field | Description |
|---|---|
| `operation` | `insert` or `update` |
| `listing_id` | MongoDB `_id` as string |
| `source` | e.g. `athome`, `immotop` |
| `transaction_type` | `sale` or `rent` |
| `location` | location string |
| `sale_price` / `rent_price` | price fields |
| `bedrooms`, `bathrooms`, `surface_m2` | key property attributes |
| `floor`, `elevator`, `furnished`, `balcony`, `parking_spaces` | amenities |
| `energy_class` | energy rating |
| `description` | full description text |
| `listing_url` | original listing URL |
| `image_urls` | first 3 image URLs |
| `agency_name`, `agent_name`, `phone_number` | contact info |
| `updated_fields` | (update only) list of fields that changed |
| `published_at` | ISO timestamp of when event was published |

---

## Significant vs ignored field changes

Not all MongoDB updates trigger a downstream event. Updates touching only these
fields are silently skipped:

- `last_updated`
- `images_dir`
- `image_urls`
- `title_history`

Any change to a meaningful field (`sale_price`, `location`, `bedrooms`, `description`,
`availability`, etc.) will trigger a publish.
