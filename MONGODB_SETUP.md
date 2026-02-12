# MongoDB Atlas Setup Guide

## Why MongoDB Atlas?

- ✅ **Free tier** — 512MB storage (enough for ~50,000 listings)
- ✅ **Cloud hosted** — Access from anywhere
- ✅ **No server maintenance** — Fully managed
- ✅ **Better for scaling** — Easy to add more storage later
- ✅ **Query flexibility** — Rich queries, aggregations, indexes

---

## Setup Steps (5-10 minutes)

### 1. Create MongoDB Atlas Account

Go to: https://www.mongodb.com/cloud/atlas/register

- Sign up (free)
- Choose "Shared" (free tier)
- Select provider: **AWS** (recommended) or Google Cloud / Azure
- Region: **eu-west-1 (Ireland)** (closest to Luxembourg)

### 2. Create Free Cluster

- Cluster name: `realestate` (or anything you like)
- Click **Create**
- Wait ~3 minutes for cluster to deploy

### 3. Create Database User

**Security → Database Access → Add New Database User**

- Authentication: **Password**
- Username: `scraper` (or your choice)
- Password: Generate a strong one (save it!)
- Privileges: **Read and write to any database**
- Click **Add User**

### 4. Whitelist Your IP

**Security → Network Access → Add IP Address**

**Option A: Your current IP** (safest)
- Click "Add Current IP Address"

**Option B: Allow from anywhere** (easier for servers)
- IP Address: `0.0.0.0/0`
- Description: "Allow all"
- ⚠️ Less secure, but convenient if scraper runs on VPS with changing IPs

### 5. Get Connection String

**Deployment → Database → Connect**

- Choose: **Connect your application**
- Driver: **Python** | Version: **3.12 or later**
- Copy the connection string (looks like this):

```
mongodb+srv://scraper:<password>@realestate.abc123.mongodb.net/?retryWrites=true&w=majority
```

**Replace `<password>` with your actual password!**

Example:
```
mongodb+srv://scraper:MyPass123@realestate.abc123.mongodb.net/?retryWrites=true&w=majority
```

### 6. Set Environment Variable

**On your computer:**
```bash
export MONGO_URI="mongodb+srv://scraper:MyPass123@realestate.abc123.mongodb.net/?retryWrites=true&w=majority"
```

**Make it permanent** (add to `~/.bashrc` or `~/.zshrc`):
```bash
echo 'export MONGO_URI="mongodb+srv://scraper:YourPass@cluster.mongodb.net/"' >> ~/.bashrc
source ~/.bashrc
```

**On Windows:**
```cmd
setx MONGO_URI "mongodb+srv://scraper:YourPass@cluster.mongodb.net/"
```

### 7. Install Python Package

```bash
pip install pymongo
```

Or with requirements.txt:
```bash
pip install -r requirements.txt
```

### 8. Test Connection

```bash
python mongo_db.py
```

Expected output:
```
✓ Connected to MongoDB Atlas: realestate.listings
✓ MongoDB indexes created
Insert result: inserted
Retrieved: {'listing_ref': 'TEST123', 'source': 'test', ...}
Stats: {'total': 1, 'buy': 1, 'rent': 0, 'with_phone': 0}
Test document deleted
```

### 9. Run the Scheduler

```bash
python mongo_scheduler.py
```

---

## File Structure (MongoDB Version)

```
Your project/
├── mongo_db.py                    # MongoDB Atlas adapter
├── mongo_scheduler.py             # Scheduler (MongoDB version)
├── athome_scraper_mongo.py        # Wrapper for athome scraper
├── immotop_scraper_mongo.py       # Wrapper for immotop scraper
├── athome_scraper.py              # Original (still works)
├── immotop_scraper.py             # Original (still works)
└── requirements.txt               # Now includes pymongo
```

---

## Viewing Your Data

### Option 1: MongoDB Atlas UI

1. Go to **Deployment → Database → Browse Collections**
2. Database: `realestate`
3. Collection: `listings`
4. Click any document to view

### Option 2: MongoDB Compass (Desktop App)

Download: https://www.mongodb.com/products/compass

1. Install Compass
2. Paste your connection string
3. Navigate to `realestate.listings`
4. Visual query builder, filters, charts

### Option 3: Python Code

```python
import mongo_db as db

# Get stats
stats = db.db_stats()
print(stats)

# Find all rentals under €2000
rentals = db.find_by_filter({
    "transaction_type": "rent",
    "rent_price": {"$lte": 2000}
})

for listing in rentals:
    print(f"{listing['title']} - €{listing['rent_price']}")

# Find new listings in last 24 hours
from datetime import datetime, timedelta
yesterday = (datetime.now() - timedelta(days=1)).isoformat()
new = db.find_new_since(yesterday)
print(f"{len(new)} new listings")
```

---

## MongoDB vs SQLite

| Feature | SQLite | MongoDB Atlas |
|---------|--------|---------------|
| **Setup** | Zero | 5 min signup |
| **Cost** | Free | Free (512MB) |
| **Location** | Local file | Cloud |
| **Access** | Same machine | Anywhere |
| **Queries** | SQL | MongoDB query language |
| **Scaling** | Limited | Easy |
| **Backups** | Manual file copy | Automatic |

---

## Example Queries (MongoDB)

### Find all athome listings with 2+ bedrooms
```python
import mongo_db as db

listings = db.find_by_filter({
    "source": "athome",
    "bedrooms": {"$gte": 2}
})
```

### Find rentals in Luxembourg under €2500
```python
listings = db.find_by_filter({
    "transaction_type": "rent",
    "location": {"$regex": "Luxembourg", "$options": "i"},
    "rent_price": {"$lte": 2500}
})
```

### Find sales with energy class A or B
```python
listings = db.find_by_filter({
    "transaction_type": "buy",
    "energy_class": {"$in": ["A", "B", "A+", "A++"]}
})
```

### Find listings with title changes
```python
listings = db.find_by_filter({
    "title_history": {"$ne": []}
})
```

---

## Troubleshooting

### "MongoServerSelectionTimeoutError"

**Cause:** Can't connect to MongoDB Atlas

**Solutions:**
1. Check your internet connection
2. Verify connection string is correct (password, cluster name)
3. Check Network Access whitelist (add your IP: `0.0.0.0/0`)
4. Try pinging: `ping realestate.abc123.mongodb.net`

### "Authentication failed"

**Cause:** Wrong username or password

**Solutions:**
1. Go to **Security → Database Access**
2. Click "Edit" on your user
3. Reset password
4. Update connection string with new password

### "pymongo not installed"

**Solution:**
```bash
pip install pymongo
```

### "Database user not authorized"

**Cause:** User doesn't have correct permissions

**Solution:**
1. **Security → Database Access**
2. Click "Edit" on user
3. Set role to: **Atlas admin** or **Read and write to any database**

---

## Migration from SQLite to MongoDB

If you have existing `listings.db` SQLite data:

```python
import sqlite3
import mongo_db as db

# Read from SQLite
conn = sqlite3.connect("listings.db")
conn.row_factory = sqlite3.Row
cursor = conn.execute("SELECT * FROM listings")

# Write to MongoDB
db.db_init()
count = 0
for row in cursor:
    listing = dict(row)
    db.db_upsert(listing, is_update=False)
    count += 1
    if count % 100 == 0:
        print(f"Migrated {count} listings...")

print(f"✓ Migrated {count} total listings")
conn.close()
```

---

## Free Tier Limits

MongoDB Atlas M0 (Free):
- **Storage:** 512MB
- **RAM:** Shared
- **Connections:** Up to 500
- **Listings:** ~50,000 (estimate, depends on data size)

When you hit the limit:
- Upgrade to M2 ($9/month) for 2GB
- Or M5 ($25/month) for 5GB
- Or clean up old listings

---

## Security Best Practices

1. **Use strong password** (20+ chars, random)
2. **Whitelist specific IPs** (not 0.0.0.0/0)
3. **Store connection string in environment variable** (not in code)
4. **Enable 2FA** on MongoDB Atlas account
5. **Rotate passwords** every 3-6 months

---

## Next Steps

Once MongoDB is working:

1. Run `python mongo_scheduler.py` to start scraping
2. View data in MongoDB Compass or Atlas UI
3. Build a simple dashboard with the data
4. Add email/Telegram notifications (same as before, just use `mongo_db` instead)

Your data is now in the cloud and accessible from anywhere! 🎉
