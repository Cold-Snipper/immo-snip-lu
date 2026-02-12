# immo-snip-lu


A production-ready web scraper for Luxembourg real estate listings from **athome.lu** and **immotop.lu**. Automatically discovers new listings, detects price changes, extracts 40+ fields including contact information, and stores everything in MongoDB Atlas or SQLite.

---

## 🎯 Features

### Core Functionality
- ✅ **Dual-site scraping**: athome.lu + immotop.lu
- ✅ **Smart early-exit**: Stops when hitting known listings 
- ✅ **Title-change detection**: Re-scrapes if listing updated
- ✅ **Duplicate prevention**: Uses listing_ref as unique identifier
- ✅ **40+ field extraction**: Price, location, rooms, bedrooms, energy class, agency, phone, etc.
- ✅ **Multiple storage options**: MongoDB Atlas (cloud) or SQLite (local)

### Data Extracted
- 📍 Location, title, description
- 💰 Sale price, rent price, monthly charges, deposit
- 🏠 Surface area, rooms, bedrooms, bathrooms, floor
- ⚡ Energy class, heating type, thermal insulation
- 🏢 Agency name, agent name, agency logo
- 📞 Phone numbers (extracted from description + "Show phone" button)
- 🖼️ Property images (URLs + optional local download)
- 📅 First seen, last updated, title change history

---

## 📦 Installation

### 1. Clone or download this repository

```bash
git clone immo-snip-lu
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

**Requirements:**
- Python 3.8+
- selenium
- beautifulsoup4
- lxml
- requests
- pymongo (for MongoDB Atlas)

### 3. Install Chrome/Chromium

The scraper uses Selenium with Chrome

**macOS:**
```bash
brew install --cask google-chrome
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install chromium-browser chromium-chromedriver
```

**Windows:**
Download from https://www.google.com/chrome/

---

## 🚀 Quick Start

### Option A: SQLite (Local Database)

**Run immediately, no setup needed:**

```bash
python simple_scheduler.py
```

Data is stored in `listings.db` (SQLite file in the same directory).

---

### Option B: MongoDB Atlas (Cloud Database - Recommended)

**Get connection string**

In Mongo Atlas click **Connect → Drivers → Python**, copy the connection string:

```
mongodb+srv://username:password@cluster0.abc123.mongodb.net/
```
Or ask Petra to provide you one.

**3. Set environment variable**

```bash
export MONGO_URI="mongodb+srv://username:password@cluster0.abc123.mongodb.net/realestate"
```

**Make it permanent:**
```bash
echo 'export MONGO_URI="mongodb+srv://username:password@cluster0.abc123.mongodb.net/realestate"' >> ~/.zshrc
source ~/.zshrc
```

**4. Run the scheduler**

```bash
python mongo_scheduler.py
```

**Full setup guide:** See [MONGODB_SETUP.md](MONGODB_SETUP.md)

---

## 📂 Project Structure

```
luxembourg-scraper/
├── athome_scraper.py          # athome.lu scraper (SQLite)
├── immotop_scraper.py         # immotop.lu scraper (SQLite)
├── simple_scheduler.py        # Runs both scrapers every 5 min (SQLite)
│
├── mongo_db.py                # MongoDB Atlas adapter
├── mongo_scheduler.py         # Runs both scrapers (MongoDB)
├── athome_scraper_mongo.py    # Wrapper for MongoDB
├── immotop_scraper_mongo.py   # Wrapper for MongoDB
│
├── parallel_scheduler.py      # Runs scrapers in parallel threads
├── requirements.txt           # Python dependencies
│
├── README.md                  # This file
├── MONGODB_SETUP.md           # MongoDB Atlas setup guide
├── DEPLOYMENT.md              # Server deployment guide
│
└── images/                    # Downloaded property photos
    └── <listing_ref>/
        ├── 1.jpg
        ├── 2.jpg
        └── ...
```

---

## 🎛️ Configuration

### Scheduler Settings

Edit the top of `simple_scheduler.py` or `mongo_scheduler.py`:

```python
MAX_PAGES_PER_INDEX = 1       # Pages to scrape per site (1 page ≈ 20-25 listings)
SAVE_IMAGES         = True    # Download property photos
HEADLESS            = True    # Run Chrome invisibly (True for servers)
DELAY_SECONDS       = 0       # Delay between requests (0 = fast)
SCAN_EVERY_MINUTES  = 5       # How often to check for new listings
```

### Scraping URLs

Both scrapers target newest listings first:

**athome.lu:**
- Sale: `https://www.athome.lu/vente?sort=date_desc`
- Rent: `https://www.athome.lu/location?sort=date_desc`

**immotop.lu:**
- Sale: `https://www.immotop.lu/vente-maisons-appartements/luxembourg-pays/?criterio=automatico`
- Rent: `https://www.immotop.lu/location-maisons-appartements/luxembourg-pays/?criterio=automatico`

---

## 📊 Database Schema

All listings stored with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `listing_ref` | TEXT | Unique ID (primary key) |
| `source` | TEXT | "athome" or "immotop" |
| `transaction_type` | TEXT | "buy" or "rent" |
| `listing_url` | TEXT | Original listing URL |
| `title` | TEXT | Listing title |
| `location` | TEXT | City/neighborhood |
| `description` | TEXT | Full description |
| `sale_price` | REAL | Price (for sale) |
| `rent_price` | REAL | Monthly rent |
| `monthly_charges` | REAL | Monthly charges/fees |
| `deposit` | REAL | Security deposit |
| `surface_m2` | REAL | Living area in m² |
| `rooms` | INTEGER | Total rooms |
| `bedrooms` | INTEGER | Number of bedrooms |
| `bathrooms` | INTEGER | Number of bathrooms |
| `shower_rooms` | INTEGER | Number of shower rooms |
| `floor` | INTEGER | Floor number |
| `year_of_construction` | INTEGER | Year built |
| `energy_class` | TEXT | A, B, C, D, E, F, G |
| `thermal_insulation_class` | TEXT | Insulation rating |
| `fitted_kitchen` | INTEGER | Has fitted kitchen (0/1) |
| `furnished` | INTEGER | Is furnished (0/1) |
| `balcony` | INTEGER | Has balcony (0/1) |
| `terrace` | INTEGER | Has terrace (0/1) |
| `terrace_m2` | REAL | Terrace size in m² |
| `balcony_m2` | REAL | Balcony size in m² |
| `garden` | INTEGER | Has garden (0/1) |
| `parking_spaces` | INTEGER | Number of parking spots |
| `elevator` | INTEGER | Has elevator (0/1) |
| `basement` | INTEGER | Has basement/cave (0/1) |
| `laundry_room` | INTEGER | Has laundry room (0/1) |
| `phone_number` | TEXT | Contact phone number |
| `phone_source` | TEXT | "description" or "button" |
| `agency_name` | TEXT | Real estate agency |
| `agency_url` | TEXT | Agency website |
| `agent_name` | TEXT | Agent's name |
| `agency_logo_url` | TEXT | Agency logo URL |
| `image_urls` | TEXT | JSON array of image URLs |
| `images_dir` | TEXT | Local directory with images |
| `first_seen` | TEXT | When first scraped (ISO datetime) |
| `last_updated` | TEXT | Last update (ISO datetime) |
| `title_history` | TEXT | JSON array of title changes |

---

## 🔍 Querying the Database

### SQLite

```bash
sqlite3 listings.db

# View all listings
SELECT * FROM listings LIMIT 10;

# Count by source
SELECT source, COUNT(*) FROM listings GROUP BY source;

# Find rentals under €2000
SELECT title, rent_price, location FROM listings 
WHERE transaction_type = 'rent' AND rent_price < 2000
ORDER BY rent_price ASC;

# Listings with phone numbers
SELECT COUNT(*) FROM listings WHERE phone_number IS NOT NULL;
```

### MongoDB (Python)

```python
import mongo_db as db

# Get stats
stats = db.db_stats()
print(stats)  # {'total': 487, 'buy': 312, 'rent': 175, 'with_phone': 423}

# Find all rentals under €2000 with 2+ bedrooms
rentals = db.find_by_filter({
    "transaction_type": "rent",
    "rent_price": {"$lte": 2000},
    "bedrooms": {"$gte": 2}
})

for listing in rentals:
    print(f"{listing['title']} - €{listing['rent_price']}/month")

# Find new listings in last 24 hours
from datetime import datetime, timedelta
yesterday = (datetime.now() - timedelta(days=1)).isoformat()
new = db.find_new_since(yesterday)
print(f"{len(new)} new listings")

# Find listings with title changes
updated = db.find_by_filter({"title_history": {"$ne": []}})
```

---

## 📈 Performance

### Speed
- **First run**: ~2-3 minutes (scrapes ~40 listings)
- **Subsequent runs**: ~5-15 seconds (early-exit after 1-2 new listings)
- **Runs every**: 5 minutes (configurable)

---

## 🛠️ Troubleshooting

### "Chrome not found"

**Install Chrome/Chromium:**

```bash
# macOS
brew install --cask google-chrome

# Ubuntu/Debian
apt install chromium-browser chromium-chromedriver

# Or download manually
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
dpkg -i google-chrome-stable_current_amd64.deb
```

### "MongoDB connection failed"

**Check your MONGO_URI:**

```bash
echo $MONGO_URI
# Should show: mongodb+srv://user:pass@cluster.mongodb.net/realestate
```

**Verify it's set correctly:**
- Password must NOT contain `<password>` placeholder
- Must include cluster ID (e.g., `cluster0.abc123.mongodb.net`)
- Must include database name (`/realestate`)


### "Database locked" (SQLite)

Another process is accessing the database. Find and kill it:

```bash
ps aux | grep python
kill -9 <PID>
```

Or use MongoDB instead (no locking issues).

---

## 🔐 Security & Privacy

### Best Practices

1. **Never commit credentials to Git:**
   ```bash
   echo "*.db" >> .gitignore
   echo ".env" >> .gitignore
   echo "mongo_scheduler.log" >> .gitignore
   ```

2. **Use environment variables for MongoDB URI:**
   ```bash
   # Don't hardcode in files!
   export MONGO_URI="mongodb+srv://..."
   ```

3. **Restrict MongoDB access:**
   - Use strong passwords (20+ characters)
   - Whitelist specific IPs (not 0.0.0.0/0 in production)
   - Enable 2FA on MongoDB Atlas account

4. **Respect website terms:**
   - 5-minute intervals (not aggressive)
   - Single-threaded scraping
   - No unnecessary requests


## 🎯 Roadmap - Future

Potential future features:
- [ ] Email/Telegram notifications for new listings
- [ ] Price drop alerts
- [ ] Web dashboard/UI for users to define filters 
- [ ] Additional Luxembourg real estate sites
- [ ] Automated listing comparison
- [ ] Export to CSV/Excel

---

## ⚡ Quick Reference

### Start scraping (SQLite)
```bash
python simple_scheduler.py
```

### Start scraping (MongoDB)
```bash
export MONGO_URI="mongodb+srv://user:pass@cluster.mongodb.net/realestate"
python mongo_scheduler.py
```

### View SQLite data
```bash
sqlite3 listings.db "SELECT COUNT(*) FROM listings;"
```

### View MongoDB data
```bash
python -c "import mongo_db as db; print(db.db_stats())"
```

### Stop scraper
```bash
# Press Ctrl+C
# Or kill the process
pkill -f "python.*scheduler"
```

---

**Happy scraping! 🏡📊**
