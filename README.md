# **fx-bharat**

[![PyPI Version](https://img.shields.io/pypi/v/fx-bharat.svg)](https://pypi.org/project/fx-bharat/)
[![Downloads](https://pepy.tech/badge/fx-bharat)](https://pepy.tech/project/fx-bharat)
![Wheel](https://img.shields.io/pypi/wheel/fx-bharat.svg)
[![License](https://img.shields.io/pypi/l/fx-bharat.svg)](https://pypi.org/project/fx-bharat/)
![Status](https://img.shields.io/pypi/status/fx-bharat.svg)
[![Python Versions](https://img.shields.io/pypi/pyversions/fx-bharat.svg)](https://pypi.org/project/fx-bharat/)
![Typed](https://img.shields.io/badge/typed-yes-blue.svg)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)
![SQLite Included](https://img.shields.io/badge/database-SQLite-lightgrey)
![isort](https://img.shields.io/badge/imports-isort-%2300aacd.svg)
![flake8](https://img.shields.io/badge/flake8-enabled-blue.svg)
![mypy](https://img.shields.io/badge/mypy-checked-2a6df4.svg)
![CI](https://github.com/vindagollc/fx-bharat/actions/workflows/ci.yml/badge.svg)

---

**FxBharat** is an end-to-end Python package that automatically retrieves foreign-exchange reference rates published by the **Reserve Bank of India (RBI)**, normalizes the downloaded Excel/HTML workbooks, and stores them in a local or remote database.

Every published wheel bundles historical forex data from:

> RBI archive ingested from **07/01/2020 → 07/01/2026**
> SBI Forex PDFs ingested from **07/01/2020 → 07/01/2026**
> LME (COPPER and ALUMINUM) ingested from **02/01/2008 → 07/01/2026**

so the package is **immediately useful** with no setup required.

---

# **📌 Table of Contents**

* [Overview](#overview)
* [Data Source](#data-source)
* [Installation](#installation)
* [Package Layout](#package-layout)
* [Database Schema](#database-schema)
* [Usage](#usage)
  * [1. Quick Start (Bundled SQLite)](#1-quick-start-using-bundled-sqlite-database)
    * [Sqlite Example](#example-default-sqlite)
  * [Source Selection (RBI vs SBI)](#source-selection-rbi-vs-sbi)
  * [2. External Database Examples](#2-connecting-to-your-own-database)
    * [Checking Database Connectivity](#checking-database-connectivity-external)
    * [PostgreSQL Example](#example-postgresql)
    * [MySQL/MariaDB Example](#example-mysqlmariadb)
    * [MongoDB Example](#example-mongodb)
  * [Ingestion Controls](#ingestion-controls)
* [Backend Requirements](#backend-requirements)
* [Running Tests](#running-tests)
* [Design Philosophy](#design-philosophy)
* [Contributing](#contributing)
* [License](#license)

---

# **Overview**

FxBharat provides:

* 🔄 Automated Selenium workflow to download daily reference rates
* 📑 Parsing of RBI Excel/HTML into clean pandas DataFrames
* 💾 Out-of-the-box storage via SQLite (bundled), PostgreSQL, MySQL/MariaDB, or MongoDB
* 📈 Easy APIs to fetch latest rates or historical rollups
* 🧩 A clean façade (`FxBharat`) to simplify ingestion and queries
* 📦 Type-annotated, structured, and production-ready ingestion pipeline

All of this works **default-first**: install the package → start querying FX rates instantly.

---

# **Data Source**

FxBharat retrieves daily *reference exchange rates* from:

* 👉 **RBI Reference Rate Archive** — [https://www.rbi.org.in/Scripts/ReferenceRateArchive.aspx](https://www.rbi.org.in/Scripts/ReferenceRateArchive.aspx)
* 👉 **SBI Forex Card Rates PDF** — [https://sbi.bank.in/documents/16012/1400784/FOREX_CARD_RATES.pdf](https://sbi.bank.in/documents/16012/1400784/FOREX_CARD_RATES.pdf)
* 👉 **LME Copper & Aluminum (Westmetall)** — [LME Copper](https://www.westmetall.com/en/markdaten.php?action=table&field=LME_Cu_cash) / [LME Aluminum](https://www.westmetall.com/en/markdaten.php?action=table&field=LME_Al_cash)

Coverage today:

* RBI archive ingested from **12/04/2022 → 20/11/2025**
* SBI Forex PDFs ingested from **01/01/2020 → 21/11/2025**

Publication cadence (IST):

* RBI updates the reference rates on the archive page around **5:00 PM**.
* SBI refreshes the Forex Card PDF around **10:00 AM**.

If you seed RBI data before the daily publish window, the package exits early when the site returns **"No Reference Rate Found."**—re-run after the data is available.

Please respect the RBI/SBI website terms of service when running ingestion. The default retry and incremental settings are tuned to minimise load on the upstream providers.

Workflow:

1. **Selenium** downloads the RBI Excel/HTML reference rate workbook
2. **BeautifulSoup4 + pandas** parse and normalize the data
3. **pypdf** parses SBI's Forex Card PDF when you opt into the SBI source
4. **SQLAlchemy or PyMongo** persist these rows into your configured backend

The resulting dataset mirrors the RBI reference rates or SBI Forex card tables while keeping a `source` column to distinguish entries.

---

# **Installation**

### Install from PyPI

```bash
pip install fx-bharat
```

The installation includes:

* Selenium
* pandas
* BeautifulSoup4
* SQLAlchemy
* SQLite support

### For local development

```bash
pip install -r requirements.txt
pip install -e .
```

---

# **Package Layout**

```
fx_bharat/
    __init__.py               # FxBharat façade
    db/
        forex.db              # Bundled SQLite snapshot
        base_backend.py       # Unified DB backend interface
        relational_backend.py # SQLAlchemy ORM helpers
        sqlite_backend.py     # SQLite adapter (default)
        postgres_backend.py   # PostgreSQL adapter
        mysql_backend.py      # MySQL/MariaDB adapter
        mongo_backend.py      # MongoDB adapter via PyMongo
        sqlite_manager.py     # SQLite utilities + schema creation
    ingestion/
        rbi_selenium.py       # Selenium automation
        rbi_workbook.py       # HTML/Excel → DataFrame converter
        rbi_csv.py            # Intermediate CSV helpers
        sbi_pdf.py            # SBI Forex Card PDF parser
        models.py             # Dataclasses for parsed rates
    seeds/
        populate_rbi_forex.py # Programmatic seeding logic
        populate_sbi_forex.py # SBI seeding logic (backfills PDFs into SQLite)
    scripts/
        populate_rbi_forex.py # Legacy CLI
        populate_sbi_forex.py # SBI CLI helper
    utils/
        date_range.py         # Date interval utilities
        logger.py             # Structured logging
        rbi.py                # RBI parsing constants
    py.typed                 # PEP 561 type hinting marker
```

---

# **Database Schema**

FxBharat stores RBI and SBI snapshots in two normalized tables. A generated
`resources/schema.sql` mirrors the schema for external databases.

```mermaid
erDiagram
    forex_rates_rbi {
        DATE rate_date PK
        TEXT currency PK
        REAL rate
        TIMESTAMP created_at
    }
    forex_rates_sbi {
        DATE rate_date PK
        TEXT currency PK
        REAL rate
        REAL tt_buy
        REAL tt_sell
        REAL bill_buy
        REAL bill_sell
        REAL travel_card_buy
        REAL travel_card_sell
        REAL cn_buy
        REAL cn_sell
        TIMESTAMP created_at
    }
    lme_copper_rates {
        DATE rate_date PK
        REAL price
        REAL price_3_month
        INTEGER stock
        TIMESTAMP created_at
    }
    lme_aluminum_rates {
        DATE rate_date PK
        REAL price
        REAL price_3_month
        INTEGER stock
        TIMESTAMP created_at
    }
    forex_rates_sbi ||--|| forex_rates_rbi : "aligned by rate_date/currency"
    lme_copper_rates ||--|| lme_aluminum_rates : "daily LME cash seller"
```

---

# **Usage**

## **1. Quick Start (Using Bundled SQLite Database)**

Most users can begin with **zero configuration**:

```python
from datetime import date
from fx_bharat import FxBharat

fx = FxBharat()  # Uses bundled SQLite forex.db

# Insert today's RBI + SBI data
fx.seed()

# Get latest available snapshots (SBI first, then RBI)
latest = fx.rate()
print(latest)
# => [
#   {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'source': 'SBI', 'rates': {...}},
#   {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}},
# ]

# Get a specific day's snapshots (optional `rate_date`)
print(fx.rate(rate_date=date(2025, 11, 1)))

# Fetch a historical window
history = fx.history(date(2025, 10, 1), date(2025, 10, 31), frequency="weekly")

for snapshot in history:
    print(snapshot["rate_date"], snapshot["source"], snapshot["rates"].get("USD"))
```

### What these methods do:

* `.seed(start_date, end_date)` → Downloads & inserts missing entries
* `.rate(rate_date=None)` → Returns **latest available** SBI and RBI observations (or specific `rate_date` snapshots) with SBI first
* `.history(start, end, frequency)` → Supports

  * `"daily"`
  * `"weekly"`
  * `"monthly"`
  * `"yearly"`
* `.history_lme(start, end, frequency, source_filter=None)` → Returns LME snapshots for COPPER/ALUMINUM with the same frequency options

### Seeding LME Copper & Aluminum

Run the new helpers to ingest daily LME cash seller prices (data is available from 2008 onwards):

```python
fx.seed_lme("COPPER")
fx.seed_lme("ALUMINUM")
```

These functions populate the bundled SQLite database and mirror into any configured external backend.

### LME History (Copper & Aluminum)

```python
from datetime import date

# Daily snapshots for both metals
lme_history = fx.history_lme(
    from_date=date(2024, 1, 1),
    to_date=date(2024, 2, 1),
    frequency="daily",
)

# Monthly snapshots for just copper
copper_history = fx.history_lme(
    from_date=date(2024, 1, 1),
    to_date=date(2024, 6, 1),
    frequency="monthly",
    source_filter="COPPER",
)
```

> Legacy note: the former `.rates()` helper now lives on as a deprecated alias of `.history()`; new code should prefer `.history()` or `.historical()`.

---

### Example: Default (Sqlite)

```python
from datetime import date

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.3.0

# Default Usage
fx = FxBharat()

# Latest Forex entries (SBI then RBI if available)
rates = fx.rate()
print(rates)

# Specific Forex entries by date (optional rate_date)
historical_rates = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rates)

# Daily Forex entries (SBI first, then RBI snapshots)
rates = fx.history(from_date=date(2025, 11, 1), to_date=date.today(), frequency='daily')
print(rates[:2])

# Monthly Forex entries
monthly_rates = fx.history(from_date=date(2025, 9, 1), to_date=date.today(), frequency='monthly')
print(monthly_rates)

# Yearly Forex entries
yearly_rates = fx.history(from_date=date(2023, 9, 1), to_date=date.today(), frequency='yearly')
print(yearly_rates)

fx.seed()
```

## Source Selection (RBI vs SBI)

FxBharat now stores RBI and SBI data in **separate tables/collections**. Query helpers always return SBI snapshots first (when present) followed by RBI snapshots. Use the unified `seed(from_date=..., to_date=..., source=...)` helper to ingest targeted ranges; calling `seed()` with no arguments replays data for both sources from the last recorded checkpoint through today (including today) and stores the SBI PDF in `resources/`.

---

## Ingestion Controls

* `source_filter` on `rate`, `history`, and `rates` lets you restrict output to `"rbi"` or `"sbi"` while keeping blended ordering.
* `source_filter` on `history_lme` accepts `"COPPER"` or `"ALUMINUM"` (case-insensitive).
* Incremental seeding is enabled by default using the new `ingestion_metadata` table; the last ingested `rate_date` per source is detected and skipped automatically during cron-style runs.
* Pass `dry_run=True` to `seed`, `seed_sbi_historical`, or `seed_rbi_forex` to validate connectivity without writing rows.
* Yearly aggregations now select the most recent snapshot per calendar year for each source.
* `seed` accepts optional `from_date`, `to_date`, and `source` parameters to restrict ingestion. When you omit them, FxBharat resumes from the last metadata checkpoint for both sources and ingests through today.

## **2. Connecting to Your Own Database**

You can use PostgreSQL, MySQL, MongoDB, or your own SQLite file.

### Checking Database Connectivity (External)
When using an external DB (PostgreSQL/MySQL/MongoDB), you may want to verify that the connection is valid before running `migrate()` or `seed()`.

FxBharat provides:
```python
success, error = fx.connection()
```
- `success` → `True/False`
- `error` → The raw exception message from the database driver

##### Example: Failed Connection Output
```python
success, error = fx.connection()

if not success:
    print("Connection failed:")
    print(error)

```
Typical output if the database does not exist:
```vbnet
(psycopg2.OperationalError) connection to server at "localhost" (127.0.0.1), port 5432 failed:
FATAL: database "forex-db" does not exist
```
This helps diagnose DSN, credentials, port issues, or missing databases before ingestion begins.


### Example: PostgreSQL

```python
from datetime import date

from fx_bharat import FxBharat

fx = FxBharat(db_config='postgresql://postgres:postgres@localhost/forex')

success, error = fx.connection()
if not success:
    print(error)
    exit(1)

fx.migrate()
# =>  will migrate the data from Sqlite to PostgreSQL (forex + LME)
# fx.migrate(from_date=date(2024, 1, 1), to_date=date(2024, 12, 31))

# Latest Forex entry
rate = fx.rate()
print(rate)
# => {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}

# Specific Forex entries by date (optional rate_date)
historical_rates = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rates)

# Weekly/daily Forex entries (SBI first, then RBI)
rates = fx.history(from_date=date(2025, 11, 1), to_date=date.today(), frequency='daily')
print(rates[:2])

# Monthly Forex entries
rates = fx.history(from_date=date(2025, 9, 1), to_date=date.today(), frequency='monthly')
print(rates)

# Yearly Forex entries
rates = fx.history(from_date=date(2023, 9, 1), to_date=date.today(), frequency='yearly')
print(rates)

# Seed SBI + RBI Forex rates into PostgreSQL as well
fx.seed()
```

### Example: MySQL/MariaDB

```python
from datetime import date

from fx_bharat import FxBharat

fx = FxBharat(db_config='mysql://user:pass@localhost:3306/forex')

success, error = fx.connection()
if not success:
    print(error)
    exit(1)

fx.migrate()
# =>  will migrate the data from Sqlite to MySQL (forex + LME)
# fx.migrate(from_date=date(2024, 1, 1), to_date=date(2024, 12, 31))

# Latest Forex entry
rate = fx.rate()
print(rate)
# => {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}

# Specific Forex entry by date (optional rate_date)
historical_rate = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rate)
# => {'rate_date': datetime.date(2025, 11, 1), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}

# weekly Forex entries
rates = fx.history(from_date=date(2025, 11, 1), to_date=date.today(), frequency='daily')
print(rates[:2])
# => [{'rate_date': datetime.date(2025, 11, 3), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}, ...]

# monthly Forex entries
rates = fx.history(from_date=date(2025, 9, 1), to_date=date.today(), frequency='monthly')
print(rates)
# => [{'rate_date': datetime.date(2025, 9, 30), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}, ...]

# yearly Forex entries
rates = fx.history(from_date=date(2023, 9, 1), to_date=date.today(), frequency='yearly')
print(rates)
# => [{'rate_date': datetime.date(2023, 12, 29), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}, ...]

# Seed SBI Forex Card rates into MySQL as well
fx.seed()
print(fx.rate())

fx.seed()
```

### Example: MongoDB

```python
from datetime import date

from fx_bharat import FxBharat

fx = FxBharat(db_config='mongodb://127.0.0.1:27017/forex')

success, error = fx.connection()
if not success:
    print(error)
    exit(1)
    
fx.migrate()
# =>  will migrate the data from Sqlite to MongoDB (forex + LME)
# fx.migrate(from_date=date(2024, 1, 1), to_date=date(2024, 12, 31))

# Latest Forex entry
rate = fx.rate()
print(rate)
# => {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}

# Specific Forex entry by date (optional rate_date)
historical_rate = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rate)
# => {'rate_date': datetime.date(2025, 11, 1), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}

# weekly Forex entries
rates = fx.history(from_date=date(2025, 11, 1), to_date=date.today(), frequency='daily')
print(rates[:2])
# => [{'rate_date': datetime.date(2025, 11, 3), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}, ...]

# monthly Forex entries
rates = fx.history(from_date=date(2025, 9, 1), to_date=date.today(), frequency='monthly')
print(rates)
# => [{'rate_date': datetime.date(2025, 9, 30), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}, ...]

# yearly Forex entries
rates = fx.history(from_date=date(2023, 9, 1), to_date=date.today(), frequency='yearly')
print(rates)
# => [{'rate_date': datetime.date(2023, 12, 29), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}, ...]

# Seed SBI Forex Card rates into MongoDB as well
fx.seed()
print(fx.rate())

fx.seed()
```

FxBharat internally sanitizes the DSN to satisfy PyMongo.

---

# **Backend Requirements**

### Optional dependency matrix

| Backend    | Required extra package(s)  |
| ---------- | -------------------------- |
| PostgreSQL | `psycopg2-binary`          |
| MySQL      | `mysqlclient` or `PyMySQL` |
| MongoDB    | `pymongo`                  |

SQLite works with **no external drivers**.

Install extras manually when needed:

```bash
pip install psycopg2-binary
pip install PyMySQL
pip install pymongo
```

---

# **Running Tests**

```bash
pytest
```

To collect coverage with the built-in settings:

```bash
pytest --cov=fx_bharat --cov-report=term-missing
coverage report
```

If you prefer `coverage run`, make sure you do **not** also pass `--cov` to pytest; running both
simultaneously can zero-out the generated `.coverage` file and lead to the 0% report seen in CI.


---

# **Design Philosophy**

FxBharat is built on the following principles:

### 🚀 Immediate usability

A full SQLite archive is bundled so users can begin querying instantly.

### 🧱 Zero-config default

`FxBharat()` alone is enough for most workflows.

### 🔌 Plug-and-play backends

The same APIs work across SQLite, PostgreSQL, MySQL, or MongoDB.

### 🛠 Extensible architecture

All ingestion and persistence layers are modular and override-able.

### 🔁 Idempotent ingestion

`seed()` can be run safely multiple times without duplicate entries.

---

# Migration Notes

See [MIGRATIONS.md](./MIGRATIONS.md) for upgrade guidance from 0.1.0 → 0.2.0 and 0.2.1, including the new ingestion metadata table and unified `seed` API.

---

# **Contributing**

Pull requests are welcome!
You can contribute to:

* New ingestion capabilities
* Error handling & retry logic
* Additional backends
* Documentation improvements
* Performance optimizations

Open an issue to discuss major changes before submitting a PR.

---

# **License**

Copyright (c) 2025 Vindago Innovations LLC 

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
