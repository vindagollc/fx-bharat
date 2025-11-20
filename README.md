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

> RBI archive ingested from **12/04/2022 → 20/11/2025**
> SBI Forex PDFs ingested from **01/01/2020 → 20/11/2025**

so the package is **immediately useful** with no setup required.

---

# **📌 Table of Contents**

* [Overview](#overview)
* [Data Source](#data-source)
* [Installation](#installation)
* [Package Layout](#package-layout)
* [Usage](#usage)
  * [1. Quick Start (Bundled SQLite)](#1-quick-start-using-bundled-sqlite-database)
    * [Sqlite Example](#example-default-sqlite)
  * [Source Selection (RBI vs SBI)](#source-selection-rbi-vs-sbi)
  * [2. External Database Examples](#2-connecting-to-your-own-database)
    * [Checking Database Connectivity](#checking-database-connectivity-external)
    * [PostgreSQL Example](#example-postgresql)
    * [MySQL/MariaDB Example](#example-mysqlmariadb)
    * [MongoDB Example](#example-mongodb)
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

Coverage today:

* RBI archive ingested from **12/04/2022 → 20/11/2025**
* SBI Forex PDFs ingested from **01/01/2020 → 20/11/2025**

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

FxBharat now stores RBI and SBI data in **separate tables/collections**. Query helpers always return SBI snapshots first (when present) followed by RBI snapshots. Use `seed_historical(..., source="RBI" | "SBI")` to ingest archival PDFs for a specific source; `seed()` pulls both sources for the current day and saves the SBI PDF into `resources/`.

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
from fx_bharat import FxBharat
from datetime import date

fx = FxBharat(db_config='postgresql://postgres:postgres@localhost/forex')

success, error = fx.conection()
if not success:
    print(error)
    exit(1)

fx.migrate()
# =>  will migrate the date from Sqlite to PostgreSQL

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
from fx_bharat import FxBharat
from datetime import date

fx = FxBharat(db_config='mysql://user:pass@localhost:3306/forex')

success, error = fx.conection()
if not success:
    print(error)
    exit(1)

fx.migrate()
# =>  will migrate the date from Sqlite to MySQL

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
from fx_bharat import FxBharat
from datetime import date

fx = FxBharat(db_config='mongodb://127.0.0.1:27017/forex')

success, error = fx.conection()
if not success:
    print(error)
    exit(1)
    
fx.migrate()
# =>  will migrate the date from Sqlite to MongoDB

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
python -m unittest discover -s tests -v
```

All tests use the standard library `unittest`.

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

