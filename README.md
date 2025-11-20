# **fx-bharat**

[![PyPI Version](https://img.shields.io/pypi/v/fx-bharat.svg)](https://pypi.org/project/fx-bharat/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/fx-bharat.svg)](https://pypi.org/project/fx-bharat/)
![Wheel](https://img.shields.io/pypi/wheel/fx-bharat.svg)
[![License](https://img.shields.io/pypi/l/fx-bharat.svg)](https://pypi.org/project/fx-bharat/)
![Status](https://img.shields.io/pypi/status/fx-bharat.svg)
[![Python Versions](https://img.shields.io/pypi/pyversions/fx-bharat.svg)](https://pypi.org/project/fx-bharat/)


---

**FxBharat** is an end-to-end Python package that automatically retrieves foreign-exchange reference rates published by the **Reserve Bank of India (RBI)**, normalizes the downloaded Excel/HTML workbooks, and stores them in a local or remote database.

Every published wheel bundles historical forex data from:

> **12/04/2022 → 18/11/2025**

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

FxBharat retrieves daily *reference exchange rates* from the official:

👉 **RBI Reference Rate Archive**
[https://www.rbi.org.in/Scripts/ReferenceRateArchive.aspx](https://www.rbi.org.in/Scripts/ReferenceRateArchive.aspx)

Workflow:

1. **Selenium** downloads the Excel/HTML reference rate workbook
2. **BeautifulSoup4 + pandas** parse and normalize the data
3. **SQLAlchemy or PyMongo** persist these rows into your configured backend

The resulting dataset mirrors RBI's public release structure.

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
        forex.db              # Bundled SQLite snapshot (12/04/2022–18/11/2025)
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
        models.py             # Dataclasses for parsed rates
    seeds/
        populate_rbi_forex.py # Programmatic seeding logic
    scripts/
        populate_rbi_forex.py # Legacy CLI
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

# Update today's rates
fx.seed(from_date=date.today(), to_date=date.today())

# Get latest available snapshot
print(fx.rate())

# Get a specific day's snapshot (optional `rate_date`)
print(fx.rate(rate_date=date(2025, 11, 1)))

# Fetch a historical window
history = fx.rates(date(2025, 10, 1), date(2025, 10, 31), frequency="weekly")

for row in history:
    print(row.rate_date, row.currency, row.value)
```

### What these methods do:

* `.seed(start_date, end_date)` → Downloads & inserts missing entries
* `.rate(rate_date=None)` → Returns **latest available** FX observation (or a specific `rate_date` if provided)
* `.rates(start, end, frequency)` → Supports

  * `"daily"`
  * `"weekly"`
  * `"monthly"`
  * `"yearly"`

---

### Example: Default (Sqlite)

```python
from fx_bharat import FxBharat
from datetime import date

# Default Usage
fx = FxBharat()

# Latest Forex entry
rate = fx.rate()
print(rate) 
# => {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}

# Specific Forex entry by date (optional rate_date)
historical_rate = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rate)
# => {'rate_date': datetime.date(2025, 11, 1), 'base_currency': 'INR', 'rates': {...}}

# weekly Forex entries
rates = fx.rates(from_date=date(2025, 11, 1), to_date=date.today(), frequency='daily')
print(rates) 
# => [{'rate_date': datetime.date(2025, 11, 3), 'base_currency': 'INR', 'rates': {'EUR': 102.4348, 'GBP': 116.6974, 'JPY': 57.59, 'USD': 88.7932}}, {'rate_date': datetime.date(2025, 11, 4), 'base_currency': 'INR', 'rates': {'EUR': 102.1384, 'GBP': 116.3168, 'JPY': 57.72, 'USD': 88.6372}}, {'rate_date': datetime.date(2025, 11, 6), 'base_currency': 'INR', 'rates': {'EUR': 101.981, 'GBP': 115.751, 'JPY': 57.58, 'USD': 88.6026}}, {'rate_date': datetime.date(2025, 11, 7), 'base_currency': 'INR', 'rates': {'EUR': 102.3041, 'GBP': 116.3691, 'JPY': 57.81, 'USD': 88.705}}, {'rate_date': datetime.date(2025, 11, 10), 'base_currency': 'INR', 'rates': {'EUR': 102.5332, 'GBP': 116.6558, 'JPY': 57.59, 'USD': 88.6761}}, {'rate_date': datetime.date(2025, 11, 11), 'base_currency': 'INR', 'rates': {'EUR': 102.5435, 'GBP': 116.8044, 'JPY': 57.53, 'USD': 88.6983}}, {'rate_date': datetime.date(2025, 11, 12), 'base_currency': 'INR', 'rates': {'EUR': 102.6431, 'GBP': 116.4544, 'JPY': 57.31, 'USD': 88.6362}}, {'rate_date': datetime.date(2025, 11, 13), 'base_currency': 'INR', 'rates': {'EUR': 102.7633, 'GBP': 116.3924, 'JPY': 57.26, 'USD': 88.716}}, {'rate_date': datetime.date(2025, 11, 14), 'base_currency': 'INR', 'rates': {'EUR': 103.3188, 'GBP': 116.7194, 'JPY': 57.44, 'USD': 88.742}}, {'rate_date': datetime.date(2025, 11, 17), 'base_currency': 'INR', 'rates': {'EUR': 102.7925, 'GBP': 116.445, 'JPY': 57.26, 'USD': 88.63}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

# monthly Forex entries
rates = fx.rates(from_date=date(2025, 9, 1), to_date=date.today(), frequency='monthly')
print(rates) 
# => [{'rate_date': datetime.date(2025, 9, 30), 'base_currency': 'INR', 'rates': {'EUR': 104.222, 'GBP': 119.354, 'JPY': 59.91, 'USD': 88.7923}}, {'rate_date': datetime.date(2025, 10, 31), 'base_currency': 'INR', 'rates': {'EUR': 102.6745, 'GBP': 116.6947, 'JPY': 57.61, 'USD': 88.7241}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

# yearly Forex entries
rates = fx.rates(from_date=date(2023, 9, 1), to_date=date.today(), frequency='yearly')
print(rates) 
# => [{'rate_date': datetime.date(2023, 12, 29), 'base_currency': 'INR', 'rates': {'EUR': 92.0049, 'GBP': 106.1053, 'JPY': 58.82, 'USD': 83.1164}}, {'rate_date': datetime.date(2024, 12, 31), 'base_currency': 'INR', 'rates': {'EUR': 89.0852, 'GBP': 107.4645, 'JPY': 54.82, 'USD': 85.6232}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

fx.seed(from_date=date.today(), to_date=date.today())
```

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
# => {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}

# Specific Forex entry by date (optional rate_date)
historical_rate = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rate)
# => {'rate_date': datetime.date(2025, 11, 1), 'base_currency': 'INR', 'rates': {...}}

# weekly Forex entries
rates = fx.rates(from_date=date(2025, 11, 1), to_date=date.today(), frequency='daily')
print(rates) 
# => [{'rate_date': datetime.date(2025, 11, 3), 'base_currency': 'INR', 'rates': {'EUR': 102.4348, 'GBP': 116.6974, 'JPY': 57.59, 'USD': 88.7932}}, {'rate_date': datetime.date(2025, 11, 4), 'base_currency': 'INR', 'rates': {'EUR': 102.1384, 'GBP': 116.3168, 'JPY': 57.72, 'USD': 88.6372}}, {'rate_date': datetime.date(2025, 11, 6), 'base_currency': 'INR', 'rates': {'EUR': 101.981, 'GBP': 115.751, 'JPY': 57.58, 'USD': 88.6026}}, {'rate_date': datetime.date(2025, 11, 7), 'base_currency': 'INR', 'rates': {'EUR': 102.3041, 'GBP': 116.3691, 'JPY': 57.81, 'USD': 88.705}}, {'rate_date': datetime.date(2025, 11, 10), 'base_currency': 'INR', 'rates': {'EUR': 102.5332, 'GBP': 116.6558, 'JPY': 57.59, 'USD': 88.6761}}, {'rate_date': datetime.date(2025, 11, 11), 'base_currency': 'INR', 'rates': {'EUR': 102.5435, 'GBP': 116.8044, 'JPY': 57.53, 'USD': 88.6983}}, {'rate_date': datetime.date(2025, 11, 12), 'base_currency': 'INR', 'rates': {'EUR': 102.6431, 'GBP': 116.4544, 'JPY': 57.31, 'USD': 88.6362}}, {'rate_date': datetime.date(2025, 11, 13), 'base_currency': 'INR', 'rates': {'EUR': 102.7633, 'GBP': 116.3924, 'JPY': 57.26, 'USD': 88.716}}, {'rate_date': datetime.date(2025, 11, 14), 'base_currency': 'INR', 'rates': {'EUR': 103.3188, 'GBP': 116.7194, 'JPY': 57.44, 'USD': 88.742}}, {'rate_date': datetime.date(2025, 11, 17), 'base_currency': 'INR', 'rates': {'EUR': 102.7925, 'GBP': 116.445, 'JPY': 57.26, 'USD': 88.63}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

# monthly Forex entries
rates = fx.rates(from_date=date(2025, 9, 1), to_date=date.today(), frequency='monthly')
print(rates) 
# => [{'rate_date': datetime.date(2025, 9, 30), 'base_currency': 'INR', 'rates': {'EUR': 104.222, 'GBP': 119.354, 'JPY': 59.91, 'USD': 88.7923}}, {'rate_date': datetime.date(2025, 10, 31), 'base_currency': 'INR', 'rates': {'EUR': 102.6745, 'GBP': 116.6947, 'JPY': 57.61, 'USD': 88.7241}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

# yearly Forex entries
rates = fx.rates(from_date=date(2023, 9, 1), to_date=date.today(), frequency='yearly')
print(rates) 
# => [{'rate_date': datetime.date(2023, 12, 29), 'base_currency': 'INR', 'rates': {'EUR': 92.0049, 'GBP': 106.1053, 'JPY': 58.82, 'USD': 83.1164}}, {'rate_date': datetime.date(2024, 12, 31), 'base_currency': 'INR', 'rates': {'EUR': 89.0852, 'GBP': 107.4645, 'JPY': 54.82, 'USD': 85.6232}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

fx.seed(from_date=date.today(), to_date=date.today())
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
# => {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}

# Specific Forex entry by date (optional rate_date)
historical_rate = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rate)
# => {'rate_date': datetime.date(2025, 11, 1), 'base_currency': 'INR', 'rates': {...}}

# weekly Forex entries
rates = fx.rates(from_date=date(2025, 11, 1), to_date=date.today(), frequency='daily')
print(rates) 
# => [{'rate_date': datetime.date(2025, 11, 3), 'base_currency': 'INR', 'rates': {'EUR': 102.4348, 'GBP': 116.6974, 'JPY': 57.59, 'USD': 88.7932}}, {'rate_date': datetime.date(2025, 11, 4), 'base_currency': 'INR', 'rates': {'EUR': 102.1384, 'GBP': 116.3168, 'JPY': 57.72, 'USD': 88.6372}}, {'rate_date': datetime.date(2025, 11, 6), 'base_currency': 'INR', 'rates': {'EUR': 101.981, 'GBP': 115.751, 'JPY': 57.58, 'USD': 88.6026}}, {'rate_date': datetime.date(2025, 11, 7), 'base_currency': 'INR', 'rates': {'EUR': 102.3041, 'GBP': 116.3691, 'JPY': 57.81, 'USD': 88.705}}, {'rate_date': datetime.date(2025, 11, 10), 'base_currency': 'INR', 'rates': {'EUR': 102.5332, 'GBP': 116.6558, 'JPY': 57.59, 'USD': 88.6761}}, {'rate_date': datetime.date(2025, 11, 11), 'base_currency': 'INR', 'rates': {'EUR': 102.5435, 'GBP': 116.8044, 'JPY': 57.53, 'USD': 88.6983}}, {'rate_date': datetime.date(2025, 11, 12), 'base_currency': 'INR', 'rates': {'EUR': 102.6431, 'GBP': 116.4544, 'JPY': 57.31, 'USD': 88.6362}}, {'rate_date': datetime.date(2025, 11, 13), 'base_currency': 'INR', 'rates': {'EUR': 102.7633, 'GBP': 116.3924, 'JPY': 57.26, 'USD': 88.716}}, {'rate_date': datetime.date(2025, 11, 14), 'base_currency': 'INR', 'rates': {'EUR': 103.3188, 'GBP': 116.7194, 'JPY': 57.44, 'USD': 88.742}}, {'rate_date': datetime.date(2025, 11, 17), 'base_currency': 'INR', 'rates': {'EUR': 102.7925, 'GBP': 116.445, 'JPY': 57.26, 'USD': 88.63}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

# monthly Forex entries
rates = fx.rates(from_date=date(2025, 9, 1), to_date=date.today(), frequency='monthly')
print(rates) 
# => [{'rate_date': datetime.date(2025, 9, 30), 'base_currency': 'INR', 'rates': {'EUR': 104.222, 'GBP': 119.354, 'JPY': 59.91, 'USD': 88.7923}}, {'rate_date': datetime.date(2025, 10, 31), 'base_currency': 'INR', 'rates': {'EUR': 102.6745, 'GBP': 116.6947, 'JPY': 57.61, 'USD': 88.7241}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

# yearly Forex entries
rates = fx.rates(from_date=date(2023, 9, 1), to_date=date.today(), frequency='yearly')
print(rates) 
# => [{'rate_date': datetime.date(2023, 12, 29), 'base_currency': 'INR', 'rates': {'EUR': 92.0049, 'GBP': 106.1053, 'JPY': 58.82, 'USD': 83.1164}}, {'rate_date': datetime.date(2024, 12, 31), 'base_currency': 'INR', 'rates': {'EUR': 89.0852, 'GBP': 107.4645, 'JPY': 54.82, 'USD': 85.6232}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

fx.seed(from_date=date.today(), to_date=date.today())
```

### Example: MongoDB

```python
from fx_bharat import FxBharat
from datetime import date

fx = FxBharat(db_config='mongodb://127.0.0.1:27017/forex')

success, error = fx.conection()
if success:
    print(error)
    exit(1)
    
fx.migrate() 
# =>  will migrate the date from Sqlite to MongoDB

# Latest Forex entry
rate = fx.rate()
print(rate) 
# => {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}

# Specific Forex entry by date (optional rate_date)
historical_rate = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rate)
# => {'rate_date': datetime.date(2025, 11, 1), 'base_currency': 'INR', 'rates': {...}}

# weekly Forex entries
rates = fx.rates(from_date=date(2025, 11, 1), to_date=date.today(), frequency='daily')
print(rates) 
# => [{'rate_date': datetime.date(2025, 11, 3), 'base_currency': 'INR', 'rates': {'EUR': 102.4348, 'GBP': 116.6974, 'JPY': 57.59, 'USD': 88.7932}}, {'rate_date': datetime.date(2025, 11, 4), 'base_currency': 'INR', 'rates': {'EUR': 102.1384, 'GBP': 116.3168, 'JPY': 57.72, 'USD': 88.6372}}, {'rate_date': datetime.date(2025, 11, 6), 'base_currency': 'INR', 'rates': {'EUR': 101.981, 'GBP': 115.751, 'JPY': 57.58, 'USD': 88.6026}}, {'rate_date': datetime.date(2025, 11, 7), 'base_currency': 'INR', 'rates': {'EUR': 102.3041, 'GBP': 116.3691, 'JPY': 57.81, 'USD': 88.705}}, {'rate_date': datetime.date(2025, 11, 10), 'base_currency': 'INR', 'rates': {'EUR': 102.5332, 'GBP': 116.6558, 'JPY': 57.59, 'USD': 88.6761}}, {'rate_date': datetime.date(2025, 11, 11), 'base_currency': 'INR', 'rates': {'EUR': 102.5435, 'GBP': 116.8044, 'JPY': 57.53, 'USD': 88.6983}}, {'rate_date': datetime.date(2025, 11, 12), 'base_currency': 'INR', 'rates': {'EUR': 102.6431, 'GBP': 116.4544, 'JPY': 57.31, 'USD': 88.6362}}, {'rate_date': datetime.date(2025, 11, 13), 'base_currency': 'INR', 'rates': {'EUR': 102.7633, 'GBP': 116.3924, 'JPY': 57.26, 'USD': 88.716}}, {'rate_date': datetime.date(2025, 11, 14), 'base_currency': 'INR', 'rates': {'EUR': 103.3188, 'GBP': 116.7194, 'JPY': 57.44, 'USD': 88.742}}, {'rate_date': datetime.date(2025, 11, 17), 'base_currency': 'INR', 'rates': {'EUR': 102.7925, 'GBP': 116.445, 'JPY': 57.26, 'USD': 88.63}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

# monthly Forex entries
rates = fx.rates(from_date=date(2025, 9, 1), to_date=date.today(), frequency='monthly')
print(rates) 
# => [{'rate_date': datetime.date(2025, 9, 30), 'base_currency': 'INR', 'rates': {'EUR': 104.222, 'GBP': 119.354, 'JPY': 59.91, 'USD': 88.7923}}, {'rate_date': datetime.date(2025, 10, 31), 'base_currency': 'INR', 'rates': {'EUR': 102.6745, 'GBP': 116.6947, 'JPY': 57.61, 'USD': 88.7241}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

# yearly Forex entries
rates = fx.rates(from_date=date(2023, 9, 1), to_date=date.today(), frequency='yearly')
print(rates) 
# => [{'rate_date': datetime.date(2023, 12, 29), 'base_currency': 'INR', 'rates': {'EUR': 92.0049, 'GBP': 106.1053, 'JPY': 58.82, 'USD': 83.1164}}, {'rate_date': datetime.date(2024, 12, 31), 'base_currency': 'INR', 'rates': {'EUR': 89.0852, 'GBP': 107.4645, 'JPY': 54.82, 'USD': 85.6232}}, {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}]

fx.seed(from_date=date.today(), to_date=date.today())
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

