-- fx-bharat bundled schema

CREATE TABLE IF NOT EXISTS forex_rates_rbi (
    rate_date DATE NOT NULL,
    currency TEXT NOT NULL,
    rate REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(rate_date, currency)
);

CREATE TABLE IF NOT EXISTS forex_rates_sbi (
    rate_date DATE NOT NULL,
    currency TEXT NOT NULL,
    rate REAL NOT NULL,
    tt_buy REAL,
    tt_sell REAL,
    bill_buy REAL,
    bill_sell REAL,
    travel_card_buy REAL,
    travel_card_sell REAL,
    cn_buy REAL,
    cn_sell REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(rate_date, currency)
);
