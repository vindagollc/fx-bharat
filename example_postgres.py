from datetime import date

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.2.0

# PostgresSQL Usage
fx = FxBharat(db_config="postgresql+asyncpg://postgres:postgres@localhost/forex")

success, error = fx.conection()  # => to check the connectivity
if not success:
    print(error)
    exit(1)

fx.migrate()
# =>  will migrate the date from Sqlite to postgres

# Latest Forex entry
rate = fx.rate()
print(rate)
# => {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}

# Specific Forex entry by date (optional rate_date)
historical_rate = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rate)
# => {'rate_date': datetime.date(2025, 11, 1), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}

# weekly Forex entries
history = fx.history(from_date=date(2025, 11, 1), to_date=date.today(), frequency="daily")
print(history[:2])
# => [{'rate_date': date(2025, 11, 3), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}, ...]

# monthly Forex entries
history = fx.history(from_date=date(2025, 9, 1), to_date=date.today(), frequency="monthly")
print(history)
# => [{'rate_date': date(2025, 9, 30), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}, ...]

# yearly Forex entries
history = fx.history(from_date=date(2023, 9, 1), to_date=date.today(), frequency="yearly")
print(history)
# => [{'rate_date': date(2023, 12, 29), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}, ...]

# SBI Forex Card rates can also be mirrored into Postgres after parsing the PDF
fx.seed(from_date=date.today(), to_date=date.today(), source="SBI")
print(fx.rate(source="SBI"))

fx.seed(from_date=date.today(), to_date=date.today())
