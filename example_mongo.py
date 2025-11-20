from datetime import date

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.3.0

# MongoDB Usage
fx = FxBharat(db_config="mongodb://127.0.0.1:27017/forex")

success, error = fx.conection()  # => to check the connectivity
if not success:
    print(error)
    exit(1)

fx.migrate()  # =>  will migrate the date from Sqlite to MongoDB

# Latest Forex entries
rate = fx.rate()
print(rate)

# Specific Forex entry by date (optional rate_date)
historical_rate = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rate)

# weekly Forex entries
history = fx.history(
    from_date=date(2025, 11, 1),
    to_date=date.today(),
    frequency="daily",
)
print(history[:2])

# monthly Forex entries
history = fx.history(
    from_date=date(2025, 9, 1),
    to_date=date.today(),
    frequency="monthly",
)
print(history)

# yearly Forex entries
history = fx.history(
    from_date=date(2023, 9, 1),
    to_date=date.today(),
    frequency="yearly",
)
print(history)

# SBI Forex Card rates can also be mirrored into MongoDB after parsing the PDF
fx.seed(resource_dir="resources")
print(fx.rate())

fx.seed_historical(from_date=date(2023, 1, 1), to_date=date(2023, 1, 31))
