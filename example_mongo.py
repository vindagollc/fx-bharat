from datetime import date

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.1.0

# PostgresSQL MongoDb
fx = FxBharat(db_config="mongodb://127.0.0.1:27017/forex")

success, error = fx.conection()  # => to check the connectivity
if not success:
    print(error)
    exit(1)

fx.migrate()  # =>  will migrate the date from Sqlite to postgres

# Latest Forex entry
rate = fx.rate()
print(rate)
# => {'rate_date': datetime.date(2025, 11, 18), 'base_currency': 'INR', 'rates': {'EUR': 102.7828, 'GBP': 116.5844, 'JPY': 57.15, 'USD': 88.6344}}

# Specific Forex entry by date (optional rate_date)
historical_rate = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rate)
# => {'rate_date': datetime.date(2025, 11, 1), 'base_currency': 'INR', 'rates': {...}}

# weekly Forex entries
rates = fx.rates(
    from_date=date(2025, 11, 1),
    to_date=date.today(),
    frequency="daily"
)
print(rates)
# => [{'rate_date': date(2025, 11, 3), 'rates': {...}}, ...]

# monthly Forex entries
rates = fx.rates(
    from_date=date(2025, 9, 1),
    to_date=date.today(),
    frequency="monthly"
)
print(rates)
# => [{'rate_date': date(2025, 9, 30), 'rates': {...}}, ...]

# yearly Forex entries
rates = fx.rates(
    from_date=date(2023, 9, 1),
    to_date=date.today(),
    frequency="yearly"
)
print(rates)
# => [{'rate_date': date(2023, 12, 29), 'rates': {...}}, ...]

fx.seed(from_date=date.today(), to_date=date.today())
