from datetime import date

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.3.0

# Default Usage
fx = FxBharat()

# Latest Forex entries
rates = fx.rate()
print(rates)

# Specific Forex entries by date (optional rate_date)
historical_rates = fx.rate(rate_date=date(2025, 11, 1))
print(historical_rates)

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
# => [{'rate_date': date(2025, 9, 30), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}, ...]

# yearly Forex entries
history = fx.history(
    from_date=date(2023, 9, 1),
    to_date=date.today(),
    frequency="yearly",
)
print(history)
# => [{'rate_date': date(2023, 12, 29), 'base_currency': 'INR', 'source': 'RBI', 'rates': {...}}, ...]

# Seed today's RBI + SBI Forex Card rates and fetch the latest snapshot
fx.seed()
print(fx.rate())

# Seed historical RBI Forex rates for a specific window
fx.seed_historical(from_date=date(2020, 1, 1), to_date=date(2025, 11, 19), source="SBI")
fx.seed_historical(from_date=date(2022, 4, 12), to_date=date(2025, 11, 19), source="RBI")
#
