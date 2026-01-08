from fx_bharat import FxBharat

# Print installed package version
print(FxBharat.__version__)

# -------------------------------------------------------------
# Instantiate FxBharat
# -------------------------------------------------------------
# When you create FxBharat() without arguments:
# - It automatically loads the bundled forex SQLite database
#   shipped with the package.
# - It auto-loads RBI + SBI historical data up to the bundled
#   cutoff date (e.g., 2022–2025 depending on package version).
# - No internet connection is required unless you call `seed()`.
# - This gives instant access to millions of rows of FX data.
fx = FxBharat()

# -------------------------------------------------------------
# Seed data
# -------------------------------------------------------------
fx.seed()
fx.seed_lme(metal="ALUMINUM")
fx.seed_lme(metal="COPPER")
#
# Use this to refresh or load new forex data from:
#   - RBI (Reference Rate Archive, Excel files)
#   - SBI (Forex card PDF/HTML pages)
#
# Notes:
# - By default, seed() loads ALL missing dates up to today.
# - You can override start_date and end_date manually.
# - Selenium is not required anymore in your agent version.
#
# Recommended usage:
#   fx.seed(start_date=date(2023,1,1), end_date=date(2024,12,31))
#
# Leave commented unless needed because seeding can take time.


# -------------------------------------------------------------
# 1. Get latest combined forex rates (RBI + SBI)
# -------------------------------------------------------------
# fx.rate() returns a LIST of structured dicts:
# [
#   {"date": <date>, "source": "rbi", "rates": {...}},
#   {"date": <date>, "source": "sbi", "rates": {...}}
# ]
#
# If both RBI and SBI have new data for today, you'll get 2 entries.
# rates = fx.rate()
# for rate in rates:
#     # `.get('rates')` returns a dictionary:
#     #   {"USD": 83.12, "EUR": 90.45, ...}
#     print(rate.get("rates"))
#
#
# # -------------------------------------------------------------
# # 2. Latest RBI-only forex rates
# # -------------------------------------------------------------
# # Only retrieves the latest RBI reference rate available in DB.
# rates = fx.rate(source_filter="rbi")
# for rate in rates:
#     print(rate.get("rates"))
#
#
# # -------------------------------------------------------------
# # 3. Latest SBI-only forex rates
# # -------------------------------------------------------------
# # SBI sometimes publishes only a subset of currencies.
# # Useful if your workflow depends on forex card or remittance rates.
# rates = fx.rate(source_filter="sbi")
# for rate in rates:
#     print(rate.get("rates"))
#
# TIP: "weekly" and "monthly" are perfect for charts and analytics.
# history = fx.history(
#     from_date=date(2025, 11, 1),
#     to_date=date.today(),
#     frequency="daily",  # or: weekly / monthly / yearly
#     # source_filter="rbi",  # Optional — defaults to all sources
# )
#
# # Print first 2 entries as a preview
# print(history[:1])

# LME history (daily snapshots for both metals)
# lme_history = fx.history_lme(
#     from_date=date(2024, 1, 1),
#     to_date=date.today(),
#     frequency="daily",
# )
# print(lme_history[:1])
