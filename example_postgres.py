from datetime import date

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.4.0

# PostgreSQL Usage (external DB required)
fx = FxBharat(db_config="postgresql://postgres:postgres@localhost/forex")

success, error = fx.connection()  # => connectivity check
if not success:
    raise SystemExit(error)

# Seed historical RBI + SBI + LME (from 2020-01-01)
fx.seed()

# Fetch latest combined snapshot (SBI first, then RBI)
print("Latest snapshot:", fx.rate())

# Fetch a specific day
print("2024-12-31 snapshot:", fx.rate(rate_date=date(2024, 12, 31)))

# LME history example
lme = fx.history_lme(date(2024, 1, 1), date(2024, 1, 31))
print("LME Jan 2024 rows:", len(lme))

# Optional: explicit daily refresh (today only)
# fx.update_daily()
