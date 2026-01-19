from datetime import date

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.4.0

# MongoDB Usage (requires pymongo)
fx = FxBharat(db_config="mongodb://127.0.0.1:27017/forex")

success, error = fx.connection()
if not success:
    raise SystemExit(error)

# Seed historical RBI + SBI + LME (from 2020-01-01)
fx.seed()

# Basic reads
print("Latest snapshot:", fx.rate())
print("SBI-only snapshot:", fx.rate(source_filter="sbi"))

# Historical window
history = fx.history(date(2024, 1, 1), date(2024, 1, 7))
print("History rows:", len(history))

# LME daily refresh for today
fx.update_daily()
