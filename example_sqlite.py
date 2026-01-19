from datetime import date
from pathlib import Path

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.4.1

# SQLite usage (explicit DB file)
db_path = Path.cwd() / "forex.db"
fx = FxBharat(db_config=f"sqlite:///{db_path}")

success, error = fx.connection()
if not success:
    raise SystemExit(error)

# Seed historical RBI + SBI + LME (from 2022-04-01)
fx.seed()

# Latest snapshot
print("Latest snapshot:", fx.rate())

# Specific date snapshot
print("2024-12-31 snapshot:", fx.rate(rate_date=date(2024, 12, 31)))

# Daily refresh for today
fx.update_daily()
