from datetime import date

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.3.0

# MongoDB Usage
fx = FxBharat(db_config="mongodb://127.0.0.1:27017/forex")

success, error = fx.conection()  # => to check the connectivity
if not success:
    print(error)
    exit(1)

fx.migrate()

# Seed LME once the migration finishes.
# fx.seed_lme("COPPER")
# fx.seed_lme("ALUMINUM")
