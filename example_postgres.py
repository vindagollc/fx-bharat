from datetime import date

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.3.1

# PostgresSQL Usage
fx = FxBharat(db_config="postgresql+asyncpg://postgres:postgres@localhost/forex")

success, error = fx.connection()  # => to check the connectivity
if not success:
    print(error)
    exit(1)

fx.migrate()
fx.migrate(from_date=date(2024, 1, 1), to_date=date(2024, 12, 31), chunk_size=500)

# Seed LME once the migration finishes.
# fx.seed_lme("COPPER")
# fx.seed_lme("ALUMINUM")
