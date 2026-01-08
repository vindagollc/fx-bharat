from datetime import date

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.3.1

# PostgresSQL Usage
fx = FxBharat(db_config="postgresql+psycopg://pgadmin:H%7B~b6%29F%27O5E%23YXwv@seg-pace-dev.postgres.database.azure.com:5432/postgres-dev")

success, error = fx.connection()  # => to check the connectivity
if not success:
    print(error)
    exit(1)

fx.migrate()
# fx.migrate(from_date=date(2022, 1, 1), to_date=date(2026, 1, 8),  chunk_size=1000)

# Seed LME once the migration finishes.
# fx.seed_lme("COPPER")
# fx.seed_lme("ALUMINUM")
