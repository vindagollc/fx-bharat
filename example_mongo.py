from datetime import date

from fx_bharat import FxBharat

print(FxBharat.__version__)  # 0.2.1

# MongoDB Usage
fx = FxBharat(db_config="mongodb://127.0.0.1:27017/forex")

success, error = fx.conection()  # => to check the connectivity
if not success:
    print(error)
    exit(1)

fx.migrate()  # =>  will migrate the date from Sqlite to MongoDB
