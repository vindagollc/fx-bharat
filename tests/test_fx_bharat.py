"""Tests for the public package facade."""

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from fx_bharat import DatabaseBackend, DatabaseConnectionInfo, FxBharat, __version__
from fx_bharat.db.mongo_backend import MongoBackend
from fx_bharat.ingestion.models import ForexRateRecord


def test_fx_bharat_class_is_exposed() -> None:
    """The package should expose the FxBharat class."""

    assert FxBharat.__version__ == __version__


def test_fx_bharat_defaults_to_sqlite() -> None:
    fx_bharat = FxBharat()

    assert fx_bharat.connection_info.backend is DatabaseBackend.SQLITE
    assert fx_bharat.sqlite_manager is not None


def test_fx_bharat_supports_external_backends() -> None:
    fx_bharat = FxBharat(db_config="mysql://user:pwd@localhost:3306/forex")

    assert fx_bharat.connection_info.backend is DatabaseBackend.MYSQL
    assert fx_bharat.connection_info.username == "user"
    assert fx_bharat.connection_info.password == "pwd"
    assert fx_bharat.connection_info.host == "localhost"
    assert fx_bharat.connection_info.port == 3306
    assert fx_bharat.sqlite_manager is None


@pytest.mark.parametrize(
    "url, backend",
    [
        ("postgresql://localhost/forex", DatabaseBackend.POSTGRES),
        ("postgres://localhost/forex", DatabaseBackend.POSTGRES),
        ("mongodb://localhost:27017/forex", DatabaseBackend.MONGODB),
    ],
)
def test_fx_bharat_normalises_backend_names(url: str, backend: DatabaseBackend) -> None:
    fx_bharat = FxBharat(db_config=url)

    assert fx_bharat.connection_info.backend is backend


@pytest.mark.parametrize(
    "scheme, backend",
    [
        ("postgresql", DatabaseBackend.POSTGRES),
        ("postgres", DatabaseBackend.POSTGRES),
        ("postgressql", DatabaseBackend.POSTGRES),
        ("mysql+pymysql", DatabaseBackend.MYSQL),
        ("sqlite", DatabaseBackend.SQLITE),
        ("mongodb+srv", DatabaseBackend.MONGODB),
    ],
)
def test_database_backend_from_scheme_handles_aliases(
    scheme: str, backend: DatabaseBackend
) -> None:
    assert DatabaseBackend.from_scheme(scheme) is backend


def test_fx_bharat_validates_urls() -> None:
    with pytest.raises(ValueError):
        FxBharat(db_config="localhost:3306/forex")


def test_database_connection_info_requires_scheme() -> None:
    with pytest.raises(ValueError, match="DB_URL must include a scheme"):
        DatabaseConnectionInfo.from_url("localhost/forex")


def test_database_connection_info_from_url() -> None:
    info = DatabaseConnectionInfo.from_url("postgres://user:pwd@db:5432/app")

    assert info.backend is DatabaseBackend.POSTGRES
    assert info.username == "user"
    assert info.password == "pwd"
    assert info.host == "db"
    assert info.port == 5432
    assert info.name == "app"


def test_database_name_is_extracted_from_query_parameters() -> None:
    info = DatabaseConnectionInfo.from_url(
        "mongodb://user:pwd@cluster0.example.com/?retryWrites=false&w=majority&appName=Cluster0DATABASE_NAME=test",
    )

    assert info.backend is DatabaseBackend.MONGODB
    assert info.name == "test"
    assert info.url.startswith(
        "mongodb://user:pwd@cluster0.example.com/test?retryWrites=false&w=majority&appName=Cluster0"
    )


def test_mongodb_srv_connections_are_supported() -> None:
    info = DatabaseConnectionInfo.from_url(
        "mongodb+srv://127.0.0.1:27017/?DATABASE_NAME=forex&retryWrites=false&w=majority&appName=Cluster0",
    )

    assert info.backend is DatabaseBackend.MONGODB
    assert info.name == "forex"
    assert info.url.startswith(
        "mongodb+srv://127.0.0.1:27017/forex?retryWrites=false&w=majority&appName=Cluster0"
    )


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql+asyncpg://postgres:postgres@localhost/forex",
        "postgressql+asyncpg://postgres:postgres@localhost/forex",
    ],
)
def test_postgres_asyncpg_urls_are_normalised(dsn: str) -> None:
    info = DatabaseConnectionInfo.from_url(dsn)

    assert info.backend is DatabaseBackend.POSTGRES
    assert info.url.startswith("postgresql://postgres:postgres@localhost/forex")


def test_database_connection_info_uses_database_name_query_parameter() -> None:
    info = DatabaseConnectionInfo.from_url("mysql://localhost/?DATABASE_NAME=forex")

    assert info.backend is DatabaseBackend.MYSQL
    assert info.name == "forex"
    assert info.url.startswith("mysql://localhost/forex")


def test_fx_bharat_accepts_prebuilt_config() -> None:
    config = DatabaseConnectionInfo.from_url("mongodb://example.com:27017/fx")

    fx_bharat = FxBharat(db_config=config)

    assert fx_bharat.connection_info is config
    assert fx_bharat.connection_info.backend is DatabaseBackend.MONGODB


@pytest.fixture()
def sqlite_fx(tmp_path: Path) -> FxBharat:
    db_path = tmp_path / "forex.db"
    config = DatabaseConnectionInfo(
        backend=DatabaseBackend.SQLITE,
        url=f"sqlite:///{db_path}",
        name=str(db_path),
        username=None,
        password=None,
        host=None,
        port=None,
    )
    return FxBharat(db_config=config)


def test_connection_reports_success_for_sqlite() -> None:
    fx_bharat = FxBharat()

    success, error = fx_bharat.connection()

    assert success is True
    assert error is None


def test_connection_reports_failure_for_unreachable_external_db() -> None:
    external = FxBharat(db_config="postgres://user:pass@db.example.com:5432/forex")

    success, error = external.connection()

    assert success is False
    assert isinstance(error, str)
    assert error


def test_connection_reports_missing_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    external = FxBharat(db_config="postgres://user:pass@db.example.com:5432/forex")

    def _missing_driver(*_: Any, **__: Any) -> None:
        raise ModuleNotFoundError("psycopg2")

    monkeypatch.setattr("fx_bharat.create_engine", _missing_driver)

    success, error = external.connection()

    assert success is False
    assert "psycopg2" in (error or "")
    assert "pip install psycopg2-binary" in (error or "")


def test_connection_uses_pymongo_for_mongodb(monkeypatch: pytest.MonkeyPatch) -> None:
    external = FxBharat(db_config="mongodb://user:pass@cluster0.example.com:27017/forex")

    class DummyClient:
        def __init__(self, url: str, serverSelectionTimeoutMS: int) -> None:  # noqa: N803
            assert url.startswith("mongodb://")
            assert serverSelectionTimeoutMS == 5000
            self.closed = False
            self.admin = self

        def command(self, name: str) -> None:
            assert name == "ping"

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr("fx_bharat.MongoClient", DummyClient)

    success, error = external.connection()

    assert success is True
    assert error is None


def test_mongodb_connection_reports_missing_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    external = FxBharat(db_config="mongodb://user:pass@cluster0.example.com:27017/forex")

    monkeypatch.setattr("fx_bharat.MongoClient", None)

    success, error = external.connection()

    assert success is False
    assert error is not None
    assert "pymongo" in error


@pytest.mark.parametrize(
    "url, attr_name, expected_kwargs",
    [
        ("postgresql://user:pass@db.example.com:5432/forex", "PostgresBackend", {}),
        ("mysql://user:pass@db.example.com:3306/forex", "MySQLBackend", {}),
        (
            "mongodb://user:pass@db.example.com:27017/forex",
            "MongoBackend",
            {"database": "forex"},
        ),
    ],
)
def test_build_external_backend_instantiates_strategy(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    attr_name: str,
    expected_kwargs: dict[str, str],
) -> None:
    captured: dict[str, object] = {}

    class DummyStrategy:
        def __init__(self, url: str, **kwargs: object) -> None:
            captured["url"] = url
            captured["kwargs"] = kwargs

    monkeypatch.setattr(f"fx_bharat.{attr_name}", DummyStrategy)
    fx_bharat = FxBharat(db_config=url)

    strategy = fx_bharat._build_external_backend()

    assert isinstance(strategy, DummyStrategy)
    assert captured["url"] == fx_bharat.connection_info.url
    assert captured["kwargs"] == expected_kwargs


def test_get_backend_strategy_raises_for_uninitialised_sqlite() -> None:
    fx_bharat = FxBharat()
    fx_bharat._backend_strategy = None

    with pytest.raises(RuntimeError):
        fx_bharat._get_backend_strategy()


def test_get_backend_strategy_caches_external_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    fx_bharat = FxBharat(db_config="postgresql://user:pass@db:5432/forex")
    sentinel = object()

    calls = {"count": 0}

    def _fake_build(self: FxBharat) -> object:  # noqa: ANN001
        calls["count"] += 1
        return sentinel

    monkeypatch.setattr(FxBharat, "_build_external_backend", _fake_build)

    first = fx_bharat._get_backend_strategy()
    second = fx_bharat._get_backend_strategy()

    assert first is second is sentinel
    assert calls["count"] == 1


def test_uses_inhouse_sqlite_reports_backend() -> None:
    sqlite_app = FxBharat()
    mysql_app = FxBharat(db_config="mysql://user:pass@db.example.com:3306/forex")

    assert sqlite_app.uses_inhouse_sqlite() is True
    assert mysql_app.uses_inhouse_sqlite() is False


def test_mongodb_backend_uses_bulk_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = MongoBackend.__new__(MongoBackend)

    class DummyUpdateOne:
        def __init__(
            self, filter: dict[str, str], update: dict[str, dict[str, Any]], *, upsert: bool
        ) -> None:
            self.filter = filter
            self.update = update
            self.upsert = upsert

    monkeypatch.setattr("fx_bharat.db.mongo_backend.UpdateOne", DummyUpdateOne)

    class DummyCollection:
        def __init__(self) -> None:
            self.calls: list[tuple[list[object], bool]] = []

        def bulk_write(self, operations: list[object], ordered: bool) -> object:  # noqa: ANN401
            self.calls.append((operations, ordered))

            class Result:
                upserted_count = 0
                modified_count = 0

            return Result()

    dummy_collection = DummyCollection()
    backend._collection = dummy_collection  # type: ignore[attr-defined]
    rows = [
        ForexRateRecord(rate_date=date(2020, 1, 1), currency="USD", rate=1.0, source="RBI"),
        ForexRateRecord(rate_date=date(2020, 1, 2), currency="USD", rate=1.1, source="RBI"),
    ]

    result = backend.insert_rates(rows)

    assert len(dummy_collection.calls) == 1
    operations, ordered = dummy_collection.calls[0]
    assert ordered is False
    assert len(operations) == len(rows)
    assert result.inserted == len(rows)


def test_conection_alias_routes_to_connection() -> None:
    fx_bharat = FxBharat()

    assert fx_bharat.conection() == fx_bharat.connection()


def _seed_sample_rates(fx: FxBharat) -> None:
    assert fx.sqlite_manager is not None
    fx.sqlite_manager.insert_rates(
        [
            ForexRateRecord(rate_date=date(2023, 1, 1), currency="USD", rate=82.0),
            ForexRateRecord(rate_date=date(2023, 1, 1), currency="EUR", rate=88.0),
            ForexRateRecord(rate_date=date(2023, 1, 2), currency="USD", rate=83.0),
            ForexRateRecord(rate_date=date(2023, 1, 8), currency="USD", rate=84.0),
            ForexRateRecord(rate_date=date(2023, 1, 8), currency="EUR", rate=90.0),
            ForexRateRecord(rate_date=date(2023, 2, 5), currency="USD", rate=85.0),
            ForexRateRecord(rate_date=date(2023, 2, 5), currency="EUR", rate=92.0),
        ]
    )


def test_rate_returns_latest_snapshot(sqlite_fx: FxBharat) -> None:
    _seed_sample_rates(sqlite_fx)

    snapshot = sqlite_fx.rate()

    assert snapshot["rate_date"] == date(2023, 2, 5)
    assert snapshot["base_currency"] == "INR"
    assert snapshot["source"] == "RBI"
    assert snapshot["rates"] == {"EUR": 92.0, "USD": 85.0}


def test_rate_supports_specific_date(sqlite_fx: FxBharat) -> None:
    _seed_sample_rates(sqlite_fx)

    snapshot = sqlite_fx.rate(date(2023, 1, 2))

    assert snapshot["rate_date"] == date(2023, 1, 2)
    assert snapshot["source"] == "RBI"
    assert snapshot["rates"] == {"USD": 83.0}


def test_rate_filters_by_source(sqlite_fx: FxBharat) -> None:
    _seed_sample_rates(sqlite_fx)
    assert sqlite_fx.sqlite_manager is not None
    sqlite_fx.sqlite_manager.insert_rates(
        [
            ForexRateRecord(rate_date=date(2023, 3, 1), currency="USD", rate=90.0, source="SBI"),
            ForexRateRecord(rate_date=date(2023, 3, 1), currency="EUR", rate=95.0, source="SBI"),
        ]
    )

    snapshot = sqlite_fx.rate(source="SBI")

    assert snapshot["rate_date"] == date(2023, 3, 1)
    assert snapshot["source"] == "SBI"
    assert snapshot["rates"] == {"EUR": 95.0, "USD": 90.0}


def test_rate_rejects_dates_before_rbi_minimum(sqlite_fx: FxBharat) -> None:
    with pytest.raises(ValueError, match="RBI do not provide the data before 12/04/2022"):
        sqlite_fx.rate(date(2022, 4, 11))


def test_history_supports_frequency(sqlite_fx: FxBharat) -> None:
    _seed_sample_rates(sqlite_fx)

    monthly = sqlite_fx.history(date(2023, 1, 1), date(2023, 2, 28), frequency="monthly")

    assert [entry["rate_date"] for entry in monthly] == [
        date(2023, 1, 8),
        date(2023, 2, 5),
    ]
    assert monthly[0]["rates"]["USD"] == 84.0
    assert monthly[1]["rates"]["EUR"] == 92.0


def test_history_validate_inputs(sqlite_fx: FxBharat) -> None:
    with pytest.raises(ValueError):
        sqlite_fx.history(date(2023, 2, 1), date(2023, 1, 1))
    with pytest.raises(ValueError):
        sqlite_fx.history(date(2023, 1, 1), date(2023, 1, 2), frequency="hourly")  # type: ignore[arg-type]


def test_history_rejects_requests_before_rbi_minimum(sqlite_fx: FxBharat) -> None:
    with pytest.raises(ValueError, match="RBI do not provide the data before 12/04/2022"):
        sqlite_fx.history(date(2022, 4, 11), date(2022, 4, 20))


def test_migrate_requires_external_backend(sqlite_fx: FxBharat) -> None:
    with pytest.raises(ValueError):
        sqlite_fx.migrate()


def test_database_connection_info_uses_database_name_query() -> None:
    info = DatabaseConnectionInfo.from_url(
        "postgres://user:pwd@db.example.com:5432/?DATABASE_NAME=forex&sslmode=prefer",
    )

    assert info.name == "forex"
    assert "DATABASE_NAME" not in info.url
    assert info.url.endswith("/forex?sslmode=prefer")


def test_fx_bharat_seed_delegates_to_populate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "seed.db"
    config = DatabaseConnectionInfo(
        backend=DatabaseBackend.SQLITE,
        url=f"sqlite:///{db_path}",
        name=str(db_path),
        username=None,
        password=None,
        host=None,
        port=None,
    )
    fx = FxBharat(db_config=config)

    called: dict[str, object] = {}

    def _fake_seed(from_str: str, to_str: str, *, db_path: Path) -> None:
        called["args"] = (from_str, to_str, Path(db_path))

    monkeypatch.setattr("fx_bharat.seeds.populate_rbi_forex.seed_rbi_forex", _fake_seed)

    fx.seed(date(2023, 1, 1), date(2023, 1, 2))

    assert called["args"][0] == "2023-01-01"
    assert called["args"][2] == db_path


def test_seed_rejects_requests_before_rbi_minimum(sqlite_fx: FxBharat) -> None:
    with pytest.raises(ValueError, match="RBI do not provide the data before 12/04/2022"):
        sqlite_fx.seed(date(2022, 4, 11), date(2022, 4, 20))


def test_seed_mirrors_rows_to_external_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    external = FxBharat(db_config="postgres://user:pwd@db:5432/fx")

    called: dict[str, object] = {}

    def _fake_seed(from_str: str, to_str: str, *, db_path: Path) -> None:
        called["range"] = (from_str, to_str)
        called["db_path"] = Path(db_path)

    monkeypatch.setattr("fx_bharat.seeds.populate_rbi_forex.seed_rbi_forex", _fake_seed)

    created_sqlite_backends: list[object] = []

    class DummySQLiteBackend:
        def __init__(self, db_path: Path | str, manager=None):  # type: ignore[no-untyped-def]
            self.db_path = Path(db_path)
            self.manager = manager
            self.fetch_called_with: tuple[date | None, date | None, str | None] | None = None
            self.closed = False
            self.rows = [
                ForexRateRecord(rate_date=date(2023, 1, 1), currency="USD", rate=82.1),
                ForexRateRecord(rate_date=date(2023, 1, 2), currency="EUR", rate=90.2),
            ]
            created_sqlite_backends.append(self)

        def fetch_range(self, start=None, end=None, source=None):  # type: ignore[no-untyped-def]
            self.fetch_called_with = (start, end, source)
            return list(self.rows)

        def close(self) -> None:
            self.closed = True

    class DummyExternalBackend:
        def __init__(self) -> None:
            self.rows: list[ForexRateRecord] = []
            self.ensure_called = 0

        def ensure_schema(self) -> None:
            self.ensure_called += 1

        def insert_rates(self, rows):  # type: ignore[no-untyped-def]
            self.rows.extend(rows)

    monkeypatch.setattr("fx_bharat.SQLiteBackend", DummySQLiteBackend)
    external._backend_strategy = DummyExternalBackend()

    external.seed(date(2023, 1, 1), date(2023, 1, 2))

    assert called["range"] == ("2023-01-01", "2023-01-02")
    assert isinstance(called["db_path"], Path)
    assert created_sqlite_backends
    sqlite_backend = created_sqlite_backends[0]
    assert sqlite_backend.fetch_called_with == (date(2023, 1, 1), date(2023, 1, 2), "RBI")
    assert sqlite_backend.closed is True

    backend = external._backend_strategy
    assert isinstance(backend, DummyExternalBackend)
    assert backend.ensure_called == 1
    assert [row.currency for row in backend.rows] == ["USD", "EUR"]


def test_migrate_copies_rows_to_external_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummySQLiteBackend:
        def __init__(self, path: Path) -> None:
            self.path = path
            self.closed = False

        def fetch_range(self):  # type: ignore[no-untyped-def]
            return [
                ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.5),
                ForexRateRecord(rate_date=date(2024, 1, 2), currency="EUR", rate=89.1),
            ]

        def close(self) -> None:
            self.closed = True

    class DummyExternalBackend:
        def __init__(self) -> None:
            self.rows: list[ForexRateRecord] = []
            self.ensure_called = 0

        def ensure_schema(self) -> None:
            self.ensure_called += 1

        def insert_rates(self, rows):  # type: ignore[no-untyped-def]
            self.rows.extend(rows)

    monkeypatch.setattr("fx_bharat.SQLiteBackend", DummySQLiteBackend)

    external = FxBharat(db_config="mysql://user:pwd@localhost:3306/fx")
    external._backend_strategy = DummyExternalBackend()

    external.migrate()

    backend = external._backend_strategy
    assert isinstance(backend, DummyExternalBackend)
    assert backend.ensure_called == 1
    assert len(backend.rows) == 2


def test_select_snapshot_dates_supports_all_frequencies() -> None:
    dates = [
        date(2024, 1, 1),
        date(2024, 1, 5),
        date(2024, 1, 12),
        date(2024, 2, 1),
        date(2025, 1, 1),
    ]

    weekly = FxBharat._select_snapshot_dates(dates, "weekly")
    monthly = FxBharat._select_snapshot_dates(dates, "monthly")
    yearly = FxBharat._select_snapshot_dates(dates, "yearly")

    assert len(weekly) == 4
    assert monthly == [date(2024, 1, 12), date(2024, 2, 1), date(2025, 1, 1)]
    assert yearly == [date(2024, 2, 1), date(2025, 1, 1)]
