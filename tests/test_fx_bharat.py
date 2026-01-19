"""Core facade tests aligned to the 0.4.x API."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from fx_bharat import DatabaseBackend, DatabaseConnectionInfo, FxBharat, __version__
from fx_bharat.db.sqlite_backend import SQLiteBackend
from fx_bharat.ingestion.models import ForexRateRecord
from fx_bharat.utils.rbi import RBI_MIN_AVAILABLE_DATE


def test_fx_bharat_class_is_exposed() -> None:
    assert FxBharat.__version__ == __version__


def test_fx_bharat_requires_db_config() -> None:
    with pytest.raises(ValueError):
        FxBharat()


@pytest.mark.parametrize(
    "scheme, backend",
    [
        ("postgresql", DatabaseBackend.POSTGRES),
        ("postgres", DatabaseBackend.POSTGRES),
        ("mysql+pymysql", DatabaseBackend.MYSQL),
        ("sqlite", DatabaseBackend.SQLITE),
        ("mongodb+srv", DatabaseBackend.MONGODB),
    ],
)
def test_database_backend_from_scheme_handles_aliases(
    scheme: str, backend: DatabaseBackend
) -> None:
    assert DatabaseBackend.from_scheme(scheme) is backend


def test_database_connection_info_from_url_and_query_name() -> None:
    info = DatabaseConnectionInfo.from_url(
        "postgres://user:pwd@db.example.com:5432/?DATABASE_NAME=forex&sslmode=prefer",
    )

    assert info.backend is DatabaseBackend.POSTGRES
    assert info.name == "forex"
    assert info.url.startswith("postgresql://user:pwd@db.example.com:5432/forex")
    assert "DATABASE_NAME" not in info.url


def test_connection_probe_missing_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    fx = FxBharat(db_config="postgres://user:pass@db.example.com:5432/forex")
    monkeypatch.setattr("fx_bharat.create_engine", None)
    monkeypatch.setattr("fx_bharat.text", None)

    ok, message = fx.connection()
    assert ok is False
    assert "SQLAlchemy is required" in (message or "")


def test_normalise_source_filter_invalid() -> None:
    with pytest.raises(ValueError):
        FxBharat._normalise_source_filter("gold")


def test_select_snapshot_dates_grouping() -> None:
    dates = [
        date(2024, 1, 1),
        date(2024, 1, 7),
        date(2024, 1, 8),
        date(2024, 2, 1),
        date(2025, 1, 1),
    ]
    assert FxBharat._select_snapshot_dates(dates, "monthly")[-1] == date(2025, 1, 1)
    assert FxBharat._select_snapshot_dates(dates, "yearly")[-1] == date(2025, 1, 1)


def test_rate_and_history_with_sqlite_backend(tmp_path) -> None:
    fx = FxBharat(db_config=f"sqlite:///{tmp_path/'fx.db'}")
    backend = fx._get_backend_strategy()
    backend.ensure_schema()
    backend.insert_rates(
        [
            ForexRateRecord(rate_date=date(2024, 4, 1), currency="USD", rate=83.0, source="RBI"),
            ForexRateRecord(rate_date=date(2024, 4, 1), currency="USD", rate=82.5, source="SBI"),
            ForexRateRecord(rate_date=date(2024, 4, 2), currency="USD", rate=84.0, source="RBI"),
        ]
    )

    snapshots = fx.rate()
    assert [snap["source"] for snap in snapshots] == ["SBI", "RBI"]
    assert snapshots[0]["rate_date"] == date(2024, 4, 1)

    history = fx.history(date(2024, 4, 1), date(2024, 4, 2))
    assert len(history) == 3
    assert {snap["source"] for snap in history} == {"SBI", "RBI"}


def test_rate_rejects_dates_before_rbi_minimum(tmp_path) -> None:
    fx = FxBharat(db_config=f"sqlite:///{tmp_path/'fx.db'}")
    with pytest.raises(ValueError):
        fx.rate(RBI_MIN_AVAILABLE_DATE - timedelta(days=1))


def test_seed_invalid_range_raises(tmp_path) -> None:
    fx = FxBharat(db_config=f"sqlite:///{tmp_path/'fx.db'}")
    with pytest.raises(ValueError):
        fx.seed(from_date=date(2024, 1, 2), to_date=date(2024, 1, 1))


def test_build_external_backend_instantiates_strategy(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class DummyStrategy:
        def __init__(self, url: str) -> None:
            captured["url"] = url

    monkeypatch.setattr("fx_bharat.PostgresBackend", DummyStrategy)
    fx = FxBharat(db_config="postgresql://user:pass@db:5432/forex")

    strategy = fx._build_external_backend()

    assert isinstance(strategy, DummyStrategy)
    assert captured["url"] == fx.connection_info.url


def test_seed_lme_wrappers_delegate(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"copper": False, "aluminum": False}

    def _stub_prices(metal: str, **_kwargs):
        called[metal.lower()] = True
        return "ok"

    monkeypatch.setattr("fx_bharat.seeds.populate_lme.seed_lme_prices", _stub_prices)

    import fx_bharat as pkg

    assert pkg.seed_lme_copper() == "ok"
    assert pkg.seed_lme_aluminum() == "ok"
    assert called == {"copper": True, "aluminum": True}
