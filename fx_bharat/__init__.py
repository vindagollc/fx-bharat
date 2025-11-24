"""Public interface for the fx_bharat package."""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Literal
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

from fx_bharat.db import DEFAULT_SQLITE_DB_PATH
from fx_bharat.db.base_backend import BackendStrategy
from fx_bharat.db.mongo_backend import MongoBackend
from fx_bharat.db.mysql_backend import MySQLBackend
from fx_bharat.db.postgres_backend import PostgresBackend
from fx_bharat.db.sqlite_backend import SQLiteBackend
from fx_bharat.db.sqlite_manager import PersistenceResult, SQLiteManager
from fx_bharat.ingestion.models import ForexRateRecord
from fx_bharat.ingestion.models import LmeRateRecord
from fx_bharat.utils.rbi import RBI_MIN_AVAILABLE_DATE, enforce_rbi_min_date

try:  # pragma: no cover - imported lazily
    from sqlalchemy import create_engine, text
except ModuleNotFoundError:  # pragma: no cover - dependency missing at runtime
    create_engine = None  # type: ignore[misc,assignment]
    text = None  # type: ignore[misc,assignment]

try:  # pragma: no cover - imported lazily
    from pymongo import MongoClient
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    MongoClient = None  # type: ignore[misc, assignment]

__all__ = [
    "__version__",
    "DatabaseBackend",
    "DatabaseConnectionInfo",
    "FxBharat",
    "seed_rbi_forex",
    "seed_sbi_forex",
    "seed_lme_prices",
    "seed_lme_copper",
    "seed_lme_aluminum",
    "SQLiteManager",
    "PersistenceResult",
    "RBISeleniumClient",
]

try:
    __version__ = importlib_metadata.version("fx-bharat")
except importlib_metadata.PackageNotFoundError:  # pragma: no cover - fallback for local runs
    __version__ = "0.3.0"


def seed_rbi_forex(*args, **kwargs):
    from fx_bharat.seeds.populate_rbi_forex import seed_rbi_forex as _seed_rbi_forex

    return _seed_rbi_forex(*args, **kwargs)


def seed_sbi_forex(*args, **kwargs):
    from fx_bharat.seeds.populate_sbi_forex import seed_sbi_forex as _seed_sbi_forex

    return _seed_sbi_forex(*args, **kwargs)


def seed_sbi_historical(*args, **kwargs):
    from fx_bharat.seeds.populate_sbi_forex import seed_sbi_historical as _seed_sbi_historical

    return _seed_sbi_historical(*args, **kwargs)


def seed_sbi_today(*args, **kwargs):
    from fx_bharat.seeds.populate_sbi_forex import seed_sbi_today as _seed_sbi_today

    return _seed_sbi_today(*args, **kwargs)


def seed_lme_prices(*args, **kwargs):
    from fx_bharat.seeds.populate_lme import seed_lme_prices as _seed_lme_prices

    return _seed_lme_prices(*args, **kwargs)


def seed_lme_copper(*args, **kwargs):
    from fx_bharat.seeds.populate_lme import seed_lme_copper as _seed_lme_copper

    return _seed_lme_copper(*args, **kwargs)


def seed_lme_aluminum(*args, **kwargs):
    from fx_bharat.seeds.populate_lme import seed_lme_aluminum as _seed_lme_aluminum

    return _seed_lme_aluminum(*args, **kwargs)


class DatabaseBackend(str, Enum):
    """Supported database engines for FxBharat."""

    SQLITE = "sqlite"
    MYSQL = "mysql"
    POSTGRES = "postgres"
    MONGODB = "mongodb"

    @classmethod
    def resolve_backend_and_scheme(cls, scheme: str) -> tuple["DatabaseBackend", str]:
        """Return backend enum + canonical scheme used in connection URLs."""

        if not scheme:
            raise ValueError("DB_URL must include a scheme (e.g. mysql:// or postgres://)")
        scheme_lower = scheme.lower()
        base_scheme, _, driver = scheme_lower.partition("+")
        if base_scheme in {"postgresql", "postgres", "postgressql"}:
            return cls.POSTGRES, "postgresql"
        if base_scheme == "sqlite":
            return cls.SQLITE, "sqlite"
        if base_scheme == "mysql":
            # Preserve optional driver hints such as ``mysql+pymysql``.
            canonical_scheme = scheme_lower if driver else "mysql"
            return cls.MYSQL, canonical_scheme
        if base_scheme == "mongodb":
            # Keep srv-style schemes intact so pymongo can route via DNS.
            canonical_scheme = scheme_lower if driver else "mongodb"
            return cls.MONGODB, canonical_scheme
        raise ValueError(
            "Unsupported database backend. Supported values are SQLite, MySQL, "
            "Postgres, and MongoDB."
        )

    @classmethod
    def from_scheme(cls, scheme: str) -> "DatabaseBackend":
        """Normalise URL schemes into a DatabaseBackend value."""

        backend, _ = cls.resolve_backend_and_scheme(scheme)
        return backend


@dataclass(slots=True)
class DatabaseConnectionInfo:
    """Represents how FxBharat should talk to the persistence layer."""

    backend: DatabaseBackend
    url: str
    name: str | None
    username: str | None
    password: str | None
    host: str | None
    port: int | None

    @classmethod
    def from_url(cls, url: str) -> "DatabaseConnectionInfo":
        """Create a connection object by parsing a database URL/DSN."""

        cleaned_url, query_db_name = cls._normalise_database_name_parameter(url)
        parsed = urlparse(cleaned_url)
        if not parsed.scheme:
            raise ValueError("DB_URL must include a scheme (e.g. mysql:// or postgres://)")
        backend, canonical_scheme = DatabaseBackend.resolve_backend_and_scheme(parsed.scheme)
        if parsed.scheme != canonical_scheme:
            parsed = parsed._replace(scheme=canonical_scheme)
            cleaned_url = urlunparse(parsed)
        resolved_port = parsed.port
        resolved_name = parsed.path[1:] if parsed.path and parsed.path != "/" else None
        if not resolved_name:
            resolved_name = query_db_name

        return cls(
            backend=backend,
            url=cleaned_url,
            name=resolved_name,
            username=parsed.username,
            password=parsed.password,
            host=parsed.hostname,
            port=resolved_port,
        )

    @staticmethod
    def _normalise_database_name_parameter(url: str) -> tuple[str, str | None]:
        """Support custom ``DATABASE_NAME`` query parameters used in examples."""

        # Some callers append ``DATABASE_NAME=foo`` without an ``&`` delimiter. Make
        # sure that parameter is parsed as its own key/value pair.
        patched_url = re.sub(
            r"(?i)(?<![?&])DATABASE_NAME=",
            "&DATABASE_NAME=",
            url,
        )
        parsed = urlparse(patched_url)
        query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        remaining_pairs: list[tuple[str, str]] = []
        database_name: str | None = None
        for key, value in query_pairs:
            if key.lower() == "database_name":
                if value:
                    database_name = value
                # Strip the custom parameter so PyMongo/SQLAlchemy don't error on it.
                continue
            remaining_pairs.append((key, value))

        new_path = parsed.path
        if (not new_path or new_path == "/") and database_name:
            new_path = f"/{database_name}"

        new_query = urlencode(remaining_pairs, doseq=True)
        cleaned = parsed._replace(query=new_query, path=new_path)
        return urlunparse(cleaned), database_name

    @property
    def is_sqlite(self) -> bool:
        """Return True when the in-house SQLite database should be used."""

        return self.backend is DatabaseBackend.SQLITE

    @property
    def is_external(self) -> bool:
        """Return True for MySQL/Postgres/MongoDB backends."""

        return not self.is_sqlite


class FxBharat:
    """Package facade that centralises DB configuration."""

    __slots__ = ("connection_info", "sqlite_manager", "backend", "_backend_strategy")

    _DRIVER_HINTS: dict[DatabaseBackend, str] = {
        DatabaseBackend.POSTGRES: "Install psycopg2 or psycopg2-binary via 'pip install psycopg2-binary'.",
        DatabaseBackend.MYSQL: "Install mysqlclient or PyMySQL via 'pip install mysqlclient' or 'pip install PyMySQL'.",
        DatabaseBackend.MONGODB: "Install pymongo via 'pip install pymongo'.",
    }

    # Provide direct access to the package version as a class attribute.
    __version__ = __version__

    def __init__(
        self,
        db_config: DatabaseConnectionInfo | str | None = None,
    ) -> None:
        """Configure how the package should persist data.

        Callers can supply either a fully fledged ``DatabaseConnectionInfo``
        object or a DSN string (``mysql://user:pwd@host/db``). When the argument
        is omitted the FxBharat instance automatically falls back to the bundled
        SQLite database so callers can be productive without additional
        infrastructure. Providing a DSN switches the connection over to an
        external database (MySQL, Postgres, MongoDB, etc.).
        """

        self.connection_info = self._build_connection_info(
            db_config=db_config,
        )
        self.backend = self.connection_info.backend.value
        self.sqlite_manager: SQLiteManager | None = None
        self._backend_strategy: BackendStrategy | None = None
        self._initialise_backend()

    @staticmethod
    def _build_connection_info(
        *,
        db_config: DatabaseConnectionInfo | str | None,
    ) -> DatabaseConnectionInfo:
        if isinstance(db_config, DatabaseConnectionInfo):
            return db_config
        if isinstance(db_config, str):
            return DatabaseConnectionInfo.from_url(db_config)
        # Default to SQLite with a sensible on-disk database name.
        sqlite_name = str(DEFAULT_SQLITE_DB_PATH)
        sqlite_url = f"sqlite:///{quote(DEFAULT_SQLITE_DB_PATH.as_posix(), safe='/:')}"
        return DatabaseConnectionInfo(
            backend=DatabaseBackend.SQLITE,
            url=sqlite_url,
            name=sqlite_name,
            username=None,
            password=None,
            host=None,
            port=None,
        )

    def _initialise_backend(self) -> None:
        backend = self.connection_info.backend
        if backend is DatabaseBackend.SQLITE:
            db_path = Path(self.connection_info.name or DEFAULT_SQLITE_DB_PATH)
            manager = SQLiteManager(db_path)
            self.sqlite_manager = manager
            self._backend_strategy = SQLiteBackend(db_path=db_path, manager=manager)
        else:
            self.sqlite_manager = None

    def _build_external_backend(self) -> BackendStrategy:
        backend = self.connection_info.backend
        if backend is DatabaseBackend.POSTGRES:
            return PostgresBackend(self.connection_info.url)
        if backend is DatabaseBackend.MYSQL:
            return MySQLBackend(self.connection_info.url)
        if backend is DatabaseBackend.MONGODB:
            return MongoBackend(self.connection_info.url, database=self.connection_info.name)
        raise ValueError(f"Unsupported backend: {backend}")

    def _get_backend_strategy(self) -> BackendStrategy:
        if self._backend_strategy is None:
            if self.connection_info.is_sqlite:
                raise RuntimeError("SQLite backend strategy should have been initialised already")
            self._backend_strategy = self._build_external_backend()
        return self._backend_strategy

    def uses_inhouse_sqlite(self) -> bool:
        """Public helper that reveals whether SQLite is being used."""

        return self.connection_info.is_sqlite

    def migrate(self) -> None:
        """Migrate the bundled SQLite data into the configured external backend."""

        if self.connection_info.is_sqlite:
            raise ValueError("Migration only supported for external databases.")
        source_backend = SQLiteBackend(DEFAULT_SQLITE_DB_PATH)
        try:
            rows = source_backend.fetch_range()
            lme_fetcher = getattr(source_backend, "fetch_lme_range", None)
            lme_copper = lme_fetcher("COPPER") if callable(lme_fetcher) else []
            lme_aluminum = lme_fetcher("ALUMINUM") if callable(lme_fetcher) else []
        finally:
            source_backend.close()
        target_backend = self._get_backend_strategy()
        target_backend.ensure_schema()
        target_backend.insert_rates(rows)
        for metal, lme_rows in {
            "COPPER": lme_copper,
            "ALUMINUM": lme_aluminum,
        }.items():
            if lme_rows:
                target_backend.insert_lme_rates(metal, lme_rows)

    def seed(
        self,
        from_date: date | None = None,
        to_date: date | None = None,
        *,
        source: Literal["RBI", "SBI", None] = None,
        resource_dir: str | Path | None = None,
        incremental: bool = True,
        dry_run: bool = False,
    ) -> None:
        """Seed forex data into SQLite and mirror into external backends."""

        if from_date and to_date and from_date > to_date:
            raise ValueError("from_date must be on or before to_date")

        today = date.today()
        sqlite_db_path = (
            Path(self.sqlite_manager.db_path)
            if self.sqlite_manager is not None
            else DEFAULT_SQLITE_DB_PATH
        )

        target_sources = self._normalise_source_filter(source.lower() if source else None)
        resolved_to = to_date or today
        user_range_specified = from_date is not None or to_date is not None
        mirror_windows: list[tuple[date | None, date | None, str | None]] = []

        for current_source in target_sources:
            if current_source == "RBI":
                start_date = from_date or RBI_MIN_AVAILABLE_DATE
                end_date = resolved_to
                enforce_rbi_min_date(start_date, end_date)
                seed_rbi_forex(
                    start_date.isoformat(),
                    end_date.isoformat(),
                    db_path=sqlite_db_path,
                    incremental=incremental if not user_range_specified else False,
                    dry_run=dry_run,
                )
                checkpoint = self._get_ingestion_checkpoint(sqlite_db_path, "RBI")
                mirror_start = from_date or (
                    checkpoint + timedelta(days=1) if checkpoint else start_date
                )
                mirror_windows.append((mirror_start, end_date, "RBI"))
                continue

            include_today = resolved_to >= today
            historical_end = today - timedelta(days=1) if include_today else resolved_to
            sbi_start_date = from_date
            if historical_end >= (sbi_start_date or historical_end):
                seed_sbi_historical(
                    db_path=sqlite_db_path,
                    resource_dir=resource_dir or Path("resources"),
                    start=sbi_start_date,
                    end=historical_end,
                    download=False,
                    incremental=incremental if not user_range_specified else False,
                    dry_run=dry_run,
                )
            if include_today:
                seed_sbi_today(
                    db_path=sqlite_db_path,
                    resource_dir=resource_dir or Path("resources"),
                    dry_run=dry_run,
                )
            checkpoint = self._get_ingestion_checkpoint(sqlite_db_path, "SBI")
            mirror_start = from_date or (
                checkpoint + timedelta(days=1) if checkpoint else (sbi_start_date or historical_end)
            )
            mirror_windows.append((mirror_start, resolved_to, "SBI"))

        if dry_run:
            return None

        if self.connection_info.is_external:
            sqlite_backend = SQLiteBackend(db_path=sqlite_db_path)
            try:
                rows: list[ForexRateRecord] = []
                for start, end, src in mirror_windows:
                    rows.extend(sqlite_backend.fetch_range(start, end, source=src))
            finally:
                sqlite_backend.close()

            target_backend = self._get_backend_strategy()
            target_backend.ensure_schema()
            target_backend.insert_rates(rows)

    def seed_lme(
        self,
        metal: Literal["COPPER", "ALUMINUM"],
        *,
        from_date: date | None = None,
        to_date: date | None = None,
        dry_run: bool = False,
    ) -> PersistenceResult:
        """Seed LME prices into SQLite and mirror to an external backend if configured."""

        sqlite_db_path = (
            Path(self.sqlite_manager.db_path)
            if self.sqlite_manager is not None
            else DEFAULT_SQLITE_DB_PATH
        )
        seed_result = seed_lme_prices(
            metal,
            db_path=sqlite_db_path,
            start=from_date,
            end=to_date,
            dry_run=dry_run,
        )
        if dry_run:
            return seed_result.rows

        if self.connection_info.is_external:
            backend = self._get_backend_strategy()
            backend.ensure_schema()
            with SQLiteManager(sqlite_db_path) as manager:
                lme_rows = manager.fetch_lme_range(metal, from_date, to_date)
            backend.insert_lme_rates(metal, lme_rows)
        return seed_result.rows

    def rate(
        self,
        rate_date: date | None = None,
        *,
        source_filter: Literal["rbi", "sbi", None] = None,
    ) -> List[Dict[str, Any]]:
        """Return a forex rate snapshot for ``rate_date`` or the latest entry."""

        backend = self._get_backend_strategy()
        snapshots: List[Dict[str, Any]] = []
        sources = self._normalise_source_filter(source_filter)

        for source in sources:
            if rate_date is not None and source == "RBI":
                enforce_rbi_min_date(rate_date)
            rows = (
                backend.fetch_range(rate_date, rate_date, source=source)
                if rate_date is not None
                else backend.fetch_range(source=source)
            )
            snapshot = self._latest_snapshot_from_rows(rows, rate_date, source)
            if snapshot:
                snapshots.append(snapshot)

        return sorted(
            snapshots,
            key=lambda snap: (0 if snap["source"] == "SBI" else 1, snap["rate_date"]),
        )

    def history(
        self,
        from_date: date,
        to_date: date,
        frequency: Literal["daily", "weekly", "monthly", "yearly"] = "daily",
        *,
        source_filter: Literal["rbi", "sbi", None] = None,
    ) -> List[Dict[str, Any]]:
        """Return forex rate snapshots within ``from_date``/``to_date``.

        ``frequency`` controls the aggregation granularity. Weekly/monthly/yearly
        buckets always return the latest snapshot in each interval.
        """

        if from_date > to_date:
            raise ValueError("from_date must not be after to_date")
        freq = frequency.lower()
        if freq not in {"daily", "weekly", "monthly", "yearly"}:
            raise ValueError("frequency must be one of: daily, weekly, monthly, yearly")
        snapshots: List[Dict[str, Any]] = []
        backend = self._get_backend_strategy()

        for source in self._normalise_source_filter(source_filter):
            if source == "RBI":
                enforce_rbi_min_date(from_date, to_date)
            rows = backend.fetch_range(from_date, to_date, source=source)
            grouped = self._group_rows_by_date(rows)
            if not grouped:
                continue
            sorted_dates = sorted(grouped.keys())
            selected = self._select_snapshot_dates(sorted_dates, freq)
            snapshots.extend(
                [self._snapshot_payload(day, grouped[day], source) for day in selected]
            )

        return sorted(
            snapshots,
            key=lambda snap: (0 if snap["source"] == "SBI" else 1, snap["rate_date"]),
        )

    def historical(
        self,
        from_date: date,
        to_date: date,
        frequency: Literal["daily", "weekly", "monthly", "yearly"] = "daily",
    ) -> List[Dict[str, Any]]:
        """Alias for :meth:`history` for readability."""

        return self.history(from_date, to_date, frequency=frequency)

    def rates(
        self,
        from_date: date,
        to_date: date,
        frequency: Literal["daily", "weekly", "monthly", "yearly"] = "daily",
        *,
        source_filter: Literal["rbi", "sbi", None] = None,
    ) -> List[Dict[str, Any]]:
        """Deprecated alias; use :meth:`history` instead."""

        warnings.warn(
            "FxBharat.rates is deprecated; use FxBharat.history or FxBharat.historical instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.history(from_date, to_date, frequency=frequency, source_filter=source_filter)

    def _get_ingestion_checkpoint(self, sqlite_db_path: Path, source: str) -> date | None:
        if self.sqlite_manager is not None:
            return self.sqlite_manager.ingestion_checkpoint(source)
        with SQLiteManager(sqlite_db_path) as manager:
            return manager.ingestion_checkpoint(source)

    @staticmethod
    def _group_rows_by_date(
        rows: Iterable[ForexRateRecord],
    ) -> Dict[date, List[ForexRateRecord]]:
        grouped: Dict[date, List[ForexRateRecord]] = {}
        for row in rows:
            grouped.setdefault(row.rate_date, []).append(row)
        return grouped

    @staticmethod
    def _latest_snapshot_from_rows(
        rows: Iterable[ForexRateRecord],
        rate_date: date | None,
        source: str,
    ) -> Dict[str, Any] | None:
        grouped = FxBharat._group_rows_by_date(rows)
        if not grouped:
            return None
        target_date = rate_date if rate_date is not None else max(grouped.keys())
        snapshot = grouped.get(target_date)
        if snapshot is None:
            return None
        return FxBharat._snapshot_payload(target_date, snapshot, source)

    @staticmethod
    def _snapshot_payload(
        rate_date: date, rates: List[ForexRateRecord], source: str
    ) -> Dict[str, Any]:
        ordered_rates: Dict[str, Any]
        if source.upper() == "SBI":
            payload_rates: Dict[str, Dict[str, float | None]] = {}
            for row in rates:
                payload_rates[row.currency] = {
                    "rate": row.rate,
                    "tt_buy": row.tt_buy,
                    "tt_sell": row.tt_sell,
                    "bill_buy": row.bill_buy,
                    "bill_sell": row.bill_sell,
                    "travel_card_buy": row.travel_card_buy,
                    "travel_card_sell": row.travel_card_sell,
                    "cn_buy": row.cn_buy,
                    "cn_sell": row.cn_sell,
                }
            ordered_rates = dict(sorted(payload_rates.items()))
        else:
            ordered_rates = dict(sorted({row.currency: row.rate for row in rates}.items()))

        return {
            "rate_date": rate_date,
            "base_currency": "INR",
            "source": source,
            "rates": ordered_rates,
        }

    @staticmethod
    def _select_snapshot_dates(dates: List[date], frequency: str) -> List[date]:
        if frequency == "daily":
            return dates
        if frequency == "weekly":
            return FxBharat._last_dates_by_key(
                dates,
                lambda value: (value.isocalendar().year, value.isocalendar().week),
            )
        if frequency == "monthly":
            return FxBharat._last_dates_by_key(dates, lambda value: (value.year, value.month))
        if frequency == "yearly":
            return FxBharat._last_dates_by_key(dates, lambda value: value.year)
        raise ValueError("Unsupported frequency")

    @staticmethod
    def _last_dates_by_key(
        dates: Iterable[date],
        key_builder: Callable[[date], Any],
    ) -> List[date]:
        buckets: Dict[Any, date] = {}
        for day in dates:
            key = key_builder(day)
            if key not in buckets or day > buckets[key]:
                buckets[key] = day
        return sorted(buckets.values())

    @staticmethod
    def _normalise_source_filter(
        source_filter: str | None = None,
    ) -> tuple[str, ...]:
        if source_filter is None:
            return ("SBI", "RBI")
        if source_filter.lower() not in {"rbi", "sbi"}:
            raise ValueError("source_filter must be one of 'rbi', 'sbi', or None")
        return (source_filter.upper(),)

    def connection(self) -> tuple[bool, str | None]:
        """Attempt to establish a database connection and report the outcome."""

        if self.connection_info.backend is DatabaseBackend.MONGODB:
            return self._probe_mongodb()
        return self._probe_relational_db()

    def conection(self) -> tuple[bool, str | None]:
        """Backward-compatible alias for ``connection`` (matches user spelling)."""

        return self.connection()

    def _missing_driver_message(self, exc: ModuleNotFoundError) -> str:
        """Return a user-friendly hint when an optional DB driver is missing."""

        module_name = exc.name or str(exc)
        hint = self._DRIVER_HINTS.get(self.connection_info.backend)
        base = (
            f"Missing optional dependency '{module_name}' required for "
            f"{self.connection_info.backend.value} connections."
        )
        if hint:
            return f"{base} {hint}"
        return base

    def _probe_relational_db(self) -> tuple[bool, str | None]:
        """Ping SQLite/MySQL/Postgres backends via SQLAlchemy."""

        if create_engine is None or text is None:  # pragma: no cover - defensive
            return False, "SQLAlchemy is required to perform connectivity checks"

        engine = None
        try:
            engine = create_engine(self.connection_info.url, future=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except ModuleNotFoundError as exc:
            return False, self._missing_driver_message(exc)
        except Exception as exc:  # pragma: no cover - SQLAlchemy provides error detail
            return False, str(exc)
        finally:
            if engine is not None:
                engine.dispose()
        return True, None

    def _probe_mongodb(self) -> tuple[bool, str | None]:
        """Ping MongoDB using pymongo since SQLAlchemy lacks a native dialect."""

        if MongoClient is None:
            return False, self._missing_driver_message(ModuleNotFoundError("pymongo"))

        client: MongoClient | None = None
        try:
            client = MongoClient(self.connection_info.url, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
        except ModuleNotFoundError as exc:
            return False, self._missing_driver_message(exc)
        except Exception as exc:  # pragma: no cover - pymongo surfaces detail
            return False, str(exc)
        finally:
            if client is not None:
                client.close()
        return True, None


def __getattr__(name: str) -> Any:
    """Lazily import heavy helpers to avoid mandatory Selenium installs."""

    if name == "seed_rbi_forex":
        from fx_bharat.seeds.populate_rbi_forex import seed_rbi_forex as _seed

        return _seed
    if name == "RBISeleniumClient":
        from fx_bharat.ingestion.rbi_selenium import RBISeleniumClient as _client

        return _client
    raise AttributeError(f"module 'fx_bharat' has no attribute {name}")
