"""Persistence helpers for fx_bharat (prefers SQLAlchemy)."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, Sequence, cast

from fx_bharat.db import DEFAULT_SQLITE_DB_PATH
from fx_bharat.ingestion.models import ForexRateRecord
from fx_bharat.utils.logger import get_logger

LOGGER = get_logger(__name__)

try:  # pragma: no cover - exercised indirectly
    from sqlalchemy import Column, Date, DateTime, Float, String, create_engine, select, text
    from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
except ModuleNotFoundError:  # pragma: no cover - fallback path
    SQLALCHEMY_AVAILABLE = False
else:  # pragma: no cover - module import time
    SQLALCHEMY_AVAILABLE = True

    class Base(DeclarativeBase):
        pass

    class _ForexRate(Base):
        __tablename__ = "forex_rates"

        rate_date = Column(Date, primary_key=True)
        currency = Column(String, primary_key=True)
        rate = Column(Float, nullable=False)
        source = Column(String, nullable=False, default="RBI")
        created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))


if TYPE_CHECKING:  # pragma: no cover - type checker helper
    from sqlalchemy.engine import Engine
else:  # pragma: no cover - fallback stub

    class Engine:  # type: ignore[too-many-ancestors]
        """Placeholder used when SQLAlchemy is unavailable."""

        pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS forex_rates (
    rate_date DATE NOT NULL,
    currency TEXT NOT NULL,
    rate REAL NOT NULL,
    source TEXT NOT NULL DEFAULT 'RBI',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(rate_date, currency)
);
"""

INSERT_IGNORE_STATEMENT = """
INSERT OR IGNORE INTO forex_rates(rate_date, currency, rate, source)
VALUES(?, ?, ?, ?);
"""

UPDATE_STATEMENT = """
UPDATE forex_rates
SET rate = ?,
    source = ?
WHERE rate_date = ? AND currency = ?;
"""


@dataclass(slots=True)
class PersistenceResult:
    """Represents how many rows were inserted or updated in a batch."""

    inserted: int = 0
    updated: int = 0

    @property
    def total(self) -> int:
        """Return the total number of affected rows."""

        return self.inserted + self.updated


class _BackendProtocol(Protocol):
    def insert_rates(
        self, rows: Sequence[ForexRateRecord]
    ) -> PersistenceResult: ...  # pragma: no cover - protocol definition

    def fetch_all(self) -> list[ForexRateRecord]: ...  # pragma: no cover - protocol definition

    def fetch_range(
        self, start: date | None = None, end: date | None = None, *, source: str | None = None
    ) -> list[ForexRateRecord]: ...  # pragma: no cover - protocol definition

    def close(self) -> None: ...  # pragma: no cover - protocol definition


class _SQLAlchemyBackend:
    """SQLAlchemy implementation used when the dependency is available."""

    def __init__(self, db_path: str | Path) -> None:
        if not SQLALCHEMY_AVAILABLE:  # pragma: no cover - defensive guard
            raise ModuleNotFoundError("SQLAlchemy is required for SQLiteManager")
        self.db_path = Path(db_path).expanduser().resolve()
        self.engine: Engine = create_engine(
            f"sqlite:///{self.db_path}",
            echo=False,
            future=True,
            connect_args={"check_same_thread": False},
        )
        Base.metadata.create_all(self.engine)
        self._SessionFactory: sessionmaker[Session] = sessionmaker(
            bind=self.engine, expire_on_commit=False, future=True
        )

    def insert_rates(self, rows: Sequence[ForexRateRecord]) -> PersistenceResult:
        result = PersistenceResult()
        with self._SessionFactory() as session:
            for row in rows:
                pk = {"rate_date": row.rate_date, "currency": row.currency}
                existing = session.get(_ForexRate, pk)
                if existing is None:
                    session.add(
                        _ForexRate(
                            rate_date=row.rate_date,
                            currency=row.currency,
                            rate=row.rate,
                            source=row.source,
                        )
                    )
                    result.inserted += 1
                else:
                    setattr(existing, "rate", row.rate)
                    setattr(existing, "source", row.source)
                    result.updated += 1
            session.commit()
        return result

    def fetch_all(self) -> list[ForexRateRecord]:
        return self.fetch_range()

    def fetch_range(
        self,
        start: date | None = None,
        end: date | None = None,
        *,
        source: str | None = None,
    ) -> list[ForexRateRecord]:
        with self._SessionFactory() as session:
            stmt = select(_ForexRate).order_by(_ForexRate.rate_date)
            if start is not None:
                stmt = stmt.where(_ForexRate.rate_date >= start)
            if end is not None:
                stmt = stmt.where(_ForexRate.rate_date <= end)
            if source is not None:
                stmt = stmt.where(_ForexRate.source == source)
            result = session.execute(stmt)
            records: list[ForexRateRecord] = []
            for row in result.scalars():
                model = cast(_ForexRate, row)
                records.append(
                    ForexRateRecord(
                        rate_date=cast(date, model.rate_date),
                        currency=cast(str, model.currency),
                        rate=cast(float, model.rate),
                        source=cast(str, model.source),
                    )
                )
            return records

    def close(self) -> None:  # pragma: no cover - trivial
        self.engine.dispose()


class _SQLiteFallbackBackend:
    """Legacy sqlite3 implementation kept for environments without SQLAlchemy."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._connection = sqlite3.connect(self.db_path)
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.row_factory = sqlite3.Row
        self._connection.execute(SCHEMA)
        self._connection.commit()

    def insert_rates(self, rows: Sequence[ForexRateRecord]) -> PersistenceResult:
        result = PersistenceResult()
        with self._connection:
            for row in rows:
                inserted = self._connection.execute(
                    INSERT_IGNORE_STATEMENT,
                    (row.rate_date.isoformat(), row.currency, row.rate, row.source),
                ).rowcount
                if inserted:
                    result.inserted += inserted
                    continue
                updated = self._connection.execute(
                    UPDATE_STATEMENT,
                    (row.rate, row.source, row.rate_date.isoformat(), row.currency),
                ).rowcount
                result.updated += updated
        return result

    def fetch_all(self) -> list[ForexRateRecord]:
        return self.fetch_range()

    def fetch_range(
        self,
        start: date | None = None,
        end: date | None = None,
        *,
        source: str | None = None,
    ) -> list[ForexRateRecord]:
        clauses: list[str] = []
        params: list[str] = []
        if start is not None:
            clauses.append("rate_date >= ?")
            params.append(start.isoformat())
        if end is not None:
            clauses.append("rate_date <= ?")
            params.append(end.isoformat())
        if source is not None:
            clauses.append("source = ?")
            params.append(source)
        where = ""
        if clauses:
            where = " WHERE " + " AND ".join(clauses)
        query = (
            "SELECT rate_date, currency, rate, source FROM forex_rates"
            f"{where} ORDER BY rate_date"
        )
        cursor = self._connection.execute(query, params)
        return [
            ForexRateRecord(
                rate_date=date.fromisoformat(row["rate_date"]),
                currency=row["currency"],
                rate=row["rate"],
                source=row["source"],
            )
            for row in cursor.fetchall()
        ]

    def close(self) -> None:  # pragma: no cover - trivial
        self._connection.close()


class SQLiteManager:
    """Facade that prefers SQLAlchemy but gracefully falls back to sqlite3."""

    def __init__(self, db_path: str | Path = DEFAULT_SQLITE_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self._backend: _BackendProtocol
        if SQLALCHEMY_AVAILABLE:
            LOGGER.info("Using SQLAlchemy backend for SQLiteManager")
            self._backend = _SQLAlchemyBackend(self.db_path)
        else:
            LOGGER.warning("SQLAlchemy not installed; using sqlite3 fallback backend")
            self._backend = _SQLiteFallbackBackend(self.db_path)

    def insert_rates(self, rows: Sequence[ForexRateRecord]) -> PersistenceResult:
        result = self._backend.insert_rates(rows)
        LOGGER.info(
            "Inserted %s rows, updated %s rows (total %s)",
            result.inserted,
            result.updated,
            result.total,
        )
        return result

    def fetch_all(self) -> list[ForexRateRecord]:
        return self._backend.fetch_all()

    def fetch_range(
        self,
        start: date | None = None,
        end: date | None = None,
        *,
        source: str | None = None,
    ) -> list[ForexRateRecord]:
        return self._backend.fetch_range(start, end, source=source)

    def close(self) -> None:  # pragma: no cover - trivial
        self._backend.close()

    def __enter__(self) -> "SQLiteManager":  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
        self.close()
