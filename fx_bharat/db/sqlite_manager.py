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

    class _RbiRate(Base):
        __tablename__ = "forex_rates_rbi"

        rate_date = Column(Date, primary_key=True)
        currency = Column(String, primary_key=True)
        rate = Column(Float, nullable=False)
        created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))

    class _SbiRate(Base):
        __tablename__ = "forex_rates_sbi"

        rate_date = Column(Date, primary_key=True)
        currency = Column(String, primary_key=True)
        rate = Column(Float, nullable=False)
        tt_buy = Column(Float, nullable=True)
        tt_sell = Column(Float, nullable=True)
        bill_buy = Column(Float, nullable=True)
        bill_sell = Column(Float, nullable=True)
        travel_card_buy = Column(Float, nullable=True)
        travel_card_sell = Column(Float, nullable=True)
        cn_buy = Column(Float, nullable=True)
        cn_sell = Column(Float, nullable=True)
        created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))


if TYPE_CHECKING:  # pragma: no cover - type checker helper
    from sqlalchemy.engine import Engine
else:  # pragma: no cover - fallback stub

    class Engine:  # type: ignore[too-many-ancestors]
        """Placeholder used when SQLAlchemy is unavailable."""

        pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS forex_rates_rbi (
    rate_date DATE NOT NULL,
    currency TEXT NOT NULL,
    rate REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(rate_date, currency)
);

CREATE TABLE IF NOT EXISTS forex_rates_sbi (
    rate_date DATE NOT NULL,
    currency TEXT NOT NULL,
    rate REAL NOT NULL,
    tt_buy REAL,
    tt_sell REAL,
    bill_buy REAL,
    bill_sell REAL,
    travel_card_buy REAL,
    travel_card_sell REAL,
    cn_buy REAL,
    cn_sell REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(rate_date, currency)
);
"""

INSERT_RBI_IGNORE_STATEMENT = """
INSERT OR IGNORE INTO forex_rates_rbi(rate_date, currency, rate)
VALUES(?, ?, ?);
"""

INSERT_SBI_IGNORE_STATEMENT = """
INSERT OR IGNORE INTO forex_rates_sbi(
    rate_date,
    currency,
    rate,
    tt_buy,
    tt_sell,
    bill_buy,
    bill_sell,
    travel_card_buy,
    travel_card_sell,
    cn_buy,
    cn_sell
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

UPDATE_RBI_STATEMENT = """
UPDATE forex_rates_rbi
SET rate = ?
WHERE rate_date = ? AND currency = ?;
"""

UPDATE_SBI_STATEMENT = """
UPDATE forex_rates_sbi
SET rate = ?,
    tt_buy = ?,
    tt_sell = ?,
    bill_buy = ?,
    bill_sell = ?,
    travel_card_buy = ?,
    travel_card_sell = ?,
    cn_buy = ?,
    cn_sell = ?
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
    def insert_rates(self, rows: Sequence[ForexRateRecord]) -> PersistenceResult:
        ...  # pragma: no cover - protocol definition

    def fetch_all(self) -> list[ForexRateRecord]:
        ...  # pragma: no cover - protocol definition

    def fetch_range(
        self, start: date | None = None, end: date | None = None, *, source: str | None = None
    ) -> list[ForexRateRecord]:
        ...  # pragma: no cover - protocol definition

    def close(self) -> None:
        ...  # pragma: no cover - protocol definition


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
                source = (row.source or "RBI").upper()
                pk = {"rate_date": row.rate_date, "currency": row.currency}
                if source == "SBI":
                    sbi_existing = session.get(_SbiRate, pk)
                    if sbi_existing is None:
                        session.add(
                            _SbiRate(
                                rate_date=row.rate_date,
                                currency=row.currency,
                                rate=row.rate,
                                tt_buy=row.tt_buy,
                                tt_sell=row.tt_sell,
                                bill_buy=row.bill_buy,
                                bill_sell=row.bill_sell,
                                travel_card_buy=row.travel_card_buy,
                                travel_card_sell=row.travel_card_sell,
                                cn_buy=row.cn_buy,
                                cn_sell=row.cn_sell,
                            )
                        )
                        result.inserted += 1
                    else:
                        setattr(sbi_existing, "rate", row.rate)
                        setattr(sbi_existing, "tt_buy", row.tt_buy)
                        setattr(sbi_existing, "tt_sell", row.tt_sell)
                        setattr(sbi_existing, "bill_buy", row.bill_buy)
                        setattr(sbi_existing, "bill_sell", row.bill_sell)
                        setattr(sbi_existing, "travel_card_buy", row.travel_card_buy)
                        setattr(sbi_existing, "travel_card_sell", row.travel_card_sell)
                        setattr(sbi_existing, "cn_buy", row.cn_buy)
                        setattr(sbi_existing, "cn_sell", row.cn_sell)
                        result.updated += 1
                else:
                    rbi_existing = session.get(_RbiRate, pk)
                    if rbi_existing is None:
                        session.add(
                            _RbiRate(
                                rate_date=row.rate_date,
                                currency=row.currency,
                                rate=row.rate,
                            )
                        )
                        result.inserted += 1
                    else:
                        setattr(rbi_existing, "rate", row.rate)
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
        records: list[ForexRateRecord] = []
        with self._SessionFactory() as session:
            if source is None or source.upper() == "SBI":
                sbi_stmt = select(_SbiRate).order_by(_SbiRate.rate_date)
                if start is not None:
                    sbi_stmt = sbi_stmt.where(_SbiRate.rate_date >= start)
                if end is not None:
                    sbi_stmt = sbi_stmt.where(_SbiRate.rate_date <= end)
                for sbi_row in session.execute(sbi_stmt).scalars():
                    sbi_model = cast(_SbiRate, sbi_row)
                    records.append(
                        ForexRateRecord(
                            rate_date=cast(date, sbi_model.rate_date),
                            currency=cast(str, sbi_model.currency),
                            rate=cast(float, sbi_model.rate),
                            source="SBI",
                            tt_buy=cast(float | None, sbi_model.tt_buy),
                            tt_sell=cast(float | None, sbi_model.tt_sell),
                            bill_buy=cast(float | None, sbi_model.bill_buy),
                            bill_sell=cast(float | None, sbi_model.bill_sell),
                            travel_card_buy=cast(float | None, sbi_model.travel_card_buy),
                            travel_card_sell=cast(float | None, sbi_model.travel_card_sell),
                            cn_buy=cast(float | None, sbi_model.cn_buy),
                            cn_sell=cast(float | None, sbi_model.cn_sell),
                        )
                    )
            if source is None or source.upper() == "RBI":
                rbi_stmt = select(_RbiRate).order_by(_RbiRate.rate_date)
                if start is not None:
                    rbi_stmt = rbi_stmt.where(_RbiRate.rate_date >= start)
                if end is not None:
                    rbi_stmt = rbi_stmt.where(_RbiRate.rate_date <= end)
                for rbi_row in session.execute(rbi_stmt).scalars():
                    rbi_model = cast(_RbiRate, rbi_row)
                    records.append(
                        ForexRateRecord(
                            rate_date=cast(date, rbi_model.rate_date),
                            currency=cast(str, rbi_model.currency),
                            rate=cast(float, rbi_model.rate),
                            source="RBI",
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
        self._connection.executescript(SCHEMA)
        self._connection.commit()

    def insert_rates(self, rows: Sequence[ForexRateRecord]) -> PersistenceResult:
        result = PersistenceResult()
        with self._connection:
            for row in rows:
                if (row.source or "RBI").upper() == "SBI":
                    inserted = self._connection.execute(
                        INSERT_SBI_IGNORE_STATEMENT,
                        (
                            row.rate_date.isoformat(),
                            row.currency,
                            row.rate,
                            row.tt_buy,
                            row.tt_sell,
                            row.bill_buy,
                            row.bill_sell,
                            row.travel_card_buy,
                            row.travel_card_sell,
                            row.cn_buy,
                            row.cn_sell,
                        ),
                    ).rowcount
                    if inserted:
                        result.inserted += inserted
                        continue
                    updated = self._connection.execute(
                        UPDATE_SBI_STATEMENT,
                        (
                            row.rate,
                            row.tt_buy,
                            row.tt_sell,
                            row.bill_buy,
                            row.bill_sell,
                            row.travel_card_buy,
                            row.travel_card_sell,
                            row.cn_buy,
                            row.cn_sell,
                            row.rate_date.isoformat(),
                            row.currency,
                        ),
                    ).rowcount
                    result.updated += updated
                else:
                    inserted = self._connection.execute(
                        INSERT_RBI_IGNORE_STATEMENT,
                        (
                            row.rate_date.isoformat(),
                            row.currency,
                            row.rate,
                        ),
                    ).rowcount
                    if inserted:
                        result.inserted += inserted
                        continue
                    updated = self._connection.execute(
                        UPDATE_RBI_STATEMENT,
                        (
                            row.rate,
                            row.rate_date.isoformat(),
                            row.currency,
                        ),
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
        def _build_query(table: str) -> tuple[str, list[str]]:
            clauses: list[str] = []
            params: list[str] = []
            if start is not None:
                clauses.append("rate_date >= ?")
                params.append(start.isoformat())
            if end is not None:
                clauses.append("rate_date <= ?")
                params.append(end.isoformat())
            where = ""
            if clauses:
                where = " WHERE " + " AND ".join(clauses)
            return (
                f"SELECT * FROM {table}{where} ORDER BY rate_date",
                params,
            )

        records: list[ForexRateRecord] = []

        if source is None or source.upper() == "SBI":
            query, params = _build_query("forex_rates_sbi")
            for row in self._connection.execute(query, params).fetchall():
                records.append(
                    ForexRateRecord(
                        rate_date=date.fromisoformat(row["rate_date"]),
                        currency=row["currency"],
                        rate=row["rate"],
                        source="SBI",
                        tt_buy=row["tt_buy"],
                        tt_sell=row["tt_sell"],
                        bill_buy=row["bill_buy"],
                        bill_sell=row["bill_sell"],
                        travel_card_buy=row["travel_card_buy"],
                        travel_card_sell=row["travel_card_sell"],
                        cn_buy=row["cn_buy"],
                        cn_sell=row["cn_sell"],
                    )
                )

        if source is None or source.upper() == "RBI":
            query, params = _build_query("forex_rates_rbi")
            for row in self._connection.execute(query, params).fetchall():
                records.append(
                    ForexRateRecord(
                        rate_date=date.fromisoformat(row["rate_date"]),
                        currency=row["currency"],
                        rate=row["rate"],
                        source="RBI",
                    )
                )

        return records

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
