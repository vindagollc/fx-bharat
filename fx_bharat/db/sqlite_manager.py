"""Persistence helpers for fx_bharat (prefers SQLAlchemy)."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, Sequence, cast

from fx_bharat.db import DEFAULT_SQLITE_DB_PATH
from fx_bharat.ingestion.models import ForexRateRecord, LmeRateRecord
from fx_bharat.utils.logger import get_logger

LOGGER = get_logger(__name__)

try:  # pragma: no cover - exercised indirectly
    from sqlalchemy import Column, Date, DateTime, Float, String, create_engine, select, text
    from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
    from sqlalchemy.sql import func
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

    class _IngestionMetadata(Base):
        __tablename__ = "ingestion_metadata"

        source = Column(String, primary_key=True)
        last_ingested_date = Column(Date, nullable=False)
        updated_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))

    class _LmeCopperRate(Base):
        __tablename__ = "lme_copper_rates"

        rate_date = Column(Date, primary_key=True)
        price = Column(Float, nullable=True)
        price_3_month = Column(Float, nullable=True)
        stock = Column(Float, nullable=True)
        created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))

    class _LmeAluminumRate(Base):
        __tablename__ = "lme_aluminum_rates"

        rate_date = Column(Date, primary_key=True)
        price = Column(Float, nullable=True)
        price_3_month = Column(Float, nullable=True)
        stock = Column(Float, nullable=True)
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

CREATE TABLE IF NOT EXISTS ingestion_metadata (
    source TEXT PRIMARY KEY,
    last_ingested_date DATE NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lme_copper_rates (
    rate_date DATE PRIMARY KEY,
    price REAL,
    price_3_month REAL,
    stock INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lme_aluminum_rates (
    rate_date DATE PRIMARY KEY,
    price REAL,
    price_3_month REAL,
    stock INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

INSERT_LME_COPPER_STATEMENT = """
INSERT OR REPLACE INTO lme_copper_rates(
    rate_date,
    price,
    price_3_month,
    stock
)
VALUES(?, ?, ?, ?);
"""

INSERT_LME_ALUMINUM_STATEMENT = """
INSERT OR REPLACE INTO lme_aluminum_rates(
    rate_date,
    price,
    price_3_month,
    stock
)
VALUES(?, ?, ?, ?);
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

    def latest_rate_date(self, source: str) -> date | None:
        ...  # pragma: no cover - protocol definition

    def ingestion_checkpoint(self, source: str) -> date | None:
        ...  # pragma: no cover - protocol definition

    def update_ingestion_checkpoint(self, source: str, rate_date: date) -> None:
        ...  # pragma: no cover - protocol definition

    def insert_lme_rates(self, metal: str, rows: Sequence[LmeRateRecord]) -> PersistenceResult:
        ...  # pragma: no cover - protocol definition

    def fetch_lme_range(
        self, metal: str, start: date | None = None, end: date | None = None
    ) -> list[LmeRateRecord]:
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
        self._ensure_lme_schema()
        self._SessionFactory: sessionmaker[Session] = sessionmaker(
            bind=self.engine, expire_on_commit=False, future=True
        )

    def _ensure_lme_schema(self) -> None:
        """Ensure older SQLite files include LME columns introduced later."""

        def _missing_columns(table: str, expected: dict[str, str]) -> dict[str, str]:
            with self.engine.connect() as connection:
                result = connection.execute(text(f"PRAGMA table_info({table})"))
                existing = {row[1] for row in result}
            return {name: column_type for name, column_type in expected.items() if name not in existing}

        lme_columns = {
            "price": "REAL",
            "price_3_month": "REAL",
            "stock": "INTEGER",
            "created_at": "TIMESTAMP",
        }
        for table in ("lme_copper_rates", "lme_aluminum_rates"):
            missing = _missing_columns(table, lme_columns)
            if not missing:
                continue
            with self.engine.begin() as connection:
                for column_name, column_type in missing.items():
                    connection.execute(
                        text(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}")
                    )

    @staticmethod
    def _resolve_lme_model(metal: str):
        normalised = metal.upper()
        if normalised in {"CU", "COPPER"}:
            return _LmeCopperRate
        if normalised in {"AL", "ALUMINUM", "ALUMINIUM"}:
            return _LmeAluminumRate
        raise ValueError(f"Unsupported LME metal: {metal}")

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

    def insert_lme_rates(self, metal: str, rows: Sequence[LmeRateRecord]) -> PersistenceResult:
        model = self._resolve_lme_model(metal)
        result = PersistenceResult()
        if not rows:
            return result
        with self._SessionFactory() as session:
            for row in rows:
                pk = {"rate_date": row.rate_date}
                existing = session.get(model, pk)
                if existing is None:
                    session.add(
                        model(
                            rate_date=row.rate_date,
                            price=row.price,
                            price_3_month=row.price_3_month,
                            stock=row.stock,
                        )
                    )
                    result.inserted += 1
                else:
                    setattr(existing, "price", row.price)
                    setattr(existing, "price_3_month", row.price_3_month)
                    setattr(existing, "stock", row.stock)
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

    def fetch_lme_range(
        self, metal: str, start: date | None = None, end: date | None = None
    ) -> list[LmeRateRecord]:
        model = self._resolve_lme_model(metal)
        records: list[LmeRateRecord] = []
        with self._SessionFactory() as session:
            stmt = select(model).order_by(model.rate_date)
            if start is not None:
                stmt = stmt.where(model.rate_date >= start)
            if end is not None:
                stmt = stmt.where(model.rate_date <= end)
            for db_row in session.execute(stmt).scalars():
                records.append(
                    LmeRateRecord(
                        rate_date=cast(date, db_row.rate_date),
                        price=cast(float | None, db_row.price),
                        price_3_month=cast(float | None, db_row.price_3_month),
                        stock=cast(int | None, db_row.stock),
                        metal="COPPER" if model is _LmeCopperRate else "ALUMINUM",
                    )
                )
        return records

    def latest_rate_date(self, source: str) -> date | None:
        stmt = select(
            func.max(_SbiRate.rate_date)
            if source.upper() == "SBI"
            else func.max(_RbiRate.rate_date)
        )
        with self._SessionFactory() as session:
            result = session.execute(stmt).scalar_one_or_none()
        return cast(date | None, result)

    def ingestion_checkpoint(self, source: str) -> date | None:
        with self._SessionFactory() as session:
            record = session.get(_IngestionMetadata, source.upper())
            return cast(date | None, record.last_ingested_date) if record else None

    def update_ingestion_checkpoint(self, source: str, rate_date: date) -> None:
        with self._SessionFactory() as session:
            existing = session.get(_IngestionMetadata, source.upper())
            if existing is None:
                session.add(_IngestionMetadata(source=source.upper(), last_ingested_date=rate_date))
            elif existing.last_ingested_date < rate_date:
                cast(Any, existing).last_ingested_date = rate_date
            else:
                return
            session.commit()

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

    @staticmethod
    def _resolve_lme_table(metal: str) -> tuple[str, str]:
        normalised = metal.upper()
        if normalised in {"CU", "COPPER"}:
            return "lme_copper_rates", INSERT_LME_COPPER_STATEMENT
        if normalised in {"AL", "ALUMINUM", "ALUMINIUM"}:
            return "lme_aluminum_rates", INSERT_LME_ALUMINUM_STATEMENT
        raise ValueError(f"Unsupported LME metal: {metal}")

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

    def insert_lme_rates(self, metal: str, rows: Sequence[LmeRateRecord]) -> PersistenceResult:
        table, statement = self._resolve_lme_table(metal)
        result = PersistenceResult()
        if not rows:
            return result
        with self._connection:
            for row in rows:
                inserted = self._connection.execute(
                    statement,
                    (
                        row.rate_date.isoformat(),
                        row.price,
                        row.price_3_month,
                        row.stock,
                    ),
                ).rowcount
                if inserted:
                    result.inserted += inserted
                else:
                    updated = self._connection.execute(
                        f"""
                        UPDATE {table}
                        SET price = ?,
                            price_3_month = ?,
                            stock = ?
                        WHERE rate_date = ?
                        """,
                        (
                            row.price,
                            row.price_3_month,
                            row.stock,
                            row.rate_date.isoformat(),
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

    def fetch_lme_range(
        self, metal: str, start: date | None = None, end: date | None = None
    ) -> list[LmeRateRecord]:
        table, _ = self._resolve_lme_table(metal)

        def _build_query() -> tuple[str, list[str]]:
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
            return (f"SELECT * FROM {table}{where} ORDER BY rate_date", params)

        query, params = _build_query()
        records: list[LmeRateRecord] = []
        for row in self._connection.execute(query, params).fetchall():
            records.append(
                LmeRateRecord(
                    rate_date=date.fromisoformat(row["rate_date"]),
                    price=row["price"],
                    price_3_month=row["price_3_month"],
                    stock=row["stock"],
                    metal="COPPER" if table.endswith("copper_rates") else "ALUMINUM",
                )
            )
        return records

    def latest_rate_date(self, source: str) -> date | None:
        table = "forex_rates_sbi" if source.upper() == "SBI" else "forex_rates_rbi"
        cursor = self._connection.execute(f"SELECT MAX(rate_date) AS latest FROM {table}")
        value = cursor.fetchone()["latest"]
        return date.fromisoformat(value) if value else None

    def ingestion_checkpoint(self, source: str) -> date | None:
        cursor = self._connection.execute(
            "SELECT last_ingested_date FROM ingestion_metadata WHERE source = ?",
            (source.upper(),),
        )
        value = cursor.fetchone()
        if value is None:
            return None
        return date.fromisoformat(value["last_ingested_date"])

    def update_ingestion_checkpoint(self, source: str, rate_date: date) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO ingestion_metadata(source, last_ingested_date)
                VALUES(?, ?)
                ON CONFLICT(source) DO UPDATE SET last_ingested_date = excluded.last_ingested_date
                WHERE excluded.last_ingested_date > ingestion_metadata.last_ingested_date
                """,
                (source.upper(), rate_date.isoformat()),
            )

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

    def insert_lme_rates(self, metal: str, rows: Sequence[LmeRateRecord]) -> PersistenceResult:
        result = self._backend.insert_lme_rates(metal, rows)
        LOGGER.info(
            "Inserted %s %s rows, updated %s rows (total %s)",
            result.inserted,
            metal,
            result.updated,
            result.total,
        )
        return result

    def fetch_lme_range(
        self, metal: str, start: date | None = None, end: date | None = None
    ) -> list[LmeRateRecord]:
        return self._backend.fetch_lme_range(metal, start, end)

    def close(self) -> None:  # pragma: no cover - trivial
        self._backend.close()

    def latest_rate_date(self, source: str) -> date | None:
        return self._backend.latest_rate_date(source)

    def ingestion_checkpoint(self, source: str) -> date | None:
        return self._backend.ingestion_checkpoint(source)

    def update_ingestion_checkpoint(self, source: str, rate_date: date) -> None:
        self._backend.update_ingestion_checkpoint(source, rate_date)

    def __enter__(self) -> "SQLiteManager":  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
        self.close()
