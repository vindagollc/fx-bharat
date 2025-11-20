"""Shared logic for SQL (Postgres/MySQL) backends."""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Sequence

from fx_bharat.db.base_backend import BackendStrategy
from fx_bharat.db.sqlite_manager import PersistenceResult
from fx_bharat.ingestion.models import ForexRateRecord
from fx_bharat.utils.logger import get_logger

try:  # pragma: no cover - optional dependency
    from sqlalchemy import create_engine, text
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    create_engine = None  # type: ignore[assignment]
    text = None  # type: ignore[assignment]

if TYPE_CHECKING:  # pragma: no cover - type checker helper
    from sqlalchemy.engine import Engine
else:  # pragma: no cover - fallback type used at runtime
    Engine = Any

LOGGER = get_logger(__name__)

SCHEMA_SQL_RBI = """
CREATE TABLE IF NOT EXISTS forex_rates_rbi (
    rate_date DATE NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    rate NUMERIC(18, 6) NOT NULL,
    base_currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(rate_date, currency_code)
);
"""

SCHEMA_SQL_SBI = """
CREATE TABLE IF NOT EXISTS forex_rates_sbi (
    rate_date DATE NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    rate NUMERIC(18, 6) NOT NULL,
    base_currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    tt_buy NUMERIC(18, 6) NULL,
    tt_sell NUMERIC(18, 6) NULL,
    bill_buy NUMERIC(18, 6) NULL,
    bill_sell NUMERIC(18, 6) NULL,
    travel_card_buy NUMERIC(18, 6) NULL,
    travel_card_sell NUMERIC(18, 6) NULL,
    cn_buy NUMERIC(18, 6) NULL,
    cn_sell NUMERIC(18, 6) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(rate_date, currency_code)
);
"""

DELETE_RBI_SQL = (
    "DELETE FROM forex_rates_rbi WHERE rate_date = :rate_date AND currency_code = :currency_code"
)
DELETE_SBI_SQL = (
    "DELETE FROM forex_rates_sbi WHERE rate_date = :rate_date AND currency_code = :currency_code"
)
INSERT_RBI_SQL = """
INSERT INTO forex_rates_rbi(rate_date, currency_code, rate, base_currency, created_at)
VALUES(:rate_date, :currency_code, :rate, :base_currency, :created_at)
"""
INSERT_SBI_SQL = """
INSERT INTO forex_rates_sbi(
    rate_date,
    currency_code,
    rate,
    base_currency,
    tt_buy,
    tt_sell,
    bill_buy,
    bill_sell,
    travel_card_buy,
    travel_card_sell,
    cn_buy,
    cn_sell,
    created_at
)
VALUES(
    :rate_date,
    :currency_code,
    :rate,
    :base_currency,
    :tt_buy,
    :tt_sell,
    :bill_buy,
    :bill_sell,
    :travel_card_buy,
    :travel_card_sell,
    :cn_buy,
    :cn_sell,
    :created_at
)
"""


class RelationalBackend(BackendStrategy):
    """Base class that encapsulates SQLAlchemy powered interactions."""

    def __init__(self, url: str) -> None:
        if create_engine is None or text is None:  # pragma: no cover - defensive guard
            raise ModuleNotFoundError("SQLAlchemy is required for relational backends")
        self.url = url
        self._engine_instance: Engine | None = None

    def _get_engine(self) -> Engine:
        if self._engine_instance is None:
            if create_engine is None:  # pragma: no cover - defensive guard
                raise ModuleNotFoundError("SQLAlchemy is required for relational backends")
            self._engine_instance = create_engine(self.url, future=True)
        return self._engine_instance

    def ensure_schema(self) -> None:
        engine = self._get_engine()
        if text is None:  # pragma: no cover - defensive guard
            raise ModuleNotFoundError("SQLAlchemy is required for relational backends")
        with engine.begin() as connection:
            LOGGER.info("Ensuring forex_rates schema exists")
            connection.execute(text("SELECT 1"))
            connection.execute(text(SCHEMA_SQL_RBI))
            connection.execute(text(SCHEMA_SQL_SBI))

    def insert_rates(self, rows: Sequence[ForexRateRecord]) -> PersistenceResult:
        result = PersistenceResult()
        if not rows:
            return result
        engine = self._get_engine()
        if text is None:  # pragma: no cover - defensive guard
            raise ModuleNotFoundError("SQLAlchemy is required for relational backends")
        with engine.begin() as connection:
            for row in rows:
                is_sbi = (row.source or "RBI").upper() == "SBI"
                params = {
                    "rate_date": row.rate_date,
                    "currency_code": row.currency,
                    "rate": row.rate,
                    "base_currency": "INR",
                    "created_at": datetime.utcnow(),
                }
                if is_sbi:
                    params.update(
                        {
                            "tt_buy": row.tt_buy,
                            "tt_sell": row.tt_sell,
                            "bill_buy": row.bill_buy,
                            "bill_sell": row.bill_sell,
                            "travel_card_buy": row.travel_card_buy,
                            "travel_card_sell": row.travel_card_sell,
                            "cn_buy": row.cn_buy,
                            "cn_sell": row.cn_sell,
                        }
                    )
                    connection.execute(text(DELETE_SBI_SQL), params)
                    connection.execute(text(INSERT_SBI_SQL), params)
                else:
                    connection.execute(text(DELETE_RBI_SQL), params)
                    connection.execute(text(INSERT_RBI_SQL), params)
                result.inserted += 1
        return result

    def fetch_range(
        self,
        start: date | None = None,
        end: date | None = None,
        *,
        source: str | None = None,
    ) -> list[ForexRateRecord]:
        engine = self._get_engine()
        if text is None:  # pragma: no cover - defensive guard
            raise ModuleNotFoundError("SQLAlchemy is required for relational backends")

        def _build_query(table: str) -> tuple[str, dict[str, object]]:
            where_clauses: list[str] = []
            params: dict[str, object] = {}
            if start is not None:
                where_clauses.append("rate_date >= :start_date")
                params["start_date"] = start
            if end is not None:
                where_clauses.append("rate_date <= :end_date")
                params["end_date"] = end
            query = f"SELECT * FROM {table} ORDER BY rate_date"
            if where_clauses:
                query = (
                    f"SELECT * FROM {table} WHERE "
                    + " AND ".join(where_clauses)
                    + " ORDER BY rate_date"
                )
            return query, params

        records: list[ForexRateRecord] = []
        with engine.connect() as connection:
            if source is None or source.upper() == "SBI":
                query, params = _build_query("forex_rates_sbi")
                for row in connection.execute(text(query), params):
                    mapping = row._mapping
                    records.append(
                        ForexRateRecord(
                            rate_date=_normalise_rate_date(mapping["rate_date"]),
                            currency=mapping["currency_code"],
                            rate=float(mapping["rate"]),
                            source="SBI",
                            tt_buy=mapping["tt_buy"],
                            tt_sell=mapping["tt_sell"],
                            bill_buy=mapping["bill_buy"],
                            bill_sell=mapping["bill_sell"],
                            travel_card_buy=mapping["travel_card_buy"],
                            travel_card_sell=mapping["travel_card_sell"],
                            cn_buy=mapping["cn_buy"],
                            cn_sell=mapping["cn_sell"],
                        )
                    )
            if source is None or source.upper() == "RBI":
                query, params = _build_query("forex_rates_rbi")
                for row in connection.execute(text(query), params):
                    mapping = row._mapping
                    records.append(
                        ForexRateRecord(
                            rate_date=_normalise_rate_date(mapping["rate_date"]),
                            currency=mapping["currency_code"],
                            rate=float(mapping["rate"]),
                            source="RBI",
                        )
                    )
        return records

    def close(self) -> None:  # pragma: no cover - trivial resource cleanup
        if self._engine_instance is not None:
            self._engine_instance.dispose()


def _normalise_rate_date(value: object) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value))


__all__ = ["RelationalBackend"]
