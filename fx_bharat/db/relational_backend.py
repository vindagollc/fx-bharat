"""Shared logic for SQL (Postgres/MySQL) backends."""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Mapping, Sequence, SupportsFloat, SupportsIndex, cast

from fx_bharat.db.base_backend import BackendStrategy
from fx_bharat.db.sqlite_manager import PersistenceResult
from fx_bharat.ingestion.models import ForexRateRecord, LmeRateRecord
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

SCHEMA_SQL_LME_COPPER = """
CREATE TABLE IF NOT EXISTS lme_copper_rates (
    rate_date DATE NOT NULL,
    price NUMERIC(18, 6) NULL,
    price_3_month NUMERIC(18, 6) NULL,
    stock NUMERIC(18, 6) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(rate_date)
);
"""

SCHEMA_SQL_LME_ALUMINUM = """
CREATE TABLE IF NOT EXISTS lme_aluminum_rates (
    rate_date DATE NOT NULL,
    price NUMERIC(18, 6) NULL,
    price_3_month NUMERIC(18, 6) NULL,
    stock NUMERIC(18, 6) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(rate_date)
);
"""

SCHEMA_SQL_INGESTION_METADATA = """
CREATE TABLE IF NOT EXISTS ingestion_metadata (
    source VARCHAR(32) PRIMARY KEY,
    last_ingested_date DATE NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

DELETE_RBI_SQL = (
    "DELETE FROM forex_rates_rbi WHERE rate_date = :rate_date " "AND currency_code = :currency_code"
)
DELETE_SBI_SQL = (
    "DELETE FROM forex_rates_sbi WHERE rate_date = :rate_date " "AND currency_code = :currency_code"
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

INSERT_LME_COPPER_SQL = """
INSERT INTO lme_copper_rates(rate_date, price, price_3_month, stock, created_at)
VALUES(:rate_date, :price, :price_3_month, :stock, :created_at)
"""

INSERT_LME_ALUMINUM_SQL = """
INSERT INTO lme_aluminum_rates(rate_date, price, price_3_month, stock, created_at)
VALUES(:rate_date, :price, :price_3_month, :stock, :created_at)
"""

DELETE_LME_COPPER_SQL = "DELETE FROM lme_copper_rates WHERE rate_date = :rate_date"
DELETE_LME_ALUMINUM_SQL = "DELETE FROM lme_aluminum_rates WHERE rate_date = :rate_date"


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

    @staticmethod
    def _resolve_lme_statements(metal: str) -> tuple[str, str, str]:
        normalised = metal.upper()
        if normalised in {"CU", "COPPER"}:
            return "COPPER", INSERT_LME_COPPER_SQL, DELETE_LME_COPPER_SQL
        if normalised in {"AL", "ALUMINUM", "ALUMINIUM"}:
            return "ALUMINUM", INSERT_LME_ALUMINUM_SQL, DELETE_LME_ALUMINUM_SQL
        raise ValueError(f"Unsupported LME metal: {metal}")

    def ensure_schema(self) -> None:
        engine = self._get_engine()
        if text is None:  # pragma: no cover - defensive guard
            raise ModuleNotFoundError("SQLAlchemy is required for relational backends")
        with engine.begin() as connection:
            LOGGER.info("Ensuring forex_rates schema exists")
            connection.execute(text("SELECT 1"))
            connection.execute(text(SCHEMA_SQL_RBI))
            connection.execute(text(SCHEMA_SQL_SBI))
            connection.execute(text(SCHEMA_SQL_INGESTION_METADATA))
            connection.execute(text(SCHEMA_SQL_LME_COPPER))
            connection.execute(text(SCHEMA_SQL_LME_ALUMINUM))
            self._ensure_lme_schema(connection)

    def _ensure_lme_schema(self, connection) -> None:
        """Patch older schemas missing LME columns."""

        if text is None:  # pragma: no cover - defensive guard
            raise ModuleNotFoundError("SQLAlchemy is required for relational backends")
        dialect = connection.engine.dialect.name
        lme_columns = {
            "price": "NUMERIC(18, 6)",
            "price_3_month": "NUMERIC(18, 6)",
            "stock": "NUMERIC(18, 6)",
            "created_at": "TIMESTAMP",
        }
        unwanted_columns = {"usd_price", "eur_price", "usd_change", "eur_change"}
        for table in ("lme_copper_rates", "lme_aluminum_rates"):
            if dialect == "postgresql":
                result = connection.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = current_schema() AND table_name = :table"
                    ),
                    {"table": table},
                )
                existing = {row[0] for row in result}
            elif dialect in {"mysql", "mariadb"}:
                result = connection.execute(text("SELECT DATABASE()"))
                schema_name = result.scalar()
                result = connection.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = :schema AND table_name = :table"
                    ),
                    {"schema": schema_name, "table": table},
                )
                existing = {row[0] for row in result}
            elif dialect == "sqlite":
                result = connection.execute(text(f"PRAGMA table_info({table})"))
                existing = {row[1] for row in result}
            else:  # pragma: no cover - unknown dialect
                continue

            missing = [name for name in lme_columns if name not in existing]
            for column_name in missing:
                connection.execute(
                    text(
                        f"ALTER TABLE {table} "
                        f"ADD COLUMN {column_name} {lme_columns[column_name]}"
                    )
                )
            extra = [name for name in unwanted_columns if name in existing]
            if not extra:
                continue
            if dialect == "postgresql":
                for column_name in extra:
                    connection.execute(
                        text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {column_name}")
                    )
            elif dialect in {"mysql", "mariadb"}:
                for column_name in extra:
                    connection.execute(text(f"ALTER TABLE {table} DROP COLUMN {column_name}"))
            elif dialect == "sqlite":
                desired = ["rate_date", "price", "price_3_month", "stock", "created_at"]
                temp_table = f"{table}_tmp"
                connection.execute(
                    text(
                        f"""
                        CREATE TABLE {temp_table} (
                            rate_date DATE NOT NULL,
                            price NUMERIC(18, 6) NULL,
                            price_3_month NUMERIC(18, 6) NULL,
                            stock NUMERIC(18, 6) NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY(rate_date)
                        )
                        """
                    )
                )
                columns_csv = ", ".join(desired)
                connection.execute(
                    text(
                        f"""
                        INSERT INTO {temp_table} ({columns_csv})
                        SELECT {columns_csv}
                        FROM {table}
                        """
                    )
                )
                connection.execute(text(f"DROP TABLE {table}"))
                connection.execute(text(f"ALTER TABLE {temp_table} RENAME TO {table}"))

    def insert_rates(self, rows: Sequence[ForexRateRecord]) -> PersistenceResult:
        result = PersistenceResult()
        if not rows:
            return result
        engine = self._get_engine()
        if text is None:  # pragma: no cover - defensive guard
            raise ModuleNotFoundError("SQLAlchemy is required for relational backends")
        dialect = engine.dialect.name

        def _build_upsert_sql(
            table: str,
            columns: Sequence[str],
            conflict: Sequence[str],
            updates: Sequence[str],
        ) -> str | None:
            placeholders = ", ".join(f":{col}" for col in columns)
            columns_csv = ", ".join(columns)
            base = f"INSERT INTO {table}({columns_csv}) VALUES({placeholders})"
            if dialect in {"postgresql", "sqlite"}:
                update_csv = ", ".join(f"{col} = EXCLUDED.{col}" for col in updates)
                conflict_csv = ", ".join(conflict)
                return f"{base} ON CONFLICT({conflict_csv}) DO UPDATE SET {update_csv}"
            if dialect in {"mysql", "mariadb"}:
                update_csv = ", ".join(f"{col} = VALUES({col})" for col in updates)
                return f"{base} ON DUPLICATE KEY UPDATE {update_csv}"
            return None

        rbi_columns = ["rate_date", "currency_code", "rate", "base_currency", "created_at"]
        rbi_updates = ["rate", "base_currency", "created_at"]
        sbi_columns = [
            "rate_date",
            "currency_code",
            "rate",
            "base_currency",
            "tt_buy",
            "tt_sell",
            "bill_buy",
            "bill_sell",
            "travel_card_buy",
            "travel_card_sell",
            "cn_buy",
            "cn_sell",
            "created_at",
        ]
        sbi_updates = [
            "rate",
            "base_currency",
            "tt_buy",
            "tt_sell",
            "bill_buy",
            "bill_sell",
            "travel_card_buy",
            "travel_card_sell",
            "cn_buy",
            "cn_sell",
            "created_at",
        ]
        rbi_sql = _build_upsert_sql(
            "forex_rates_rbi",
            rbi_columns,
            ["rate_date", "currency_code"],
            rbi_updates,
        )
        sbi_sql = _build_upsert_sql(
            "forex_rates_sbi",
            sbi_columns,
            ["rate_date", "currency_code"],
            sbi_updates,
        )

        rbi_rows: list[ForexRateRecord] = []
        sbi_rows: list[ForexRateRecord] = []
        for row in rows:
            if (row.source or "RBI").upper() == "SBI":
                sbi_rows.append(row)
            else:
                rbi_rows.append(row)

        def _postgres_bulk_upsert(
            connection,
            table: str,
            columns: Sequence[str],
            conflict: Sequence[str],
            updates: Sequence[str],
            params_list: Sequence[Mapping[str, object]],
        ) -> bool:
            try:  # pragma: no cover - optional dependency
                from psycopg2.extras import execute_values
            except ModuleNotFoundError:
                return False
            raw = getattr(connection, "connection", None)
            if raw is None:
                return False
            values = [[params[column] for column in columns] for params in params_list]
            if not values:
                return True
            columns_csv = ", ".join(columns)
            conflict_csv = ", ".join(conflict)
            update_csv = ", ".join(f"{col} = EXCLUDED.{col}" for col in updates)
            sql = (
                f"INSERT INTO {table} ({columns_csv}) VALUES %s "
                f"ON CONFLICT({conflict_csv}) DO UPDATE SET {update_csv}"
            )
            with raw.cursor() as cursor:
                execute_values(cursor, sql, values)
            return True

        def _mysql_bulk_upsert(
            connection,
            table: str,
            columns: Sequence[str],
            updates: Sequence[str],
            params_list: Sequence[Mapping[str, object]],
        ) -> bool:
            raw = getattr(connection, "connection", None)
            if raw is None:
                return False
            if not params_list:
                return True
            placeholders = ", ".join(["%s"] * len(columns))
            columns_csv = ", ".join(columns)
            update_csv = ", ".join(f"{col} = VALUES({col})" for col in updates)
            sql = (
                f"INSERT INTO {table} ({columns_csv}) VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {update_csv}"
            )
            values = [tuple(params[column] for column in columns) for params in params_list]
            with raw.cursor() as cursor:
                cursor.executemany(sql, values)
            return True

        with engine.begin() as connection:
            if rbi_rows:
                now = datetime.utcnow()
                rbi_params: list[dict[str, object]] = [
                    {
                        "rate_date": row.rate_date,
                        "currency_code": row.currency,
                        "rate": row.rate,
                        "base_currency": "INR",
                        "created_at": now,
                    }
                    for row in rbi_rows
                ]
                fast_inserted = False
                if dialect == "postgresql":
                    fast_inserted = _postgres_bulk_upsert(
                        connection,
                        "forex_rates_rbi",
                        rbi_columns,
                        ["rate_date", "currency_code"],
                        rbi_updates,
                        rbi_params,
                    )
                elif dialect in {"mysql", "mariadb"}:
                    fast_inserted = _mysql_bulk_upsert(
                        connection,
                        "forex_rates_rbi",
                        rbi_columns,
                        rbi_updates,
                        rbi_params,
                    )
                if not fast_inserted:
                    if rbi_sql is None:
                        for params in rbi_params:
                            connection.execute(text(DELETE_RBI_SQL), params)
                            connection.execute(text(INSERT_RBI_SQL), params)
                    else:
                        connection.execute(text(rbi_sql), rbi_params)
                result.inserted += len(rbi_rows)
            if sbi_rows:
                now = datetime.utcnow()
                sbi_params: list[dict[str, object]] = [
                    {
                        "rate_date": row.rate_date,
                        "currency_code": row.currency,
                        "rate": row.rate,
                        "base_currency": "INR",
                        "tt_buy": row.tt_buy,
                        "tt_sell": row.tt_sell,
                        "bill_buy": row.bill_buy,
                        "bill_sell": row.bill_sell,
                        "travel_card_buy": row.travel_card_buy,
                        "travel_card_sell": row.travel_card_sell,
                        "cn_buy": row.cn_buy,
                        "cn_sell": row.cn_sell,
                        "created_at": now,
                    }
                    for row in sbi_rows
                ]
                fast_inserted = False
                if dialect == "postgresql":
                    fast_inserted = _postgres_bulk_upsert(
                        connection,
                        "forex_rates_sbi",
                        sbi_columns,
                        ["rate_date", "currency_code"],
                        sbi_updates,
                        sbi_params,
                    )
                elif dialect in {"mysql", "mariadb"}:
                    fast_inserted = _mysql_bulk_upsert(
                        connection,
                        "forex_rates_sbi",
                        sbi_columns,
                        sbi_updates,
                        sbi_params,
                    )
                if not fast_inserted:
                    if sbi_sql is None:
                        for params in sbi_params:
                            connection.execute(text(DELETE_SBI_SQL), params)
                            connection.execute(text(INSERT_SBI_SQL), params)
                    else:
                        connection.execute(text(sbi_sql), sbi_params)
                result.inserted += len(sbi_rows)
        return result

    def insert_lme_rates(self, metal: str, rows: Sequence[LmeRateRecord]) -> PersistenceResult:
        result = PersistenceResult()
        if not rows:
            return result
        _, insert_sql, delete_sql = self._resolve_lme_statements(metal)
        engine = self._get_engine()
        if text is None:  # pragma: no cover - defensive guard
            raise ModuleNotFoundError("SQLAlchemy is required for relational backends")
        dialect = engine.dialect.name
        columns = ["rate_date", "price", "price_3_month", "stock", "created_at"]
        updates = ["price", "price_3_month", "stock", "created_at"]

        def _build_upsert_sql(table: str) -> str | None:
            placeholders = ", ".join(f":{col}" for col in columns)
            columns_csv = ", ".join(columns)
            base = f"INSERT INTO {table}({columns_csv}) VALUES({placeholders})"
            if dialect in {"postgresql", "sqlite"}:
                update_csv = ", ".join(f"{col} = EXCLUDED.{col}" for col in updates)
                return f"{base} ON CONFLICT(rate_date) DO UPDATE SET {update_csv}"
            if dialect in {"mysql", "mariadb"}:
                update_csv = ", ".join(f"{col} = VALUES({col})" for col in updates)
                return f"{base} ON DUPLICATE KEY UPDATE {update_csv}"
            return None

        table = "lme_copper_rates" if metal.upper() in {"CU", "COPPER"} else "lme_aluminum_rates"
        upsert_sql = _build_upsert_sql(table)
        params: list[dict[str, object]] = [
            {
                "rate_date": row.rate_date,
                "price": row.price,
                "price_3_month": row.price_3_month,
                "stock": row.stock,
                "created_at": datetime.utcnow(),
            }
            for row in rows
        ]

        def _postgres_bulk_upsert(
            connection, table: str, columns: Sequence[str], updates: Sequence[str]
        ) -> bool:
            try:  # pragma: no cover - optional dependency
                from psycopg2.extras import execute_values
            except ModuleNotFoundError:
                return False
            raw = getattr(connection, "connection", None)
            if raw is None:
                return False
            values = [[row[column] for column in columns] for row in params]
            if not values:
                return True
            columns_csv = ", ".join(columns)
            update_csv = ", ".join(f"{col} = EXCLUDED.{col}" for col in updates)
            sql = (
                f"INSERT INTO {table} ({columns_csv}) VALUES %s "
                f"ON CONFLICT(rate_date) DO UPDATE SET {update_csv}"
            )
            with raw.cursor() as cursor:
                execute_values(cursor, sql, values)
            return True

        def _mysql_bulk_upsert(connection, table: str, columns: Sequence[str]) -> bool:
            raw = getattr(connection, "connection", None)
            if raw is None:
                return False
            if not params:
                return True
            placeholders = ", ".join(["%s"] * len(columns))
            columns_csv = ", ".join(columns)
            update_csv = ", ".join(f"{col} = VALUES({col})" for col in updates)
            sql = (
                f"INSERT INTO {table} ({columns_csv}) VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {update_csv}"
            )
            values = [tuple(row[column] for column in columns) for row in params]
            with raw.cursor() as cursor:
                cursor.executemany(sql, values)
            return True

        with engine.begin() as connection:
            fast_inserted = False
            if dialect == "postgresql":
                fast_inserted = _postgres_bulk_upsert(connection, table, columns, updates)
            elif dialect in {"mysql", "mariadb"}:
                fast_inserted = _mysql_bulk_upsert(connection, table, columns)
            if not fast_inserted:
                if upsert_sql is None:
                    for row_params in params:
                        connection.execute(text(delete_sql), row_params)
                        connection.execute(text(insert_sql), row_params)
                else:
                    connection.execute(text(upsert_sql), params)
        result.inserted += len(rows)
        return result

    def update_ingestion_checkpoint(self, source: str, rate_date: date) -> None:
        engine = self._get_engine()
        if text is None:  # pragma: no cover - defensive guard
            raise ModuleNotFoundError("SQLAlchemy is required for relational backends")
        with engine.begin() as connection:
            dialect = connection.engine.dialect.name
            params = {"source": source.upper(), "last_ingested_date": rate_date}
            if dialect == "postgresql":
                connection.execute(
                    text(
                        """
                        INSERT INTO ingestion_metadata(source, last_ingested_date)
                        VALUES(:source, :last_ingested_date)
                        ON CONFLICT(source) DO UPDATE
                        SET last_ingested_date = EXCLUDED.last_ingested_date
                        WHERE EXCLUDED.last_ingested_date > ingestion_metadata.last_ingested_date
                        """
                    ),
                    params,
                )
            elif dialect in {"mysql", "mariadb"}:
                connection.execute(
                    text(
                        """
                        INSERT INTO ingestion_metadata(source, last_ingested_date)
                        VALUES(:source, :last_ingested_date)
                        ON DUPLICATE KEY UPDATE
                        last_ingested_date = IF(
                            VALUES(last_ingested_date) > last_ingested_date,
                            VALUES(last_ingested_date),
                            last_ingested_date
                        )
                        """
                    ),
                    params,
                )
            elif dialect == "sqlite":
                connection.execute(
                    text(
                        """
                        INSERT INTO ingestion_metadata(source, last_ingested_date)
                        VALUES(:source, :last_ingested_date)
                        ON CONFLICT(source) DO UPDATE
                        SET last_ingested_date = excluded.last_ingested_date
                        WHERE excluded.last_ingested_date > ingestion_metadata.last_ingested_date
                        """
                    ),
                    params,
                )

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

    def fetch_lme_range(
        self, metal: str, start: date | None = None, end: date | None = None
    ) -> list[LmeRateRecord]:
        engine = self._get_engine()
        if text is None:  # pragma: no cover - defensive guard
            raise ModuleNotFoundError("SQLAlchemy is required for relational backends")
        normalised, _, _ = self._resolve_lme_statements(metal)
        table = "lme_copper_rates" if normalised == "COPPER" else "lme_aluminum_rates"

        def _build_query() -> tuple[str, dict[str, object]]:
            clauses: list[str] = []
            params: dict[str, object] = {}
            if start is not None:
                clauses.append("rate_date >= :start_date")
                params["start_date"] = start
            if end is not None:
                clauses.append("rate_date <= :end_date")
                params["end_date"] = end
            query = f"SELECT * FROM {table} ORDER BY rate_date"
            if clauses:
                query = (
                    f"SELECT * FROM {table} WHERE " + " AND ".join(clauses) + " ORDER BY rate_date"
                )
            return query, params

        query, params = _build_query()
        records: list[LmeRateRecord] = []
        with engine.connect() as connection:
            for row in connection.execute(text(query), params):
                mapping = row._mapping
                stock_value = casting_float(mapping.get("stock"))
                records.append(
                    LmeRateRecord(
                        rate_date=_normalise_rate_date(mapping["rate_date"]),
                        price=casting_float(mapping.get("price")),
                        price_3_month=casting_float(mapping.get("price_3_month")),
                        stock=int(stock_value) if stock_value is not None else None,
                        metal=normalised,
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


def casting_float(value: object | None) -> float | None:
    try:
        if value is None:
            return None
        return float(cast("SupportsFloat | SupportsIndex | str | bytes | bytearray", value))
    except (TypeError, ValueError):  # pragma: no cover - defensive parsing guard
        return None


__all__ = ["RelationalBackend"]
