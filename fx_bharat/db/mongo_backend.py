"""MongoDB backend strategy."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Sequence

from fx_bharat.db.base_backend import BackendStrategy
from fx_bharat.db.sqlite_manager import PersistenceResult
from fx_bharat.ingestion.models import ForexRateRecord
from fx_bharat.utils.logger import get_logger

try:  # pragma: no cover - optional dependency
    from pymongo import MongoClient, UpdateOne
    from pymongo.collection import Collection
    from pymongo.errors import PyMongoError
except ModuleNotFoundError:  # pragma: no cover - handled dynamically
    MongoClient = None  # type: ignore[assignment]
    UpdateOne = None  # type: ignore[assignment]
    Collection = None  # type: ignore[assignment]
    PyMongoError = Exception  # type: ignore[assignment]

LOGGER = get_logger(__name__)


class MongoBackend(BackendStrategy):
    """Backend strategy that persists forex rates inside MongoDB."""

    def __init__(self, url: str, *, database: str | None = None) -> None:
        if MongoClient is None:  # pragma: no cover - defensive
            raise ModuleNotFoundError("pymongo is required for MongoDB backends")
        self.url = url
        self._client = MongoClient(url)
        db = self._client.get_default_database() if database is None else self._client[database]
        if db is None:
            raise ValueError("MongoDB connection URI must include a database name")
        self._collection: Collection = db["forex_rates"]

    def ensure_schema(self) -> None:
        try:
            LOGGER.info("Ensuring MongoDB forex_rates collection exists")
            self._client.admin.command("ping")
            self._collection.create_index([("rate_date", 1), ("currency_code", 1)], unique=True)
        except PyMongoError as exc:  # pragma: no cover - error path
            raise RuntimeError(f"Failed to ensure MongoDB schema: {exc}") from exc

    def insert_rates(self, rows: Sequence[ForexRateRecord]) -> PersistenceResult:
        result = PersistenceResult()
        if not rows:
            return result
        bulk_ops = []
        for row in rows:
            doc = {
                "rate_date": row.rate_date.isoformat(),
                "currency_code": row.currency,
                "rate": row.rate,
                "base_currency": "INR",
                "source": row.source,
                "created_at": datetime.utcnow(),
            }
            optional_fields = {
                "tt_buy": row.tt_buy,
                "tt_sell": row.tt_sell,
                "bill_buy": row.bill_buy,
                "bill_sell": row.bill_sell,
                "travel_card_buy": row.travel_card_buy,
                "travel_card_sell": row.travel_card_sell,
            }
            for key, value in optional_fields.items():
                if value is not None:
                    doc[key] = value
            bulk_ops.append(
                UpdateOne(
                    {
                        "rate_date": doc["rate_date"],
                        "currency_code": doc["currency_code"],
                    },
                    {"$set": doc},
                    upsert=True,
                )
            )
        try:
            self._collection.bulk_write(bulk_ops, ordered=False)
            result.inserted += len(rows)
        except PyMongoError as exc:  # pragma: no cover - error path
            raise RuntimeError(f"Failed to insert MongoDB rates: {exc}") from exc
        return result

    def fetch_range(
        self,
        start: date | None = None,
        end: date | None = None,
        *,
        source: str | None = None,
    ) -> list[ForexRateRecord]:
        query: dict[str, Any] = {}
        if start is not None or end is not None:
            range_query: dict[str, str] = {}
            if start is not None:
                range_query["$gte"] = start.isoformat()
            if end is not None:
                range_query["$lte"] = end.isoformat()
            query["rate_date"] = range_query
        if source is not None:
            query["source"] = source
        docs = self._collection.find(query).sort("rate_date", 1)
        return [
            ForexRateRecord(
                rate_date=date.fromisoformat(doc["rate_date"]),
                currency=doc["currency_code"],
                rate=float(doc["rate"]),
                source=doc.get("source", "RBI"),
                tt_buy=doc.get("tt_buy"),
                tt_sell=doc.get("tt_sell"),
                bill_buy=doc.get("bill_buy"),
                bill_sell=doc.get("bill_sell"),
                travel_card_buy=doc.get("travel_card_buy"),
                travel_card_sell=doc.get("travel_card_sell"),
            )
            for doc in docs
        ]

    def close(self) -> None:  # pragma: no cover - trivial cleanup
        self._client.close()


__all__ = ["MongoBackend"]
