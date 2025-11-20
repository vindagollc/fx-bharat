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
        self._rbi_collection: Collection = db["forex_rates_rbi"]
        self._sbi_collection: Collection = db["forex_rates_sbi"]
        self._collection: Collection | None = None

    def ensure_schema(self) -> None:
        try:
            LOGGER.info("Ensuring MongoDB forex rate collections exist")
            self._client.admin.command("ping")
            self._rbi_collection.create_index([("rate_date", 1), ("currency_code", 1)], unique=True)
            self._sbi_collection.create_index([("rate_date", 1), ("currency_code", 1)], unique=True)
        except PyMongoError as exc:  # pragma: no cover - error path
            raise RuntimeError(f"Failed to ensure MongoDB schema: {exc}") from exc

    def insert_rates(self, rows: Sequence[ForexRateRecord]) -> PersistenceResult:
        result = PersistenceResult()
        if not rows:
            return result
        rbi_ops: list[UpdateOne] = []
        sbi_ops: list[UpdateOne] = []
        for row in rows:
            target_ops = sbi_ops if (row.source or "RBI").upper() == "SBI" else rbi_ops
            doc = {
                "rate_date": row.rate_date.isoformat(),
                "currency_code": row.currency,
                "rate": row.rate,
                "base_currency": "INR",
                "created_at": datetime.utcnow(),
            }
            optional_fields = {
                "tt_buy": row.tt_buy,
                "tt_sell": row.tt_sell,
                "bill_buy": row.bill_buy,
                "bill_sell": row.bill_sell,
                "travel_card_buy": row.travel_card_buy,
                "travel_card_sell": row.travel_card_sell,
                "cn_buy": row.cn_buy,
                "cn_sell": row.cn_sell,
            }
            for key, value in optional_fields.items():
                if value is not None:
                    doc[key] = value
            target_ops.append(
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
            rbi_collection = getattr(self, "_rbi_collection", None)
            if rbi_collection is None:
                rbi_collection = getattr(self, "_collection", None)

            sbi_collection = getattr(self, "_sbi_collection", None)
            if sbi_collection is None:
                sbi_collection = getattr(self, "_collection", None)

            if rbi_ops and rbi_collection is not None:
                rbi_collection.bulk_write(rbi_ops, ordered=False)
            if sbi_ops and sbi_collection is not None:
                sbi_collection.bulk_write(sbi_ops, ordered=False)
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
        def _collection_query(collection: Collection) -> list[ForexRateRecord]:
            query: dict[str, Any] = {}
            if start is not None or end is not None:
                range_query: dict[str, str] = {}
                if start is not None:
                    range_query["$gte"] = start.isoformat()
                if end is not None:
                    range_query["$lte"] = end.isoformat()
                query["rate_date"] = range_query
            docs = collection.find(query).sort("rate_date", 1)
            source_label = "RBI"
            if getattr(collection, "name", "").endswith("sbi"):
                source_label = "SBI"
            return [
                ForexRateRecord(
                    rate_date=date.fromisoformat(doc["rate_date"]),
                    currency=doc.get("currency_code", doc.get("currency", "")),
                    rate=float(doc["rate"]),
                    source=source_label,
                    tt_buy=doc.get("tt_buy"),
                    tt_sell=doc.get("tt_sell"),
                    bill_buy=doc.get("bill_buy"),
                    bill_sell=doc.get("bill_sell"),
                    travel_card_buy=doc.get("travel_card_buy"),
                    travel_card_sell=doc.get("travel_card_sell"),
                    cn_buy=doc.get("cn_buy"),
                    cn_sell=doc.get("cn_sell"),
                )
                for doc in docs
            ]

        records: list[ForexRateRecord] = []
        sbi_collection = getattr(self, "_sbi_collection", None)
        if sbi_collection is None:
            sbi_collection = getattr(self, "_collection", None)

        rbi_collection = getattr(self, "_rbi_collection", None)
        if rbi_collection is None:
            rbi_collection = getattr(self, "_collection", None)
        if source is None or source.upper() == "SBI":
            if sbi_collection is not None:
                records.extend(_collection_query(sbi_collection))
        if source is None or source.upper() == "RBI":
            if rbi_collection is not None:
                records.extend(_collection_query(rbi_collection))
        return records

    def close(self) -> None:  # pragma: no cover - trivial cleanup
        self._client.close()


__all__ = ["MongoBackend"]
