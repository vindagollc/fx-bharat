from __future__ import annotations

from datetime import date
from pathlib import Path

from fx_bharat.ingestion.strategy import IngestionStrategy


class _DummyStrategy:
    def fetch(self, start_date: date, end_date: date, *, destination: Path | None = None) -> Path:
        target = destination or Path("default.dat")
        target.write_text(f"{start_date.isoformat()}-{end_date.isoformat()}")
        return target


def test_ingestion_strategy_contract(tmp_path: Path) -> None:
    strategy: IngestionStrategy = _DummyStrategy()
    dest = tmp_path / "artifact.dat"

    result = strategy.fetch(date(2024, 1, 1), date(2024, 1, 2), destination=dest)

    assert result == dest
    assert result.read_text() == "2024-01-01-2024-01-02"
