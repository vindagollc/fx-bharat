from __future__ import annotations

from datetime import date

from fx_bharat.db.sqlite_backend import SQLiteBackend
from fx_bharat.seeds import populate_rbi_forex as rbi_module


def test_seed_rbi_forex_dry_run(tmp_path) -> None:
    backend = SQLiteBackend(db_path=tmp_path / "rbi.db")
    result = rbi_module.seed_rbi_forex(
        "2024-01-01",
        "2024-01-02",
        backend=backend,
        dry_run=True,
    )
    assert result.total == 0


def test_seed_rbi_forex_skips_when_checkpoint_ahead(tmp_path) -> None:
    backend = SQLiteBackend(db_path=tmp_path / "rbi.db")
    backend.update_ingestion_checkpoint("RBI", date(2024, 1, 10))

    result = rbi_module.seed_rbi_forex(
        "2024-01-01",
        "2024-01-02",
        backend=backend,
        incremental=True,
    )

    assert result.total == 0


def test_rbi_parse_args_defaults(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "prog",
            "--from",
            "2024-01-01",
            "--to",
            "2024-01-31",
            "--db-url",
            "sqlite:///tmp.db",
        ],
    )
    args = rbi_module.parse_args()
    assert args.headless is True
    assert args.download_dir is None
    assert args.db_url == "sqlite:///tmp.db"
