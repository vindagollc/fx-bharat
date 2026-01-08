from __future__ import annotations

from datetime import date

from fx_bharat.seeds import populate_rbi_forex as rbi_module


def test_seed_rbi_forex_dry_run(tmp_path) -> None:
    result = rbi_module.seed_rbi_forex(
        "2024-01-01",
        "2024-01-02",
        db_path=tmp_path / "rbi.db",
        dry_run=True,
    )
    assert result.total == 0


def test_seed_rbi_forex_skips_when_checkpoint_ahead(tmp_path) -> None:
    db_path = tmp_path / "rbi.db"
    with rbi_module.SQLiteManager(db_path) as manager:
        manager.update_ingestion_checkpoint("RBI", date(2024, 1, 10))

    result = rbi_module.seed_rbi_forex(
        "2024-01-01",
        "2024-01-02",
        db_path=db_path,
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
        ],
    )
    args = rbi_module.parse_args()
    assert args.headless is True
    assert args.download_dir is None
