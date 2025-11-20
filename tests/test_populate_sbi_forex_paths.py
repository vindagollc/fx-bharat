from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

from fx_bharat.seeds.populate_sbi_forex import _iter_pdf_paths, parse_args


def test_iter_pdf_paths_filters_dates(tmp_path: Path) -> None:
    valid = [
        tmp_path / "2024-01-02.pdf",
        tmp_path / "2024-01-05.pdf",
        tmp_path / "2024-01-10.pdf",
    ]
    for path in valid:
        path.write_bytes(b"dummy")
    (tmp_path / "README.txt").write_text("ignore")
    (tmp_path / "not-a-date.pdf").write_bytes(b"skip")

    paths = list(_iter_pdf_paths(tmp_path, start=date(2024, 1, 3), end=date(2024, 1, 7)))

    assert paths == [tmp_path / "2024-01-05.pdf"]


def test_iter_pdf_paths_missing_directory() -> None:
    assert list(_iter_pdf_paths(Path("/nonexistent"), None, None) or []) == []


def test_parse_args_default_values(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "prog",
        ],
    )

    args = parse_args()

    assert args.resource_dir == "resources"
    assert args.db_path
    assert args.start is None
    assert args.end is None
    assert args.download is False
