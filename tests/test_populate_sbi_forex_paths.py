from __future__ import annotations

from datetime import date
from pathlib import Path

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
            "--db-url",
            "sqlite:///tmp.db",
        ],
    )

    args = parse_args()

    assert args.resource_dir == "resources"
    assert args.db_url == "sqlite:///tmp.db"
    assert args.start is None
    assert args.end is None
    assert args.download is False


def test_download_archive_pdfs_skips_existing(tmp_path, monkeypatch) -> None:
    from fx_bharat.seeds.populate_sbi_forex import _download_archive_pdfs

    # Existing file should be reused; urlretrieve must not be called.
    existing_date = date(2024, 1, 5)
    dest = tmp_path / "2024" / "1" / f"{existing_date}.pdf"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("existing")

    called = {"count": 0}

    def _fake_urlretrieve(url, filename):  # noqa: ANN001
        called["count"] += 1
        Path(filename).write_text("downloaded")

    monkeypatch.setattr("fx_bharat.seeds.populate_sbi_forex.urlretrieve", _fake_urlretrieve)

    results = _download_archive_pdfs([existing_date], tmp_path, "http://example.com")

    assert called["count"] == 0
    assert results == [dest]


def test_download_archive_pdfs_downloads_missing(tmp_path, monkeypatch) -> None:
    from fx_bharat.seeds.populate_sbi_forex import _download_archive_pdfs

    target_date = date(2024, 1, 6)
    called = {"count": 0}

    def _fake_urlretrieve(url, filename):  # noqa: ANN001
        called["count"] += 1
        Path(filename).write_text("downloaded")

    monkeypatch.setattr("fx_bharat.seeds.populate_sbi_forex.urlretrieve", _fake_urlretrieve)

    results = _download_archive_pdfs([target_date], tmp_path, "http://example.com", max_workers=1)

    expected = tmp_path / "2024" / "1" / f"{target_date}.pdf"
    assert called["count"] == 1
    assert results == [expected]
    assert expected.exists()
