from __future__ import annotations

from datetime import date

from fx_bharat.db.sqlite_manager import SQLiteManager
from fx_bharat.ingestion.models import LmeRateRecord
from fx_bharat.seeds.populate_lme import _filter_rows, _normalise_metal, seed_lme_prices


def test_normalise_metal_accepts_aliases() -> None:
    assert _normalise_metal("cu") == "COPPER"
    assert _normalise_metal("aluminium") == "ALUMINUM"


def test_filter_rows_applies_date_bounds() -> None:
    rows = [
        LmeRateRecord(
            rate_date=date(2024, 1, 1), price=1.0, price_3_month=None, stock=None, metal="COPPER"
        ),
        LmeRateRecord(
            rate_date=date(2024, 1, 5), price=2.0, price_3_month=None, stock=None, metal="COPPER"
        ),
        LmeRateRecord(
            rate_date=date(2024, 1, 9), price=3.0, price_3_month=None, stock=None, metal="COPPER"
        ),
    ]
    filtered = _filter_rows(rows, start=date(2024, 1, 2), end=date(2024, 1, 8))

    assert [row.rate_date for row in filtered] == [date(2024, 1, 5)]


def test_seed_lme_prices_with_html_inserts_rows(tmp_path) -> None:
    html = """
    <table>
        <tr><th>Date</th><th>LME Copper Cash-Settlement</th><th>Stock</th></tr>
        <tr><td>02.01.2024</td><td>8,500.00</td><td>123,000</td></tr>
    </table>
    """
    db_path = tmp_path / "lme_seed.db"
    result = seed_lme_prices("COPPER", db_path=db_path, html=html)

    assert result.metal == "COPPER"
    assert result.rows.total == 1

    with SQLiteManager(db_path) as manager:
        fetched = manager.fetch_lme_range("COPPER")
    assert len(fetched) == 1
    assert fetched[0].price == 8500.0


def test_seed_lme_prices_dry_run_skips_ingestion(tmp_path) -> None:
    result = seed_lme_prices("ALUMINUM", db_path=tmp_path / "lme_dry.db", dry_run=True)

    assert result.metal == "ALUMINUM"
    assert result.rows.total == 0
