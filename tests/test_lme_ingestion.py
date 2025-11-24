from datetime import date
from fx_bharat.db.sqlite_manager import SQLiteManager
from fx_bharat.ingestion.lme import LME_URLS, parse_lme_table
from fx_bharat.ingestion.models import LmeRateRecord


def test_parse_lme_table_handles_currency_columns():
    html = """
    <table>
        <tr><th>Date</th><th>USD</th><th>EUR</th><th>USD +/-</th><th>EUR +/-</th></tr>
        <tr><td>21.11.2024</td><td>8,500.00</td><td>7,800.00</td><td>50</td><td>-20</td></tr>
        <tr><td>20.11.2024</td><td>8,450.00</td><td>7,750.00</td><td>-10</td><td>-15</td></tr>
    </table>
    """
    result = parse_lme_table(html, "copper")
    assert result.metal == "COPPER"
    assert [record.rate_date for record in result.rows] == [
        date(2024, 11, 21),
        date(2024, 11, 20),
    ]
    assert result.rows[0].usd_price == 8500.0
    assert result.rows[0].eur_price == 7800.0
    assert result.rows[0].usd_change == 50.0
    assert result.rows[0].eur_change == -20.0


def test_sqlite_manager_supports_lme(tmp_path):
    db_path = tmp_path / "lme.db"
    manager = SQLiteManager(db_path)
    try:
        records = [
            LmeRateRecord(
                rate_date=date(2024, 11, 21),
                usd_price=8500.0,
                eur_price=7800.0,
                usd_change=50.0,
                eur_change=-20.0,
                metal="COPPER",
            ),
            LmeRateRecord(
                rate_date=date(2024, 11, 22),
                usd_price=8550.0,
                eur_price=7850.0,
                metal="ALUMINUM",
            ),
        ]
        manager.insert_lme_rates("COPPER", [records[0]])
        manager.insert_lme_rates("ALUMINUM", [records[1]])
        fetched_cu = manager.fetch_lme_range("COPPER")
        fetched_al = manager.fetch_lme_range("ALUMINUM")
    finally:
        manager.close()
    assert len(fetched_cu) == 1
    assert fetched_cu[0].usd_price == 8500.0
    assert len(fetched_al) == 1
    assert fetched_al[0].metal == "ALUMINUM"


def test_lme_urls_defined():
    assert "COPPER" in LME_URLS
    assert "ALUMINUM" in LME_URLS
    assert "LME_Cu_cash" in LME_URLS["COPPER"]
    assert "LME_Al_cash" in LME_URLS["ALUMINUM"]
