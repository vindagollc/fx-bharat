from __future__ import annotations

import pytest

from fx_bharat.ingestion.lme import (
    _coerce_date,
    _normalise_metal,
    _parse_float,
    _parse_int,
    parse_lme_table,
)


def test_lme_parse_requires_tables() -> None:
    with pytest.raises(ValueError):
        parse_lme_table("<html></html>", "copper")


def test_lme_normalise_metal_rejects_invalid() -> None:
    with pytest.raises(ValueError):
        _normalise_metal("gold")


def test_lme_parse_helpers_handle_invalid_values() -> None:
    assert _parse_float(None) is None
    assert _parse_float("N/A") is None
    assert _parse_int("10.5") == 10
    assert _parse_int("bad") is None
    assert _coerce_date(object()) is None


def test_lme_parse_requires_data_rows() -> None:
    html = """
    <table>
        <tr><th>Date</th><th>Cash</th></tr>
    </table>
    """
    with pytest.raises(ValueError):
        parse_lme_table(html, "COPPER")
