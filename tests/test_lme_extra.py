from __future__ import annotations

import pytest

from fx_bharat.ingestion.lme import _normalise_metal, parse_lme_table


def test_lme_parse_requires_tables() -> None:
    with pytest.raises(ValueError):
        parse_lme_table("<html></html>", "copper")


def test_lme_normalise_metal_rejects_invalid() -> None:
    with pytest.raises(ValueError):
        _normalise_metal("gold")
