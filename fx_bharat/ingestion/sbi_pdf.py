"""Parse SBI forex card rate PDFs into ``ForexRateRecord`` rows."""

from __future__ import annotations

import re
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable
from urllib.request import urlretrieve

from fx_bharat.ingestion.models import ForexRateRecord
from fx_bharat.utils.logger import get_logger

LOGGER = get_logger(__name__)

SBI_FOREX_PDF_URL = "https://sbi.bank.in/documents/16012/1400784/FOREX_CARD_RATES.pdf"


@dataclass(slots=True)
class SBIPDFParseResult:
    """Represents the parsed content of a single SBI forex PDF."""

    rate_date: date
    rates: list[ForexRateRecord]


class SBIPDFParser:
    """Convert SBI forex PDF documents into structured rows."""

    _CURRENCY_ALIAS_MAP: dict[str, str] = {
        "UAE DIRHAM": "AED",
        "AUS DOLLAR": "AUD",
        "CAD DOLLAR": "CAD",
        "DANISH KRONE": "DKK",
        "EURO": "EUR",
        "HK DOLLAR": "HKD",
        "JAP YEN": "JPY",
        "NOR KRONE": "NOK",
        "NZ DOLLAR": "NZD",
        "SWISS FRANC": "CHF",
        "SG DOLLAR": "SGD",
        "STERLING": "GBP",
        "SA RAND": "ZAR",
        "SAUDI RIYAL": "SAR",
        "SWED KRONA": "SEK",
        "USD": "USD",
    }

    _CURRENCY_PATTERN = re.compile(r"\b([A-Z]{3})\b\s+([0-9]+(?:\.[0-9]+)?)")
    _DATE_PATTERNS = (
        re.compile(r"(\d{4})[-/](\d{2})[-/](\d{2})"),
        re.compile(r"(\d{2})[-/](\d{2})[-/](\d{4})"),
    )

    def parse(self, pdf_path: str | Path, *, rate_date: date | None = None) -> SBIPDFParseResult:
        text = self._extract_text(Path(pdf_path))
        resolved_date = rate_date or self._infer_date(text, pdf_path)
        rows = list(self._extract_rates(text, resolved_date))
        return SBIPDFParseResult(rate_date=resolved_date, rates=rows)

    def _extract_text(self, path: Path) -> str:
        try:
            from pypdf import PdfReader  # type: ignore[import-not-found]
        except ModuleNotFoundError:  # pragma: no cover - dependency missing
            PdfReader = None  # type: ignore[assignment]

        if PdfReader is not None:
            try:
                reader = PdfReader(path)
                return "\n".join(page.extract_text() or "" for page in reader.pages)
            except Exception as exc:  # pragma: no cover - defensive fallback
                LOGGER.warning("Falling back to raw text parsing for %s (%s)", path, exc)

        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc:  # pragma: no cover - defensive fallback
            LOGGER.error("Unable to read %s: %s", path, exc)
            raise

    def _infer_date(self, text: str, pdf_path: str | Path) -> date:
        for idx, pattern in enumerate(self._DATE_PATTERNS):
            match = pattern.search(text)
            if not match:
                continue
            groups = match.groups()
            if idx == 0:
                year, month, day = (int(groups[0]), int(groups[1]), int(groups[2]))
            else:
                day, month, year = (int(groups[0]), int(groups[1]), int(groups[2]))
            return date(year, month, day)
        try:
            return date.fromisoformat(Path(pdf_path).stem)
        except ValueError:
            return datetime.utcnow().date()

    def _extract_rates(self, text: str, rate_date: date) -> Iterable[ForexRateRecord]:
        cleaned_text = re.sub(r"[,\t]+", " ", text.upper())
        rates: dict[str, float] = {}

        for alias, code in self._CURRENCY_ALIAS_MAP.items():
            name_pattern = re.compile(alias + r"\s+([0-9]+(?:\.[0-9]+)?)")
            for match in name_pattern.finditer(cleaned_text):
                rates[code] = float(match.group(1))

        for code, rate in self._CURRENCY_PATTERN.findall(cleaned_text):
            rates[code] = float(rate)

        for code, rate in rates.items():
            yield ForexRateRecord(rate_date=rate_date, currency=code, rate=rate, source="SBI")


class SBIPDFDownloader:
    """Download the latest SBI forex PDF from the public endpoint."""

    def __init__(self, download_dir: str | Path | None = None) -> None:
        self.download_dir = Path(download_dir) if download_dir else Path(tempfile.mkdtemp())
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def fetch_latest(self) -> Path:
        destination = self.download_dir / Path(SBI_FOREX_PDF_URL).name
        LOGGER.info("Downloading SBI forex PDF to %s", destination)
        urlretrieve(SBI_FOREX_PDF_URL, destination)
        return destination


__all__ = ["SBI_FOREX_PDF_URL", "SBIPDFDownloader", "SBIPDFParser", "SBIPDFParseResult"]
