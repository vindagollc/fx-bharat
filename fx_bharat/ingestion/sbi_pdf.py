"""Parse SBI forex card rate PDFs into ``ForexRateRecord`` rows."""

from __future__ import annotations

import re
import tempfile
import zlib
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
        re.compile(r"DATE\s*[:]?\s*(\d{2})[-/](\d{2})[-/](\d{4})"),
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

        # Manual PDF stream extraction to cope when ``pypdf`` is unavailable
        raw_bytes = path.read_bytes()
        texts: list[str] = []
        for match in re.finditer(b"stream\r?\n", raw_bytes):
            start = match.end()
            end = raw_bytes.find(b"endstream", start)
            if end == -1:
                continue
            stream = raw_bytes[start:end].lstrip(b"\r\n").rstrip(b"\r\n")
            try:
                stream = zlib.decompress(stream)
            except Exception:
                pass
            for text_match in re.finditer(rb"\(([^)]*)\)", stream):
                try:
                    texts.append(text_match.group(1).decode("latin-1"))
                except Exception:
                    continue

        if texts:
            return "".join(texts)

        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc:  # pragma: no cover - defensive fallback
            LOGGER.error("Unable to read %s: %s", path, exc)
            raise

    def _infer_date(self, text: str, pdf_path: str | Path) -> date:
        for idx, pattern in enumerate(self._DATE_PATTERNS):
            match = pattern.search(text.upper())
            if not match:
                continue
            groups = match.groups()
            try:
                if idx == 0:
                    day, month, year = (
                        int(groups[0]),
                        int(groups[1]),
                        int(groups[2]),
                    )
                elif idx == 1:
                    year, month, day = (
                        int(groups[0]),
                        int(groups[1]),
                        int(groups[2]),
                    )
                else:
                    day, month, year = (
                        int(groups[0]),
                        int(groups[1]),
                        int(groups[2]),
                    )
                return date(year, month, day)
            except ValueError:
                LOGGER.warning("Ignoring invalid date %s in %s", match.group(0), pdf_path)
                continue
        try:
            return date.fromisoformat(Path(pdf_path).stem)
        except ValueError:
            LOGGER.warning("Falling back to UTC date for %s", pdf_path)
            return datetime.utcnow().date()

    def _extract_rates(self, text: str, rate_date: date) -> Iterable[ForexRateRecord]:
        cleaned_text = re.sub(r"[,\t]+", " ", text.upper())
        cleaned_text = re.sub(r"(\d\.\d{2})(?=\d)", r"\1 ", cleaned_text)
        cleaned_text = re.sub(r"(\d)\s*\.\s*(\d)", r"\1.\2", cleaned_text)
        tokens = sorted(
            set(self._CURRENCY_ALIAS_MAP.keys()) | set(self._CURRENCY_ALIAS_MAP.values()),
            key=len,
            reverse=True,
        )
        token_pattern = r"(" + "|".join(re.escape(token) for token in tokens) + r")"
        cleaned_text = re.sub(token_pattern, r"\n\1", cleaned_text)
        cleaned_text = re.sub(r" +", " ", cleaned_text)
        lines = [line.strip() for line in cleaned_text.splitlines() if line.strip()]

        seen: set[str] = set()

        for line in lines:
            matched = False
            for alias, code in self._CURRENCY_ALIAS_MAP.items():
                if alias not in line:
                    continue
                if code in seen:
                    matched = True
                    break
                numbers = [float(value) for value in re.findall(r"[0-9]+(?:\.[0-9]+)?", line)]
                if len(numbers) < 8:
                    continue
                (
                    tt_buy,
                    tt_sell,
                    bill_buy,
                    bill_sell,
                    travel_card_buy,
                    travel_card_sell,
                    cn_buy,
                    cn_sell,
                ) = numbers[:8]
                yield ForexRateRecord(
                    rate_date=rate_date,
                    currency=code,
                    rate=tt_buy,
                    source="SBI",
                    tt_buy=tt_buy,
                    tt_sell=tt_sell,
                    bill_buy=bill_buy,
                    bill_sell=bill_sell,
                    travel_card_buy=travel_card_buy,
                    travel_card_sell=travel_card_sell,
                    cn_buy=cn_buy,
                    cn_sell=cn_sell,
                )
                seen.add(code)
                matched = True
            if matched:
                continue
            code_match = re.match(r"([A-Z]{3})\b", line)
            if not code_match:
                continue
            code = code_match.group(1)
            if code in seen:
                continue
            numbers = [float(value) for value in re.findall(r"[0-9]+(?:\.[0-9]+)?", line)]
            if len(numbers) < 8:
                continue
            (
                tt_buy,
                tt_sell,
                bill_buy,
                bill_sell,
                travel_card_buy,
                travel_card_sell,
                cn_buy,
                cn_sell,
            ) = numbers[:8]
            yield ForexRateRecord(
                rate_date=rate_date,
                currency=code,
                rate=tt_buy,
                source="SBI",
                tt_buy=tt_buy,
                tt_sell=tt_sell,
                bill_buy=bill_buy,
                bill_sell=bill_sell,
                travel_card_buy=travel_card_buy,
                travel_card_sell=travel_card_sell,
                cn_buy=cn_buy,
                cn_sell=cn_sell,
            )
            seen.add(code)


class SBIPDFDownloader:
    """Download the latest SBI forex PDF from the public endpoint."""

    def __init__(self, download_dir: str | Path | None = None) -> None:
        self.download_dir = Path(download_dir) if download_dir else Path(tempfile.mkdtemp())
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def fetch_latest(self, destination: str | Path | None = None) -> Path:
        """Fetch the PDF and optionally persist it to ``destination``.

        When ``destination`` is omitted, the PDF is stored in ``download_dir``.
        """

        destination_path = (
            Path(destination) if destination else self.download_dir / Path(SBI_FOREX_PDF_URL).name
        )
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Downloading SBI forex PDF to %s", destination_path)
        urlretrieve(SBI_FOREX_PDF_URL, destination_path)
        return destination_path


__all__ = ["SBI_FOREX_PDF_URL", "SBIPDFDownloader", "SBIPDFParser", "SBIPDFParseResult"]
