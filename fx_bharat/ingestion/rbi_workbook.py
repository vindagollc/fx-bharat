"""Convert downloaded RBI workbooks into clean CSV files."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from numbers import Real
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Sequence

from fx_bharat.utils.logger import get_logger

try:  # pragma: no cover - optional dependency
    import pandas as _pd  # type: ignore
except Exception:  # pragma: no cover - keep lightweight installs working
    _pd = None  # type: ignore

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

LOGGER = get_logger(__name__)
CSV_HEADER = ("Date", "USD", "GBP", "EURO", "YEN")
RATE_COLUMNS = CSV_HEADER[1:]


@dataclass(slots=True)
class RBIWorkbookConverter:
    """Convert raw RBI workbook downloads into normalized CSV files."""

    date_format: str = "%d/%m/%Y"
    read_html_flavor: str = "bs4"
    use_pandas: bool = True
    cleanup_source: bool = True

    def to_csv(
        self,
        workbook_path: str | Path,
        *,
        start_date: date,
        end_date: date,
        output_dir: Path | None = None,
        remove_source: bool | None = None,
    ) -> Path:
        """Convert the RBI workbook into a CSV stored next to the download."""

        path = Path(workbook_path)
        if not path.exists():
            raise FileNotFoundError(path)

        rows = self._extract_rows(path)
        if not rows:
            raise ValueError("Workbook does not contain any forex data rows")

        directory = Path(output_dir) if output_dir else path.parent
        directory.mkdir(parents=True, exist_ok=True)
        csv_path = directory / self._build_filename(start_date, end_date)
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(CSV_HEADER)
            writer.writerows(rows)
        if remove_source is None:
            should_remove = self.cleanup_source
        else:
            should_remove = remove_source
        if should_remove:
            self._cleanup_source(path)
        return csv_path

    def _extract_rows(self, path: Path) -> list[list[str]]:
        if self.use_pandas and _pd is not None:
            try:
                frame = self._load_dataframe(path)
            except ImportError as exc:
                LOGGER.warning(
                    "pandas.read_html dependencies missing (%s); falling back to HTML parser",
                    exc,
                )
            else:
                return self._normalize_dataframe(frame)
        return self._normalize_rows(self._read_html_rows(path))

    def _load_dataframe(self, path: Path) -> "pd.DataFrame":
        if _pd is None:  # pragma: no cover - guarded by caller
            raise RuntimeError("pandas is not available")
        tables = _pd.read_html(path, flavor=self.read_html_flavor)
        if not tables:
            raise ValueError("RBI workbook is missing tabular data")
        return tables[0]

    def _normalize_dataframe(self, frame: "pd.DataFrame") -> list[list[str]]:
        working = frame.copy()
        if working.shape[1] < len(CSV_HEADER):
            raise ValueError("Workbook does not contain the expected number of columns")
        working = working.iloc[:, : len(CSV_HEADER)]
        working.columns = CSV_HEADER
        working = working[working["Date"].astype(str).str.strip().str.lower() != "date"]

        working["Date"] = _pd.to_datetime(  # type: ignore[operator]
            working["Date"], errors="coerce", dayfirst=True
        )
        for column in RATE_COLUMNS:
            working[column] = _pd.to_numeric(  # type: ignore[operator]
                working[column], errors="coerce"
            )

        working = working.dropna(subset=["Date"])
        working = working.dropna(how="all", subset=list(RATE_COLUMNS))
        working = working.sort_values("Date")

        rows: list[list[str]] = []
        for _, row in working.iterrows():
            formatted_row = [row["Date"].strftime(self.date_format)]
            formatted_row.extend(
                "" if _pd.isna(row[column]) else f"{row[column]}" for column in RATE_COLUMNS
            )
            rows.append(formatted_row)
        return rows

    def _read_html_rows(self, path: Path) -> list[list[str]]:
        parser = _HTMLTableParser()
        parser.feed(path.read_text(encoding="utf-8", errors="ignore"))
        return parser.rows

    def _normalize_rows(self, rows: Iterable[Sequence[object]]) -> list[list[str]]:
        cleaned: list[list[str]] = []
        for raw_row in rows:
            row = list(raw_row)
            if not row:
                continue
            first_cell = str(row[0]).strip()
            if not first_cell or first_cell.lower() == "date":
                continue
            try:
                parsed_date = self._parse_date(first_cell)
            except ValueError:
                continue
            normalized = [parsed_date.strftime(self.date_format)]
            for idx in range(1, len(CSV_HEADER)):
                value = row[idx] if idx < len(row) else ""
                normalized.append(self._stringify_rate(value))
            if any(cell for cell in normalized[1:]):
                cleaned.append(normalized)
        return cleaned

    @staticmethod
    def _parse_date(value: str) -> datetime:
        for fmt in ("%d/%m/%Y", "%d-%b-%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        raise ValueError(value)

    @staticmethod
    def _stringify_rate(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, Real):
            return str(float(value))
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                return ""
            try:
                return str(float(trimmed))
            except ValueError:
                return ""
        return ""

    @staticmethod
    def _build_filename(start_date: date, end_date: date) -> str:
        safe_from = start_date.strftime("%d-%m-%Y")
        safe_to = end_date.strftime("%d-%m-%Y")
        return f"RBI_Reference_Rates_{safe_from}_to_{safe_to}.csv"

    @staticmethod
    def _cleanup_source(path: Path) -> None:
        """Delete the original workbook once its CSV counterpart exists."""

        try:
            path.unlink()
            LOGGER.debug("Removed intermediate workbook %s", path)
        except FileNotFoundError:  # pragma: no cover - best effort cleanup
            LOGGER.debug("Workbook %s already removed before cleanup", path)


class _HTMLTableParser(HTMLParser):
    """Very small HTML table parser geared towards the RBI workbook layout."""

    def __init__(self) -> None:
        super().__init__()
        self.in_cell = False
        self.current: list[str] = []
        self.rows: list[list[str]] = []
        self._buffer: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # pragma: no cover - HTMLParser internals
        if tag.lower() == "tr":
            self.current = []
        elif tag.lower() in {"td", "th"}:
            self.in_cell = True
            self._buffer = []

    def handle_data(self, data: str) -> None:  # pragma: no cover - HTMLParser internals
        if self.in_cell:
            self._buffer.append(data)

    def handle_endtag(self, tag: str) -> None:  # pragma: no cover - HTMLParser internals
        lower = tag.lower()
        if lower in {"td", "th"} and self.in_cell:
            text = "".join(self._buffer).strip()
            self.current.append(text)
            self.in_cell = False
        elif lower == "tr" and self.current:
            self.rows.append(self.current)
            self.current = []


__all__ = ["RBIWorkbookConverter", "CSV_HEADER", "RATE_COLUMNS"]
