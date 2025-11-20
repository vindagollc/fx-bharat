"""Pure requests-based downloader for RBI reference rate Excel files (no Selenium!)."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional, TYPE_CHECKING

from fx_bharat.utils.logger import get_logger

if TYPE_CHECKING:  # pragma: no cover - imported only for type checking
    import requests
    from bs4 import BeautifulSoup


LOGGER = get_logger(__name__)
RBI_ARCHIVE_URL = "https://www.rbi.org.in/Scripts/ReferenceRateArchive.aspx"


@dataclass
class RBIPageLocators:
    """Form field names used by the RBI reference rate archive."""

    from_date_name: str = "ctl00$ContentPlaceHolder1$FromDateTextBox"
    to_date_name: str = "ctl00$ContentPlaceHolder1$ToDateTextBox"
    go_button_name: str = "ctl00$ContentPlaceHolder1$btnGo"
    download_link_id: str = "ctl00_ContentPlaceHolder1_lnkDownload"


class RBIRequestsClient:
    """Fast, reliable RBI Excel downloader using only requests + BeautifulSoup."""

    def __init__(
        self,
        *,
        download_dir: Optional[Path] = None,
        timeout: int = 30,
        date_format: str = "%d/%m/%Y",
        max_attempts: int = 5,
        backoff_seconds: float = 2.0,
    ) -> None:
        self.download_dir = Path(download_dir) if download_dir else Path.cwd() / "rbi_downloads"
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self.date_format = date_format
        self.locators = RBIPageLocators()
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        import requests

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": RBI_ARCHIVE_URL,
            }
        )

    def _get_page_with_dates(self, start_date: date, end_date: date):
        from bs4 import BeautifulSoup

        formatted_start = start_date.strftime(self.date_format)
        formatted_end = end_date.strftime(self.date_format)

        response = self.session.get(RBI_ARCHIVE_URL, timeout=self.timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        hidden_fields = self._extract_hidden_fields(soup)

        payload = {
            **hidden_fields,
            self.locators.from_date_name: formatted_start,
            self.locators.to_date_name: formatted_end,
            self.locators.go_button_name: "Go",
        }

        post_response = self.session.post(
            RBI_ARCHIVE_URL,
            data=payload,
            headers={"Referer": RBI_ARCHIVE_URL},
            timeout=self.timeout,
        )
        post_response.raise_for_status()
        return BeautifulSoup(post_response.text, "html.parser")

    def fetch_excel(self, start_date: date, end_date: date) -> Path:
        """Download RBI reference rates Excel for the given date range."""

        if start_date > end_date:
            raise ValueError("start_date must not exceed end_date")

        LOGGER.info("Downloading RBI rates from %s to %s", start_date, end_date)

        attempt = 0
        last_error: Exception | None = None
        while attempt < self.max_attempts:
            attempt += 1
            try:
                result_soup = self._get_page_with_dates(start_date, end_date)
                break
            except Exception as exc:  # pragma: no cover - network dependent
                last_error = exc
                LOGGER.warning(
                    "Attempt %s/%s to download RBI rates failed: %s",
                    attempt,
                    self.max_attempts,
                    exc,
                )
                if attempt >= self.max_attempts:
                    raise
                time.sleep(self.backoff_seconds * attempt)

        assert last_error is None  # for type checkers
        result_soup = result_soup  # noqa: PLW0127 (redefinition for clarity)
        link_tag = result_soup.find("a", id=self.locators.download_link_id)
        if not link_tag or "href" not in link_tag.attrs:
            raise RuntimeError(f"No Excel download link found for {start_date} – {end_date}")

        excel_response = self._trigger_download(result_soup, link_tag["href"])
        excel_response.raise_for_status()

        filename = self._infer_filename(excel_response.headers.get("Content-Disposition", ""))
        final_path = self.download_dir / filename
        with open(final_path, "wb") as f:
            for chunk in excel_response.iter_content(chunk_size=8192):
                f.write(chunk)

        LOGGER.info("Saved RBI Excel → %s", final_path)
        return final_path.resolve()

    def _trigger_download(self, soup: "BeautifulSoup", href: str):
        import requests

        if href.startswith("javascript:"):
            match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
            if not match:
                raise RuntimeError("Failed to parse __doPostBack from download link")
            target, arg = match.groups()
            payload = {
                "__EVENTTARGET": target,
                "__EVENTARGUMENT": arg,
                **self._extract_hidden_fields(soup),
            }
            return self.session.post(RBI_ARCHIVE_URL, data=payload, stream=True, timeout=self.timeout)

        excel_url = requests.compat.urljoin(RBI_ARCHIVE_URL, href)
        return self.session.get(excel_url, stream=True, timeout=self.timeout)

    @staticmethod
    def _infer_filename(content_disposition: str) -> str:
        match = re.findall(r"filename=?\"?([^\"]+)\"?", content_disposition)
        return match[0] if match else "rbi_reference_rates.xls"

    @staticmethod
    def _extract_hidden_fields(soup: "BeautifulSoup") -> dict[str, str]:
        required = ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]
        fields: dict[str, str] = {}
        for name in required:
            element = soup.find("input", {"name": name})
            if not element or "value" not in element.attrs:
                raise RuntimeError(f"Missing required form field: {name}")
            fields[name] = element["value"]
        return fields

    def __enter__(self) -> "RBIRequestsClient":  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # pragma: no cover - trivial
        return None


# Backward compatible alias for older imports.
RBISeleniumClient = RBIRequestsClient
