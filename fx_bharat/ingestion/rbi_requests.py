"""Pure requests-based downloader for RBI reference rate Excel files (no Selenium!)."""

from __future__ import annotations

import random
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
        archive_urls: Optional[list[str]] = None,
        user_agents: Optional[list[str]] = None,
    ) -> None:
        self.download_dir = Path(download_dir) if download_dir else Path.cwd() / "rbi_downloads"
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self.date_format = date_format
        self.locators = RBIPageLocators()
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.archive_urls = archive_urls or [
            "https://www.rbi.org.in/Scripts/ReferenceRateArchive.aspx",
            "https://rbi.org.in/Scripts/ReferenceRateArchive.aspx",
        ]
        self.user_agents = user_agents or [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
        ]
        self._reset_session()

    def _reset_session(self) -> None:
        import requests

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": random.choice(self.user_agents),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": RBI_ARCHIVE_URL,
                "Origin": "https://www.rbi.org.in",
            }
        )

    def _get_page_with_dates(self, start_date: date, end_date: date):
        from bs4 import BeautifulSoup

        formatted_start = start_date.strftime(self.date_format)
        formatted_end = end_date.strftime(self.date_format)

        last_exc: Exception | None = None
        for archive_url in self.archive_urls:
            try:
                response = self.session.get(archive_url, timeout=self.timeout)
                self._raise_with_context(response, archive_url)
                soup = BeautifulSoup(response.text, "html.parser")

                hidden_fields = self._extract_hidden_fields(soup)

                payload = {
                    **hidden_fields,
                    self.locators.from_date_name: formatted_start,
                    self.locators.to_date_name: formatted_end,
                    self.locators.go_button_name: "Go",
                }

                post_response = self.session.post(
                    archive_url,
                    data=payload,
                    headers={"Referer": archive_url},
                    timeout=self.timeout,
                )
                self._raise_with_context(post_response, archive_url)
                return BeautifulSoup(post_response.text, "html.parser")
            except Exception as exc:  # pragma: no cover - network dependent
                last_exc = exc
                LOGGER.debug("Failed to fetch RBI page via %s: %s", archive_url, exc)
                continue

        raise RuntimeError("Unable to reach RBI archive after trying all endpoints") from last_exc

    def fetch_excel(self, start_date: date, end_date: date) -> Path:
        """Download RBI reference rates Excel for the given date range."""

        if start_date > end_date:
            raise ValueError("start_date must not exceed end_date")

        LOGGER.info("Downloading RBI rates from %s to %s", start_date, end_date)

        attempt = 0
        last_error: Exception | None = None
        while attempt < self.max_attempts:
            attempt += 1
            if attempt > 1:
                # Start with a fresh session to avoid sticky anti-bot cookies between attempts.
                self._reset_session()
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
                jitter = random.uniform(0.5, 1.5)
                time.sleep(self.backoff_seconds * attempt * jitter)

        assert last_error is None  # for type checkers
        result_soup = result_soup  # noqa: PLW0127 (redefinition for clarity)
        link_tag = result_soup.find("a", id=self.locators.download_link_id)
        if not link_tag or "href" not in link_tag.attrs:
            raise RuntimeError(f"No Excel download link found for {start_date} – {end_date}")

        excel_response = self._trigger_download(result_soup, link_tag["href"])
        self._raise_with_context(excel_response, excel_response.url)

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

    def _raise_with_context(self, response: "requests.Response", url: str) -> None:
        import requests

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:  # pragma: no cover - network dependent
            status = response.status_code
            hints: list[str] = []
            if status in {403, 418, 429}:
                hints.append(
                    "RBI is actively blocking automated downloads; wait a moment or try a smaller date window."
                )
                hints.append("Manual download from the RBI archive page may be required if blocking persists.")
            hint_text = f" {' '.join(hints)}" if hints else ""
            raise RuntimeError(f"RBI archive responded with HTTP {status} for {url}.{hint_text}") from exc

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
