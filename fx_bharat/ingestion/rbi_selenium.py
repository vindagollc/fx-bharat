"""Automation helpers for downloading RBI forex reference rate Excel files."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional, Sequence

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait

try:  # pragma: no cover - optional dependency
    from tenacity import RetryError, retry, stop_after_attempt, wait_exponential
except ModuleNotFoundError:  # pragma: no cover - lightweight fallback
    class RetryError(Exception):
        pass

    def retry(*args, **kwargs):  # type: ignore[no-redef]
        def decorator(func):
            return func

        return decorator

    def stop_after_attempt(*_: int, **__):  # type: ignore[no-redef]
        return None

    def wait_exponential(*_: int, **__):  # type: ignore[no-redef]
        return None

from fx_bharat.utils.logger import get_logger

LOGGER = get_logger(__name__)
RBI_ARCHIVE_URL = "https://www.rbi.org.in/Scripts/ReferenceRateArchive.aspx"


@dataclass
class RBIPageLocators:
    """Selectors used to interact with the RBI reference rate form."""

    from_date_locators: tuple[tuple[str, str], ...] = (
        (By.ID, "txtFromDate"),
        (By.NAME, "txtFromDate"),
        (By.ID, "ctl00_ContentPlaceHolder1_FromDateTextBox"),
        (By.NAME, "ctl00$ContentPlaceHolder1$FromDateTextBox"),
    )
    to_date_locators: tuple[tuple[str, str], ...] = (
        (By.ID, "txtToDate"),
        (By.NAME, "txtToDate"),
        (By.ID, "ctl00_ContentPlaceHolder1_ToDateTextBox"),
        (By.NAME, "ctl00$ContentPlaceHolder1$ToDateTextBox"),
    )
    go_button_locators: tuple[tuple[str, str], ...] = (
        (By.ID, "btnSubmit"),
        (By.NAME, "btnSubmit"),
        (By.ID, "ctl00_ContentPlaceHolder1_btnGo"),
        (By.NAME, "ctl00$ContentPlaceHolder1$btnGo"),
    )
    download_link_locators: tuple[tuple[str, str], ...] = (
        (By.ID, "lnkDownloadExcel"),
        (By.ID, "ctl00_ContentPlaceHolder1_lnkDownload"),
        (By.NAME, "ctl00$ContentPlaceHolder1$lnkDownload"),
    )
    form_iframe_css: str | None = None


class RBISeleniumClient:
    """Selenium-based client responsible for downloading RBI Excel files."""

    def __init__(
        self,
        *,
        download_dir: Optional[Path] = None,
        headless: bool = True,
        timeout: int = 60,
        locators: RBIPageLocators | None = None,
        date_format: str = "%d/%m/%Y",
        driver: webdriver.Chrome | None = None,
    ) -> None:
        if download_dir is None:
            self.download_dir = Path.cwd() / "rbi_downloads"
        else:
            self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._owns_download_dir = False
        # RBI downloads can occasionally take longer than the initial page
        # interactions, so provide a generous default timeout while still
        # allowing callers to override it.
        self.timeout = timeout
        self.locators = locators or RBIPageLocators()
        self.date_format = date_format
        self._owns_driver = driver is None
        if driver is None:
            options = Options()
            if headless:
                options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)
            prefs = {
                "download.default_directory": str(self.download_dir.resolve()),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "plugins.always_open_pdf_externally": True,
            }
            options.add_experimental_option("prefs", prefs)
            self.driver = webdriver.Chrome(options=options)
            self.driver.maximize_window()
        else:
            self.driver = driver

    def close(self) -> None:
        """Close the browser and cleanup."""

        if getattr(self, "driver", None) is not None and self._owns_driver:
            self.driver.quit()
        if getattr(self, "download_dir", None) and self._owns_download_dir:
            shutil.rmtree(self.download_dir, ignore_errors=True)

    def __enter__(self) -> "RBISeleniumClient":  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
        self.close()

    def fetch_excel(self, start_date: date, end_date: date) -> Path:
        """Download the Excel file for the provided date range."""

        if start_date > end_date:
            raise ValueError("start date must not exceed end date")

        try:
            downloaded_file = self._download_with_retries(start_date, end_date)
        except RetryError as exc:  # pragma: no cover - selenium heavy
            LOGGER.error("Exhausted retries while downloading RBI data: %s", exc)
            raise exc.last_attempt.exception()
        safe_start = start_date.isoformat()
        safe_end = end_date.isoformat()
        final_name = (
            f"rbi_reference_rates_{safe_start}_to_{safe_end}{downloaded_file.suffix.lower()}"
        )
        final_path = downloaded_file.with_name(final_name)
        if downloaded_file != final_path:
            downloaded_file.rename(final_path)
        return final_path

    # ``IngestionStrategy`` compatibility shim
    def fetch(self, start_date: date, end_date: date, *, destination: Path | None = None) -> Path:
        path = self.fetch_excel(start_date, end_date)
        if destination is not None:
            destination_path = Path(destination)
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            return path.rename(destination_path)
        return path

    @retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(3))
    def _download_with_retries(self, start_date: date, end_date: date) -> Path:
        self.driver.get(RBI_ARCHIVE_URL)
        wait = WebDriverWait(self.driver, self.timeout)
        self._wait_for_page_ready(wait)
        self._fill_date_field(wait, start_date, self.locators.from_date_locators)
        self._fill_date_field(wait, end_date, self.locators.to_date_locators)
        go_button = self._wait_for_clickable(wait, self.locators.go_button_locators)
        self._click_via_js(go_button)
        download_link = self._wait_for_clickable(wait, self.locators.download_link_locators)
        self._click_via_js(download_link)
        return self._wait_for_download()

    def _fill_date_field(
        self,
        wait: WebDriverWait,
        value: date,
        locators: Sequence[tuple[str, str]],
    ) -> None:
        element = self._wait_for_any_locator(wait, locators)
        formatted = value.strftime(self.date_format)
        js_fill_date = """
        const el = arguments[0];
        el.removeAttribute('readonly');
        el.value = arguments[1];
        el.dispatchEvent(new Event('change', { bubbles: true }));
        el.dispatchEvent(new Event('focus'));
        el.dispatchEvent(new Event('blur'));
        if (typeof window.jQuery !== 'undefined' && jQuery.ui && jQuery.ui.datepicker) {
            jQuery(el).datepicker('setDate', arguments[1]);
        }
        """
        self.driver.execute_script(js_fill_date, element, formatted)

    def _wait_for_download(self) -> Path:
        """Wait for Chrome to complete downloading the Excel file."""

        partial_patterns = ("*.crdownload", "*.tmp")
        valid_extensions = {".xls", ".xlsx", ".xlsm"}

        def _download_ready(_: webdriver.Chrome) -> Path | bool:
            files = [
                item
                for item in self.download_dir.iterdir()
                if item.is_file() and item.suffix.lower() in valid_extensions
            ]
            partial = [
                tmp for pattern in partial_patterns for tmp in self.download_dir.glob(pattern)
            ]
            if files and not partial:
                return max(files, key=lambda p: p.stat().st_mtime)
            return False

        wait = WebDriverWait(self.driver, self.timeout, poll_frequency=1.0)
        result = wait.until(_download_ready)
        if isinstance(result, Path):
            return result
        raise RuntimeError("Download wait completed without returning a file path")

    def _wait_for_page_ready(self, wait: WebDriverWait) -> None:
        wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
        self._maybe_switch_to_form_frame()
        self._wait_for_any_locator(wait, self.locators.from_date_locators)

    def _maybe_switch_to_form_frame(self) -> None:
        if self.locators.form_iframe_css:
            frame = self.driver.find_element(By.CSS_SELECTOR, self.locators.form_iframe_css)
            self.driver.switch_to.frame(frame)
            return
        if self._element_exists(self.locators.from_date_locators):
            return
        frames = self.driver.find_elements(By.TAG_NAME, "iframe")
        for index, frame in enumerate(frames):
            self.driver.switch_to.frame(frame)
            if self._element_exists(self.locators.from_date_locators):
                LOGGER.debug(
                    "Switched to iframe %s for RBI reference rate form",
                    frame.get_attribute("id") or frame.get_attribute("name") or str(index),
                )
                return
            self.driver.switch_to.default_content()
        self.driver.switch_to.default_content()

    def _element_exists(self, locators: Sequence[tuple[str, str]]) -> bool:
        for locator in locators:
            try:
                self.driver.find_element(*locator)
                return True
            except NoSuchElementException:
                continue
        return False

    def _wait_for_any_locator(
        self, wait: WebDriverWait, locators: Sequence[tuple[str, str]]
    ) -> WebElement:
        def _locate(driver: webdriver.Chrome) -> WebElement | bool:
            for locator in locators:
                try:
                    return driver.find_element(*locator)
                except NoSuchElementException:
                    continue
            return False

        result = wait.until(_locate)
        if isinstance(result, WebElement):
            return result
        raise NoSuchElementException("Unable to locate any matching element")

    def _wait_for_clickable(
        self, wait: WebDriverWait, locators: Sequence[tuple[str, str]]
    ) -> WebElement:
        def _locate(driver: webdriver.Chrome) -> WebElement | bool:
            for locator in locators:
                try:
                    element = driver.find_element(*locator)
                except NoSuchElementException:
                    continue
                if element.is_displayed() and element.is_enabled():
                    return element
            return False

        result = wait.until(_locate)
        if isinstance(result, WebElement):
            return result
        raise NoSuchElementException("Unable to locate a clickable element")

    def _click_via_js(self, element: WebElement) -> None:
        """Scroll the element into view and trigger a JavaScript click."""

        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        self.driver.execute_script("arguments[0].click();", element)
