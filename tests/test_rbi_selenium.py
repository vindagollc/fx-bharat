from __future__ import annotations

import pytest
from selenium.common.exceptions import NoSuchElementException, TimeoutException

from fx_bharat.ingestion import rbi_selenium
from fx_bharat.ingestion.rbi_selenium import RBINoReferenceRateError, RBISeleniumClient


class _DummyElement:
    def __init__(self, displayed: bool = True) -> None:
        self._displayed = displayed

    def is_displayed(self) -> bool:
        return self._displayed


class _DummyDriver:
    def __init__(self, *, has_banner: bool) -> None:
        self.has_banner = has_banner

    def find_element(self, *_, **__):
        if self.has_banner:
            return _DummyElement(displayed=True)
        raise NoSuchElementException("No banner present")


class _ImmediateWebDriverWait:
    def __init__(self, driver, _timeout):
        self.driver = driver

    def until(self, method):
        result = method(self.driver)
        if result:
            return result
        raise TimeoutException("condition not met")


def _patch_wait(monkeypatch, driver):
    monkeypatch.setattr(
        rbi_selenium,
        "WebDriverWait",
        lambda _driver, _timeout: _ImmediateWebDriverWait(driver, _timeout),
    )


def test_raise_if_no_reference_rate_detects_banner(monkeypatch):
    driver = _DummyDriver(has_banner=True)
    _patch_wait(monkeypatch, driver)
    client = RBISeleniumClient(driver=driver)

    with pytest.raises(RBINoReferenceRateError):
        client._raise_if_no_reference_rate()


def test_raise_if_no_reference_rate_allows_flow_when_absent(monkeypatch):
    driver = _DummyDriver(has_banner=False)
    _patch_wait(monkeypatch, driver)
    client = RBISeleniumClient(driver=driver)

    client._raise_if_no_reference_rate()  # should not raise
