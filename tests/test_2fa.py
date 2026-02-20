"""Unit tests for the 2FA code prompt flow."""
from __future__ import annotations

import threading
import time
from unittest.mock import patch
from urllib.request import urlopen

import pytest

from tvDatafeed.main import TvDatafeed


def _submit_after_delay(url: str, path: str, delay: float = 0.3):
    """Send a GET request to the 2FA server after a delay.

    The delay lets serve_forever() start before we send the request.
    """
    def _do():
        time.sleep(delay)
        urlopen(f"{url}{path}")
    t = threading.Thread(target=_do, daemon=True)
    t.start()
    return t


class Test2FA:
    """Tests for the __prompt_2fa local HTTP server."""

    @pytest.mark.timeout(10)
    def test_submit_code_returns_value(self):
        """Submitting a code via /code?v= returns that code."""
        def fake_open(url):
            _submit_after_delay(url, "/code?v=123456")

        with patch("webbrowser.open", side_effect=fake_open):
            code = TvDatafeed._TvDatafeed__prompt_2fa()

        assert code == "123456"

    @pytest.mark.timeout(10)
    def test_serves_html_form(self):
        """GET / returns the 2FA HTML form."""
        html_content = {}

        def fake_open(url):
            def _fetch_then_submit():
                time.sleep(0.3)
                resp = urlopen(url, timeout=5)
                html_content["html"] = resp.read().decode()
                urlopen(f"{url}/code?v=999999")
            threading.Thread(target=_fetch_then_submit, daemon=True).start()

        with patch("webbrowser.open", side_effect=fake_open):
            TvDatafeed._TvDatafeed__prompt_2fa()

        html = html_content["html"]
        assert "TradingView 2FA" in html
        assert "placeholder" in html
        assert "/code?v=" in html

    @pytest.mark.timeout(10)
    def test_empty_code_returns_none(self):
        """Submitting an empty code via /code?v= returns None."""
        def fake_open(url):
            _submit_after_delay(url, "/code?v=")

        with patch("webbrowser.open", side_effect=fake_open):
            code = TvDatafeed._TvDatafeed__prompt_2fa()

        assert code is None

    @pytest.mark.timeout(10)
    def test_totp_code_format(self):
        """A 6-digit TOTP code round-trips correctly."""
        def fake_open(url):
            _submit_after_delay(url, "/code?v=042517")

        with patch("webbrowser.open", side_effect=fake_open):
            code = TvDatafeed._TvDatafeed__prompt_2fa()

        assert code == "042517"
        # Verify it can be cast to int (as the signin flow does)
        assert int(code) == 42517
