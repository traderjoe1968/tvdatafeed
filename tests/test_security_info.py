"""Tests for TvDatafeed.get_security_info() and the associated TOML helpers.

Covers:
  - CBOT:ZC1! (corn continuous futures) — verifies futures-specific fields
  - NASDAQ:AAPL           — verifies stock-specific fields
  - TOML write / no-duplicate behaviour
  - Module-level _toml_read / _toml_append helpers
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

import pytest

from tvDatafeed.main import TvDatafeed, _toml_append, _toml_read, _toml_value

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════
#  Unit tests for TOML helpers (no network required)
# ══════════════════════════════════════════════════════════════════════

class TestTomlHelpers:
    """Pure-Python TOML helper tests — no TradingView connection needed."""

    def test_toml_value_types(self):
        assert _toml_value(None)  == '""'
        assert _toml_value(True)  == "true"
        assert _toml_value(False) == "false"
        assert _toml_value(42)    == "42"
        assert _toml_value(3.14)  == repr(3.14)
        assert _toml_value("hello") == '"hello"'
        assert _toml_value('say "hi"') == '"say \\"hi\\""'
        assert _toml_value(["a", "b"]) == '["a", "b"]'
        assert _toml_value(["futures"]) == '["futures"]'

    def test_toml_read_missing_file(self, tmp_path):
        result = _toml_read(tmp_path / "nonexistent.toml")
        assert result == {}

    def test_toml_round_trip(self, tmp_path):
        path = tmp_path / "test.toml"
        data = {
            "description": 'Corn Futures',
            "exchange":    "CBOT",
            "pricescale":  10,
            "is_tradable": True,
            "fractional":  False,
            "typespecs":   ["futures"],
        }
        _toml_append(path, "CBOT:ZC1!", data)

        result = _toml_read(path)
        assert "CBOT:ZC1!" in result
        sec = result["CBOT:ZC1!"]
        assert sec["description"] == "Corn Futures"
        assert sec["exchange"]    == "CBOT"
        assert sec["pricescale"]  == 10
        assert sec["is_tradable"] is True
        assert sec["fractional"]  is False

    def test_toml_no_duplicate(self, tmp_path):
        path = tmp_path / "test.toml"
        _toml_append(path, "CBOT:ZC1!", {"exchange": "CBOT"})
        _toml_append(path, "CBOT:ZC1!", {"exchange": "CHANGED"})  # must be ignored

        result = _toml_read(path)
        assert result["CBOT:ZC1!"]["exchange"] == "CBOT"  # original value unchanged

    def test_toml_multiple_sections(self, tmp_path):
        path = tmp_path / "test.toml"
        _toml_append(path, "CBOT:ZC1!",  {"type": "futures"})
        _toml_append(path, "NASDAQ:AAPL", {"type": "stock"})

        result = _toml_read(path)
        assert result["CBOT:ZC1!"]["type"]  == "futures"
        assert result["NASDAQ:AAPL"]["type"] == "stock"


# ══════════════════════════════════════════════════════════════════════
#  Integration tests — require cached TradingView token
# ══════════════════════════════════════════════════════════════════════

class TestSecurityInfo:
    """Integration tests for get_security_info() via real WebSocket."""

    @staticmethod
    def _fetch(tv, symbol, exchange, fut_contract=None, toml_path=None, max_attempts=3):
        """Retry wrapper: TV occasionally drops connections between rapid calls."""
        for attempt in range(1, max_attempts + 1):
            info = tv.get_security_info(
                symbol=symbol, exchange=exchange,
                fut_contract=fut_contract, toml_path=toml_path,
            )
            if info:
                return info
            if attempt < max_attempts:
                logger.warning("get_security_info returned empty (attempt %d/%d) — retrying",
                               attempt, max_attempts)
                time.sleep(5 * attempt)
        return {}

    @pytest.mark.timeout(60)
    def test_zc1_security_info(self, tv_cached_token):
        """CBOT:ZC1! should return futures metadata with key fields populated."""
        info = self._fetch(tv_cached_token, "ZC", "CBOT", fut_contract=1)
        assert info, "get_security_info returned empty dict for CBOT:ZC1!"
        assert info.get("symbol") == "CBOT:ZC1!"
        logger.info("ZC1! info keys: %s", sorted(info.keys()))

        # Fields present on virtually all symbols
        assert "description" in info, f"missing 'description': {info}"
        assert "exchange"    in info, f"missing 'exchange': {info}"
        assert "type"        in info, f"missing 'type': {info}"
        assert "tick_size"   in info, f"missing 'tick_size': {info}"
        assert "point_value" in info, f"missing 'point_value': {info}"

        # Should be identified as futures
        assert info["type"] in ("futures", "continuous_contract"), (
            f"expected futures type, got: {info['type']}"
        )
        assert info["tick_size"] > 0
        assert info["point_value"] > 0
        logger.info("ZC1! security info: description=%r exchange=%r type=%r tick_size=%s point_value=%s",
                    info.get("description"), info.get("exchange"),
                    info.get("type"), info.get("tick_size"), info.get("point_value"))

    @pytest.mark.timeout(60)
    def test_aapl_security_info(self, tv_cached_token):
        """NASDAQ:AAPL should return stock metadata with key fields populated."""
        info = self._fetch(tv_cached_token, "AAPL", "NASDAQ")
        assert info, "get_security_info returned empty dict for NASDAQ:AAPL"
        assert info.get("symbol") == "NASDAQ:AAPL"
        logger.info("AAPL info keys: %s", sorted(info.keys()))

        assert "description" in info, f"missing 'description': {info}"
        assert "exchange"    in info, f"missing 'exchange': {info}"
        assert "type"        in info, f"missing 'type': {info}"
        assert "tick_size"   in info, f"missing 'tick_size': {info}"
        assert "point_value" in info, f"missing 'point_value': {info}"

        assert info["type"] == "stock", f"expected 'stock', got: {info['type']}"
        assert "apple" in info["description"].lower(), (
            f"unexpected description for AAPL: {info['description']!r}"
        )
        logger.info("AAPL security info: description=%r exchange=%r tick_size=%s point_value=%s",
                    info.get("description"), info.get("exchange"),
                    info.get("tick_size"), info.get("point_value"))

    @pytest.mark.timeout(120)
    def test_toml_write_zc1_and_aapl(self, tv_cached_token, tmp_path):
        """Write both symbols to the same TOML file; verify content and no duplication."""
        toml_file = tmp_path / "security_info.toml"

        info_zc = self._fetch(tv_cached_token, "ZC", "CBOT", fut_contract=1, toml_path=toml_file)
        assert info_zc, "no info returned for CBOT:ZC1!"
        assert toml_file.exists(), "TOML file was not created"

        info_aapl = self._fetch(tv_cached_token, "AAPL", "NASDAQ", toml_path=toml_file)
        assert info_aapl, "no info returned for NASDAQ:AAPL"

        # Both sections should be present (keyed as filename stems: SYMBOL_EXCHANGE)
        stored = _toml_read(toml_file)
        assert "ZC1_CBOT"    in stored, f"missing ZC1_CBOT in TOML: {stored.keys()}"
        assert "AAPL_NASDAQ" in stored, f"missing AAPL_NASDAQ in TOML: {stored.keys()}"

        logger.info("TOML file contents:\n%s", toml_file.read_text())

    @pytest.mark.timeout(60)
    def test_toml_no_duplicate_on_second_call(self, tv_cached_token, tmp_path):
        """Second call for same symbol returns cached data without duplicating the TOML section."""
        toml_file = tmp_path / "security_info.toml"

        self._fetch(tv_cached_token, "AAPL", "NASDAQ", toml_path=toml_file)
        # Second call: TOML hit → no network, no write
        self._fetch(tv_cached_token, "AAPL", "NASDAQ", toml_path=toml_file)

        content = toml_file.read_text(encoding="utf-8")
        # Count section header lines (key format: SYMBOL_EXCHANGE, no quoting needed)
        header_count = sum(
            1 for line in content.splitlines() if line.strip() == "[AAPL_NASDAQ]"
        )
        assert header_count == 1, (
            f"section header appeared {header_count} times — duplicate written:\n{content}"
        )

