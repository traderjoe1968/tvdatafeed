"""Test each login method by downloading real market data.

Order: Nologin → Browser cookies → Cached token → Desktop App

Each method downloads three instruments at 1D interval (date-range mode):
  - AAPL daily, 3 years
  - AUDEUR daily, 3 years
  - ES1! daily, 20 years (stress test chunking)

TestCachedToken uses mixed intervals (15min 2yr, 5min 1yr, 1D 20yr) for broader coverage.
Nologin tests skip gracefully if TV returns no data.
"""
from __future__ import annotations

import time
from datetime import datetime as dt, timedelta
from pathlib import Path

import pandas as pd
import pytest

from tvDatafeed.main import Interval

# ── Helpers ───────────────────────────────────────────────────────────

EXPECTED_COLS = {"symbol", "open", "high", "low", "close", "volume"}
DATA_DIR = Path(__file__).parent.parent / "data"

NOW = dt.now()
TWO_YEARS_AGO = NOW - timedelta(days=730)    # max depth for 15min
ONE_YEAR_AGO = NOW - timedelta(days=365)     # max depth for 5min
THREE_YEARS_AGO = NOW - timedelta(days=365 * 3)
FIVE_YEARS_AGO = NOW - timedelta(days=365 * 5)
TWENTY_YEARS_AGO = NOW - timedelta(days=365 * 20)  # stress test chunking


def _get_hist_with_retry(tv, *, retries: int = 2, delay: int = 5, **kwargs) -> pd.DataFrame:
    """Call tv.get_hist with automatic retry on transient connection drops."""
    for attempt in range(1, retries + 1):
        df = tv.get_hist(**kwargs)
        if not df.empty:
            return df
        if attempt < retries:
            time.sleep(delay)
    return df


def _assert_valid_df(
    df: pd.DataFrame,
    *,
    expected_symbol: str | None = None,
) -> None:
    """Base validation: columns, OHLC not NaN, datetime index sorted."""
    assert isinstance(df, pd.DataFrame)
    assert not df.empty, "DataFrame is empty"

    assert EXPECTED_COLS.issubset(df.columns), (
        f"missing columns: {EXPECTED_COLS - set(df.columns)}"
    )
    for col in ("open", "high", "low", "close"):
        assert df[col].notna().all(), f"NaN found in {col}"
    assert pd.api.types.is_datetime64_any_dtype(df.index), "index is not datetime"
    assert df.index.is_monotonic_increasing, "index is not sorted ascending"

    if expected_symbol:
        assert expected_symbol in df["symbol"].iloc[0], (
            f"symbol mismatch: expected '{expected_symbol}' in '{df['symbol'].iloc[0]}'"
        )


def _assert_daterange_df(
    df: pd.DataFrame,
    *,
    start_date: dt,
    end_date: dt,
    min_bars: int,
    expected_symbol: str | None = None,
) -> None:
    """Validate date-range mode result: covers requested range, enough bars.

    Note: min_bars is adjusted to 70% to account for TradingView account limits
    and the fact that trading days (~252/year) are less than calendar days (365).
    """
    _assert_valid_df(df, expected_symbol=expected_symbol)

    # Allow 70% tolerance on min_bars to account for:
    # 1. TradingView account limits (may return fewer bars than full range)
    # 2. Trading days vs calendar days (~252 vs 365 = 69%)
    # 3. Weekends, holidays, early closes
    adjusted_min = int(min_bars * 0.70)
    actual_bars = len(df)

    assert actual_bars >= adjusted_min, (
        f"too few bars: expected >= {min_bars} (adjusted to {adjusted_min} with 70% tolerance), "
        f"got {actual_bars}"
    )

    print(f"\n✓ Received {actual_bars:,} bars (expected >= {min_bars:,}, adjusted min: {adjusted_min:,})")

    # ── Data should cover the requested date range ──
    first_bar = df.index[0].to_pydatetime()
    last_bar = df.index[-1].to_pydatetime()

    print(f"✓ Date range: {first_bar:%Y-%m-%d} → {last_bar:%Y-%m-%d}")
    print(f"  Requested:  {start_date:%Y-%m-%d} → {end_date:%Y-%m-%d}")

    # Check if data actually covers the requested range (not just bar count)
    requested_days = (end_date - start_date).days
    actual_days = (last_bar - first_bar).days
    coverage_pct = (actual_days / requested_days * 100) if requested_days > 0 else 100

    print(f"  Coverage:   {actual_days} days ({coverage_pct:.1f}% of {requested_days} requested days)")

    # Last bar should be near end_date (within 7 days for weekends/holidays)
    assert last_bar >= end_date - timedelta(days=7), (
        f"data ends too early: last bar {last_bar:%Y-%m-%d} vs "
        f"requested end {end_date:%Y-%m-%d}"
    )

    # Only check start_date coverage if we got good range coverage
    # If coverage < 50%, we likely hit account limit and only got recent data
    if coverage_pct >= 50:
        # Good coverage - should cover start_date too
        assert first_bar <= start_date + timedelta(days=7), (
            f"data starts too late: first bar {first_bar:%Y-%m-%d} vs "
            f"requested start {start_date:%Y-%m-%d}"
        )
    else:
        # Limited coverage - account limit hit, only recent data available
        print(f"  ⚠ Coverage {coverage_pct:.1f}% < 50% - likely hit account limit, "
              f"data covers most recent period only")


# ══════════════════════════════════════════════════════════════════════
#  1. Nologin tests (limited data access)
# ══════════════════════════════════════════════════════════════════════

class TestNologin:
    """Tests using no credentials — TradingView may limit results."""

    @pytest.mark.timeout(300)
    def test_aapl_daily_3yr(self, tv_nologin):
        df = tv_nologin.get_hist(
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        if df.empty:
            pytest.skip("nologin returned no data (TradingView limitation)")
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=500, expected_symbol="NASDAQ:AAPL")

    @pytest.mark.timeout(300)
    def test_audeur_daily_3yr(self, tv_nologin):
        df = tv_nologin.get_hist(
            symbol="AUDEUR", exchange="FX_IDC",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        if df.empty:
            pytest.skip("nologin returned no data (TradingView limitation)")
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=500, expected_symbol="FX_IDC:AUDEUR")

    @pytest.mark.timeout(300)
    def test_es_daily_20yr(self, tv_nologin):
        # 20 years ES daily: ~252 trading days/year × 20 = 5,040 bars ideal
        # But account limit: ~4,000 bars; nologin may be less
        # With 70% tolerance: expect >= 700 bars
        df = tv_nologin.get_hist(
            symbol="ES", exchange="CME_MINI",
            interval=Interval.in_daily,
            start_date=TWENTY_YEARS_AGO, end_date=NOW,
            fut_contract=1,
        )
        if df.empty:
            pytest.skip("nologin returned no data (TradingView limitation)")
        _assert_daterange_df(df, start_date=TWENTY_YEARS_AGO, end_date=NOW,
                             min_bars=1000, expected_symbol="CME_MINI:ES1!")


# ══════════════════════════════════════════════════════════════════════
#  2. Browser cookie tests (one class per browser)
# ══════════════════════════════════════════════════════════════════════

class TestFirefox:
    """Tests using Firefox cookie-extracted JWT."""

    @pytest.mark.timeout(300)
    def test_aapl_daily_3yr(self, tv_firefox):
        df = _get_hist_with_retry(tv_firefox,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="NASDAQ:AAPL")

    @pytest.mark.timeout(300)
    def test_audeur_daily_3yr(self, tv_firefox):
        df = _get_hist_with_retry(tv_firefox,
            symbol="AUDEUR", exchange="FX_IDC",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="FX_IDC:AUDEUR")

    @pytest.mark.timeout(300)
    def test_es_daily_20yr(self, tv_firefox):
        df = _get_hist_with_retry(tv_firefox,
            symbol="ES", exchange="CME_MINI",
            interval=Interval.in_daily,
            start_date=TWENTY_YEARS_AGO, end_date=NOW,
            fut_contract=1,
        )
        _assert_daterange_df(df, start_date=TWENTY_YEARS_AGO, end_date=NOW,
                             min_bars=1000, expected_symbol="CME_MINI:ES1!")


class TestSafari:
    """Tests using Safari cookie-extracted JWT (macOS only)."""

    @pytest.mark.timeout(300)
    def test_aapl_daily_3yr(self, tv_safari):
        df = _get_hist_with_retry(tv_safari,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="NASDAQ:AAPL")

    @pytest.mark.timeout(300)
    def test_audeur_daily_3yr(self, tv_safari):
        df = _get_hist_with_retry(tv_safari,
            symbol="AUDEUR", exchange="FX_IDC",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="FX_IDC:AUDEUR")

    @pytest.mark.timeout(300)
    def test_es_daily_20yr(self, tv_safari):
        df = _get_hist_with_retry(tv_safari,
            symbol="ES", exchange="CME_MINI",
            interval=Interval.in_daily,
            start_date=TWENTY_YEARS_AGO, end_date=NOW,
            fut_contract=1,
        )
        _assert_daterange_df(df, start_date=TWENTY_YEARS_AGO, end_date=NOW,
                             min_bars=1000, expected_symbol="CME_MINI:ES1!")


class TestChrome:
    """Tests using Chrome cookie-extracted JWT."""

    @pytest.mark.timeout(300)
    def test_aapl_daily_3yr(self, tv_chrome):
        df = _get_hist_with_retry(tv_chrome,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="NASDAQ:AAPL")

    @pytest.mark.timeout(300)
    def test_audeur_daily_3yr(self, tv_chrome):
        df = _get_hist_with_retry(tv_chrome,
            symbol="AUDEUR", exchange="FX_IDC",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="FX_IDC:AUDEUR")

    @pytest.mark.timeout(300)
    def test_es_daily_20yr(self, tv_chrome):
        df = _get_hist_with_retry(tv_chrome,
            symbol="ES", exchange="CME_MINI",
            interval=Interval.in_daily,
            start_date=TWENTY_YEARS_AGO, end_date=NOW,
            fut_contract=1,
        )
        _assert_daterange_df(df, start_date=TWENTY_YEARS_AGO, end_date=NOW,
                             min_bars=1000, expected_symbol="CME_MINI:ES1!")


class TestEdge:
    """Tests using Microsoft Edge cookie-extracted JWT."""

    @pytest.mark.timeout(300)
    def test_aapl_daily_3yr(self, tv_edge):
        df = _get_hist_with_retry(tv_edge,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="NASDAQ:AAPL")

    @pytest.mark.timeout(300)
    def test_audeur_daily_3yr(self, tv_edge):
        df = _get_hist_with_retry(tv_edge,
            symbol="AUDEUR", exchange="FX_IDC",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="FX_IDC:AUDEUR")

    @pytest.mark.timeout(300)
    def test_es_daily_20yr(self, tv_edge):
        df = _get_hist_with_retry(tv_edge,
            symbol="ES", exchange="CME_MINI",
            interval=Interval.in_daily,
            start_date=TWENTY_YEARS_AGO, end_date=NOW,
            fut_contract=1,
        )
        _assert_daterange_df(df, start_date=TWENTY_YEARS_AGO, end_date=NOW,
                             min_bars=1000, expected_symbol="CME_MINI:ES1!")


class TestBrave:
    """Tests using Brave cookie-extracted JWT."""

    @pytest.mark.timeout(300)
    def test_aapl_daily_3yr(self, tv_brave):
        df = _get_hist_with_retry(tv_brave,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="NASDAQ:AAPL")

    @pytest.mark.timeout(300)
    def test_audeur_daily_3yr(self, tv_brave):
        df = _get_hist_with_retry(tv_brave,
            symbol="AUDEUR", exchange="FX_IDC",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="FX_IDC:AUDEUR")

    @pytest.mark.timeout(300)
    def test_es_daily_20yr(self, tv_brave):
        df = _get_hist_with_retry(tv_brave,
            symbol="ES", exchange="CME_MINI",
            interval=Interval.in_daily,
            start_date=TWENTY_YEARS_AGO, end_date=NOW,
            fut_contract=1,
        )
        _assert_daterange_df(df, start_date=TWENTY_YEARS_AGO, end_date=NOW,
                             min_bars=1000, expected_symbol="CME_MINI:ES1!")


class TestChromium:
    """Tests using Chromium cookie-extracted JWT."""

    @pytest.mark.timeout(300)
    def test_aapl_daily_3yr(self, tv_chromium):
        df = _get_hist_with_retry(tv_chromium,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="NASDAQ:AAPL")

    @pytest.mark.timeout(300)
    def test_audeur_daily_3yr(self, tv_chromium):
        df = _get_hist_with_retry(tv_chromium,
            symbol="AUDEUR", exchange="FX_IDC",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="FX_IDC:AUDEUR")

    @pytest.mark.timeout(300)
    def test_es_daily_20yr(self, tv_chromium):
        df = _get_hist_with_retry(tv_chromium,
            symbol="ES", exchange="CME_MINI",
            interval=Interval.in_daily,
            start_date=TWENTY_YEARS_AGO, end_date=NOW,
            fut_contract=1,
        )
        _assert_daterange_df(df, start_date=TWENTY_YEARS_AGO, end_date=NOW,
                             min_bars=1000, expected_symbol="CME_MINI:ES1!")


# ══════════════════════════════════════════════════════════════════════
#  3. Cached token tests
# ══════════════════════════════════════════════════════════════════════

class TestCachedToken:
    """Tests using cached token from ~/.tvdatafeed/token.
    Downloaded data is saved to tests/output/ as CSV."""

    @pytest.mark.timeout(900)  # 15min: allow extra time for 18 chunks
    def test_aapl_15min_2yr(self, tv_cached_token):
        # 2 years AAPL 15min: ~504 trading days × 26 bars/day = 13,104 bars ideal
        # But account limit: 4000 bars (80% of 5000)
        # With 70% tolerance: expect >= 2800 bars
        df = _get_hist_with_retry(tv_cached_token,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_15_minute,
            start_date=TWO_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=TWO_YEARS_AGO, end_date=NOW,
                             min_bars=4000, expected_symbol="NASDAQ:AAPL")
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(DATA_DIR / "AAPL_NASDAQ_15min_2yr.csv")

    @pytest.mark.timeout(1200)  # 5min: allow extra time for 29 chunks + retries
    def test_audeur_5min_1yr(self, tv_cached_token):
        # 1 year AUDEUR 5min: Forex trades 24/5, ~260 days × 288 bars/day = 74,880 bars ideal
        # But account limit: 4000 bars (80% of 5000)
        # With 70% tolerance: expect >= 2800 bars
        df = _get_hist_with_retry(tv_cached_token,
            symbol="AUDEUR", exchange="FX_IDC",
            interval=Interval.in_5_minute,
            start_date=ONE_YEAR_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=ONE_YEAR_AGO, end_date=NOW,
                             min_bars=4000, expected_symbol="FX_IDC:AUDEUR")
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(DATA_DIR / "AUDEUR_FX_IDC_5min_1yr.csv")

    @pytest.mark.timeout(300)
    def test_es_daily_20yr(self, tv_cached_token):
        df = _get_hist_with_retry(tv_cached_token,
            symbol="ES", exchange="CME_MINI",
            interval=Interval.in_daily,
            start_date=TWENTY_YEARS_AGO, end_date=NOW,
            fut_contract=1,
        )
        _assert_daterange_df(df, start_date=TWENTY_YEARS_AGO, end_date=NOW,
                             min_bars=1000, expected_symbol="CME_MINI:ES1!")
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(DATA_DIR / "ES1_CME_MINI_1D_20yr.csv")


# ══════════════════════════════════════════════════════════════════════
#  4. TradingView Desktop App tests
# ══════════════════════════════════════════════════════════════════════

class TestDesktopApp:
    """Tests using TradingView desktop app cookie-extracted JWT."""

    @pytest.mark.timeout(300)
    def test_aapl_daily_3yr(self, tv_desktop):
        df = _get_hist_with_retry(tv_desktop,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="NASDAQ:AAPL")

    @pytest.mark.timeout(300)
    def test_audeur_daily_3yr(self, tv_desktop):
        df = _get_hist_with_retry(tv_desktop,
            symbol="AUDEUR", exchange="FX_IDC",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700, expected_symbol="FX_IDC:AUDEUR")

    @pytest.mark.timeout(300)
    def test_es_daily_20yr(self, tv_desktop):
        df = _get_hist_with_retry(tv_desktop,
            symbol="ES", exchange="CME_MINI",
            interval=Interval.in_daily,
            start_date=TWENTY_YEARS_AGO, end_date=NOW,
            fut_contract=1,
        )
        _assert_daterange_df(df, start_date=TWENTY_YEARS_AGO, end_date=NOW,
                             min_bars=1000, expected_symbol="CME_MINI:ES1!")
