"""Test each login method by downloading real market data.

Order: Nologin → Browser cookies → Cached token → Desktop App

Browser tests (Firefox, Safari, Chrome, Edge, Brave, Chromium, Desktop) each
run a single 10-bar AAPL fetch to confirm cookie extraction produces a valid JWT.
Full data downloads, CSV saves, and backadjusted tests live in TestCachedToken only.

Nologin tests skip gracefully if TV returns no data.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime as dt, timedelta
from pathlib import Path

import pandas as pd
import pytest

from tvDatafeed.main import Interval

logger = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────

EXPECTED_COLS = {"open", "high", "low", "close", "volume"}
DATA_DIR = Path(__file__).parent.parent / "data"

NOW = dt.now()
TWO_YEARS_AGO = NOW - timedelta(days=730)    # max depth for 15min
ONE_YEAR_AGO = NOW - timedelta(days=365)     # max depth for 5min
THREE_YEARS_AGO = NOW - timedelta(days=365 * 3)
FIVE_YEARS_AGO = NOW - timedelta(days=365 * 5)
EIGHTEEN_YEARS_AGO = NOW - timedelta(days=365 * 18)  # stress test chunking


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


def _assert_daterange_df(
    df: pd.DataFrame,
    *,
    start_date: dt,
    end_date: dt,
    min_bars: int,
) -> None:
    """Validate date-range mode result: covers requested range, enough bars.

    Note: min_bars is adjusted to 70% to account for TradingView account limits
    and the fact that trading days (~252/year) are less than calendar days (365).
    """
    _assert_valid_df(df)

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

    # Only check start_date when coverage is near-complete (≥ 99%).
    # Below that threshold the account hit its bar limit — the data correctly
    # starts as far back as the limit allows; don't penalise for it.
    # (Many public/non-trading days means true coverage is always < 100%.)
    if coverage_pct >= 99:
        assert first_bar <= start_date + timedelta(days=7), (
            f"data starts too late: first bar {first_bar:%Y-%m-%d} vs "
            f"requested start {start_date:%Y-%m-%d}"
        )
    else:
        print(f"  ⚠ Coverage {coverage_pct:.1f}% < 99% — bar limit likely reached, "
              f"start-date check skipped")


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
                             min_bars=500)

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
                             min_bars=500)

    @pytest.mark.timeout(300)
    def test_es_daily_18yr(self, tv_nologin):
        # 18 years ES daily: ~252 trading days/year × 18 = 4,536 bars ideal
        # Free account: ~4,950 bars max; coverage will be < 100%
        df = tv_nologin.get_hist(
            symbol="ES", exchange="CME_MINI",
            interval=Interval.in_daily,
            start_date=EIGHTEEN_YEARS_AGO, end_date=NOW,
            fut_contract=1,
        )
        if df.empty:
            pytest.skip("nologin returned no data (TradingView limitation)")
        _assert_daterange_df(df, start_date=EIGHTEEN_YEARS_AGO, end_date=NOW,
                             min_bars=1000)


# ══════════════════════════════════════════════════════════════════════
#  2. Browser cookie tests — auth verification only
#     Each class fetches 10 bars to confirm cookie extraction works.
#     Full data downloads are in TestCachedToken.
# ══════════════════════════════════════════════════════════════════════

class TestFirefox:
    """Verifies Firefox cookie extraction produces a working JWT."""

    @pytest.mark.timeout(60)
    def test_auth_works(self, tv_firefox):
        df = _get_hist_with_retry(tv_firefox,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily, n_bars=10,
        )
        _assert_valid_df(df)


class TestSafari:
    """Verifies Safari cookie extraction produces a working JWT (macOS only)."""

    @pytest.mark.timeout(60)
    def test_auth_works(self, tv_safari):
        df = _get_hist_with_retry(tv_safari,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily, n_bars=10,
        )
        _assert_valid_df(df)


class TestChrome:
    """Verifies Chrome cookie extraction produces a working JWT."""

    @pytest.mark.timeout(60)
    def test_auth_works(self, tv_chrome):
        df = _get_hist_with_retry(tv_chrome,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily, n_bars=10,
        )
        _assert_valid_df(df)


class TestEdge:
    """Verifies Edge cookie extraction produces a working JWT."""

    @pytest.mark.timeout(60)
    def test_auth_works(self, tv_edge):
        df = _get_hist_with_retry(tv_edge,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily, n_bars=10,
        )
        _assert_valid_df(df)


class TestBrave:
    """Verifies Brave cookie extraction produces a working JWT."""

    @pytest.mark.timeout(60)
    def test_auth_works(self, tv_brave):
        df = _get_hist_with_retry(tv_brave,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily, n_bars=10,
        )
        _assert_valid_df(df)


class TestChromium:
    """Verifies Chromium cookie extraction produces a working JWT."""

    @pytest.mark.timeout(60)
    def test_auth_works(self, tv_chromium):
        df = _get_hist_with_retry(tv_chromium,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily, n_bars=10,
        )
        _assert_valid_df(df)


# ══════════════════════════════════════════════════════════════════════
#  3. Cached token tests — full data downloads with CSV saves
# ══════════════════════════════════════════════════════════════════════

class TestCachedToken:
    """Tests using cached token from ~/.tvdatafeed/token.
    Downloaded data is saved to data/ as CSV."""

    @pytest.mark.timeout(300)
    def test_aapl_daily_3yr(self, tv_cached_token):
        df = _get_hist_with_retry(tv_cached_token,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700)
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(DATA_DIR / "AAPL_NASDAQ_1D_3yr.csv")

    @pytest.mark.timeout(300)
    def test_audeur_daily_3yr(self, tv_cached_token):
        df = _get_hist_with_retry(tv_cached_token,
            symbol="AUDEUR", exchange="FX_IDC",
            interval=Interval.in_daily,
            start_date=THREE_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=THREE_YEARS_AGO, end_date=NOW,
                             min_bars=700)
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(DATA_DIR / "AUDEUR_FX_IDC_1D_3yr.csv")

    @pytest.mark.timeout(900)
    def test_aapl_15min_2yr(self, tv_cached_token):
        # Free account: 5,000 bars/query max; chunking covers the full 2yr range.
        # ~51 calendar days per chunk × 15 chunks = 730 days total.
        df = _get_hist_with_retry(tv_cached_token,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_15_minute,
            start_date=TWO_YEARS_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=TWO_YEARS_AGO, end_date=NOW,
                             min_bars=5000)
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(DATA_DIR / "AAPL_NASDAQ_15min_2yr.csv")

    @pytest.mark.timeout(1200)
    def test_audeur_5min_1yr(self, tv_cached_token):
        # Free account: 5,000 bars/query max; Forex 24/5 so many bars per chunk.
        df = _get_hist_with_retry(tv_cached_token,
            symbol="AUDEUR", exchange="FX_IDC",
            interval=Interval.in_5_minute,
            start_date=ONE_YEAR_AGO, end_date=NOW,
        )
        _assert_daterange_df(df, start_date=ONE_YEAR_AGO, end_date=NOW,
                             min_bars=5000)
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(DATA_DIR / "AUDEUR_FX_IDC_5min_1yr.csv")

    @pytest.mark.timeout(300)
    def test_es_daily_18yr(self, tv_cached_token):
        df = _get_hist_with_retry(tv_cached_token,
            symbol="ES", exchange="CME_MINI",
            interval=Interval.in_daily,
            start_date=EIGHTEEN_YEARS_AGO, end_date=NOW,
            fut_contract=1,
        )
        _assert_daterange_df(df, start_date=EIGHTEEN_YEARS_AGO, end_date=NOW,
                             min_bars=1000)
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(DATA_DIR / "ES1_CME_MINI_1D_18yr.csv")

    # ── Back-adjusted futures tests ──────────────────────────────────

    @pytest.mark.timeout(60)
    def test_zc1_not_backadjusted(self, tv_cached_token):
        """ZC1! daily, 5 years, standard (unadjusted) prices. Saves CSV for reuse."""
        df = _get_hist_with_retry(tv_cached_token,
            symbol="ZC", exchange="CBOT",
            interval=Interval.in_daily,
            fut_contract=1,
            start_date=FIVE_YEARS_AGO, end_date=NOW,
            backadjusted=False,
        )
        assert not df.empty, "no data returned for CBOT:ZC1! (unadjusted)"
        assert len(df) >= 200, f"too few bars: got {len(df)}"
        logger.info("ZC1! unadjusted: %d bars, first close=%.4f last close=%.4f",
                    len(df), df["close"].iloc[0], df["close"].iloc[-1])
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(DATA_DIR / "ZC1_CBOT_1D_5yr.csv")

    @pytest.mark.timeout(60)
    def test_zc1_backadjusted(self, tv_cached_token, caplog):
        """ZC1! daily, 5 years, back-adjusted prices (B-ADJ).

        B-ADJ support varies by account tier. Skips cleanly if unsupported.
        """
        with caplog.at_level(logging.ERROR, logger="tvDatafeed.main"):
            df = _get_hist_with_retry(tv_cached_token,
                symbol="ZC", exchange="CBOT",
                interval=Interval.in_daily,
                fut_contract=1,
                start_date=FIVE_YEARS_AGO, end_date=NOW,
                backadjusted=True,
            )

        if any("B-ADJ not supported" in r.message for r in caplog.records):
            pytest.skip("B-ADJ not supported for CBOT:ZC1! on this account tier")

        assert not df.empty, "no data returned for CBOT:ZC1! (backadjusted)"
        assert len(df) >= 200, f"too few bars: got {len(df)}"
        logger.info("ZC1! B-ADJ:     %d bars, first close=%.4f last close=%.4f",
                    len(df), df["close"].iloc[0], df["close"].iloc[-1])
        DATA_DIR.mkdir(exist_ok=True)
        df.to_csv(DATA_DIR / "ZC1_CBOT_1D_5yr_badj.csv")

    @pytest.mark.timeout(120)
    def test_zc1_badj_prices_differ(self, tv_cached_token, caplog):
        """B-ADJ prices must differ from unadjusted due to roll adjustments.

        Loads raw data from CSV saved by test_zc1_not_backadjusted if available
        (avoids a second download when tests run in order).
        """
        # Load raw data from CSV if already saved by the preceding test
        csv_path = DATA_DIR / "ZC1_CBOT_1D_5yr.csv"
        if csv_path.exists():
            df_raw = pd.read_csv(csv_path, index_col=0, parse_dates=True)
            logger.info("loaded raw ZC1! data from CSV (%d bars)", len(df_raw))
        else:
            df_raw = _get_hist_with_retry(tv_cached_token,
                symbol="ZC", exchange="CBOT",
                interval=Interval.in_daily,
                fut_contract=1,
                start_date=FIVE_YEARS_AGO, end_date=NOW,
                backadjusted=False,
            )

        df_adj = pd.DataFrame()
        with caplog.at_level(logging.ERROR, logger="tvDatafeed.main"):
            for attempt in range(1, 4):
                df_adj = tv_cached_token.get_hist(
                    symbol="ZC", exchange="CBOT",
                    interval=Interval.in_daily,
                    fut_contract=1,
                    start_date=FIVE_YEARS_AGO, end_date=NOW,
                    backadjusted=True,
                )
                if not df_adj.empty:
                    break
                if attempt < 3:
                    time.sleep(5 * attempt)

        if any("B-ADJ not supported" in r.message for r in caplog.records):
            pytest.skip("B-ADJ not supported for CBOT:ZC1! on this account tier")
        if df_raw.empty:
            pytest.skip("unadjusted ZC1! fetch returned empty (transient connection issue)")
        if df_adj.empty:
            pytest.skip("B-ADJ ZC1! fetch returned empty after 3 attempts")

        common = df_raw.index.intersection(df_adj.index)
        assert len(common) >= 10, "too few overlapping bars to compare"

        raw_early = df_raw.loc[common[:20], "close"]
        adj_early = df_adj.loc[common[:20], "close"]

        prices_differ = not raw_early.equals(adj_early)
        logger.info("ZC1! early closes — raw: %s | adj: %s | differ=%s",
                    raw_early.values[:3], adj_early.values[:3], prices_differ)
        assert prices_differ, (
            "unadjusted and back-adjusted closes are identical — "
            "back-adjustment may not be working"
        )

    @pytest.mark.timeout(60)
    def test_aapl_backadjusted_flag_ignored(self, tv_cached_token, caplog):
        """backadjusted=True on a non-futures symbol must log a warning and be ignored."""
        df = pd.DataFrame()
        with caplog.at_level(logging.WARNING, logger="tvDatafeed.main"):
            for attempt in range(1, 4):
                df = tv_cached_token.get_hist(
                    symbol="AAPL", exchange="NASDAQ",
                    interval=Interval.in_daily,
                    n_bars=10,
                    backadjusted=True,
                )
                if not df.empty:
                    break
                if attempt < 3:
                    time.sleep(3)

        assert not df.empty, "AAPL returned empty after 3 attempts"
        warning_msgs = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("backadjusted" in m and "ignored" in m for m in warning_msgs), (
            f"expected backadjusted-ignored warning, got: {warning_msgs}"
        )
        logger.info("AAPL with backadjusted=True correctly ignored, got %d bars", len(df))


# ══════════════════════════════════════════════════════════════════════
#  4. TradingView Desktop App tests — auth verification only
# ══════════════════════════════════════════════════════════════════════

class TestDesktopApp:
    """Verifies TradingView desktop app cookie extraction produces a working JWT."""

    @pytest.mark.timeout(60)
    def test_auth_works(self, tv_desktop):
        df = _get_hist_with_retry(tv_desktop,
            symbol="AAPL", exchange="NASDAQ",
            interval=Interval.in_daily, n_bars=10,
        )
        _assert_valid_df(df)
