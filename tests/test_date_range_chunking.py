from __future__ import annotations

from datetime import datetime as dt, timedelta

import pandas as pd
import pytest

from tvDatafeed.main import Interval, TvDatafeed


class RangeRecordingTvDatafeed(TvDatafeed):
    fixed_now = dt(2026, 5, 2)

    def __init__(self) -> None:
        self.pro_plan = "pro_premium"
        self.token = "test-token"
        self.ranges: list[tuple[dt, dt]] = []

    def _now(self) -> dt:
        return self.fixed_now

    def _fetch_range(
        self,
        symbol: str,
        interval: str,
        range_str: str,
        extended_session: bool = False,
        backadjusted: bool = False,
    ) -> pd.DataFrame | None:
        start_ms, end_ms = (int(part) for part in range_str.removeprefix("r,").split(":"))
        # get_hist applies a 30-minute intraday buffer before sending the range.
        buffer_ms = 1_800_000
        start = dt.fromtimestamp((start_ms + buffer_ms) / 1000)
        end = dt.fromtimestamp((end_ms + buffer_ms) / 1000)
        self.ranges.append((start, end))
        return pd.DataFrame(
            {
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [1.0],
            },
            index=pd.DatetimeIndex([start]),
        )


def test_intraday_date_range_exceeding_available_data_is_trimmed() -> None:
    tv = RangeRecordingTvDatafeed()
    start_date = dt(2026, 1, 1)
    end_date = dt(2026, 5, 1)
    expected_available_start = tv.fixed_now - timedelta(minutes=19_800)

    df = tv.get_hist(
        symbol="ES1!",
        exchange="CME_MINI",
        interval=Interval.in_1_minute,
        start_date=start_date,
        end_date=end_date,
        chunk_days=60,
        sleep_seconds=0,
    )

    assert not df.empty
    assert tv.ranges[0][0] == expected_available_start
    assert tv.ranges[-1][1] == end_date
    assert all((end - start) <= timedelta(days=60) for start, end in tv.ranges)
    assert all(tv.ranges[i][1] == tv.ranges[i + 1][0] for i in range(len(tv.ranges) - 1))


def test_intraday_trim_prints_user_feedback(capsys) -> None:
    tv = RangeRecordingTvDatafeed()

    tv.get_hist(
        symbol="ES1!",
        exchange="CME_MINI",
        interval=Interval.in_1_minute,
        start_date=dt(2026, 1, 1),
        end_date=dt(2026, 5, 1),
        chunk_days=60,
        sleep_seconds=0,
    )

    out = capsys.readouterr().out
    assert "exceeds estimated available data" in out
    assert "trimming start from 2026-01-01 00:00 to 2026-04-18 06:00" in out


def test_intraday_range_before_available_window_returns_empty_with_feedback(capsys) -> None:
    tv = RangeRecordingTvDatafeed()

    df = tv.get_hist(
        symbol="ES1!",
        exchange="CME_MINI",
        interval=Interval.in_1_minute,
        start_date=dt(2020, 1, 1),
        end_date=dt(2020, 1, 2),
        chunk_days=1,
        sleep_seconds=0,
    )

    out = capsys.readouterr().out
    assert df.empty
    assert not tv.ranges
    assert "outside the estimated available window" in out
    assert "returning empty DataFrame" in out


@pytest.mark.parametrize(
    ("start_date", "end_date"),
    [
        ("2016-01-01", "2016-12-31"),
        ("2022-01-01", "2022-12-31"),
    ],
)
def test_es1_one_minute_historical_years_are_outside_available_window(
    start_date: str,
    end_date: str,
    capsys,
) -> None:
    tv = RangeRecordingTvDatafeed()

    df = tv.get_hist(
        symbol="CME_MINI:ES1!",
        exchange="CME_MINI",
        interval=Interval.in_1_minute,
        start_date=start_date,
        end_date=end_date,
        chunk_days=60,
        sleep_seconds=0,
    )

    out = capsys.readouterr().out
    assert df.empty
    assert not tv.ranges
    assert f"{start_date} 00:00" in out
    assert f"{end_date} 00:00" in out
    assert "outside the estimated available window" in out
