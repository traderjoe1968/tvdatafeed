from __future__ import annotations

from datetime import datetime as dt, timedelta

import pandas as pd

from tvDatafeed.main import Interval, TvDatafeed


class RangeRecordingTvDatafeed(TvDatafeed):
    def __init__(self) -> None:
        self.pro_plan = "pro_premium"
        self.token = "test-token"
        self.ranges: list[tuple[dt, dt]] = []

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


def test_intraday_date_range_exceeding_history_depth_is_not_clamped() -> None:
    tv = RangeRecordingTvDatafeed()
    start_date = dt(2020, 1, 1)
    end_date = dt(2021, 1, 1)

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
    assert tv.ranges[0][0] == start_date
    assert tv.ranges[-1][1] == end_date
    assert all((end - start) <= timedelta(days=60) for start, end in tv.ranges)
    assert all(tv.ranges[i][1] == tv.ranges[i + 1][0] for i in range(len(tv.ranges) - 1))
