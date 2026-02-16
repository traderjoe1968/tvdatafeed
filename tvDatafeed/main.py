import datetime
import enum
import json
import logging
import random
import re
import string
import time
import pandas as pd
from datetime import datetime as dt, timedelta
from websocket import create_connection
import requests

logger = logging.getLogger(__name__)

class Interval(enum.Enum):
    in_1_minute = "1"
    in_3_minute = "3"
    in_5_minute = "5"
    in_15_minute = "15"
    in_30_minute = "30"
    in_45_minute = "45"
    in_1_hour = "1H"
    in_2_hour = "2H"
    in_3_hour = "3H"
    in_4_hour = "4H"
    in_daily = "1D"
    in_weekly = "1W"
    in_monthly = "1M"

# seconds per bar (used only for optional buffer)
INTERVAL_SECONDS = {
    "1": 60, "3": 180, "5": 300, "15": 900, "30": 1800, "45": 2700,
    "1H": 3600, "2H": 7200, "3H": 10800, "4H": 14400,
    "1D": 86400, "1W": 604800, "1M": 2592000,
}

class TvDatafeed:
    __sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = json.dumps({"Origin": "https://data.tradingview.com"})
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout = 5

    def __init__(self, username: str = None, password: str = None) -> None:
        self.ws_debug = False
        self.token = self.__auth(username, password)
        if self.token is None:
            self.token = "unauthorized_user_token"
            logger.warning("you are using nologin method, data you access may be limited")
        self.ws = None
        self.session = self.__generate_session()
        self.chart_session = self.__generate_chart_session()

    def __auth(self, username, password):

        if (username is None or password is None):
            token = None

        else:
            data = {"username": username,
                    "password": password,
                    "remember": "on"}
            try:
                response = requests.post(
                    url=self.__sign_in_url, data=data, headers=self.__signin_headers)
                token = response.json()['user']['auth_token']
            except Exception as e:
                logger.error('error while signin')
                token = None

        return token

    def __create_connection(self):
        logging.debug("creating websocket connection")
        self.ws = create_connection(
            "wss://data.tradingview.com/socket.io/websocket", headers=self.__ws_headers, timeout=self.__ws_timeout
        )

    @staticmethod
    def __filter_raw_message(text):
        try:
            found = re.search('"m":"(.+?)",', text).group(1)
            found2 = re.search('"p":(.+?"}"])}', text).group(1)

            return found, found2
        except AttributeError:
            logger.error("error in filter_raw_message")

    @staticmethod
    def __generate_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "qs_" + random_string

    @staticmethod
    def __generate_chart_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "cs_" + random_string

    @staticmethod
    def __prepend_header(st):
        return "~m~" + str(len(st)) + "~m~" + st

    @staticmethod
    def __construct_message(func, param_list):
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def __create_message(self, func, paramList):
        return self.__prepend_header(self.__construct_message(func, paramList))

    def __send_message(self, func, args):
        m = self.__create_message(func, args)
        if self.ws_debug:
            print(m)
        self.ws.send(m)

    @staticmethod
    def __create_df(raw_data, symbol):
        try:
            out = re.search('"s":\[(.+?)\}\]', raw_data).group(1)
            x = out.split(',{"')
            data = list()
            volume_data = True

            for xi in x:
                xi = re.split("\[|:|,|\]", xi)
                ts = datetime.datetime.fromtimestamp(float(xi[4]))

                row = [ts]

                for i in range(5, 10):

                    # skip converting volume data if does not exists
                    if not volume_data and i == 9:
                        row.append(0.0)
                        continue
                    try:
                        row.append(float(xi[i]))

                    except ValueError:
                        volume_data = False
                        row.append(0.0)
                        logger.debug('no volume data')

                data.append(row)

            data = pd.DataFrame(
                data, columns=["datetime", "open",
                               "high", "low", "close", "volume"]
            ).set_index("datetime")
            data.insert(0, "symbol", value=symbol)
            return data
        except AttributeError:
            logger.error("no data, please check the exchange and symbol")

    @staticmethod
    def __format_symbol(symbol, exchange, contract: int = None):

        if ":" in symbol:
            pass
        elif contract is None:
            symbol = f"{exchange}:{symbol}"

        elif isinstance(contract, int):
            symbol = f"{exchange}:{symbol}{contract}!"

        else:
            raise ValueError("not a valid contract")

        return symbol

    def get_hist(
        self,
        symbol: str,
        exchange: str = "NSE",
        interval: Interval = Interval.in_daily,
        n_bars: int = 10,
        start_date: dt | str | None = None,
        end_date: dt | str | None = None,
        fut_contract: int | None = None,
        extended_session: bool = False,
        chunk_days: int = 180,      # ← change this for larger/smaller chunks (90–400 safe)
        sleep_seconds: int = 3,     # ← sleep between chunks to stay under rate limits
    ) -> pd.DataFrame:
        """
        If start_date/end_date are given → date-range mode with automatic chunking.
        Otherwise falls back to original n_bars behaviour.
        """
        if start_date is None and end_date is None:
            # ==================== ORIGINAL n_bars MODE (unchanged) ====================
            symbol = self.__format_symbol(symbol=symbol, exchange=exchange, contract=fut_contract)
            interval_val = interval.value

            self.__create_connection()
            self.__send_message("set_auth_token", [self.token])
            self.__send_message("chart_create_session", [self.chart_session, ""])
            self.__send_message("quote_create_session", [self.session])
            self.__send_message("quote_set_fields", [self.session, "ch", "chp", "current_session", "description",
                                                     "local_description", "language", "exchange", "fractional",
                                                     "is_tradable", "lp", "lp_time", "minmov", "minmove2",
                                                     "original_name", "pricescale", "pro_name", "short_name",
                                                     "type", "update_mode", "volume", "currency_code", "rchp", "rtc"])
            self.__send_message("quote_add_symbols", [self.session, symbol, {"flags": ["force_permission"]}])
            self.__send_message("quote_fast_symbols", [self.session, symbol])
            self.__send_message("resolve_symbol", [self.chart_session, "symbol_1",
                                                   f'={{"symbol":"{symbol}","adjustment":"splits","session":"{"regular" if not extended_session else "extended"}"}}'])
            self.__send_message("create_series", [self.chart_session, "s1", "s1", "symbol_1", interval_val, n_bars])
            self.__send_message("switch_timezone", [self.chart_session, "exchange"])

            raw_data = ""
            logger.debug(f"getting {n_bars} bars for {symbol}...")
            while True:
                try:
                    result = self.ws.recv()
                    raw_data += result + "\n"
                    if "series_completed" in result:
                        break
                except Exception as e:
                    logger.error(e)
                    break
            self.ws.close()
            return self.__create_df(raw_data, symbol)

        # ==================== NEW DATE-RANGE MODE WITH CHUNKING ====================
        # normalise dates
        if isinstance(start_date, str):
            start_date = dt.fromisoformat(start_date.replace("Z", "+00:00"))
        if isinstance(end_date, str):
            end_date = dt.fromisoformat(end_date.replace("Z", "+00:00"))
        if start_date is None:
            start_date = dt(2000, 1, 1)
        if end_date is None or end_date > dt.now():
            end_date = dt.now()
        if start_date >= end_date:
            raise ValueError("start_date must be before end_date")

        symbol_formatted = self.__format_symbol(symbol, exchange, fut_contract)
        interval_val = interval.value

        df_list = []
        current_start = start_date

        while current_start < end_date:
            current_end = min(current_start + timedelta(days=chunk_days), end_date)

            start_ts = int(current_start.timestamp() * 1000)   # milliseconds
            end_ts   = int(current_end.timestamp() * 1000)

            # optional 30-min buffer for intraday (as used in the original PR)
            if INTERVAL_SECONDS.get(interval_val, 60) < 86400:
                start_ts -= 1_800_000
                end_ts   -= 1_800_000

            range_str = f"r,{start_ts}:{end_ts}"

            df_chunk = self._fetch_range(symbol_formatted, interval_val, range_str, extended_session)
            if not df_chunk.empty:
                df_list.append(df_chunk)

            current_start = current_end
            time.sleep(sleep_seconds)   # rate-limit safety

        if df_list:
            df = pd.concat(df_list)
            df = df[~df.index.duplicated(keep='first')].sort_index()
            df = df[(df.index >= start_date) & (df.index <= end_date)]
            return df
        return pd.DataFrame()

    def _fetch_range(self, symbol: str, interval: str, range_str: str, extended_session: bool = False) -> pd.DataFrame:
        """Internal helper that fetches one chunk using the range request."""
        self.__create_connection()
        self.__send_message("set_auth_token", [self.token])
        self.__send_message("chart_create_session", [self.chart_session, ""])
        self.__send_message("quote_create_session", [self.session])
        self.__send_message("quote_set_fields", [self.session, "ch", "chp", "current_session", "description",
                                                 "local_description", "language", "exchange", "fractional",
                                                 "is_tradable", "lp", "lp_time", "minmov", "minmove2",
                                                 "original_name", "pricescale", "pro_name", "short_name",
                                                 "type", "update_mode", "volume", "currency_code", "rchp", "rtc"])
        self.__send_message("quote_add_symbols", [self.session, symbol, {"flags": ["force_permission"]}])
        self.__send_message("quote_fast_symbols", [self.session, symbol])
        self.__send_message("resolve_symbol", [self.chart_session, "symbol_1",
                                               f'={{"symbol":"{symbol}","adjustment":"splits","session":"{"regular" if not extended_session else "extended"}"}}'])

        self.__send_message("create_series", [self.chart_session, "s1", "s1", "symbol_1", interval, 100])  # dummy
        self.__send_message("switch_timezone", [self.chart_session, "exchange"])

        time.sleep(0.5)
        self.__send_message("modify_series", [self.chart_session, "s1", "s1", "symbol_1", interval, range_str])

        raw_data = ""
        logger.debug(f"fetching range {range_str} for {symbol}...")
        while True:
            try:
                result = self.ws.recv()
                raw_data += result + "\n"
                if "series_completed" in result:
                    break
            except Exception as e:
                logger.error(e)
                break
        self.ws.close()
        return self.__create_df(raw_data, symbol)


# Example usage for your 10-year 5-min request
if __name__ == "__main__":
    tv = TvDatafeed()   # or TvDatafeed(username, password) for more bars

    df = tv.get_hist(
        symbol="BTCUSD",
        exchange="COINBASE",
        interval=Interval.in_5_minute,
        start_date=dt(2016, 1, 1),
        end_date=dt.now(),
        chunk_days=180,      # safe default
        sleep_seconds=3
    )

    print(df)
    df.to_csv("10y_5min_BTCUSD.csv")