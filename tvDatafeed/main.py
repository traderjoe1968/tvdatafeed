from typing import Dict, List
import datetime
import enum
import json
import logging
import random
import re
import string
import pandas as pd
import requests
import json
import logging
import asyncio
from websockets import connect  # Replaced `websocket` with `websockets`

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


class TvDatafeed:
    __sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = json.dumps({"Origin": "https://data.tradingview.com"})
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout = 5

    def __init__(
        self,
        username: str = None,
        password: str = None,
    ) -> None:
        """Create TvDatafeed object

        Args:
            username (str, optional): tradingview username. Defaults to None.
            password (str, optional): tradingview password. Defaults to None.
        """

        self.ws_debug = False

        self.token = self.__auth(username, password)

        if self.token is None:
            self.token = "unauthorized_user_token"
            logger.warning(
                "you are using nologin method, data you access may be limited"
            )

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
    def __parse_data(raw_data, is_return_dataframe:bool) -> List[List]:
        out = re.search('"s":\[(.+?)\}\]', raw_data).group(1)
        x = out.split(',{"')
        data = list()
        volume_data = True

        for xi in x:
            xi = re.split("\[|:|,|\]", xi)
            ts = datetime.datetime.fromtimestamp(float(xi[4])) if is_return_dataframe else int(xi[4].split('.')[0])

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

        return data

    @staticmethod
    def __create_df(parsed_data, symbol) -> pd.DataFrame:
        try:
            df = pd.DataFrame(
                parsed_data, columns=["datetime", "open",
                               "high", "low", "close", "volume"]
            ).set_index("datetime")
            df.insert(0, "symbol", value=symbol)
            return df
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

    async def __fetch_symbol_data(self, symbol: str, exchange: str, interval: Interval, n_bars: int, fut_contract: int, extended_session: bool, dataFrame: bool) -> pd.DataFrame|List[List]:
        """Helper function to asynchronously fetch symbol data."""
        try:
            symbol = self.__format_symbol(symbol, exchange, fut_contract)
            interval = interval.value

            async with connect(
                "wss://data.tradingview.com/socket.io/websocket",
                origin="https://data.tradingview.com"
            ) as websocket:
                # Authentication and session setup
                await websocket.send(self.__create_message("set_auth_token", [self.token]))
                await websocket.send(self.__create_message("chart_create_session", [self.chart_session, ""]))
                await websocket.send(self.__create_message("quote_create_session", [self.session]))
                await websocket.send(self.__create_message(
                    "quote_set_fields",
                    [
                        self.session,
                        "ch", "chp", "current_session", "description",
                        "local_description", "language", "exchange",
                        "fractional", "is_tradable", "lp", "lp_time",
                        "minmov", "minmove2", "original_name", "pricescale",
                        "pro_name", "short_name", "type", "update_mode", "volume",
                        "currency_code", "rchp", "rtc",
                    ]
                ))
                await websocket.send(self.__create_message("quote_add_symbols", [self.session, symbol, {"flags": ["force_permission"]}]))
                await websocket.send(self.__create_message("quote_fast_symbols", [self.session, symbol]))

                # Symbol resolution and series creation
                await websocket.send(
                    self.__create_message(
                        "resolve_symbol",
                        [
                            self.chart_session,
                            "symbol_1",
                            f'={{"symbol":"{symbol}","adjustment":"splits","session":"{"regular" if not extended_session else "extended"}"}}',
                        ],
                    )
                )
                await websocket.send(self.__create_message("create_series", [self.chart_session, "s1", "s1", "symbol_1", interval, n_bars]))
                await websocket.send(self.__create_message("switch_timezone", [self.chart_session, "exchange"]))

                raw_data = ""

                # Fetch and parse raw data asynchronously
                while True:
                    try:
                        result = await websocket.recv()
                        raw_data += result + "\n"
                    except Exception as e:
                        logger.error(e)
                        break

                    if "series_completed" in result:
                        break

            # Return formatted data
            if dataFrame:
                parsed_data = self.__parse_data(raw_data, dataFrame)
                return self.__create_df(parsed_data, symbol)
            else:
                return self.__parse_data(raw_data, dataFrame)
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None

    async def get_hist_async(self, symbols: list[str], exchange: str = "NSE", interval: Interval = Interval.in_daily, n_bars: int = 10, dataFrame: bool = True, fut_contract: int = None, extended_session: bool = False) -> Dict[str, List[List]|pd.DataFrame]:
        """Fetch historical data for multiple symbols asynchronously."""
        tasks = [
            self.__fetch_symbol_data(symbol, exchange, interval, n_bars, fut_contract, extended_session, dataFrame)
            for symbol in symbols
        ]
        results = await asyncio.gather(*tasks)
        
        return {sym: data for sym, data in zip(symbols, results)}

    def get_hist(self, symbols: list[str]|str, exchange: str = "NSE", interval: Interval = Interval.in_daily, n_bars: int = 10, dataFrame: bool = True, fut_contract: int = None, extended_session: bool = False) -> pd.DataFrame|Dict[str, List[List]|pd.DataFrame]|List[List]:
        """Fetch historical data for a single or multiple symbols.

        Args:
            symbols (list[str] | str): Single symbol or list of symbols.
            exchange (str, optional): Exchange. Defaults to "NSE".
            interval (Interval, optional): Interval. Defaults to Interval.in_daily.
            n_bars (int, optional): Number of bars. Defaults to 10.
            dataFrame (bool, optional): Return as DataFrame. Defaults to True.
            fut_contract (int, optional): Future contract. Defaults to None.
            extended_session (bool, optional): Extended session. Defaults to False.

        Returns:
            pd.DataFrame | Dict[str, List[List] | pd.DataFrame] | List[List]: Historical data.
        """
        if isinstance(symbols, str):
            return asyncio.run(self.__fetch_symbol_data(symbols, exchange, interval, n_bars, fut_contract, extended_session, dataFrame))

        return asyncio.run(self.get_hist_async(symbols, exchange, interval, n_bars, dataFrame, fut_contract, extended_session))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    tv = TvDatafeed()

    symbols = ['SBIN', 'EICHERMOT', 'INFY', 'BHARTIARTL', 'NESTLEIND', 'ASIANPAINT', 'ITC']
    print(tv.get_hist(symbols, "NSE", n_bars=500))
    print(tv.get_hist("NIFTY", "NSE", fut_contract=1))
    print(tv.get_hist(
            "EICHERMOT",
            "NSE",
            interval=Interval.in_1_hour,
            n_bars=500,
            extended_session=False,
            dataFrame=False
        )
    )
