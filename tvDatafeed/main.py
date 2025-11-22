import datetime
import enum
import json
import logging
import random
import re
import string
from typing import Dict, List
import pandas as pd
import asyncio
from websocket import create_connection, WebSocketTimeoutException
import requests
import json
from dotenv import load_dotenv
from os import getenv
import pyotp
load_dotenv()

logger = logging.getLogger(__name__)


tokendata = "token.txt"


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
    __sign_in_totp = 'https://www.tradingview.com/accounts/two-factor/signin/totp/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = {"Origin": "https://data.tradingview.com"}
    __ws_proheaders = {"Origin": "https://prodata.tradingview.com"}
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout = 10

    def __init__(
        self,
        username: str = None,
        password: str = None,
        pro: bool =False
    ) -> None:
        """Create TvDatafeed object

        Args:
            username (str, optional): tradingview username. Defaults to None.
            password (str, optional): tradingview password. Defaults to None.
        """

        self.ws_debug = False

        self.pro = pro
                
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
        
        try:
            with open(tokendata, 'r') as f:
                token = f.read()
        except IOError:
            if (username is None or password is None):
                token = None

            else:
                data = {"username": username,
                        "password": password,
                        "remember": "on"}
                try:
                    with requests.Session() as s:
                        response = s.post(url=self.__sign_in_url, data=data, headers=self.__signin_headers)
                        if "2FA" in response.text:
                            response = s.post(url=self.__sign_in_totp, data={"code": self.__getcode()}, headers=self.__signin_headers)
                            token = response.json()['user']['auth_token']
                            with open(tokendata, 'w') as f:
                                    f.write(token)
                        else:
                            token = response.json()['user']['auth_token']

                except Exception as e:
                    logger.error('error while signin', e)
                    token = None

        return token

    @staticmethod
    def __getcode():
        totp_key = getenv('TOTP_KEY', None)
        if totp_key:
            return pyotp.TOTP(totp_key).now()
        
        code = input("Enter 2FA code: ")
        return code
    
    def __delete_token(self):
        self.token = None
        raise Exception("error with token - exiting")    
    
    def __create_connection(self):
        logging.debug("creating websocket connection")
        if self.pro:
            self.ws = create_connection("wss://prodata.tradingview.com/socket.io/websocket", headers=self.__ws_proheaders, timeout=self.__ws_timeout)
        else:
             self.ws = create_connection("wss://data.tradingview.com/socket.io/websocket", headers=self.__ws_headers, timeout=self.__ws_timeout)

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
        try:
            out = re.search(r'"s":\[(.+?)\}\]', raw_data).group(1)
            x = out.split(',{"')
            data = list()
            volume_data = True

            epoch = datetime.datetime(1970, 1, 1)

            for xi in x:
                xi = re.split(r"\[|:|,|\]", xi)

                if is_return_dataframe:
                    try:
                        ts_raw = float(xi[4])
                    except ValueError:
                        # malformed row, skip
                        continue

                    try:
                        # works for negative timestamps too (pre-1970)
                        ts = epoch + datetime.timedelta(0, ts_raw)
                    except OverflowError:
                        # insane timestamp, skip
                        continue
                else:
                    try:
                        ts = int(xi[4].split('.')[0])
                    except ValueError:
                        # malformed row, skip
                        continue

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
        except Exception as e:
            logger.exception(e)
    
    @staticmethod
    def __create_df(data, symbol):
        try:
            df = pd.DataFrame(
                data, columns=["datetime", "open",
                               "high", "low", "close", "volume"]
            ).set_index("datetime")
            df.insert(0, "symbol", value=symbol)
            return df
        except AttributeError:
            logger.error("no data, please check the exchange and symbol")
        except Exception as e:
            logger.exception(e)

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
    
    def __initialize_ws(self):
        self.__create_connection()

        self.__send_message("set_auth_token", [self.token])
        self.__send_message("chart_create_session", [self.chart_session, ""])
        self.__send_message("quote_create_session", [self.session])
        self.__send_message(
            "quote_set_fields",
            [
                self.session,
                "ch",
                "chp",
                "current_session",
                "description",
                "local_description",
                "language",
                "exchange",
                "fractional",
                "is_tradable",
                "lp",
                "lp_time",
                "minmov",
                "minmove2",
                "original_name",
                "pricescale",
                "pro_name",
                "short_name",
                "type",
                "update_mode",
                "volume",
                "currency_code",
                "rchp",
                "rtc",
            ],
        )

    async def __fetch_symbol_data(
        self,
        symbol: str,
        exchange: str = "NSE",
        interval: Interval = Interval.in_daily,
        n_bars: int = 10,
        fut_contract: int = None,
        extended_session: bool = False,
        dataFrame: bool = True
    ) -> pd.DataFrame | List[List]:
        """get single symbol historical data

        Args:
            symbol (str): symbol name
            exchange (str, optional): exchange, not required if symbol is in format EXCHANGE:SYMBOL. Defaults to None.
            interval (str, optional): chart interval. Defaults to 'D'.
            n_bars (int, optional): no of bars to download, max 5000. Defaults to 10.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front, 2 for continuous next contract in front . Defaults to None.
            extended_session (bool, optional): regular session if False, extended session if True, Defaults to False.

        Returns:
            pd.Dataframe: dataframe with sohlcv as columns
        """
        symbol = self.__format_symbol(
            symbol=symbol, exchange=exchange, contract=fut_contract
        )

        interval = interval.value

        self.__initialize_ws()

        self.__send_message(
            "quote_add_symbols", [self.session, symbol,
                                  {"flags": ["force_permission"]}]
        )
        self.__send_message("quote_fast_symbols", [self.session, symbol])

        self.__send_message(
            "resolve_symbol",
            [
                self.chart_session,
                "symbol_1",
                '={"symbol":"'
                + symbol
                + '","adjustment":"splits","session":'
                + ('"regular"' if not extended_session else '"extended"')
                + "}",
            ],
        )
        self.__send_message(
            "create_series",
            [self.chart_session, "s1", "s1", "symbol_1", interval, n_bars],
        )
        self.__send_message("switch_timezone", [
                            self.chart_session, "exchange"])

        raw_data = ""

        logger.debug(f"getting data for {symbol}...")
        while True:
            try:
                result = self.ws.recv()
                raw_data = raw_data + result + "\n"
            except WebSocketTimeoutException as e:
                logger.error(e)
                break
            except Exception as e:
                self.__delete_token()
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
        
    async def get_hist_async(
            self,
            symbols: list[str],
            exchange: str = "NSE",
            interval: Interval = Interval.in_daily,
            n_bars: int = 10,
            fut_contract: int = None,
            extended_session: bool = False,
            dataFrame: bool = True,
        ) -> Dict[str, List[List]|pd.DataFrame]:
        """Fetch historical data for multiple symbols asynchronously."""
        tasks = [
            asyncio.create_task(self.__fetch_symbol_data(symbol, exchange, interval, n_bars, fut_contract, extended_session, dataFrame))
            for symbol in symbols
        ]
        results = await asyncio.gather(*tasks)
        
        return {sym: data for sym, data in zip(symbols, results)}

    def get_hist(
            self,
            symbol: List[str]|str,
            exchange: str = "NSE",
            interval: Interval = Interval.in_daily,
            n_bars: int = 10,
            dataFrame: bool = True,
            fut_contract: int = None,
            extended_session: bool = False
        ) -> pd.DataFrame|Dict[str, List[List]|pd.DataFrame]|List[List]:
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
        if isinstance(symbol, str):
            return asyncio.run(self.__fetch_symbol_data(symbol, exchange=exchange, interval=interval, n_bars=n_bars, fut_contract=fut_contract, extended_session=extended_session, dataFrame=dataFrame))

        return asyncio.run(self.get_hist_async(symbol, exchange=exchange, interval=interval, n_bars=n_bars, fut_contract=fut_contract, extended_session=extended_session, dataFrame=dataFrame))

    def search_symbol(self, text: str, exchange: str = ''):
        url = self.__search_url.format(text, exchange)

        symbols_list = []
        try:
            resp = requests.get(url)

            symbols_list = json.loads(resp.text.replace(
                '</em>', '').replace('<em>', ''))
        except Exception as e:
            logger.error(e)

        return symbols_list


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    username = getenv('MAIL')
    password = getenv('PASSWORD')
    
    tv = TvDatafeed(username, password, pro=True, )
    print(tv.get_hist(["PFC", "UNIONBANK", "BDL"], "NSE", Interval.in_5_minute, n_bars=11000))