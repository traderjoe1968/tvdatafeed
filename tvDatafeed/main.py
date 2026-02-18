import datetime
import enum
import json
import logging
import random
import re
import string
import time
from pathlib import Path
import pandas as pd  # noqa
from datetime import datetime as dt, timedelta
from websocket import WebSocket, create_connection
import requests

logger = logging.getLogger(__name__)

token_path = Path.home() / ".tvdatafeed" / "token"
token_path.parent.mkdir(parents=True, exist_ok=True)

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

_FRAME_SPLITTER = re.compile(r'~m~\d+~m~')

class TvDatafeed:
    __sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    __sign_in_totp = 'https://www.tradingview.com/accounts/two-factor/signin/totp/'
    __sign_in_sms = 'https://www.tradingview.com/accounts/two-factor/signin/sms/'
    __sign_in_email = 'https://www.tradingview.com/accounts/two-factor/signin/email/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = json.dumps({"Origin": "https://data.tradingview.com"})
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout = 5

    def __init__(self, username: str | None = None, password: str | None = None, token: str | None = None) -> None:
        self.ws_debug = False
        if token:
            self.token = token
            self.__save_token(token)
        else:
            self.token = self.__auth(username, password)
        if self.token is None:
            self.token = "unauthorized_user_token"
            logger.warning("you are using nologin method, data you access may be limited")
        self.ws: WebSocket | None = None
        self.session = self.__generate_session()
        self.chart_session = self.__generate_chart_session()

    def __auth(self, username, password):
        # try cached token first
        try:
            token = token_path.read_text().strip()
            if token:
                logger.info("loaded cached auth token from %s", token_path)
                return token
        except (IOError, OSError):
            pass

        if username is None or password is None:
            return None

        data = {"username": username,
                "password": password,
                "remember": "on"}
        max_retries = 10
        delay = 30  # seconds; TV rate limit typically resets in 5-10 min, doubles each retry

        for attempt in range(1, max_retries + 1):
            try:
                with requests.Session() as s:
                    response = s.post(
                        url=self.__sign_in_url, data=data, headers=self.__signin_headers)
                    resp_json = response.json()
                    error_code = resp_json.get("code", "")

                    if error_code == "rate_limit":
                        if attempt < max_retries:
                            total_waited = sum(30 * 2**i for i in range(attempt))
                            logger.warning(
                                "Rate limited by TradingView — waiting %ds before retry "
                                "(attempt %d/%d, ~%ds total waited so far)",
                                delay, attempt, max_retries, total_waited)
                            time.sleep(delay)
                            delay *= 2
                            continue
                        logger.error(
                            "Still rate limited after %d attempts. Use token= parameter to bypass login:\n"
                            "  TvDatafeed(token='your_sessionid') or set TV_TOKEN in .env",
                            max_retries)
                        return None

                    if error_code == "recaptcha_required":
                        logger.error(
                            "CAPTCHA required — programmatic login blocked by TradingView.\n"
                            "  Use token= parameter instead:\n"
                            "  1. Log in at tradingview.com in your browser\n"
                            "  2. DevTools (F12) → Application → Cookies → copy 'sessionid'\n"
                            "  3. Pass it as: TvDatafeed(token='your_sessionid')\n"
                            "     or set TV_TOKEN in your .env file")
                        return None

                    if error_code == "2FA_required":
                        two_factor_types = [t["name"] for t in resp_json.get("two_factor_types", [])]
                        logger.info("2FA required, types: %s", two_factor_types)
                        code = self.__prompt_2fa()
                        if code is None:
                            raise ValueError("2FA code not provided")
                        if "totp" in two_factor_types:
                            response = s.post(
                                url=self.__sign_in_totp, data={"code": int(code)},
                                headers=self.__signin_headers)
                        elif "sms" in two_factor_types:
                            response = s.post(
                                url=self.__sign_in_sms, data={"code": int(code)},
                                headers=self.__signin_headers)
                        elif "email" in two_factor_types:
                            response = s.post(
                                url=self.__sign_in_email, data={"code": int(code)},
                                headers=self.__signin_headers)
                        else:
                            raise ValueError(f"unsupported 2FA type: {two_factor_types}")
                        resp_json = response.json()

                    if "error" in resp_json:
                        raise ValueError(
                            f"signin error [{resp_json.get('code', '')}]: {resp_json.get('error', '')}")

                    token = resp_json['user']['auth_token']
                    self.__save_token(token)
                    return token

            except (ValueError, KeyError) as e:
                logger.error("error while signin: %s", e)
                return None
            except Exception as e:
                logger.error("error while signin: %s", e)
                return None

        return None

    @staticmethod
    def __prompt_2fa():
        """Open a local web page in the browser to collect the 2FA code."""
        import http.server
        import threading
        import webbrowser

        code = None

        _HTML = """<!DOCTYPE html><html><head><meta charset="utf-8">
<title>TradingView 2FA</title>
<style>
  body{font-family:system-ui,sans-serif;display:flex;justify-content:center;
       align-items:center;height:100vh;margin:0;background:#1e222d}
  .card{background:#2a2e39;padding:40px;border-radius:12px;text-align:center;
        box-shadow:0 8px 32px rgba(0,0,0,.4)}
  h2{color:#d1d4dc;margin:0 0 20px}
  input{font-size:24px;width:180px;padding:10px;text-align:center;
        border:2px solid #434651;border-radius:8px;background:#131722;
        color:#d1d4dc;outline:none}
  input:focus{border-color:#2962ff}
  button{margin-top:16px;padding:10px 32px;font-size:16px;border:none;
         border-radius:8px;background:#2962ff;color:#fff;cursor:pointer}
  button:hover{background:#1e53e5}
  .done{color:#26a69a;font-size:18px;margin-top:12px;display:none}
</style></head><body><div class="card">
  <h2>TradingView 2FA Code</h2>
  <form id="f"><input id="c" type="text" maxlength="8" autofocus
    placeholder="000000"><br><button type="submit">Submit</button></form>
  <div class="done" id="d">Code received — you can close this tab.</div>
</div><script>
document.getElementById('f').onsubmit=function(e){
  e.preventDefault();
  var c=document.getElementById('c').value.trim();
  if(!c)return;
  fetch('/code?v='+c).then(function(){
    document.getElementById('f').style.display='none';
    document.getElementById('d').style.display='block';
  });
};
</script></body></html>"""

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                nonlocal code
                if self.path.startswith('/code?v='):
                    code = self.path.split('=', 1)[1]
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'ok')
                    threading.Thread(target=self.server.shutdown, daemon=True).start()
                else:
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/html')
                    self.end_headers()
                    self.wfile.write(_HTML.encode())
            def log_message(self, format: str, *args: object) -> None:
                pass  # silence request logs

        srv = http.server.HTTPServer(('127.0.0.1', 0), Handler)
        port = srv.server_address[1]
        url = f'http://127.0.0.1:{port}'

        logger.info("2FA required — opening browser at %s", url)
        webbrowser.open(url)
        srv.serve_forever()
        srv.server_close()
        return code if code else None

    @staticmethod
    def __save_token(token):
        token_path.write_text(token)
        logger.info("saved auth token to %s", token_path)

    @staticmethod
    def __delete_token():
        token_path.unlink(missing_ok=True)
        logger.info("deleted cached auth token")

    def __create_connection(self):
        logging.debug("creating websocket connection")
        self.ws = create_connection(
            "wss://data.tradingview.com/socket.io/websocket", headers=self.__ws_headers, timeout=self.__ws_timeout
        )

    @staticmethod
    def __parse_ws_packets(raw_data):
        """Split raw ~m~ framed websocket data into parsed JSON packets."""
        packets = []
        for part in _FRAME_SPLITTER.split(raw_data):
            if not part or part.startswith('~h~'):
                continue
            try:
                packets.append(json.loads(part))
            except (json.JSONDecodeError, ValueError):
                pass
        return packets

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
        assert self.ws is not None
        self.ws.send(m)

    @staticmethod
    def __create_df(packets, symbol):
        """Build a DataFrame from parsed websocket JSON packets."""
        data = []
        has_oi = False
        for packet in packets:
            if not isinstance(packet, dict) or packet.get("m") not in ("timescale_update", "du"):
                continue
            try:
                series = packet["p"][1].get("s1", packet["p"][1].get("sds_1", {}))
                bars = series.get("s", [])
            except (IndexError, KeyError, AttributeError):
                continue
            for bar in bars:
                try:
                    v = bar["v"]
                    ts = datetime.datetime.fromtimestamp(v[0])
                    open_, high, low, close = v[1], v[2], v[3], v[4]
                    vol = v[5] if len(v) > 5 else 0.0
                    oi = v[6] if len(v) > 6 else None
                    if oi is not None:
                        has_oi = True
                    data.append([ts, open_, high, low, close, vol, oi])
                except (KeyError, IndexError, TypeError, ValueError) as e:
                    logger.debug("skipping malformed bar: %s", e)
                    continue

        if not data:
            logger.error("no data, please check the exchange and symbol")
            return pd.DataFrame()

        columns = ["datetime", "open", "high", "low", "close", "volume", "OI"]
        df = pd.DataFrame(data, columns=columns).set_index("datetime")
        if not has_oi:
            df = df.drop(columns=["OI"])
        df.insert(0, "symbol", value=symbol)
        return df

    @staticmethod
    def __format_symbol(symbol, exchange, contract: int | None = None):

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
        fut_contract: int | None = None,  # Continuous futures: 1=front, 2=next, e.g. get_hist("ES","CME",fut_contract=1) → CME:ES1!  For specific expiry use symbol directly: get_hist("ESH2025","CME")
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
            assert self.ws is not None
            while True:
                try:
                    result = self.ws.recv()
                    raw_data += result + "\n"  # ty:ignore[unsupported-operator]
                    if "series_completed" in result:  # ty:ignore[unsupported-operator]
                        break
                except Exception as e:
                    logger.error(e)
                    break
            self.ws.close()
            packets = self.__parse_ws_packets(raw_data)
            return self.__create_df(packets, symbol)

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
        assert self.ws is not None
        while True:
            try:
                result = self.ws.recv()
                raw_data += result + "\n"  # ty:ignore[unsupported-operator]
                if "series_completed" in result:  # ty:ignore[unsupported-operator]
                    break
            except Exception as e:
                logger.error(e)
                break
        self.ws.close()
        packets = self.__parse_ws_packets(raw_data)
        return self.__create_df(packets, symbol)


# Example usage for your 10-year 5-min request
if __name__ == "__main__":
    import argparse
    import os

    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s: %(message)s")

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    parser = argparse.ArgumentParser()
    parser.add_argument("--username", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--token", default=None, help="TradingView sessionid cookie from browser DevTools")
    args = parser.parse_args()

    username = args.username or os.environ.get("TV_USERNAME")
    password = args.password or os.environ.get("TV_PASSWORD")
    token = args.token or os.environ.get("TV_TOKEN")

    tv = TvDatafeed(username, password, token=token)
    Sym="ES1!"
    Exch="CME"
    df = tv.get_hist(
        symbol=Sym,
        exchange=Exch,
        interval=Interval.in_daily,
        start_date=dt(2025, 1, 1),
        end_date=dt.now(),
        chunk_days=180,      # safe default
        sleep_seconds=3
    )

    print(df)
    df.to_csv(f"{Sym}_{Exch}.csv")