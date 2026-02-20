from __future__ import annotations

import datetime
import enum
import glob as globmod
import hashlib
import json
import logging
import os
import platform
import random
import re
import shutil
import sqlite3
import string
import struct
import subprocess
import tempfile
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

# Approximate maximum historical depth TradingView provides per interval.
# These are conservative estimates — actual depth varies by symbol.
_INTERVAL_MAX_DAYS = {
    "1": 180,       # 1-minute:  ~6 months
    "3": 365,       # 3-minute:  ~1 year
    "5": 365,       # 5-minute:  ~1 year
    "15": 730,      # 15-minute: ~2 years
    "30": 730,      # 30-minute: ~2 years
    "45": 730,      # 45-minute: ~2 years
    "1H": 730,      # 1-hour:    ~2 years
    "2H": 730,      # 2-hour:    ~2 years
    "3H": 730,      # 3-hour:    ~2 years
    "4H": 730,      # 4-hour:    ~2 years
    # Daily and above: essentially unlimited (10+ years)
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
        self.pro_plan = ""  # set during token recovery; e.g. 'pro', 'pro_premium', or '' for free
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

    # ── Cookie store paths (platform-specific) ──────────────────────────

    _DESKTOP_COOKIE_PATHS = {
        "Darwin": Path.home() / "Library" / "Application Support" / "TradingView" / "Cookies",
        "Linux": Path.home() / ".config" / "TradingView" / "Cookies",
        "Windows": Path.home() / "AppData" / "Roaming" / "TradingView" / "Cookies",
    }

    _FIREFOX_COOKIE_GLOBS = {
        "Darwin": str(Path.home() / "Library" / "Application Support" / "Firefox" / "Profiles" / "*" / "cookies.sqlite"),
        "Linux": str(Path.home() / ".mozilla" / "firefox" / "*" / "cookies.sqlite"),
        "Windows": str(Path.home() / "AppData" / "Roaming" / "Mozilla" / "Firefox" / "Profiles" / "*" / "cookies.sqlite"),
    }

    _CHROMIUM_BROWSERS = {
        "Darwin": {
            "Chrome": Path.home() / "Library" / "Application Support" / "Google" / "Chrome" / "Default" / "Cookies",
            "Edge": Path.home() / "Library" / "Application Support" / "Microsoft Edge" / "Default" / "Cookies",
            "Brave": Path.home() / "Library" / "Application Support" / "BraveSoftware" / "Brave-Browser" / "Default" / "Cookies",
            "Chromium": Path.home() / "Library" / "Application Support" / "Chromium" / "Default" / "Cookies",
            "Arc": Path.home() / "Library" / "Application Support" / "Arc" / "User Data" / "Default" / "Cookies",
        },
        "Linux": {
            "Chrome": Path.home() / ".config" / "google-chrome" / "Default" / "Cookies",
            "Edge": Path.home() / ".config" / "microsoft-edge" / "Default" / "Cookies",
            "Brave": Path.home() / ".config" / "BraveSoftware" / "Brave-Browser" / "Default" / "Cookies",
            "Chromium": Path.home() / ".config" / "chromium" / "Default" / "Cookies",
        },
        "Windows": {
            "Chrome": Path.home() / "AppData" / "Local" / "Google" / "Chrome" / "User Data" / "Default" / "Cookies",
            "Edge": Path.home() / "AppData" / "Local" / "Microsoft" / "Edge" / "User Data" / "Default" / "Cookies",
            "Brave": Path.home() / "AppData" / "Local" / "BraveSoftware" / "Brave-Browser" / "User Data" / "Default" / "Cookies",
        },
    }

    # ── Cookie reading helpers ────────────────────────────────────────

    @staticmethod
    def _cookies_to_jwt(cookies: dict) -> tuple[str | None, str]:
        """Convert sessionid/sessionid_sign cookies to a JWT auth_token via tradingview.com.
        Returns (jwt, pro_plan) where pro_plan is e.g. 'pro', 'pro_premium', or '' for free."""
        try:
            resp = requests.get(
                "https://www.tradingview.com/",
                cookies=cookies,
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.tradingview.com"},
                timeout=10,
            )
            jwt_match = re.search(r'"auth_token":"([^"]+)"', resp.text)
            plan_match = re.search(r'"pro_plan":"([^"]*)"', resp.text)
            jwt = jwt_match.group(1) if jwt_match else None
            pro_plan = plan_match.group(1) if plan_match else ""
            return jwt, pro_plan
        except Exception as e:
            logger.debug("failed to fetch auth_token from cookies: %s", e)
        return None, ""

    @staticmethod
    def _read_chromium_cookie_db(cookie_path: Path) -> dict | None:
        """Read TV sessionid from a Chromium-based cookie DB, handling encryption per-platform."""
        if not cookie_path.exists():
            return None

        # Copy to temp file to avoid locking issues
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite")
        tmp.close()
        try:
            shutil.copy2(cookie_path, tmp.name)
            conn = sqlite3.connect(f"file:{tmp.name}?mode=ro", uri=True)
            rows = conn.execute(
                "SELECT name, value, encrypted_value FROM cookies "
                "WHERE host_key LIKE '%tradingview.com' AND name IN ('sessionid', 'sessionid_sign')"
            ).fetchall()
            conn.close()
        except Exception as e:
            logger.debug("could not read chromium cookies from %s: %s", cookie_path, e)
            return None
        finally:
            os.unlink(tmp.name)

        if not rows:
            return None

        cookies = {}
        for name, value, encrypted_value in rows:
            if value:
                cookies[name] = value
            elif encrypted_value:
                decrypted = TvDatafeed._decrypt_chromium_cookie(encrypted_value, cookie_path)
                if decrypted:
                    cookies[name] = decrypted

        return cookies if cookies.get("sessionid") else None

    @staticmethod
    def _decrypt_chromium_cookie(encrypted_value: bytes, cookie_path: Path) -> str | None:
        """Decrypt a Chromium encrypted cookie value. Platform-specific."""
        system = platform.system()

        if system == "Darwin":
            # macOS: Keychain password → PBKDF2 → AES-128-CBC
            if not encrypted_value.startswith(b"v10"):
                return None
            try:
                password = subprocess.check_output(
                    ["security", "find-generic-password", "-w", "-s", "Chrome Safe Storage", "-a", "Chrome"],
                    stderr=subprocess.DEVNULL,
                ).decode().strip()
            except Exception:
                # Try other browser names for the keychain entry
                for app_name in ("Chromium", "Microsoft Edge", "Brave", "Arc"):
                    try:
                        password = subprocess.check_output(
                            ["security", "find-generic-password", "-w", "-s", f"{app_name} Safe Storage", "-a", app_name],
                            stderr=subprocess.DEVNULL,
                        ).decode().strip()
                        break
                    except Exception:
                        continue
                else:
                    return None

            key = hashlib.pbkdf2_hmac("sha1", password.encode(), b"saltysalt", 1003, dklen=16)
            ciphertext = encrypted_value[3:]  # strip 'v10'
            iv = b" " * 16
            # Use openssl for AES decryption (avoids requiring `cryptography` package)
            try:
                tmp = tempfile.NamedTemporaryFile(delete=False)
                tmp.write(ciphertext)
                tmp.close()
                result = subprocess.check_output(
                    ["openssl", "enc", "-d", "-aes-128-cbc", "-K", key.hex(), "-iv", iv.hex(), "-in", tmp.name],
                    stderr=subprocess.DEVNULL,
                )
                os.unlink(tmp.name)
                return result.decode("utf-8", errors="ignore").strip()
            except Exception:
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass
                return None

        elif system == "Windows":
            # Windows: DPAPI via ctypes
            if not encrypted_value.startswith(b"v10") and not encrypted_value.startswith(b"\x01\x00\x00\x00"):
                return None
            try:
                import ctypes
                import ctypes.wintypes

                class DATA_BLOB(ctypes.Structure):
                    _fields_ = [("cbData", ctypes.wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

                blob_in = DATA_BLOB(len(encrypted_value), ctypes.create_string_buffer(encrypted_value, len(encrypted_value)))
                blob_out = DATA_BLOB()
                if ctypes.windll.crypt32.CryptUnprotectData(  # type: ignore[attr-defined]
                    ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)
                ):
                    raw = ctypes.string_at(blob_out.pbData, blob_out.cbData)
                    ctypes.windll.kernel32.LocalFree(blob_out.pbData)  # type: ignore[attr-defined]
                    return raw.decode("utf-8", errors="ignore")
            except Exception:
                pass
            return None

        elif system == "Linux":
            # Linux: try plaintext first, then PBKDF2 with 'peanuts' password
            if encrypted_value.startswith(b"v11") or encrypted_value.startswith(b"v10"):
                key = hashlib.pbkdf2_hmac("sha1", b"peanuts", b"saltysalt", 1, dklen=16)
                ciphertext = encrypted_value[3:]
                iv = b" " * 16
                try:
                    tmp = tempfile.NamedTemporaryFile(delete=False)
                    tmp.write(ciphertext)
                    tmp.close()
                    result = subprocess.check_output(
                        ["openssl", "enc", "-d", "-aes-128-cbc", "-K", key.hex(), "-iv", iv.hex(), "-in", tmp.name],
                        stderr=subprocess.DEVNULL,
                    )
                    os.unlink(tmp.name)
                    return result.decode("utf-8", errors="ignore").strip()
                except Exception:
                    try:
                        os.unlink(tmp.name)
                    except OSError:
                        pass
            return None

        return None

    @staticmethod
    def _read_safari_cookies() -> dict | None:
        """Read TradingView cookies from Safari's binary cookie store (macOS only)."""
        if platform.system() != "Darwin":
            return None
        cookie_file = Path.home() / "Library" / "Cookies" / "Cookies.binarycookies"
        if not cookie_file.exists():
            return None
        try:
            data = cookie_file.read_bytes()
            if data[:4] != b"cook":
                return None
            num_pages = struct.unpack(">I", data[4:8])[0]
            page_sizes = struct.unpack(">" + "I" * num_pages, data[8 : 8 + 4 * num_pages])
            offset = 8 + 4 * num_pages
            cookies = {}
            for ps in page_sizes:
                page = data[offset : offset + ps]
                offset += ps
                if len(page) < 12 or page[:4] != b"\x00\x00\x01\x00":
                    continue
                nc = struct.unpack("<I", page[4:8])[0]
                cookie_offsets = struct.unpack("<" + "I" * nc, page[8 : 8 + 4 * nc])
                for co in cookie_offsets:
                    c = page[co:]
                    if len(c) < 20:
                        continue
                    size = struct.unpack("<I", c[0:4])[0]
                    if len(c) < size:
                        continue
                    url_off = struct.unpack("<I", c[16:20])[0]
                    name_off = struct.unpack("<I", c[20:24])[0]
                    val_off = struct.unpack("<I", c[28:32])[0]
                    name = c[name_off : c.index(b"\x00", name_off)].decode("utf-8", errors="ignore")
                    value = c[val_off : c.index(b"\x00", val_off)].decode("utf-8", errors="ignore")
                    url = c[url_off : c.index(b"\x00", url_off)].decode("utf-8", errors="ignore")
                    if "tradingview.com" in url and name in ("sessionid", "sessionid_sign"):
                        cookies[name] = value
            if cookies.get("sessionid"):
                logger.debug("found TV cookies in Safari")
                return cookies
        except Exception as e:
            logger.debug("could not read Safari cookies: %s", e)
        return None

    @staticmethod
    def _read_session_cookies() -> dict | None:
        """Search all available cookie stores for TradingView session cookies.
        Returns {"sessionid": ..., "sessionid_sign": ...} or None."""
        system = platform.system()

        # 1. TradingView desktop app (plaintext Chromium SQLite)
        cookie_path = TvDatafeed._DESKTOP_COOKIE_PATHS.get(system)
        if cookie_path and cookie_path.exists():
            try:
                conn = sqlite3.connect(f"file:{cookie_path}?mode=ro", uri=True)
                rows = conn.execute(
                    "SELECT name, value FROM cookies "
                    "WHERE host_key LIKE '%tradingview.com' AND name IN ('sessionid', 'sessionid_sign')"
                ).fetchall()
                conn.close()
                cookies = dict(rows)
                if cookies.get("sessionid"):
                    logger.debug("found TV cookies in desktop app")
                    return cookies
            except Exception as e:
                logger.debug("could not read desktop cookie DB: %s", e)

        # 2. Firefox (plaintext SQLite — may be locked, so copy first)
        ff_glob = TvDatafeed._FIREFOX_COOKIE_GLOBS.get(system, "")
        for db_path in sorted(globmod.glob(ff_glob), key=os.path.getmtime, reverse=True):
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite")
            tmp.close()
            try:
                shutil.copy2(db_path, tmp.name)
                conn = sqlite3.connect(f"file:{tmp.name}?mode=ro", uri=True)
                rows = conn.execute(
                    "SELECT name, value FROM moz_cookies "
                    "WHERE host LIKE '%tradingview.com' AND name IN ('sessionid', 'sessionid_sign')"
                ).fetchall()
                conn.close()
                cookies = dict(rows)
                if cookies.get("sessionid"):
                    logger.debug("found TV cookies in Firefox (%s)", db_path)
                    return cookies
            except Exception as e:
                logger.debug("could not read Firefox cookies from %s: %s", db_path, e)
            finally:
                os.unlink(tmp.name)

        # 3. Safari (macOS only — binary cookie format)
        safari_cookies = TvDatafeed._read_safari_cookies()
        if safari_cookies:
            return safari_cookies

        # 4. Chromium-based browsers (encrypted)
        browsers = TvDatafeed._CHROMIUM_BROWSERS.get(system, {})
        for browser_name, cookie_db_path in browsers.items():
            cookies = TvDatafeed._read_chromium_cookie_db(cookie_db_path)
            if cookies:
                logger.debug("found TV cookies in %s", browser_name)
                return cookies

        return None

    # ── Token recovery methods ────────────────────────────────────────

    def __try_recover_token_from_desktop(self):
        """Step 1: Auto-extract JWT from TradingView desktop app (with user permission)."""
        cookies = None
        cookie_path = TvDatafeed._DESKTOP_COOKIE_PATHS.get(platform.system())
        if cookie_path and cookie_path.exists():
            try:
                conn = sqlite3.connect(f"file:{cookie_path}?mode=ro", uri=True)
                rows = conn.execute(
                    "SELECT name, value FROM cookies "
                    "WHERE host_key LIKE '%tradingview.com' AND name IN ('sessionid', 'sessionid_sign')"
                ).fetchall()
                conn.close()
                cookies = dict(rows)
                if not cookies.get("sessionid"):
                    cookies = None
            except Exception:
                cookies = None

        if not cookies:
            return False

        jwt, pro_plan = self._cookies_to_jwt(cookies)
        if not jwt or jwt == self.token:
            return False

        plan_label = pro_plan if pro_plan else "free (non-pro)"
        print(
            f"\nFound active session in TradingView desktop app (plan: {plan_label}).\n"
            "Use this session to connect? [y/N] ", end="", flush=True
        )
        try:
            answer = input().strip().lower()
        except (EOFError, KeyboardInterrupt):
            answer = ""
        if answer in ("y", "yes"):
            self.token = jwt
            self.pro_plan = pro_plan
            self.__save_token(jwt)
            logger.info("using token from TradingView desktop app (plan: %s)", plan_label)
            return True
        return False

    def __try_recover_token_via_browser_login(self):
        """Step 2: Open a browser page that guides login and auto-detects when done."""
        import http.server
        import threading
        import webbrowser

        jwt_result = [None]  # mutable container for closure

        _HTML = """<!DOCTYPE html><html><head><meta charset="utf-8">
<title>TradingView Login</title>
<style>
  body{font-family:system-ui,sans-serif;display:flex;justify-content:center;
       align-items:center;min-height:100vh;margin:0;background:#1e222d}
  .card{background:#2a2e39;padding:40px;border-radius:12px;
        box-shadow:0 8px 32px rgba(0,0,0,.4);max-width:480px;width:100%;text-align:center}
  h2{color:#d1d4dc;margin:0 0 8px}
  .sub{color:#787b86;font-size:14px;margin-bottom:24px}
  .login-btn{display:inline-block;padding:14px 36px;font-size:16px;border:none;
         border-radius:8px;background:#2962ff;color:#fff;cursor:pointer;
         text-decoration:none;font-weight:600}
  .login-btn:hover{background:#1e53e5}
  .status{margin-top:24px;color:#787b86;font-size:14px}
  .spinner{display:inline-block;width:18px;height:18px;border:2px solid #434651;
           border-top-color:#2962ff;border-radius:50%;animation:spin 0.8s linear infinite;
           vertical-align:middle;margin-right:8px}
  @keyframes spin{to{transform:rotate(360deg)}}
  .success{color:#26a69a;font-size:18px;font-weight:600}
  .cancel{color:#787b86;font-size:13px;margin-top:16px;cursor:pointer;text-decoration:underline}
  .cancel:hover{color:#d1d4dc}
  .hidden{display:none}
</style></head><body><div class="card">
  <h2>TradingView Session Expired</h2>
  <p class="sub">Log in to TradingView to continue. Your session will be detected automatically.</p>
  <div id="step1">
    <a class="login-btn" href="https://www.tradingview.com/accounts/signin/" target="_blank"
       id="loginBtn">Log in to TradingView</a>
    <div class="status" id="waiting" style="display:none">
      <span class="spinner"></span> Waiting for login...
    </div>
    <div class="cancel" id="cancelBtn" style="display:none" onclick="doCancel()">Cancel</div>
  </div>
  <div id="step2" class="hidden">
    <div class="success">Login detected! You can close this tab.</div>
  </div>
</div><script>
var polling=false, timer=null;
document.getElementById('loginBtn').onclick=function(){
  if(!polling){
    polling=true;
    document.getElementById('waiting').style.display='block';
    document.getElementById('cancelBtn').style.display='block';
    poll();
  }
};
function poll(){
  fetch('/poll').then(r=>r.json()).then(function(d){
    if(d.status==='ok'){
      document.getElementById('step1').classList.add('hidden');
      document.getElementById('step2').classList.remove('hidden');
      setTimeout(function(){ fetch('/done'); }, 1000);
    } else if(polling){
      timer=setTimeout(poll, 3000);
    }
  }).catch(function(){ if(polling) timer=setTimeout(poll, 3000); });
}
function doCancel(){
  polling=false;
  if(timer) clearTimeout(timer);
  fetch('/cancel').then(function(){ window.close(); });
}
</script></body></html>"""

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/poll":
                    cookies = TvDatafeed._read_session_cookies()
                    jwt, pro_plan = TvDatafeed._cookies_to_jwt(cookies) if cookies else (None, "")
                    if jwt:
                        jwt_result[0] = (jwt, pro_plan)
                        self._json_response({"status": "ok"})
                    else:
                        self._json_response({"status": "waiting"})
                elif self.path == "/done":
                    self._json_response({"status": "ok"})
                    threading.Thread(target=self.server.shutdown, daemon=True).start()
                elif self.path == "/cancel":
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"ok")
                    threading.Thread(target=self.server.shutdown, daemon=True).start()
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html")
                    self.end_headers()
                    self.wfile.write(_HTML.encode())

            def _json_response(self, data):
                body = json.dumps(data).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                pass

        srv = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        port = srv.server_address[1]
        url = f"http://127.0.0.1:{port}"

        print(f"\nOpening browser — log in to TradingView to continue...\n  {url}\n")
        webbrowser.open(url)
        srv.serve_forever()
        srv.server_close()

        if jwt_result[0]:
            jwt, pro_plan = jwt_result[0]
            self.token = jwt
            self.pro_plan = pro_plan
            self.__save_token(jwt)
            plan_label = pro_plan if pro_plan else "free (non-pro)"
            logger.info("token recovered via browser login (plan: %s)", plan_label)
            return True
        return False

    # ── Auth check & connection ───────────────────────────────────────

    def __create_connection(self):
        logging.debug("creating websocket connection")
        self.ws = create_connection(
            "wss://data.tradingview.com/socket.io/websocket", headers=self.__ws_headers, timeout=self.__ws_timeout
        )

    def __check_auth(self):
        """Send auth token and verify it's accepted. Returns True if valid."""
        if getattr(self, '_auth_failed', False):
            return False
        assert self.ws is not None
        self.__send_message("set_auth_token", [self.token])
        for _ in range(3):
            try:
                result = self.ws.recv()
                if "protocol_error" in result:
                    packets = self.__parse_ws_packets(result)
                    for pkt in packets:
                        if isinstance(pkt, dict) and pkt.get("m") == "protocol_error":
                            err_msg = pkt.get("p", [""])[0] if pkt.get("p") else ""
                            logger.error("TradingView auth failed: %s", err_msg)
                            self.__delete_token()
                            if self.__try_recover_token_from_desktop() or self.__try_recover_token_via_browser_login():
                                self.ws.close()
                                self.__create_connection()
                                return self.__check_auth()
                            logger.error("could not recover auth token automatically")
                            self._auth_failed = True
                            return False
            except Exception:
                break
        return True

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

    # TradingView bar limits per subscription plan
    _PLAN_BAR_LIMITS = {
        "pro_premium": 20_000,
        "pro_plus": 10_000,
        "pro": 10_000,
        "": 5_000,  # free / nologin
    }

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
        chunk_days: int | None = None,  # Auto-calculated from account type & interval if not set
        sleep_seconds: int = 3,     # ← sleep between chunks to stay under rate limits
    ) -> pd.DataFrame:
        """
        If start_date/end_date are given → date-range mode with automatic chunking.
        Otherwise falls back to original n_bars behaviour.
        """
        plan_label = self.pro_plan if self.pro_plan else "free"
        max_bars = self._PLAN_BAR_LIMITS.get(self.pro_plan, 5_000)
        if self.token == "unauthorized_user_token":
            plan_label = "nologin"
        logger.info("account: %s | max bars/query: %s", plan_label, f"{max_bars:,}")

        if start_date is None and end_date is None:
            # ==================== ORIGINAL n_bars MODE (unchanged) ====================
            symbol = self.__format_symbol(symbol=symbol, exchange=exchange, contract=fut_contract)
            interval_val = interval.value

            self.__create_connection()
            if not self.__check_auth():
                return pd.DataFrame()
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
                    if "symbol_error" in result:  # ty:ignore[unsupported-operator]
                        logger.error("invalid symbol: %s — check exchange and symbol name on TradingView", symbol)
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

        # Clamp start_date if it exceeds TradingView's historical depth for this interval
        max_hist_days = _INTERVAL_MAX_DAYS.get(interval_val)
        if max_hist_days is not None:
            earliest_available = end_date - timedelta(days=max_hist_days)
            if start_date < earliest_available:
                logger.warning(
                    "TradingView only provides ~%d days of %s data — "
                    "clamping start_date from %s to %s",
                    max_hist_days, interval_val,
                    start_date.strftime("%Y-%m-%d"),
                    earliest_available.strftime("%Y-%m-%d"),
                )
                start_date = earliest_available

        # Auto-calculate chunk_days from account bar limit and interval
        if chunk_days is None:
            safe_bars = int(max_bars * 0.8)
            interval_secs = INTERVAL_SECONDS.get(interval_val, 86400)
            chunk_days = max(1, (safe_bars * interval_secs) // 86400)
            logger.info("auto chunk size: %d calendar days per chunk (based on %s safe bars limit, %s interval)",
                        chunk_days, f"{safe_bars:,}", interval_val)

        total_days = (end_date - start_date).days
        n_chunks = max(1, -(-total_days // chunk_days))  # ceiling division
        secs_per_chunk = 5 + sleep_seconds  # ~5s websocket overhead + sleep
        est_total = n_chunks * secs_per_chunk
        est_min, est_sec = divmod(est_total, 60)
        logger.info(
            "date range: %s → %s (%d calendar days, %d chunks × %d days/chunk) | est. time: %dm %ds",
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"),
            total_days, n_chunks, chunk_days, est_min, est_sec,
        )

        df_list = []
        current_start = start_date
        chunk_num = 0
        consecutive_empty = 0

        while current_start < end_date:
            current_end = min(current_start + timedelta(days=chunk_days), end_date)
            chunk_num += 1

            start_ts = int(current_start.timestamp() * 1000)   # milliseconds
            end_ts   = int(current_end.timestamp() * 1000)

            # optional 30-min buffer for intraday (as used in the original PR)
            if INTERVAL_SECONDS.get(interval_val, 60) < 86400:
                start_ts -= 1_800_000
                end_ts   -= 1_800_000

            range_str = f"r,{start_ts}:{end_ts}"
            logger.info("chunk %d/%d: %s → %s", chunk_num, n_chunks,
                        current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d"))

            df_chunk = pd.DataFrame()
            for attempt in range(1, 4):  # up to 3 attempts per chunk
                df_chunk = self._fetch_range(symbol_formatted, interval_val, range_str, extended_session)
                if not df_chunk.empty:
                    break
                if attempt < 3:
                    retry_delay = sleep_seconds * attempt
                    logger.warning(
                        "chunk %d/%d returned no data (attempt %d/3) — "
                        "retrying in %ds",
                        chunk_num, n_chunks, attempt, retry_delay,
                    )
                    time.sleep(retry_delay)

            if not df_chunk.empty:
                df_list.append(df_chunk)
                consecutive_empty = 0
            else:
                consecutive_empty += 1
                logger.warning("chunk %d/%d failed after 3 attempts", chunk_num, n_chunks)
                if consecutive_empty >= 3:
                    logger.warning(
                        "%d consecutive chunks failed — stopping (likely rate limited or "
                        "reached maximum available historical data for %s interval)",
                        consecutive_empty, interval_val,
                    )
                    break

            current_start = current_end
            time.sleep(sleep_seconds)   # rate-limit safety

        if df_list:
            df = pd.concat(df_list)
            df = df[~df.index.duplicated(keep='first')].sort_index()
            df = df[(df.index >= start_date) & (df.index <= end_date)]
            if not df.empty:
                # Calculate approximate trading days and expected bars for context
                interval_secs = INTERVAL_SECONDS.get(interval_val, 86400)
                trading_days_per_year = 252  # approximate
                total_calendar_days = (end_date - start_date).days
                est_trading_days = int(total_calendar_days * trading_days_per_year / 365)

                if interval_secs < 86400:  # intraday
                    # US market: 6.5 hours/day = 390 minutes
                    bars_per_day = int((6.5 * 3600) / interval_secs)
                    est_bars = est_trading_days * bars_per_day
                    logger.info("received %d bars (%s → %s) | est. %s trading days × %d bars/day ≈ %s expected (limited by account)",
                                len(df),
                                df.index[0].strftime("%Y-%m-%d"),
                                df.index[-1].strftime("%Y-%m-%d"),
                                f"{est_trading_days:,}", bars_per_day, f"{est_bars:,}")
                else:  # daily or higher
                    logger.info("received %d bars (%s → %s) | est. %s trading days expected",
                                len(df),
                                df.index[0].strftime("%Y-%m-%d"),
                                df.index[-1].strftime("%Y-%m-%d"),
                                f"{est_trading_days:,}")
            return df
        return pd.DataFrame()

    def _fetch_range(self, symbol: str, interval: str, range_str: str, extended_session: bool = False) -> pd.DataFrame:
        """Internal helper that fetches one chunk using the range request."""
        self.session = self.__generate_session()
        self.chart_session = self.__generate_chart_session()
        self.ws = create_connection(
            "wss://data.tradingview.com/socket.io/websocket",
            headers=self.__ws_headers, timeout=30,
        )
        if not self.__check_auth():
            return pd.DataFrame()
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

        max_bars = int(self._PLAN_BAR_LIMITS.get(self.pro_plan, 5_000) * 0.8)
        self.__send_message("create_series", [self.chart_session, "s1", "s1", "symbol_1", interval, max_bars, range_str])
        self.__send_message("switch_timezone", [self.chart_session, "exchange"])

        raw_data = ""
        logger.debug(f"fetching range {range_str} for {symbol}...")
        assert self.ws is not None
        while True:
            try:
                result = self.ws.recv()
                raw_data += result + "\n"  # ty:ignore[unsupported-operator]
                if "series_completed" in result:  # ty:ignore[unsupported-operator]
                    break
                if "symbol_error" in result:  # ty:ignore[unsupported-operator]
                    logger.error("invalid symbol: %s — check exchange and symbol name on TradingView", symbol)
                    break
            except Exception as e:
                logger.error(e)
                break
        self.ws.close()
        packets = self.__parse_ws_packets(raw_data)
        return self.__create_df(packets, symbol)


# Example usage for your request
if __name__ == "__main__":
    import argparse
    import os

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
    Sym="ES"
    Exch="CME_MINI"

    df = tv.get_hist(
        symbol=Sym,
        exchange=Exch,
        interval=Interval.in_daily,
        start_date=dt(2000, 1, 1),
        end_date=dt.now(),
        fut_contract=1,
        extended_session=True,
        sleep_seconds=3,
    )

    print(df)
    Path("data").mkdir(exist_ok=True)
    df.to_csv(f"data/{Sym}_{Exch}.csv")