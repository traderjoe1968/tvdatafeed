"""Shared fixtures for tvDatafeed login method tests."""
from __future__ import annotations

import http.server
import json
import logging
import platform
import threading
import webbrowser
from pathlib import Path

import pytest

from tvDatafeed.main import TvDatafeed

logger = logging.getLogger("tvdatafeed.tests")

# Estimated seconds per test, keyed by substrings in the test node ID.
# Order matters: first match wins.
_ESTIMATES = [
    ("5min_1yr",  "~90s  (intraday chunks, 1 year)"),
    ("15min_2yr", "~120s (intraday chunks, 2 years)"),
    ("daily_5yr", "~15s  (single daily chunk)"),
    ("daily_3yr", "~10s  (single daily chunk)"),
]


def pytest_runtest_call(item: pytest.Item) -> None:
    """Log the estimated duration at the start of each test's call phase."""
    name = item.nodeid
    for pattern, estimate in _ESTIMATES:
        if pattern in name:
            logger.info("[est. %s]", estimate)
            return
    logger.info("[est. unknown]")


# ── Helper: build a TvDatafeed from raw cookies ─────────────────────

def _recover_via_browser_login(source: str) -> tuple[str, str] | None:
    """Open TradingView login page, poll for fresh cookies, return (jwt, pro_plan) or None.

    Mirrors TvDatafeed.__try_recover_token_via_browser_login but works
    standalone (no TvDatafeed instance needed).
    """
    jwt_result = [None]

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
  <h2>SESSION_TITLE</h2>
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
</script></body></html>""".replace("SESSION_TITLE", f"{source} Session Expired")

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

        def log_message(self, fmt, *args):
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    port = srv.server_address[1]
    url = f"http://127.0.0.1:{port}"

    logger.warning(
        "%s cookies expired — opening browser for login recovery at %s",
        source, url,
    )
    webbrowser.open(url)
    srv.serve_forever()
    srv.server_close()

    return jwt_result[0]


def _tv_from_cookies(cookies: dict, source: str) -> TvDatafeed:
    """Convert cookies → JWT → TvDatafeed.

    If the session is expired, opens a browser login page for interactive
    recovery instead of skipping.  This opportunistically exercises the
    real TradingView login flow — including 2FA if the account requires it.
    The recovered token is cached to ~/.tvdatafeed/token so subsequent
    test fixtures (e.g. TestCachedToken) benefit automatically.
    Skips only if the user cancels recovery.
    """
    jwt, pro_plan = TvDatafeed._cookies_to_jwt(cookies)
    if not jwt:
        result = _recover_via_browser_login(source)
        if result is None:
            pytest.skip(
                f"{source} has TradingView cookies but session is expired — "
                f"recovery was cancelled"
            )
        jwt, pro_plan = result
        # Cache the recovered token so TestCachedToken and later fixtures
        # can reuse it without another login prompt.
        token_path = Path.home() / ".tvdatafeed" / "token"
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(jwt)
        logger.info(
            "recovered %s session via browser login (plan: %s) — "
            "token cached to %s",
            source, pro_plan or "free", token_path,
        )
    tv = TvDatafeed(token=jwt)
    tv.pro_plan = pro_plan
    return tv


# ══════════════════════════════════════════════════════════════════════
#  Nologin
# ══════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="session")
def tv_nologin():
    """TvDatafeed with no credentials (limited access)."""
    return TvDatafeed()


# ══════════════════════════════════════════════════════════════════════
#  Cached token
# ══════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="session")
def tv_cached_token():
    """TvDatafeed from cached token file (~/.tvdatafeed/token)."""
    token_path = Path.home() / ".tvdatafeed" / "token"
    if not token_path.exists():
        pytest.skip("no cached token at ~/.tvdatafeed/token")
    token = token_path.read_text().strip()
    if not token or token == "unauthorized_user_token":
        pytest.skip("cached token is empty or unauthorized")
    return TvDatafeed(token=token)


# ══════════════════════════════════════════════════════════════════════
#  Browser-specific cookie fixtures
# ══════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="session")
def tv_firefox():
    """TvDatafeed via Firefox cookies."""
    import glob as globmod
    import os
    import shutil
    import sqlite3
    import tempfile

    system = platform.system()
    ff_glob = TvDatafeed._FIREFOX_COOKIE_GLOBS.get(system, "")
    profiles = sorted(globmod.glob(ff_glob), key=os.path.getmtime, reverse=True)
    if not profiles:
        pytest.skip("Firefox is not installed")

    for db_path in profiles:
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
                os.unlink(tmp.name)
                return _tv_from_cookies(cookies, "Firefox")
        except Exception:
            pass
        finally:
            if Path(tmp.name).exists():
                os.unlink(tmp.name)
    pytest.skip("Firefox is installed but has no TradingView session — log in to TradingView in Firefox")


@pytest.fixture(scope="session")
def tv_safari():
    """TvDatafeed via Safari cookies (macOS only)."""
    if platform.system() != "Darwin":
        pytest.skip("Safari is only available on macOS")
    cookie_file = Path.home() / "Library" / "Cookies" / "Cookies.binarycookies"
    if not cookie_file.exists():
        pytest.skip("Safari cookie store not found")
    cookies = TvDatafeed._read_safari_cookies()
    if not cookies:
        pytest.skip("Safari has no TradingView session — log in to TradingView in Safari")
    return _tv_from_cookies(cookies, "Safari")


def _chromium_fixture(browser_name: str):
    """Factory for Chromium-based browser fixtures."""
    @pytest.fixture(scope="session")
    def _fixture():
        system = platform.system()
        browsers = TvDatafeed._CHROMIUM_BROWSERS.get(system, {})
        cookie_path = browsers.get(browser_name)
        if not cookie_path:
            pytest.skip(f"{browser_name} is not supported on {system}")
        if not cookie_path.exists():
            pytest.skip(f"{browser_name} is not installed")
        cookies = TvDatafeed._read_chromium_cookie_db(cookie_path)
        if not cookies:
            pytest.skip(
                f"{browser_name} is installed but has no TradingView session — "
                f"log in to TradingView in {browser_name}"
            )
        return _tv_from_cookies(cookies, browser_name)
    _fixture.__name__ = f"tv_{browser_name.lower()}"
    _fixture.__doc__ = f"TvDatafeed via {browser_name} cookies."
    return _fixture


tv_chrome = _chromium_fixture("Chrome")
tv_edge = _chromium_fixture("Edge")
tv_brave = _chromium_fixture("Brave")
tv_chromium = _chromium_fixture("Chromium")


# ══════════════════════════════════════════════════════════════════════
#  TradingView Desktop App
# ══════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="session")
def tv_desktop():
    """TvDatafeed via TradingView desktop app cookies."""
    import sqlite3

    system = platform.system()
    cookie_path = TvDatafeed._DESKTOP_COOKIE_PATHS.get(system)
    if not cookie_path or not cookie_path.exists():
        pytest.skip("TradingView desktop app is not installed")
    try:
        conn = sqlite3.connect(f"file:{cookie_path}?mode=ro", uri=True)
        rows = conn.execute(
            "SELECT name, value FROM cookies "
            "WHERE host_key LIKE '%tradingview.com' AND name IN ('sessionid', 'sessionid_sign')"
        ).fetchall()
        conn.close()
        cookies = dict(rows)
        if not cookies.get("sessionid"):
            pytest.skip(
                "TradingView desktop app is installed but has no active session — "
                "log in to the desktop app"
            )
        return _tv_from_cookies(cookies, "Desktop App")
    except Exception as e:
        pytest.skip(f"could not read desktop app cookies: {e}")
