# tvDatafeed — Project Specification

TradingView historical and live data downloader via unofficial websocket API. Downloads OHLCV data for any symbol on TradingView (stocks, forex, futures, crypto) with automatic authentication, date-range chunking, and multi-browser cookie extraction.

## Project Structure

```
tvdatafeed/
├── pyproject.toml              # Build config (hatchling), deps, pytest config
├── .python-version             # 3.13 (local dev preference)
├── .env.example                # TV_USERNAME, TV_PASSWORD
├── tvDatafeed/
│   ├── __init__.py             # Exports: TvDatafeed, Interval, Seis, TvDatafeedLive, Consumer
│   ├── main.py                 # Core: TvDatafeed class, Interval enum (~1075 lines)
│   ├── datafeed.py             # TvDatafeedLive: threaded live monitoring subclass
│   ├── seis.py                 # Seis: Symbol-Exchange-Interval-Set container
│   └── consumer.py             # Consumer: threaded callback processor for live data
└── tests/
    ├── conftest.py             # Fixtures: tv_nologin, tv_firefox, tv_safari, tv_chrome, tv_edge, tv_brave, tv_chromium, tv_cached_token, tv_desktop
    └── test_login_methods.py   # 27 tests across 9 login methods × 3 instruments (all date-range mode)
```

## Packaging

- **Build system**: `hatchling` (modern PEP 517)
- **Python**: `>=3.9` (uses `from __future__ import annotations` for `X | Y` union syntax compatibility)
- **Runtime deps**: `pandas>=1.0.5`, `websocket-client>=0.57.0`, `pyotp>=2.9.0`, `python-dotenv>=1.1.1`, `requests`
- **Test deps**: `pytest>=7.0`, `pytest-timeout>=2.0` (install via `pip install -e ".[test]"`)
- **Wheel packages**: `["tvDatafeed"]` (capital D)
- **Version**: defined in both `pyproject.toml` and `__init__.py` — keep in sync

## Core Architecture

### TradingView Websocket Protocol

- **Endpoint**: `wss://data.tradingview.com/socket.io/websocket`
- **Origin header**: `https://data.tradingview.com`
- **Frame format**: `~m~{length}~m~{json_payload}` — messages are length-prefixed
- **Frame splitting**: regex `~m~\d+~m~` splits raw data into individual JSON payloads
- **Heartbeat**: `~h~` frames are ignored
- **Session IDs**: `qs_` prefix (quote session) + `cs_` prefix (chart session), each with 12 random lowercase chars

### Message Sequence (n_bars mode)

1. `set_auth_token` — send JWT token
2. `chart_create_session` — create chart session
3. `quote_create_session` — create quote session
4. `quote_set_fields` — request field types (ch, chp, description, lp, volume, etc.)
5. `quote_add_symbols` — add symbol with `force_permission` flag
6. `quote_fast_symbols` — enable fast updates
7. `resolve_symbol` — resolve with adjustment=splits, session=regular/extended
8. `create_series` — `[chart_session, "s1", "s1", "symbol_1", interval, n_bars]`
9. `switch_timezone` — set to exchange timezone
10. Read until `series_completed` or `symbol_error`

### Message Sequence (date-range mode)

Same as above except `create_series` has 7 parameters:
```
[chart_session, "s1", "s1", "symbol_1", interval, max_bars, range_str]
```
Where `range_str = "r,{start_ms}:{end_ms}"` (milliseconds).

### Data Parsing

- Response packets with `m: "timescale_update"` or `m: "du"` contain bar data
- Bar array path: `packet["p"][1]["s1"]["s"]` (or `"sds_1"` key)
- Each bar: `{"v": [timestamp, open, high, low, close, volume, OI?]}`
- OI (open interest) is optional 7th element, present for futures
- Result DataFrame: columns = `[symbol, open, high, low, close, volume]` + optional `OI`, index = `datetime`

## Authentication

### Auth Priority Order

1. **Explicit token** — `TvDatafeed(token="jwt_string")`
2. **Cached token** — reads `~/.tvdatafeed/token` file
3. **Username/password** — POST to `tradingview.com/accounts/signin/`
4. **Nologin** — uses `"unauthorized_user_token"` (limited data access)

### Token Recovery Flow (on `protocol_error`)

When auth fails at websocket level:
1. **Desktop app auto-extract** — reads TradingView desktop app cookies, converts to JWT via `_cookies_to_jwt()`, prompts user `[y/N]`
2. **Browser login popup** — opens local HTTP server, shows "Log in to TradingView" button linking to tradingview.com/accounts/signin/, polls `_read_session_cookies()` every 3 seconds until login detected

### 2FA Support

When signin returns `code: "2FA_required"`:
- Opens local HTTP server with styled input page for TOTP/SMS/email code
- Supports: `totp` (via pyotp), `sms`, `email`
- User enters code in browser, submitted via fetch to local server

### Subscription Plan Bar Limits

```python
_PLAN_BAR_LIMITS = {
    "pro_premium": 20_000,
    "pro_plus": 10_000,
    "pro": 10_000,
    "": 5_000,  # free / nologin
}
```

## Cookie Extraction

### Cookie-to-JWT Conversion

`_cookies_to_jwt(cookies)` sends `sessionid`/`sessionid_sign` cookies to `https://www.tradingview.com/`, scrapes `auth_token` and `pro_plan` from HTML response via regex. Returns `(jwt, pro_plan)`.

### Cookie Sources (search order in `_read_session_cookies`)

1. **TradingView Desktop App** — plaintext Chromium SQLite, `cookies` table with `host_key LIKE '%tradingview.com'`
2. **Firefox** — plaintext SQLite in `moz_cookies` table, copies to temp file first (avoids lock)
3. **Safari** — macOS only, binary `Cookies.binarycookies` format parsed with `struct`
4. **Chromium browsers** — Chrome, Edge, Brave, Chromium (encrypted cookies)

### Cookie Store Paths

**Desktop App** (`_DESKTOP_COOKIE_PATHS`):
- macOS: `~/Library/Application Support/TradingView/Cookies`
- Linux: `~/.config/TradingView/Cookies`
- Windows: `~/AppData/Roaming/TradingView/Cookies`

**Firefox** (`_FIREFOX_COOKIE_GLOBS`):
- macOS: `~/Library/Application Support/Firefox/Profiles/*/cookies.sqlite`
- Linux: `~/.mozilla/firefox/*/cookies.sqlite`
- Windows: `~/AppData/Roaming/Mozilla/Firefox/Profiles/*/cookies.sqlite`

**Chromium** (`_CHROMIUM_BROWSERS`):
- Chrome, Edge, Brave, Chromium, Arc (macOS/Linux/Windows paths)
- Note: Arc is in the codebase but excluded from tests (only main browsers tested)

### Chromium Cookie Decryption (`_decrypt_chromium_cookie`)

- **macOS**: Keychain password → PBKDF2 (SHA1, salt=`saltysalt`, iterations=1003, 16-byte key) → AES-128-CBC (IV=16 spaces). Tries "Chrome Safe Storage" first, then Chromium/Edge/Brave/Arc. Uses `openssl enc` CLI for decryption.
- **Windows**: DPAPI via `ctypes.windll.crypt32.CryptUnprotectData`
- **Linux**: hardcoded password `peanuts` → PBKDF2 (SHA1, salt=`saltysalt`, iterations=1, 16-byte key) → AES-128-CBC

### Safari Binary Cookie Parsing (`_read_safari_cookies`)

Parses `~/Library/Cookies/Cookies.binarycookies`:
- Magic: `cook` (4 bytes)
- Page count: big-endian uint32
- Page sizes: big-endian uint32 array
- Each page: magic `\x00\x00\x01\x00`, cookie count, cookie offsets
- Each cookie: size, URL offset, name offset, value offset — parsed with `struct.unpack`

## Date-Range Mode & Chunking

### `get_hist()` Two Modes

- **n_bars mode**: `get_hist(symbol, exchange, interval, n_bars=10)` — original behavior, single websocket request
- **Date-range mode**: `get_hist(symbol, exchange, interval, start_date=..., end_date=...)` — automatic chunking

### Auto Chunk Calculation

```python
safe_bars = int(max_bars * 0.8)  # 80% of plan limit
chunk_days = max(1, (safe_bars * interval_seconds) // 86400)
```

Example: pro_premium (20k bars) at daily = 16,000 chunk_days; at 15min = 166 chunk_days.

### Chunking Logic

1. Split `[start_date, end_date]` into chunks of `chunk_days` calendar days
2. For each chunk: create fresh websocket connection with new session IDs
3. Build range string: `r,{start_ms}:{end_ms}` (30-min buffer subtracted for intraday)
4. Sleep `sleep_seconds` (default 3) between chunks for rate limiting
5. Concatenate all chunks, deduplicate by index, sort, filter to requested range

### `_fetch_range()` Method

Creates its own websocket connection per chunk with 30-second timeout (vs 5s for n_bars). Uses `create_series` with 7 params including range string. Returns DataFrame for that chunk.

## Symbol Formatting

- Standard: `EXCHANGE:SYMBOL` (e.g., `NASDAQ:AAPL`)
- Continuous futures: `EXCHANGE:SYMBOL{N}!` where N = contract number (e.g., `CME_MINI:ES1!` for front month)
- If symbol already contains `:`, used as-is

## Live Data (TvDatafeedLive)

### Class Hierarchy

`TvDatafeedLive(TvDatafeed)` — adds threaded live monitoring via `Seis` and `Consumer` objects.

### Seis (Symbol-Exchange-Interval Set)

- Immutable container for `(symbol, exchange, interval)` triple
- Holds list of `Consumer` instances
- Tracks last update datetime for new-data detection
- Equality based on symbol+exchange+interval

### Consumer (Threaded Callback)

- `threading.Thread` subclass with internal `queue.Queue` buffer
- Receives bar data via `put(data)`, calls `callback(seis, data)` in its thread
- `put(None)` signals shutdown
- Auto-removes itself from Seis on callback exception

### _SeisesAndTrigger (Internal Dict)

- Groups Seis objects by interval value
- Manages expiry datetime per interval group
- `wait()` blocks until next interval expires (interruptible via threading.Event)
- `get_expired()` returns list of expired intervals and advances their next-trigger time

### Main Loop

`_main_loop()` runs in a dedicated thread:
1. `_sat.wait()` blocks until an interval expires
2. For each expired interval, for each Seis in that group:
   - Fetch 2 bars via `get_hist(n_bars=2)` — bar[0] is latest closed, bar[1] is current open
   - Check `seis.is_new_data()` — compare datetime to last seen
   - If new: drop the unclosed bar, push to all consumers
   - Retry up to `RETRY_LIMIT=50` times on failure
3. Loop until `_sat.quit()` called

### Thread Safety

All public methods acquire `self._lock` (threading.Lock) before modifying shared state. `get_hist()` in live mode also acquires the lock to avoid concurrent websocket use.

## Token Caching

- Path: `~/.tvdatafeed/token` (created with `parents=True, exist_ok=True`)
- Written on: successful login, token recovery, explicit token parameter
- Read on: `__auth()` before attempting username/password login
- Deleted on: `protocol_error` (invalid/expired token)

## Rate Limiting

- Username/password login: exponential backoff starting at 30s, doubles each retry, max 10 retries
- CAPTCHA detection: returns `code: "recaptcha_required"` — logs instructions to use token instead
- Date-range chunks: configurable `sleep_seconds` between chunks (default 3s)

## Tests

### Test Framework

- pytest with `pytest-timeout` (global 300s, per-test overrides)
- All tests use real TradingView connections (no mocks)
- Tests skip gracefully when browser/credentials unavailable

### Test Order

1. **TestNologin** (3 tests) — no credentials, skip if empty result
2. **TestFirefox** (3 tests) — Firefox cookies
3. **TestSafari** (3 tests) — Safari cookies (macOS only)
4. **TestChrome** (3 tests) — Chrome cookies
5. **TestEdge** (3 tests) — Edge cookies
6. **TestBrave** (3 tests) — Brave cookies
7. **TestChromium** (3 tests) — Chromium cookies
8. **TestCachedToken** (3 tests) — `~/.tvdatafeed/token`
9. **TestDesktopApp** (4 tests) — Desktop app + 1 date-range chunking test

### Test Instruments (per login method)

All tests use date-range mode to exercise the chunking logic.

| Instrument | Symbol | Exchange | Interval | Duration | min_bars | Notes |
|------------|--------|----------|----------|----------|----------|-------|
| AAPL | AAPL | NASDAQ | 15min | 3 years | 5000 (1000 nologin) | Intraday chunking |
| AUD/EUR | AUDEUR | FX_IDC | 5min | 3 years | 10000 (1000 nologin) | Forex, most chunks |
| E-mini S&P | ES | CME_MINI | 1D | 5 years | 1000 | fut_contract=1, daily chunking |

### Validation Helpers

- `_assert_valid_df()` — columns present, OHLC not NaN, datetime index sorted, symbol match
- `_assert_daterange_df()` — covers requested range within 7-day tolerance, span >= 85% of requested, min bar count
- `_get_hist_with_retry()` — retries on empty DataFrame (transient connection drops)

### Fixture Skip Messages (3-level detection)

- Browser not installed: `"{Browser} is not installed"`
- Browser installed, no TV session: `"{Browser} is installed but has no TradingView session — log in to TradingView in {Browser}"`
- Cookies found but expired: `"{Browser} has TradingView cookies but session is expired — log in to TradingView in {Browser} to refresh"`

## Running

```bash
# Install
pip install -e ".[test]"

# Run tests
pytest tests/ -v --timeout=300

# CLI usage
python -m tvDatafeed --token YOUR_TOKEN

# Python usage
from tvDatafeed import TvDatafeed, Interval
tv = TvDatafeed()  # nologin
tv = TvDatafeed(token="your_sessionid")  # with token
df = tv.get_hist("AAPL", "NASDAQ", Interval.in_daily, n_bars=100)
df = tv.get_hist("ES", "CME_MINI", Interval.in_daily,
                 start_date="2022-01-01", end_date="2025-01-01",
                 fut_contract=1, chunk_days=365)
```

## Environment Variables

- `TV_USERNAME` — TradingView username (for password login)
- `TV_PASSWORD` — TradingView password
- `TV_TOKEN` — TradingView sessionid cookie (preferred over username/password)
