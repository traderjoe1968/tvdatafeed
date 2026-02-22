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
│   ├── main.py                 # Core: TvDatafeed class, Interval enum
│   ├── datafeed.py             # TvDatafeedLive: threaded live monitoring subclass
│   ├── seis.py                 # Seis: Symbol-Exchange-Interval-Set container
│   └── consumer.py             # Consumer: threaded callback processor for live data
└── tests/
    ├── conftest.py             # Fixtures: tv_nologin, tv_firefox, tv_safari, tv_chrome, tv_edge, tv_brave, tv_chromium, tv_cached_token, tv_desktop
    ├── test_login_methods.py   # Auth + data download tests (Nologin, Browsers, CachedToken, Desktop)
    └── test_security_info.py   # TOML unit tests + get_security_info() integration tests
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
- Result DataFrame: columns = `[open, high, low, close, volume]` + optional `OI`, index = `datetime` (no `symbol` column)

## Authentication

### Auth Priority Order

1. **Explicit token** — `TvDatafeed(token="jwt_string")`
2. **Cached token** — reads `~/.tvdatafeed/token` file
3. **Username/password** — POST to `tradingview.com/accounts/signin/`
4. **Nologin** — uses `"unauthorized_user_token"` (limited data access)

### Token Recovery Flow (on `protocol_error`)

When auth fails at websocket level:
1. **File-cache check** — if `~/.tvdatafeed/token` contains a *different* token from the one that failed (written by another fixture/instance), retry silently with that token
2. **Desktop app auto-extract** — reads TradingView desktop app cookies, converts to JWT via `_cookies_to_jwt()`, prompts user `[y/N]`
3. **Browser login popup** — opens local HTTP server, shows "Log in to TradingView" button linking to tradingview.com/accounts/signin/, polls `_read_session_cookies()` every 3 seconds until login detected

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

- **macOS**: Module-level `_chromium_key_cache: dict[str, bytes]` caches the derived AES key by Keychain service name — the macOS authorization dialog fires at most once per process. On cache miss: `security find-generic-password` → PBKDF2 (SHA1, salt=`saltysalt`, iterations=1003, 16-byte key) → AES-128-CBC (IV=16 spaces). Tries Chrome/Chromium/Edge/Brave/Arc Keychain entries in order.
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
safe_bars = int(max_bars * 0.99)  # 99% of plan limit
chunk_days = max(1, (safe_bars * interval_seconds) // 86400)
```

Example: pro_premium (20k bars) at daily = ~19,800 chunk_days; free (5k bars) at 15min = ~51 chunk_days.

### Chunking Logic

1. Split `[start_date, end_date]` into chunks of `chunk_days` calendar days
2. For each chunk: create fresh websocket connection with new session IDs
3. Build range string: `r,{start_ms}:{end_ms}` (30-min buffer subtracted for intraday)
4. Sleep `sleep_seconds` (default 3) between chunks for rate limiting
5. Concatenate all chunks, deduplicate by index, sort, filter to requested range
6. Progress log: `"date range: YYYY-MM-DD → YYYY-MM-DD (N calendar days, N chunks)"`

### `_fetch_range()` Method

Creates its own websocket connection per chunk with 30-second timeout (vs 5s for n_bars). Uses `create_series` with 7 params including range string. Returns DataFrame for that chunk.

## Symbol Formatting

- Standard: `EXCHANGE:SYMBOL` (e.g., `NASDAQ:AAPL`)
- Continuous futures: `EXCHANGE:SYMBOL{N}!` where N = contract number (e.g., `CME_MINI:ES1!` for front month)
- If symbol already contains `:`, used as-is

## Security Info (`get_security_info`)

### Purpose

Fetches TradingView symbol metadata (description, type, tick_size, point_value, etc.) via WebSocket and optionally caches results to a TOML file.

### Signature

```python
get_security_info(
    symbol: str,
    exchange: str = "NSE",
    fut_contract: int | None = None,
    toml_path: str | Path | None = None,
) -> dict
```

Returns a curated dict of metadata fields. Returns `{}` on invalid symbol or connection failure.

### Curated Fields

| Field | Source | Notes |
|-------|--------|-------|
| `symbol` | formatted | e.g. `"CBOT:ZC1!"` |
| `description` | symbol_resolved | human name |
| `type` | symbol_resolved | `"futures"`, `"stock"`, etc. |
| `listed_exchange` | symbol_resolved | primary listing exchange |
| `exchange` | symbol_resolved | display exchange |
| `currency_code` | symbol_resolved | e.g. `"USD"` |
| `current_contract` | symbol_resolved | futures only, e.g. `"ZCH2026"` |
| `sector` / `industry` | symbol_resolved | stocks only |
| `isin` / `cusip` / `figi` / `url` | symbol_resolved | when available |
| `point_value` | `pointvalue` field | value of 1 full point |
| `tick_size` | `minmov / pricescale` | minimum price increment |

### Message Sequence

1. Same auth/session setup as n_bars mode
2. `resolve_symbol` — resolve with `adjustment=splits`
3. `create_series` with 2 bars — **required** to trigger `symbol_resolved` packet
4. Read until `series_completed` or `symbol_error`
5. Parse `symbol_resolved` packet (`p[2]` dict) for metadata

### TOML Helpers (module-level)

- `_toml_value(v) -> str` — format Python value as TOML literal (handles None, bool, int, float, str, list)
- `_toml_read(path) -> dict` — parse TOML file; uses `tomllib` (3.11+) or a simple fallback; returns `{}` if file missing
- `_toml_append(path, section, data) -> None` — appends a new `[section]` block; skips silently if section already present (no-duplicate guarantee)

### TOML Section Key Format

Symbol is converted to a filesystem-safe key: `EXCHANGE:SYMBOL!` → `SYMBOL_EXCHANGE`

Examples:
- `CBOT:ZC1!` → `ZC1_CBOT`
- `NASDAQ:AAPL` → `AAPL_NASDAQ`
- `CME_MINI:ES1!` → `ES1_CME_MINI`

Keys in this format never contain special characters so TOML quoting is not required.

### Cache-on-Read

If `toml_path` is provided and the derived key already exists in the file, `get_security_info` returns the cached dict immediately without opening a WebSocket connection.

### Example TOML Output

```toml
[ZC1_CBOT]
symbol = "CBOT:ZC1!"
description = "Corn Futures"
type = "futures"
listed_exchange = "CBOT"
exchange = "CBOT"
currency_code = "USD"
current_contract = "ZCH2026"
point_value = 50.0
tick_size = 0.25

[AAPL_NASDAQ]
symbol = "NASDAQ:AAPL"
description = "Apple Inc."
type = "stock"
listed_exchange = "NASDAQ"
exchange = "Cboe One"
currency_code = "USD"
sector = "Electronic Technology"
industry = "Telecommunications Equipment"
isin = "US0378331005"
cusip = "037833100"
figi = "BBG000B9XRY4"
url = "apple.com"
point_value = 1.0
tick_size = 0.01
```

### Tests (`tests/test_security_info.py`)

| Class | Tests | Network |
|-------|-------|---------|
| `TestTomlHelpers` | value types, missing file, round-trip, no-duplicate, multiple sections | No |
| `TestSecurityInfo` | ZC1! futures fields, AAPL stock fields, TOML write both, no-duplicate on 2nd call | Yes |

Integration tests use a `_fetch` retry wrapper (3 attempts, 5s × attempt sleep) to handle transient connection drops.

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
- Deleted on: `protocol_error` (invalid/expired token) — but only after checking for a fresher token in the file first

## Rate Limiting

- Username/password login: exponential backoff starting at 30s, doubles each retry, max 10 retries
- CAPTCHA detection: returns `code: "recaptcha_required"` — logs instructions to use token instead
- Date-range chunks: configurable `sleep_seconds` between chunks (default 3s)

## Tests

### Test Framework

- pytest with `pytest-timeout` (global 300s, per-test overrides)
- All tests use real TradingView connections (no mocks)
- Tests skip gracefully when browser/credentials unavailable

### Test Structure

**`test_login_methods.py`** — split into 4 groups:

1. **TestNologin** (3 tests) — AAPL daily 3yr, AUDEUR daily 3yr, ES daily 18yr; skip if empty
2. **Browser classes** × 7 (Firefox, Safari, Chrome, Edge, Brave, Chromium, Desktop) — **1 test each**: `test_auth_works` fetches 10 bars of AAPL daily to confirm JWT extraction works. No data downloads.
3. **TestCachedToken** (9 tests) — all data downloads + CSV saves + backadjusted tests:
   - `test_aapl_daily_3yr` → `AAPL_NASDAQ_1D_3yr.csv`
   - `test_audeur_daily_3yr` → `AUDEUR_FX_IDC_1D_3yr.csv`
   - `test_aapl_15min_2yr` → `AAPL_NASDAQ_15min_2yr.csv`
   - `test_audeur_5min_1yr` → `AUDEUR_FX_IDC_5min_1yr.csv`
   - `test_es_daily_18yr` → `ES1_CME_MINI_1D_18yr.csv`
   - `test_zc1_not_backadjusted` → `ZC1_CBOT_1D_5yr.csv`
   - `test_zc1_backadjusted` → `ZC1_CBOT_1D_5yr_badj.csv` (skips if B-ADJ unsupported)
   - `test_zc1_badj_prices_differ` — loads raw from CSV if exists, downloads adj, compares
   - `test_aapl_backadjusted_flag_ignored` — verifies warning logged for non-futures symbol
4. **TestDesktopApp** (1 test) — `test_auth_works`, same pattern as browser classes

**`test_security_info.py`**:
1. **TestTomlHelpers** (5 tests) — pure-Python, no network
2. **TestSecurityInfo** (4 tests) — real WebSocket, uses `tv_cached_token` fixture

### Validation Helpers (`test_login_methods.py`)

- `_assert_valid_df(df)` — columns present, OHLC not NaN, datetime index sorted
- `_assert_daterange_df(df, start_date, end_date, min_bars)` — 70% bar-count tolerance, last bar within 7 days of end_date, start-date check only when coverage ≥ 99%
- `_get_hist_with_retry(tv, retries=2, delay=5, **kwargs)` — retries on empty DataFrame

### Conftest Token Recovery (3-tier)

When browser cookies are expired, `_tv_from_cookies()` tries in order:
1. In-memory `_jwt_memory_cache` (another fixture already recovered this run)
2. File cache `~/.tvdatafeed/token` (from a previous run)
3. Browser login popup (fires once, result cached for all subsequent fixtures)

### Fixture Skip Messages (3-level detection)

- Browser not installed: `"{Browser} is not installed"`
- Browser installed, no TV session: `"{Browser} is installed but has no TradingView session — log in to TradingView in {Browser}"`
- Cookies found but expired: `"{Browser} has TradingView cookies but session is expired — recovery was cancelled"`

## Running

```bash
# Install
pip install -e ".[test]"

# Run tests
pytest tests/ -v --timeout=300

# Python usage
from tvDatafeed import TvDatafeed, Interval
tv = TvDatafeed()                          # nologin
tv = TvDatafeed(token="your_jwt_token")   # with cached token

# n_bars mode
df = tv.get_hist("AAPL", "NASDAQ", Interval.in_daily, n_bars=100)

# Date-range mode (automatic chunking)
df = tv.get_hist("ES", "CME_MINI", Interval.in_daily,
                 start_date="2022-01-01", end_date="2025-01-01",
                 fut_contract=1)

# Back-adjusted continuous futures
df = tv.get_hist("ZC", "CBOT", Interval.in_daily,
                 start_date="2020-01-01", end_date="2025-01-01",
                 fut_contract=1, backadjusted=True)

# Security info (fetches metadata, caches to TOML)
info = tv.get_security_info("AAPL", "NASDAQ", toml_path="security_info.toml")
info = tv.get_security_info("ES", "CME_MINI", fut_contract=1, toml_path="security_info.toml")
```

## Environment Variables

- `TV_USERNAME` — TradingView username (for password login)
- `TV_PASSWORD` — TradingView password
- `TV_TOKEN` — TradingView sessionid cookie (preferred over username/password)
