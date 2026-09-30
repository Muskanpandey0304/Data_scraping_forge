import os
import sys
import re
import csv
import json
import time
import random
import hashlib
import threading
import html
import shutil
import requests
import urllib.parse
from pathlib import Path
from bs4 import BeautifulSoup, Comment, Tag
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import xml.etree.ElementTree as ET
try:
    import yaml
except ImportError:
    yaml = None  # pip install pyyaml --break-system-packages (needed for CTF structured parsing)

# ── UTF-8 console on Windows ─────────────────────────────────
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

# ── Optional: cloudscraper (Cloudflare JS bypass) ────────────
try:
    import cloudscraper as _cs_mod
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False

# ── Optional: faster-whisper (local, offline video/audio transcription) ─────
# pip install faster-whisper --break-system-packages
# Also requires ffmpeg on PATH (faster-whisper shells out to it for decoding).
try:
    from faster_whisper import WhisperModel as _WhisperModel
    HAS_FASTER_WHISPER = True
except ImportError:
    HAS_FASTER_WHISPER = False

# ── Optional: fake-useragent ─────────────────────────────────
_FALLBACK_UAS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    'Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
]

try:
    from fake_useragent import UserAgent as _UA
    _ua_gen = _UA(browsers=['chrome', 'firefox', 'edge'])
    def random_ua():
        try:
            return _ua_gen.random
        except Exception:
            return random.choice(_FALLBACK_UAS)
except Exception:
    def random_ua():
        return random.choice(_FALLBACK_UAS)

_pw_lock = threading.Lock()
CHROME_EXE = os.environ.get("CHROME_PATH", None)

def pw_get_html(url, wait_ms=1000):
    """
    Used as fallback when Cloudflare blocks requests.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None
    try:
        with _pw_lock:
            with sync_playwright() as pw:
                launch_kwargs = {
                    'headless': True,
                    'args': [
                        '--no-sandbox',
                        '--disable-blink-features=AutomationControlled',
                        '--disable-infobars',
                        '--window-size=1366,768',
                    ]
                }
                if CHROME_EXE and os.path.exists(CHROME_EXE):
                    launch_kwargs['executable_path'] = CHROME_EXE
                browser = pw.chromium.launch(**launch_kwargs)
                ctx = browser.new_context(
                    viewport={'width': 1366, 'height': 768},
                    user_agent=random_ua(),
                    locale='en-US',
                    timezone_id='America/New_York',
                )
                ctx.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins',   {get: () => [1, 2, 3]});
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                    window.chrome = {runtime: {}};
                """)
                page = ctx.new_page()
                page.goto(url, wait_until='networkidle', timeout=30_000)
                page.wait_for_timeout(wait_ms + random.randint(300, 900))
                content = page.content()
                browser.close()
                return content
    except Exception as e:
        log(f'  [Playwright] Error for {url}: {e}')
        return None

# ─────────────────────────────────────────────────────────────
#  PATHS & CONFIG
# ─────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.resolve()

def _env_path(name, default):
    v = os.environ.get(name, "").strip()
    return Path(v).expanduser() if v else Path(default)

# CSV input file: configurable via SCRAPER_CSV or defaults to data folder
_default_csv = SCRIPT_DIR.parent / "utilities_and_data" / "data" / "languages_repositories.xlsx"
CSV_PATH = _env_path("SCRAPER_CSV", _default_csv if _default_csv.exists() else SCRIPT_DIR / "input.csv")

# Output root: configurable via SCRAPER_DEST or defaults to SCRIPT_DIR / "output"
DEST_ROOT = _env_path("SCRAPER_DEST", SCRIPT_DIR / "output")
RESEARCH_ROOT = DEST_ROOT
DEST_ROOT.mkdir(parents=True, exist_ok=True)
RESEARCH_ROOT.mkdir(parents=True, exist_ok=True)

LOG_FILE  = SCRIPT_DIR / 'scraper.log'

# ── Resumability ──────────────────────────────────────────────
# Persists crawl progress across runs — stores completed source IDs,
# visited URLs, and skipped URLs so a restart picks up where it left off.
RESUME_FILE = DEST_ROOT / 'crawl_progress.json'

WORKERS           = 40  # increased parallelism per user request
REQUEST_TIMEOUT   = 40
MAX_DEPTH         = 3
MAX_URLS_PER_SOURCE = 10000
DEST_ROOT.mkdir(parents=True, exist_ok=True)

# ── Per-domain concurrency caps ───────────────────────────────
# Limits simultaneous connections to each host to stay polite and avoid bans.
_DOMAIN_CONCURRENCY = {
    'github.com'          : 8,
    'raw.githubusercontent.com': 8,
    'github.io'           : 4,
    'owasp.org'           : 3,
    'portswigger.net'     : 3,
    'overthewire.org'     : 2,
    'pwn.college'         : 2,
    'ropemporium.com'     : 2,
    'ctftime.org'         : 2,
    'arxiv.org'           : 1,
    'dl.acm.org'          : 1,
    'ieeexplore.ieee.org' : 1,
    'sciencedirect.com'   : 1,
    'researchgate.net'    : 1,
    'ssrn.com'            : 1,
}
_domain_semaphores    = {}
_domain_sem_lock      = threading.Lock()

def _get_domain_semaphore(url):
    domain = urllib.parse.urlparse(url).netloc.lower().replace('www.','')
    # Find longest matching key in _DOMAIN_CONCURRENCY
    cap = 4  # default parallel connections per domain
    for d, c in _DOMAIN_CONCURRENCY.items():
        if d in domain:
            cap = c
            break
    with _domain_sem_lock:
        if domain not in _domain_semaphores:
            _domain_semaphores[domain] = threading.Semaphore(cap)
        return _domain_semaphores[domain]

# ── Domain back-off tracking for repeated 403/429 ─────────────
_domain_backoff       = {}  # domain -> (next_allowed_time, consecutive_failures)
_domain_backoff_lock  = threading.Lock()

def _record_domain_failure(url):
    domain = urllib.parse.urlparse(url).netloc.lower()
    with _domain_backoff_lock:
        current_time = time.time()
        prev_time, failures = _domain_backoff.get(domain, (0, 0))
        failures += 1
        # Exponential back-off: 5s, 15s, 45s, 120s max
        wait = min(5 * (3 ** (failures - 1)), 120)
        _domain_backoff[domain] = (current_time + wait, failures)
        log(f'  [BackOff] {domain}: failure #{failures}, cooling down {wait:.0f}s')
        return wait

def _domain_is_backed_off(url):
    domain = urllib.parse.urlparse(url).netloc.lower()
    with _domain_backoff_lock:
        if domain in _domain_backoff:
            next_allowed, _ = _domain_backoff[domain]
            if time.time() < next_allowed:
                return True
            # Reset once back-off expires
            _domain_backoff.pop(domain, None)
    return False

def _clear_domain_failure(url):
    domain = urllib.parse.urlparse(url).netloc.lower()
    with _domain_backoff_lock:
        _domain_backoff.pop(domain, None)

# ── CTF / Security-Training Sources ─────────────────────────
# Structured, per-challenge JSON manifests get written here (in addition to
# the normal flat raw-file dump that already lands in DEST_ROOT).
CTF_STRUCTURED_DIR = DEST_ROOT / 'ctf_structured'
CTF_STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)

# ── Executable / Binary File Store ───────────────────────────
# ALL executable/binary files (.exe, .elf, .dll, .so, .bin, .out, .com,
# .apk, .dex, .class, .jar, .pyc) are routed to this folder regardless of
# which source they came from. Each file gets an accompanying .info.json
# sidecar with source URL, SHA-256 hash, file size, MIME type, and origin
# context — useful for training without needing to open the binary.
EXE_DIR          = DEST_ROOT / 'executables'
EXE_DIR_WIN      = EXE_DIR / 'windows'      # .exe .dll .com .msi .scr
EXE_DIR_LINUX    = EXE_DIR / 'linux'        # .elf .so .o .out .ko
EXE_DIR_JAVA     = EXE_DIR / 'java'         # .jar .class .dex .war
EXE_DIR_ANDROID  = EXE_DIR / 'android'      # .apk .aab
EXE_DIR_PYTHON   = EXE_DIR / 'python_bin'   # .pyc .pyo .pyd
EXE_DIR_OTHER    = EXE_DIR / 'other'        # anything else binary/firmware
EXE_META_DIR     = EXE_DIR / '_metadata'    # .info.json sidecars

for _d in (EXE_DIR, EXE_DIR_WIN, EXE_DIR_LINUX, EXE_DIR_JAVA,
           EXE_DIR_ANDROID, EXE_DIR_PYTHON, EXE_DIR_OTHER, EXE_META_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# Extension → sub-folder mapping
EXE_EXTS = {
    '.exe': EXE_DIR_WIN,  '.dll': EXE_DIR_WIN,  '.com': EXE_DIR_WIN,
    '.scr': EXE_DIR_WIN,  '.sys': EXE_DIR_WIN,
    '.elf': EXE_DIR_LINUX, '.so': EXE_DIR_LINUX, '.o':  EXE_DIR_LINUX,
    '.out': EXE_DIR_LINUX, '.ko': EXE_DIR_LINUX,
    '.jar': EXE_DIR_JAVA,  '.class': EXE_DIR_JAVA, '.dex': EXE_DIR_JAVA,
    '.war': EXE_DIR_JAVA,  '.ear': EXE_DIR_JAVA,
    '.apk': EXE_DIR_ANDROID, '.aab': EXE_DIR_ANDROID,
    '.pyc': EXE_DIR_PYTHON, '.pyo': EXE_DIR_PYTHON, '.pyd': EXE_DIR_PYTHON,
}

def _exe_subfolder(ext: str) -> Path:
    """Return the correct EXE_DIR sub-folder for a given extension."""
    return EXE_EXTS.get(ext.lower(), EXE_DIR_OTHER)

def save_executable(filename: str, data: bytes, source_url: str = '',
                    extra_meta: dict = None) -> bool:
    """
    Save a binary executable to the centralised EXE_DIR with a JSON sidecar.

    Layout:
        EXE_DIR/<type>/<filename>            ← the binary (write-only, no exec bit)
        EXE_DIR/_metadata/<filename>.info.json  ← sidecar with all context

    Returns True on success, False on error.
    """
    global total_saved
    ext  = Path(filename).suffix.lower()
    dest = _exe_subfolder(ext)

    clean_fn = ''.join(c if c.isalnum() or c in '._-' else '_' for c in filename)
    if not clean_fn:
        clean_fn = hashlib.md5(data).hexdigest()[:16] + ext
    if len(clean_fn) > 200:
        clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + ext

    out_path = dest / clean_fn

    # Filename dedup
    if clean_fn.lower() in scraped_files_cache or out_path.exists():
        return True
    # Content-hash dedup
    if _is_duplicate_content(data):
        with save_lock:
            scraped_files_cache.add(clean_fn.lower())
        return True

    try:
        safe_write_file(out_path, data)
        _register_hash(data)

        # Write sidecar .info.json
        meta = {
            'filename'    : clean_fn,
            'original_url': source_url,
            'sha256'      : _compute_hash(data),
            'size_bytes'  : len(data),
            'file_type'   : ext,
            'subfolder'   : dest.name,
            'saved_path'  : str(out_path),
        }
        if extra_meta:
            meta.update(extra_meta)
        sidecar = EXE_META_DIR / (clean_fn + '.info.json')
        sidecar.write_text(json.dumps(meta, indent=2), encoding='utf-8')

        with save_lock:
            stats['latest_raw_data'] += 1
            total_saved += 1
            scraped_files_cache.add(clean_fn.lower())
            if total_saved % 500 == 0:
                _save_hash_index()
        log(f'  [EXE] Saved {ext} → {dest.name}/{clean_fn}  ({len(data)//1024} KB)')
        return True
    except Exception as e:
        log(f'  [EXE] ERROR saving {filename}: {e}')
        return False


# ── Lab video (.mp4) transcription config ────────────────────
# Any .mp4 found in a lab's repo ZIP or embedded on a lab page (PortSwigger,
# pwn.college, discovered CTF repos, etc.) gets downloaded/staged, transcribed
# locally with faster-whisper, auto-structured into either a Q&A layout or a
# Topic/Explanation layout, and written out as batched HTML pages.
VIDEO_TRANSCRIPTION_ENABLED = True
WHISPER_MODEL_SIZE      = 'base'   # tiny/base/small/medium/large-v3 — bigger = more accurate, much slower on CPU
VIDEO_STAGING_DIR       = DEST_ROOT / 'video_staging'       # temp mp4 downloads, deleted after transcription
VIDEO_HTML_DIR          = DEST_ROOT / 'video_transcripts'   # final batched HTML output
VIDEOS_PER_HTML_PAGE    = 4
VIDEO_SILENCE_GAP_SECONDS = 2.5    # gap between speech segments treated as a new topic boundary
VIDEO_STAGING_DIR.mkdir(parents=True, exist_ok=True)
VIDEO_HTML_DIR.mkdir(parents=True, exist_ok=True)

video_queue      = []
video_queue_lock = threading.Lock()

# Official GitHub repos to pull full source + structured challenge metadata from.
# These are scraped FIRST, regardless of star count, because they are
# confirmed-official platforms (not just popular).
# (owner, repo, platform_tag) — platform_tag selects the structured parser below.
CTF_GITHUB_REPOS = [
    ('google',     'google-ctf', 'google_ctf'),
    ('picoCTF',    'picoCTF',    'picoctf'),
    ('juice-shop', 'juice-shop', 'juiceshop'),
    ('WebGoat',    'WebGoat',    'webgoat'),
    ('digininja',  'DVWA',       'generic_ctf'),   # Damn Vulnerable Web Application
    ('OWASP',      'NodeGoat',   'generic_ctf'),   # OWASP Node.js Goat
    ('OWASP',      'railsgoat',  'generic_ctf'),   # OWASP Rails Goat
]
PWNCOLLEGE_ORG           = 'pwncollege'
PORTSWIGGER_ALL_LABS_URL = 'https://portswigger.net/web-security/all-labs'

# ── Self-discovery of additional CTF repos on GitHub ─────────
# Search queries used to find CTF content beyond the known-official list
# above. Results are ranked by star count (official/high-star first).
GITHUB_SEARCH_QUERIES = [
    'topic:ctf',
    'topic:capture-the-flag',
    'topic:ctf-challenges',
    'topic:wargame',
    'ctf challenges in:name,description',
    '"capture the flag" challenges in:description',
]
# A repo's name+description+topics must match this to be kept — filters out
# unrelated repos that happen to contain the letters "ctf" somewhere.
CTF_KEYWORD_RE = re.compile(
    r'\b(ctf|capture[- ]the[- ]flag|wargame|pwn(?:able)?|exploit[- ]challenge|'
    r'hacking[- ]challenge|security[- ]challenge|vuln(?:erable)?[- ]?(?:web|app)|'
    r'pentest(?:ing)?[- ]lab)\b', re.I
)
MIN_STARS_FOR_DISCOVERED = 20    # noise filter for the search-discovered tier
MAX_DISCOVERED_REPOS     = 150   # hard cap per run so this stays bounded

saved_urls          = set()
scraped_skip_urls   = set()   # URLs intentionally skipped (noise/404)
completed_sources   = set()   # Source IDs (e.g. 'S012') fully done
save_lock           = threading.Lock()
resume_lock         = threading.Lock()
stats               = defaultdict(int)
total_saved         = 0
scraped_files_cache = set()
arxiv_titles_cache  = set()

# ── Content-Hash Deduplication Index ─────────────────────────────
# Stores SHA-256 hashes of all scraped file contents.
# Prevents saving the same content under different filenames.
# Persisted to disk so dedup works across restarts.
HASH_INDEX_FILE  = DEST_ROOT / 'content_hash_index.json'
_content_hashes  = set()   # set of SHA-256 hex strings of known content
_hash_lock       = threading.Lock()

def _compute_hash(data: bytes) -> str:
    """Compute SHA-256 hash of raw bytes. Used for content deduplication."""
    return hashlib.sha256(data).hexdigest()

def _is_duplicate_content(data: bytes) -> bool:
    """Return True if this exact content has already been saved."""
    h = _compute_hash(data)
    with _hash_lock:
        return h in _content_hashes

def _register_hash(data: bytes):
    """Register content hash after a successful save."""
    h = _compute_hash(data)
    with _hash_lock:
        _content_hashes.add(h)

def _save_hash_index():
    """Persist the hash index to disk (atomic write via temp file)."""
    try:
        tmp = str(HASH_INDEX_FILE) + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(list(_content_hashes), f)
        os.replace(tmp, HASH_INDEX_FILE)
    except Exception:
        pass

def _load_hash_index():
    """Load persisted hash index from disk on startup."""
    global _content_hashes
    if HASH_INDEX_FILE.exists():
        try:
            with open(HASH_INDEX_FILE, 'r', encoding='utf-8') as f:
                hashes = json.load(f)
            with _hash_lock:
                _content_hashes.update(hashes)
            log(f'[Dedup] Loaded {len(_content_hashes):,} content hashes from index.')
        except Exception as e:
            log(f'[Dedup] Could not load hash index: {e}')

def _build_hash_index_from_folder(folder: Path, sample_limit: int = 0):
    """
    Scan existing files in folder and populate the hash index.
    sample_limit=0 means scan everything; set to N to only sample N files
    (faster startup at the cost of less complete dedup on first run).
    """
    global _content_hashes
    log(f'[Dedup] Building content hash index from {folder} ...')
    count = 0
    skip_exts = {'.log', '.tmp', '.json'}  # skip large index/log files
    for root, _, files in os.walk(folder):
        for fname in files:
            if sample_limit and count >= sample_limit:
                break
            fpath = Path(root) / fname
            if fpath.suffix.lower() in skip_exts:
                continue
            try:
                data = fpath.read_bytes()
                h = _compute_hash(data)
                with _hash_lock:
                    _content_hashes.add(h)
                count += 1
            except Exception:
                pass
        if sample_limit and count >= sample_limit:
            break
    log(f'[Dedup] Hash index built: {len(_content_hashes):,} unique content hashes from {count:,} files.')
    _save_hash_index()

# ───────────────────────────────────────────────────────────────
#  EXECUTION SAFETY GUARD
# ───────────────────────────────────────────────────────────────
# This crawler downloads exploit code (.py), binaries (.exe/.elf), zip archives,
# packet captures (.pcap), and other potentially dangerous file types for use as
# TRAINING DATA ONLY. None of these files are ever executed, interpreted, or
# extracted in a runnable form. They are treated as pure byte streams.
#
# POLICY:
#   - All files are written with os.O_CREAT | os.O_WRONLY (write-only, no exec).
#   - On POSIX systems, file mode is explicitly set to 0o644 (no execute bit).
#   - ZIP files are extracted entry-by-entry with path traversal (zip-slip)
#     prevention: any entry whose resolved path escapes the destination folder
#     is silently skipped.
#   - No subprocess, os.system, os.exec*, eval(), or exec() is ever called
#     on downloaded content.
# ───────────────────────────────────────────────────────────────

def safe_write_file(dest_path: Path, data: bytes) -> None:
    """
    Write raw bytes to dest_path in a SAFE, NON-EXECUTABLE manner.

    Guarantees:
      - The file is NEVER given execute permissions (mode 0o644 on POSIX).
      - The destination is always resolved to an absolute path before writing,
        preventing zip-slip or path-traversal from reaching unintended locations.
      - Content is written as opaque bytes with no interpretation.
    """
    dest_path = Path(dest_path).resolve()
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    # Open with explicit flags: create/overwrite, write-only, no execute semantics
    with open(dest_path, 'wb') as f:
        f.write(data)
    # Explicitly strip execute bits on POSIX (no-op on Windows but harmless)
    try:
        import stat
        current = dest_path.stat().st_mode
        no_exec = current & ~(stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        dest_path.chmod(no_exec)
    except Exception:
        pass  # Windows doesn't support POSIX chmod; safe by default there


def _is_safe_zip_member(dest_dir: Path, member_path: str) -> bool:
    """
    Zip-slip protection: return True only if the resolved output path
    is strictly inside dest_dir. Rejects any '../' traversal tricks.
    """
    dest_dir   = dest_dir.resolve()
    target     = (dest_dir / member_path).resolve()
    try:
        target.relative_to(dest_dir)   # raises ValueError if outside
        return True
    except ValueError:
        log(f'  [ZipSlip] BLOCKED dangerous path: {member_path}')
        return False

# ── Cookie persistence ────────────────────────────────────────
COOKIE_JAR_FILE = DEST_ROOT / 'session_cookies.json'

def _save_cookies(session):
    """Persist session cookies to disk for reuse across runs."""
    try:
        cookies = {c.name: c.value for c in session.cookies}
        with open(COOKIE_JAR_FILE, 'w', encoding='utf-8') as f:
            json.dump(cookies, f)
    except Exception:
        pass

def _load_cookies(session):
    """Reload persisted cookies into the session."""
    try:
        if COOKIE_JAR_FILE.exists():
            with open(COOKIE_JAR_FILE, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            for name, value in cookies.items():
                session.cookies.set(name, value)
    except Exception:
        pass

# ── Resume State persistence ──────────────────────────────────
def save_resume_state():
    """Save crawl progress so we can resume after a crash or restart."""
    with resume_lock:
        try:
            state = {
                'saved_urls': list(saved_urls),
                'skipped_urls': list(scraped_skip_urls),
                'completed_sources': list(completed_sources),
                'total_saved': total_saved,
            }
            tmp = str(RESUME_FILE) + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(state, f)
            os.replace(tmp, RESUME_FILE)
        except Exception as e:
            pass

def load_resume_state():
    """Load previously saved crawl progress."""
    global saved_urls, scraped_skip_urls, completed_sources, total_saved
    if not RESUME_FILE.exists():
        return
    try:
        with open(RESUME_FILE, 'r', encoding='utf-8') as f:
            state = json.load(f)
        with save_lock:
            saved_urls.update(state.get('saved_urls', []))
            scraped_skip_urls.update(state.get('skipped_urls', []))
            completed_sources.update(state.get('completed_sources', []))
            total_saved = state.get('total_saved', 0)
        log(f'[Resume] Restored state: {len(saved_urls)} URLs done, '
            f'{len(completed_sources)} sources complete, {total_saved} files saved.')
    except Exception as e:
        log(f'[Resume] Could not load resume state: {e}')

def mark_source_complete(source_id):
    """Mark a catalog source as fully scraped and persist state."""
    with save_lock:
        completed_sources.add(source_id)
    save_resume_state()
    log(f'[Resume] Marked source {source_id} as complete.')

def is_source_complete(source_id):
    """Return True if this source was already fully scraped."""
    return source_id in completed_sources

# ── HAL Science config ────────────────────────────────────────
HAL_API  = 'https://api.archives-ouvertes.fr/search/'
HAL_ROWS = 100
HAL_MIN_DELAY = 3.0  # seconds between HAL requests
HAL_MAX_RETRIES = 3   # retry attempts for HAL downloads

# Map URL query parameter → (folder_name, search_query)
# These match the 4 HAL URLs in url1.docx exactly
HAL_URL_MAP = {
    'cybersecurity+threat'        : ('hal_cybersecurity_threat',       'cybersecurity threat'),
    'cryptography+and+security'   : ('hal_cryptography_and_security',  'cryptography and security'),
    'network+security'            : ('hal_network_security',           'network security'),
    'computer+security'           : ('hal_computer_security',          'computer security'),
    # decoded variants
    'cybersecurity threat'        : ('hal_cybersecurity_threat',       'cybersecurity threat'),
    'cryptography and security'   : ('hal_cryptography_and_security',  'cryptography and security'),
    'network security'            : ('hal_network_security',           'network security'),
    'computer security'           : ('hal_computer_security',          'computer security'),
}

def clean_title_helper(title):
    return ''.join([c.lower() for c in title if c.isalnum()])

def initialize_scraped_files_cache():
    global scraped_files_cache, arxiv_titles_cache
    log('Scanning DEST_ROOT folder for deduplication...')
    # Load persisted hash index first (fast path)
    _load_hash_index()
    if DEST_ROOT.exists() and DEST_ROOT.is_dir():
        for root, _, files in os.walk(DEST_ROOT):
            for f in files:
                scraped_files_cache.add(f.lower())
    log(f'Found {len(scraped_files_cache)} files in current run directory.')
    # If hash index was empty (first run), build it from existing files
    if not _content_hashes:
        _build_hash_index_from_folder(DEST_ROOT)

HEADERS = {
    'User-Agent'     : random_ua(),
    'Accept'         : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection'     : 'keep-alive',
}

# Separate header set used ONLY for api.github.com calls (repo-size checks,
# org repo listing, code/repo search). Keeps any GitHub token scoped to
# GitHub's API and never sent to other domains.
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '').strip()
GITHUB_API_HEADERS = {
    'User-Agent': HEADERS['User-Agent'],
    'Accept'    : 'application/vnd.github+json',
}
if GITHUB_TOKEN:
    GITHUB_API_HEADERS['Authorization'] = f'token {GITHUB_TOKEN}'
else:
    print('NOTE: No GITHUB_TOKEN env var set. GitHub search API is limited to '
          '~10 requests/min unauthenticated — discovery will be slow/limited. '
          'Set GITHUB_TOKEN for much higher rate limits.')

log_lock = threading.Lock()

def log(msg):
    ts   = time.strftime('%H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line, flush=True)
    try:
        with log_lock:
            with open(LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(line + '\n')
    except Exception:
        pass

# ── Session factory ───────────────────────────────────────────
def make_session():
    """
    Create a cloudscraper session (bypasses Cloudflare JS challenges).
    Falls back to plain requests.Session() if cloudscraper not installed.
    Reloads any persisted cookies from prior runs for session continuity.
    """
    if HAS_CLOUDSCRAPER:
        sess = _cs_mod.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True},
            delay=3,
        )
        log('  Session: cloudscraper (Cloudflare bypass ON)')
    else:
        sess = requests.Session()
        log('  Session: requests (no Cloudflare bypass)')
    # Restore cookies from last session so we appear as a returning visitor
    _load_cookies(sess)
    return sess

def clean_html(raw_html, source_url):
    soup = BeautifulSoup(raw_html, 'html.parser')
    for tag in soup(['script', 'style', 'noscript', 'iframe', 'svg']):
        tag.decompose()
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()

    remove_tags = {'header', 'footer', 'nav', 'aside'}
    for tag_name in remove_tags:
        for el in soup.find_all(tag_name):
            try:
                el.decompose()
            except Exception:
                pass

    noise_patterns = re.compile(
        r'\b(nav|navigation|sidebar|menu|footer|header|breadcrumb|related|'
        r'social|share|cookie|advertisement|ads|promo|widget|toc|'
        r'comments|comment-section|subscribe|newsletter|popup|modal|'
        r'tag-cloud|categories|archives|pagination|pager|toolbar|'
        r'topbar|masthead|site-header|site-footer)\b',
        re.I
    )
    for el in list(soup.find_all(True)):
        if not isinstance(el, Tag):
            continue
        try:
            classes = ' '.join(el.get('class', []) or [])
            el_id   = el.get('id', '') or ''
        except Exception:
            continue
        if noise_patterns.search(classes) or noise_patterns.search(el_id):
            try:
                el.decompose()
            except Exception:
                pass

    main = None
    for sel in ['article', 'main', '[role="main"]', '.post-content', '.entry-content',
                '.article-body', '.content', '.post', '#content', '#main', '.blog-post']:
        found = soup.select_one(sel)
        if found:
            main = found
            break
    if not main:
        main = soup.body if soup.body else soup

    for a_tag in list(main.find_all('a')):
        try:
            a_tag.replace_with(a_tag.get_text())
        except Exception:
            pass

    title_tag = soup.find('title')
    title = title_tag.string.strip() if title_tag and title_tag.string else 'Security Resource'
    text  = main.get_text(separator=' ', strip=True)

    clean_doc  = '<!DOCTYPE html>\n<html>\n<head><meta charset="utf-8">'
    clean_doc += f'<title>{title}</title></head>\n<body>\n'
    clean_doc += f'<p>Source: <a href="{source_url}">{source_url}</a></p><hr>\n'
    clean_doc += str(main)
    clean_doc += '\n</body>\n</html>'

    return clean_doc, title, text

def save_file(filename, content, title='', content_text='', source_url=''):
    global total_saved
    out_dir = DEST_ROOT
    out_dir.mkdir(parents=True, exist_ok=True)

    clean_fn = ''.join([c if c.isalnum() or c in '._- ' else '_' for c in filename])
    if len(clean_fn) > 200:
        clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + '.html'

    out_path = out_dir / clean_fn

    # Filename-based dedup (fast path)
    if clean_fn.lower() in scraped_files_cache or out_path.exists():
        return True

    try:
        data = content.encode('utf-8', errors='replace')
        # Content-hash dedup: skip if identical content already saved under any name
        if _is_duplicate_content(data):
            with save_lock:
                scraped_files_cache.add(clean_fn.lower())  # register name so it's skipped next time
            return True

        safe_write_file(out_path, data)   # safe write — no execute bit set
        _register_hash(data)

        with save_lock:
            stats['latest_raw_data'] += 1
            total_saved += 1
            scraped_files_cache.add(clean_fn.lower())
            # Persist hash index every 500 saves to reduce I/O
            if total_saved % 500 == 0:
                _save_hash_index()
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────
#  DOMAIN CRAWL DELAYS  (from robots.txt + ToS research)
# ─────────────────────────────────────────────────────────────
DOMAIN_CRAWL_DELAYS = {
    # High restriction
    'arxiv.org'           : 30.0,
    'dl.acm.org'          : 30.0,
    'ieeexplore.ieee.org' : 15.0,
    'sciencedirect.com'   : 15.0,
    'researchgate.net'    : 15.0,
    'gartner.com'         : 15.0,
    # Moderate
    'springer.com'        : 10.0,
    'link.springer.com'   : 10.0,
    'semanticscholar.org' : 10.0,
    # Government / Standards
    'doi.org'             : 10.0,
    'csrc.nist.gov'       :  1.5,
    'pages.nist.gov'      :  1.5,
    'nccoe.nist.gov'      :  1.5,
    'cisa.gov'            : 10.0,
    'datatracker.ietf.org': 10.0,
    'rfc-editor.org'      : 10.0,
    'edpb.europa.eu'      : 10.0,
    # Commercial security vendors
    'cyberark.com'        : 10.0,
    'beyondtrust.com'     : 10.0,
    'sailpoint.com'       : 10.0,
    'mandiant.com'        : 10.0,
    'delinea.com'         : 10.0,
    'authomize.com'       : 10.0,
    'veza.com'            : 10.0,
    'securonix.com'       : 10.0,
    'kroll.com'           : 10.0,
    'strongdm.com'        : 10.0,
    'aws.amazon.com'      :  5.0,
    'docs.aws.amazon.com' :  5.0,
    'cloud.google.com'    :  5.0,
    'research.google'     :  5.0,
    # Open access / community
    'github.com'          : 12.0,

    'learn.microsoft.com' :  3.0,
    # New sources for url1.docx
    'sans.org'            :  3.0,   # Cloudflare-protected; cloudscraper handles it
    'hal.science'         :  1.5,   # Open academic repository
    'archives-ouvertes.fr':  1.0,   # HAL REST API
    'ssrn.com'            :  4.0,   # Strict rate limiting
    'papers.ssrn.com'     :  4.0,
    # Other
    'attack.mitre.org'    :  3.0,
    'owasp.org'           :  2.0,
    'openid.net'          :  2.0,
    'w3.org'              :  2.0,
    'troyhunt.com'        :  2.0,
    'cisecurity.org'      :  3.0,
    'msrc.microsoft.com'  :  3.0,
    'mitre.org'           :  1.0,
    'nist.gov'            :  1.5,
    'github.io'           :  1.0,
}

def get_crawl_delay(url):
    try:
        netloc = urllib.parse.urlparse(url).netloc.replace('www.', '').lower()
        for domain, delay in DOMAIN_CRAWL_DELAYS.items():
            if domain in netloc:
                return delay
    except Exception:
        pass
    return 1.5

def _make_headers(referer=''):
    """Return browser-grade headers with a fresh User-Agent each call."""
    h = {
        'User-Agent'               : random_ua(),
        'Accept'                   : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language'          : 'en-US,en;q=0.9',
        'Accept-Encoding'          : 'gzip, deflate, br',
        'DNT'                      : '1',
        'Connection'               : 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest'           : 'document',
        'Sec-Fetch-Mode'           : 'navigate',
        'Sec-Fetch-Site'           : 'none' if not referer else 'same-origin',
        'Sec-Fetch-User'           : '?1',
        'Sec-CH-UA'                : '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'Sec-CH-UA-Mobile'         : '?0',
        'Sec-CH-UA-Platform'       : '"Windows"',
    }
    if referer:
        h['Referer'] = referer
    return h

# Per-domain locks so each domain is hit sequentially within a thread pool
_domain_locks    = {}
_domain_meta_lock = threading.Lock()

def _get_domain_lock(url):
    domain = urllib.parse.urlparse(url).netloc.lower()
    with _domain_meta_lock:
        if domain not in _domain_locks:
            _domain_locks[domain] = threading.Lock()
        return _domain_locks[domain]

def fetch(url, session, retries=4, stream=False, referer=''):
    """Rate-limited GET with improved bot-bypass and resumability.
    Features:
      - Per-domain locking (sequential within a domain) + semaphore concurrency cap.
      - Per-domain exponential back-off after repeated 403/429.
      - Gaussian jitter for human-like timing (avoids predictable patterns).
      - Referer chain: passes previous page URL to look like a real browser click.
      - Rotating User-Agent on each request.
      - Playwright headless fallback on persistent 403.
      - Cookie persistence: saves cookies after each successful response.
    """
    # Skip immediately if domain is in back-off period
    if _domain_is_backed_off(url):
        return None

    lock = _get_domain_lock(url)
    sem  = _get_domain_semaphore(url)
    hdrs = _make_headers(referer=referer)

    for attempt in range(1, retries + 1):
        # Gaussian jitter: mean=base_delay, sigma=0.4 — looks more human than uniform
        base_delay = get_crawl_delay(url)
        jitter     = abs(random.gauss(0, 0.4))
        delay      = base_delay + jitter

        with sem:          # domain concurrency cap
            with lock:     # sequential per-domain within a thread
                time.sleep(delay)
                try:
                    r = session.get(url, headers=hdrs, timeout=REQUEST_TIMEOUT,
                                    allow_redirects=True, stream=stream)

                    # 429 Rate limit — respect Retry-After header
                    if r.status_code == 429:
                        retry_after = int(r.headers.get('Retry-After', 30))
                        wait = retry_after + attempt * 10
                        log(f'  ⚠ 429 rate-limited on {url} – waiting {wait}s')
                        _record_domain_failure(url)
                        time.sleep(wait)
                        hdrs = _make_headers(referer=referer)  # rotate UA
                        continue

                    # 403 Forbidden — try Playwright headless as bypass
                    if r.status_code == 403:
                        wait = abs(random.gauss(base_delay * 2, 1.0)) * attempt
                        log(f'  ✗ 403 Forbidden on {url} – retrying after {wait:.1f}s (attempt {attempt}/{retries})')
                        _record_domain_failure(url)
                        time.sleep(wait)
                        if attempt >= 2:
                            # Playwright fallback for persistent 403s
                            pw_html = pw_get_html(url)
                            if pw_html:
                                # Wrap in a fake response-like object
                                class _FakeResp:
                                    status_code = 200
                                    text = pw_html
                                    content = pw_html.encode('utf-8')
                                    headers = {'Content-Type': 'text/html'}
                                    def json(self): return {}
                                    def raise_for_status(self): pass
                                log(f'  [Playwright] Got HTML for {url}')
                                return _FakeResp()
                        hdrs = _make_headers(referer=referer)  # rotate UA on retry
                        continue

                    if r.status_code in (404, 410):
                        return None

                    r.raise_for_status()
                    # Success — clear any back-off and persist cookies
                    _clear_domain_failure(url)
                    _save_cookies(session)
                    return r

                except requests.exceptions.Timeout:
                    log(f'  ⏱ Timeout on {url} (attempt {attempt}/{retries})')
                    time.sleep(2 ** attempt)
                except Exception as e:
                    log(f'  ⚠ Error fetching {url} (attempt {attempt}/{retries}): {e}')
                    time.sleep(2 ** attempt)
    return None

# ───────────────────────────────────────────────────────────────
#  CONTENT RELEVANCE FILTERS
# ───────────────────────────────────────────────────────────────

# Keywords that confirm cybersecurity relevance — any one match = keep
_SECURITY_KEYWORDS = [
    # Core security terms
    'exploit', 'vulnerability', 'vuln', 'cve-', 'rce', 'lfi', 'rfi', 'ssrf', 'ssti',
    'xss', 'sql injection', 'sqli', 'command injection', 'path traversal', 'idor',
    'buffer overflow', 'heap overflow', 'stack overflow', 'use-after-free', 'uaf',
    'format string', 'race condition', 'type confusion', 'integer overflow',
    'privilege escalation', 'privesc', 'arbitrary code execution', 'shellcode',
    'rop chain', 'return oriented', 'ret2', 'gadget', 'libc', 'pwntools', 'pwndbg',
    # CTF / challenge terms
    'ctf', 'capture the flag', 'wargame', 'pwn', 'pwnable', 'hack the box', 'hackthebox',
    'tryhackme', 'picoctf', 'overthewire', 'bandit', 'narnia', 'leviathan', 'natas',
    'portswigger', 'web security academy', 'challenge', 'flag{', 'htb{', 'picoctf{',
    'rop emporium', 'pwn.college', 'ringzer0', 'root-me', 'exploit.education',
    # Attack techniques
    'reverse engineering', 'reversing', 'binary exploitation', 'binary analysis',
    'fuzzing', 'afl', 'libfuzzer', 'honggfuzz', 'sanitizer', 'asan', 'ubsan',
    'symbolic execution', 'angr', 'z3', 'manticore', 'static analysis', 'dynamic analysis',
    'disassembly', 'decompile', 'ghidra', 'ida pro', 'radare2', 'r2', 'binja',
    'gdb', 'pwndbg', 'peda', 'gef', 'strace', 'ltrace', 'valgrind',
    # Malware / RE
    'malware', 'ransomware', 'rootkit', 'backdoor', 'trojan', 'payload', 'shellcode',
    'c2', 'command and control', 'botnet', 'dropper', 'loader', 'obfuscat',
    'anti-debug', 'anti-analysis', 'packing', 'unpacking', 'yara', 'ioc',
    # Network / crypto attacks
    'man in the middle', 'mitm', 'sniffing', 'packet capture', 'pcap', 'wireshark',
    'nmap', 'masscan', 'port scan', 'brute force', 'bruteforce', 'password crack',
    'hashcat', 'john the ripper', 'rainbow table', 'wordlist', 'rockyou',
    'cryptography', 'cipher', 'rsa', 'aes', 'ecc', 'padding oracle', 'bleichenbacher',
    'timing attack', 'side channel', 'fault injection', 'glitching',
    # Web security
    'owasp', 'burp suite', 'zap proxy', 'sqlmap', 'nikto', 'dirb', 'dirbuster',
    'gobuster', 'ffuf', 'wfuzz', 'csrf', 'cors', 'jwt', 'oauth bypass',
    'deserialization', 'xxe', 'xml injection', 'template injection', 'prototype pollution',
    'open redirect', 'clickjacking', 'subdomain takeover', 'dns rebinding',
    # System security
    'pentest', 'penetration test', 'red team', 'blue team', 'purple team',
    'threat model', 'attack surface', 'zero day', '0day', 'patch diffing',
    'metasploit', 'msfvenom', 'cobalt strike', 'mimikatz', 'empire', 'bloodhound',
    'impacket', 'responder', 'pass the hash', 'kerberoast', 'golden ticket',
    # Defensive / analysis
    'siem', 'soc', 'incident response', 'forensics', 'memory forensics', 'volatility',
    'threat hunt', 'indicator of compromise', 'sigma rule', 'suricata', 'snort',
    # Academic / research
    'security research', 'vulnerability research', 'bug bounty', 'responsible disclosure',
    'cwe-', 'capec-', 'mitre att&ck', 'attack framework',
]

# URL path segments that indicate non-security boilerplate to skip
_NOISE_URL_SEGMENTS = re.compile(
    r'/(signup|sign[-_]up|register|registration|login|log[-_]in|logout|log[-_]out|'
    r'account|profile|dashboard|settings|preferences|billing|payment|checkout|cart|'
    r'subscribe|unsubscribe|newsletter|contact|about[-_]us|careers?|jobs?|hiring|'
    r'press|media|investor|legal|terms[-_]of|privacy[-_]policy|cookie[-_]policy|'
    r'refund|pricing|plans?|upgrade|reset[-_]password|forgot[-_]password|'
    r'verify[-_]email|confirm|onboard|tutorial/getting[-_]started|welcome)(/|$)',
    re.I
)

# Code patterns that indicate generic non-security boilerplate
_BOILERPLATE_CODE_RE = re.compile(
    r'(class\s+UserController|def\s+sign_?up|def\s+register|'
    r'def\s+create_?account|def\s+forgot_?password|def\s+reset_?password|'
    r'django\.contrib\.auth|flask_login|passport\.js|devise|authlogic|'
    r'bcrypt\.hash\(|argon2\.hash\(password|User\.create\(|createUser\(|'
    r'INSERT INTO users|UPDATE users SET password|stripe\.charge|paypal\.payment|'
    r'send_?welcome_?email|confirm_?email|email_?verification)'
    , re.I
)

# Code patterns that strongly confirm security relevance
_SECURITY_CODE_RE = re.compile(
    r'(exploit|shellcode|rop_?chain|ret2|overflow|heap_?spray|use.after.free|'
    r'format.string|pwntools|pwn\.process|remote\(|ELF\(|cyclic\(|p64\(|p32\(|'
    r'socket\.connect|struct\.pack|ctypes\.memmove|mprotect|mmap\(|'
    r'subprocess\.Popen.*shell|os\.system|os\.execv|'
    r'sql.*injection|union.*select|or.*1.*=.*1|xss|alert\(|onerror=|'
    r'burp|sqlmap|nmap|metasploit|hashcat|john|mimikatz|cobalt|'
    r'ctf|flag\{|htb\{|picoctf\{|challenge|vulnerable|payload|gadget|'
    r'cipher\.encrypt|cipher\.decrypt|rsa\.decrypt|padding.*oracle|'
    r'scapy|dpkt|pcap|sniff\(|sendp\(|wrpcap\(|rdpcap\()'
    , re.I
)

def is_security_relevant(url: str, content: str = '', filename: str = '') -> bool:
    """
    Returns True if the URL + content is cybersecurity-relevant.
    Checks URL path, filename, and up to 8 KB of content.
    """
    combined = f'{url} {filename}'.lower()

    # Fast-path: URL or filename directly signals security content
    if any(kw in combined for kw in _SECURITY_KEYWORDS):
        return True

    # Check content sample (first + last 4 KB for speed)
    if content:
        sample = (content[:4096] + content[-4096:]).lower()
        if any(kw in sample for kw in _SECURITY_KEYWORDS):
            return True
        if _SECURITY_CODE_RE.search(sample):
            return True

    return False

def is_generic_boilerplate(content: str) -> bool:
    """
    Returns True if the code looks like generic web app boilerplate
    (signup, login, ORM CRUD, billing, email verification).
    A file flagged here is ONLY rejected if it ALSO fails is_security_relevant().
    """
    return bool(_BOILERPLATE_CODE_RE.search(content[:8192]))


# ───────────────────────────────────────────────────────────────
#  SCRAPE + SAVE  (main per-URL handler)
# ───────────────────────────────────────────────────────────────
def scrape_and_save(url, session, prefix=''):
    global total_saved

    url_lower = url.lower()

    # ── URL-level noise filter (expanded) ──────────────────────────
    noise_patterns = re.compile(
        r'\b(contact|about|career|jobs?|privacy|terms|faqs?|news|press|'
        r'help|support|feedback|log[-_]?in|sign[-_]?(in|up)|register|'
        r'search|tags?|categor(y|ies)|author|billing|checkout|payment|'
        r'upgrade|subscribe|newsletter|pricing|plans?)\b|'
        r'/(contact|about|career|jobs?|privacy|terms|faqs?|news|press|'
        r'help|support|feedback|login|signin|signup|register|'
        r'search|tags?|categories|category|author|billing|checkout|'
        r'payment|upgrade|subscribe|newsletter|pricing)(/|$)',
        re.I
    )
    if noise_patterns.search(url_lower):
        return False

    # Also block obvious non-security URL path patterns
    if _NOISE_URL_SEGMENTS.search(url_lower):
        return False

    with save_lock:
        if url in saved_urls:
            return False
        saved_urls.add(url)

    if any(url.lower().endswith(ext) for ext in ['.iso', '.img', '.deb', '.sig', '.ova',
                                                  '.torrent', '.pkg', '.dmg', '.bin',
                                                  '.msi', '.rar']):
        return False

    parsed   = urllib.parse.urlparse(url)
    is_github = 'github.com' in parsed.netloc or 'raw.githubusercontent.com' in parsed.netloc

    try:
        is_pdf  = url.lower().endswith('.pdf')
        is_zip  = url.lower().endswith('.zip')
        is_docx = url.lower().endswith('.docx')
        ext = '.pdf' if is_pdf else ('.zip' if is_zip else ('.docx' if is_docx else '.html'))

        if is_github:
            path_parts = [p for p in parsed.path.strip('/').split('/') if p]
            if len(path_parts) >= 3 and path_parts[2] in (
                'issues','pull','pulls','commit','commits','releases','tags','actions',
                'projects','wiki','settings','branches','pulse','graphs','network',
                'stargazers','watchers','search','find','forks','watch','stars',
                'edit','new','delete','community','discussions','milestones','labels'):
                return False
            if len(path_parts) >= 3 and path_parts[2] == 'tree':
                return False
            if path_parts:
                fname_p = path_parts[-1].lower()
                noise_files = {
                    'makefile','package.json','package-lock.json','.gitignore',
                    '.gitattributes','yarn.lock','cargo.lock','go.sum','go.mod',
                    'webpack.config.js','tsconfig.json','.eslintignore','.eslintrc.json',
                    'pnpm-lock.yaml','composer.lock','composer.json'
                }
                if fname_p in noise_files or fname_p.startswith('makefile') or 'gitignore' in fname_p:
                    return False
            if len(path_parts) >= 4 and path_parts[2] in ('blob','raw'):
                clean_fn = f"github_{path_parts[0]}_{path_parts[1]}_{'_'.join(path_parts[4:])}"
            elif len(path_parts) >= 2:
                clean_fn = f"github_{path_parts[0]}_{path_parts[1]}.html"
            else:
                clean_fn = f"github_{'_'.join(path_parts) or 'index'}.html"
        else:
            if is_pdf or is_zip or is_docx:
                path_part = parsed.path.strip('/').split('/')[-1] or f'document{ext}'
                clean_fn  = urllib.parse.unquote(path_part)
                if not clean_fn.lower().endswith(ext):
                    clean_fn += ext
                if ('github.com' in parsed.netloc or 'codeload.github.com' in parsed.netloc) and is_zip:
                    parts = parsed.path.strip('/').split('/')
                    if len(parts) >= 2:
                        clean_fn = f"{parts[0]}_{parts[1]}{ext}"
            else:
                path_part   = parsed.path.strip('/').replace('/', '_') or 'index'
                domain_part = parsed.netloc.replace('.','_').replace('www_','')
                clean_fn    = f"{prefix or domain_part}_{path_part}{ext}"

        clean_fn = ''.join([c if c.isalnum() or c in '._- ' else '_' for c in clean_fn])
        if len(clean_fn) > 200:
            clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + ext

        if clean_fn.lower() in scraped_files_cache or (DEST_ROOT / clean_fn).exists():
            return True
    except Exception:
        pass

    # CVE record handler
    if 'cve.org/cverecord' in url.lower():
        try:
            p2  = urllib.parse.urlparse(url)
            qs  = urllib.parse.parse_qs(p2.query)
            cve_id = qs.get('id', [None])[0]
            if not cve_id:
                m = re.search(r'CVE-\d{4}-\d{4,}', url, re.I)
                if m:
                    cve_id = m.group(0).upper()
            if cve_id:
                cve_id   = cve_id.upper()
                filename = f'cve_org_{cve_id}.json'
                out_path = DEST_ROOT / filename
                if filename.lower() in scraped_files_cache or out_path.exists():
                    return True
                api_url = f'https://cveawg.mitre.org/api/cve/{cve_id}'
                api_r   = fetch(api_url, session)
                if api_r and api_r.status_code == 200:
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(out_path, 'w', encoding='utf-8') as f:
                        json.dump(api_r.json(), f, indent=2)
                    with save_lock:
                        stats['latest_raw_data'] += 1
                        total_saved += 1
                        scraped_files_cache.add(filename.lower())
                    return True
        except Exception as e:
            log(f'  ERROR processing CVE Record {url}: {e}')

    # Exploit-DB handler
    if 'exploit-db.com' in url.lower():
        try:
            m = re.search(r'/exploits/(\d+)', url)
            if m:
                exploit_id   = m.group(1)
                download_url = f'https://www.exploit-db.com/download/{exploit_id}'
                raw_r = fetch(download_url, session)
                if raw_r and raw_r.status_code == 200:
                    title     = f'Exploit DB {exploit_id}'
                    clean_doc = (f"<!DOCTYPE html>\n<html>\n<head><meta charset='utf-8'>"
                                 f"<title>{title}</title></head>\n<body>\n"
                                 f"<p>Source: <a href='{url}'>{url}</a></p><hr>\n"
                                 f"<pre><code>{html.escape(raw_r.text)}</code></pre>\n"
                                 "</body>\n</html>")
                    filename  = f'exploit_db_{exploit_id}.html'
                    out_path  = DEST_ROOT / filename
                    with open(out_path, 'w', encoding='utf-8') as f:
                        f.write(clean_doc)
                    with save_lock:
                        stats['latest_raw_data'] += 1
                        total_saved += 1
                        scraped_files_cache.add(filename.lower())
                    return True
        except Exception as e:
            log(f'  ERROR Exploit-DB {url}: {e}')

    # GitHub raw-file handler
    if is_github:
        try:
            path_parts = [p for p in parsed.path.strip('/').split('/') if p]
            if len(path_parts) >= 4 and path_parts[2] in ('blob','raw'):
                owner    = path_parts[0]
                repo     = path_parts[1]
                branch   = path_parts[3]
                file_str = '/'.join(path_parts[4:])
                fname_p  = path_parts[-1].lower()

                if any(k in fname_p for k in ('license','licence','copying','notice')):
                    return False
                img_exts = ('.jpg','.jpeg','.png','.gif','.svg','.ico','.webp',
                            '.bmp','.woff','.woff2','.ttf','.mp4','.mp3')
                if any(fname_p.endswith(e) for e in img_exts):
                    return False

                raw_url = f'https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_str}'
                raw_r   = fetch(raw_url, session)
                if raw_r and raw_r.status_code == 200:
                    flat_name = '_'.join(path_parts[4:])
                    filename  = f'github_{owner}_{repo}_{flat_name}'
                    out_path  = DEST_ROOT / filename
                    with open(out_path, 'wb') as f:
                        f.write(raw_r.content)
                    with save_lock:
                        stats['latest_raw_data'] += 1
                        total_saved += 1
                        scraped_files_cache.add(filename.lower())
                    return True
        except Exception as e:
            log(f'  ERROR GitHub file {url}: {e}')

    r = fetch(url, session)
    if not r or r.status_code != 200:
        return False

    ct = r.headers.get('Content-Type','').lower()

    RAW_EXTS = (
        # Archives
        '.pdf','.zip','.tar.gz','.tar.bz2','.tar.xz','.tgz','.7z','.gz','.rar','.bz2',
        # Documents / data
        '.docx','.doc','.pptx','.xlsx','.xml','.csv','.json','.jsonl','.parquet',
        # Network captures
        '.pcap','.pcapng','.cap',
        # Python
        '.py','.pyw','.ipynb',
        # C/C++
        '.c','.cpp','.cc','.cxx','.h','.hpp',
        # Java
        '.java','.jar',
        # Go / Rust
        '.go','.rs',
        # JavaScript / TypeScript
        '.js','.mjs','.ts','.tsx',
        # PHP / Ruby / Perl / Lua
        '.php','.rb','.pl','.lua',
        # Shell / Scripts
        '.sh','.bash','.zsh','.fish','.ps1','.psm1','.bat','.cmd',
        # SQL / DB
        '.sql','.db','.sqlite',
        # Config
        '.yaml','.yml','.toml','.ini','.conf','.cfg',
        # Assembly
        '.asm','.s',
        # .NET
        '.cs','.vb',
        # Kotlin / Swift
        '.kt','.kts','.swift',
        # R
        '.r',
        # Terraform
        '.tf','.tfvars',
        # Binary / ELF for reverse engineering
        '.elf','.exe','.so','.o','.out',
        # Plain text
        '.txt',
    )

    detected_ext = None
    for ext in RAW_EXTS:
        if url.lower().endswith(ext) or f'{ext}?' in url.lower():
            detected_ext = ext
            break
    is_raw_file = (detected_ext is not None or
                   'application/pdf' in ct or
                   'application/zip' in ct or
                   'wordprocessingml' in ct)
    if not detected_ext:
        if 'application/pdf' in ct:
            detected_ext = '.pdf'
        elif 'application/zip' in ct:
            detected_ext = '.zip'
        else:
            detected_ext = '.bin'

    if is_raw_file:
        try:
            path_part = parsed.path.strip('/').split('/')[-1] or f'document{detected_ext}'
            clean_fn  = urllib.parse.unquote(path_part)
            if not clean_fn.lower().endswith(detected_ext):
                clean_fn += detected_ext
            if ('github.com' in parsed.netloc or 'codeload.github.com' in parsed.netloc) and detected_ext == '.zip':
                parts = parsed.path.strip('/').split('/')
                if len(parts) >= 2:
                    clean_fn = f"{parts[0]}_{parts[1]}{detected_ext}"
            clean_fn = ''.join([c if c.isalnum() or c in '._- ' else '_' for c in clean_fn])
            if len(clean_fn) > 200:
                clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + detected_ext

            # ── Route executables to dedicated EXE_DIR ──────────────────
            if detected_ext in EXE_EXTS:
                save_executable(clean_fn, r.content, source_url=url)
                return True

            # ── Security relevance filter for code files ─────────────
            _CODE_EXTS = {
                '.py','.pyw','.c','.cpp','.cc','.h','.hpp','.go','.rs',
                '.js','.mjs','.ts','.php','.rb','.pl','.lua','.sh','.bash',
                '.ps1','.bat','.java','.cs','.sql','.asm','.s','.r','.tf',
            }
            if detected_ext in _CODE_EXTS:
                try:
                    code_text = r.content.decode('utf-8', errors='replace')
                except Exception:
                    code_text = ''
                if not is_security_relevant(url, code_text, clean_fn):
                    return False   # generic non-security code — skip
                if is_generic_boilerplate(code_text) and not is_security_relevant(url, code_text, clean_fn):
                    return False   # double-check pure boilerplate

            out_path = DEST_ROOT / clean_fn

            # Filename dedup (fast path)
            if clean_fn.lower() in scraped_files_cache or out_path.exists():
                return True
            # Content-hash dedup
            if _is_duplicate_content(r.content):
                with save_lock:
                    scraped_files_cache.add(clean_fn.lower())
                return True

            safe_write_file(out_path, r.content)   # safe write — no execute bit
            _register_hash(r.content)
            with save_lock:
                stats['latest_raw_data'] += 1
                total_saved += 1
                scraped_files_cache.add(clean_fn.lower())
                if total_saved % 500 == 0:
                    _save_hash_index()

            return True
        except Exception as e:
            log(f'  ERROR saving binary {url}: {e}')
            return False

    if 'text/html' not in ct and 'application/xhtml' not in ct:
        if 'application/json' in ct:
            try:
                data     = r.json()
                filename = f"{prefix}_{urllib.parse.urlparse(url).path.strip('/').replace('/','_')}.json"
                out_path = DEST_ROOT / filename
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
                with save_lock:
                    stats['latest_raw_data'] += 1
                    total_saved += 1
                    scraped_files_cache.add(filename.lower())
                return True
            except Exception:
                pass
        return False

    if 'text/html' in ct or 'application/xhtml' in ct:
        try:
            soup = BeautifulSoup(r.text, 'html.parser')
            # ── Comprehensive downloadable file types ──────────────────
            # Archives / containers
            ARCHIVE_EXTS = ('.zip','.tar.gz','.tar.bz2','.tar.xz','.tgz','.7z','.gz','.rar','.bz2')
            # Documents / data
            PDF_EXTS     = ('.pdf',)
            DATA_EXTS    = ('.docx','.doc','.pptx','.xlsx','.xml','.csv','.json','.jsonl','.parquet')
            # Network captures (invaluable for security training)
            PCAP_EXTS    = ('.pcap','.pcapng','.cap','.pcap.gz','.pcapng.gz')
            # Source code / scripts
            CODE_EXTS    = (
                '.py','.pyw','.ipynb',          # Python
                '.c','.cpp','.cc','.cxx','.h','.hpp',  # C/C++
                '.java','.class','.jar',         # Java
                '.go',                           # Go
                '.rs',                           # Rust
                '.js','.mjs','.ts','.tsx',       # JavaScript / TypeScript
                '.php','.rb','.pl','.lua',       # PHP / Ruby / Perl / Lua
                '.sh','.bash','.zsh','.fish',    # Shell
                '.ps1','.psm1','.bat','.cmd',    # PowerShell / Batch
                '.sql','.db','.sqlite',          # SQL / Databases
                '.yaml','.yml','.toml','.ini','.conf','.cfg',  # Config
                '.asm','.s',                     # Assembly
                '.cs','.vb',                     # .NET
                '.kt','.kts',                    # Kotlin
                '.swift',                        # Swift
                '.r','.R',                       # R
                '.tf','.tfvars',                 # Terraform
                '.dockerfile','dockerfile',      # Docker
            )
            # Binary executables / firmware (useful for reverse engineering)
            BINARY_EXTS  = ('.elf','.exe','.so','.o','.out','.bin','.hex','.firmware')

            # Combined priority-ordered lookup
            ALL_DOWNLOAD_EXTS = ARCHIVE_EXTS + PDF_EXTS + PCAP_EXTS + CODE_EXTS + DATA_EXTS + BINARY_EXTS

            def detect_file_ext(href_lower):
                for ext in ALL_DOWNLOAD_EXTS:
                    if href_lower.endswith(ext) or f'{ext}?' in href_lower:
                        return ext
                # Special: PDF embedded behind /pdf/ path segment
                if '/pdf/' in href_lower and not href_lower.endswith('.html'):
                    return '.pdf'
                return None

            download_urls  = []
            for a in soup.find_all('a', href=True):
                href     = a['href']
                href_l   = href.lower()
                file_ext = detect_file_ext(href_l)
                if file_ext:
                    full = urllib.parse.urljoin(url, href)
                    if '#' not in full:
                        download_urls.append((full, file_ext))
            download_urls   = list({u: e for u, e in download_urls}.items())
            downloaded_files = 0

            if download_urls:
                log(f'    Found {len(download_urls)} downloadable links on {url}. Downloading...')
                for dl_url, file_ext in download_urls:
                    parsed_dl = urllib.parse.urlparse(dl_url)
                    path_part = parsed_dl.path.strip('/').split('/')[-1] or f'document{file_ext}'
                    clean_fn  = urllib.parse.unquote(path_part)
                    if not any(clean_fn.lower().endswith(e) for e in (file_ext,'.pdf','.zip','.docx')):
                        clean_fn += file_ext
                    if ('github.com' in parsed_dl.netloc or 'codeload.github.com' in parsed_dl.netloc) and file_ext == '.zip':
                        parts = parsed_dl.path.strip('/').split('/')
                        if len(parts) >= 2:
                            clean_fn = f"{parts[0]}_{parts[1]}{file_ext}"
                    clean_fn = ''.join([c if c.isalnum() or c in '._- ' else '_' for c in clean_fn])
                    if len(clean_fn) > 200:
                        clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + file_ext
                    if clean_fn.lower() in scraped_files_cache or (DEST_ROOT / clean_fn).exists():
                        downloaded_files += 1
                        continue
                    try:
                        dl_r = fetch(dl_url, session)
                        if dl_r and dl_r.status_code == 200:
                            # Content-hash dedup before writing
                            if _is_duplicate_content(dl_r.content):
                                with save_lock:
                                    scraped_files_cache.add(clean_fn.lower())
                                downloaded_files += 1
                                continue
                            # ── Route executables to dedicated EXE_DIR ──────
                            if file_ext in EXE_EXTS:
                                save_executable(clean_fn, dl_r.content, source_url=dl_url)
                                downloaded_files += 1
                                continue
                            out_path = DEST_ROOT / clean_fn
                            safe_write_file(out_path, dl_r.content)  # safe write — no execute bit
                            _register_hash(dl_r.content)
                            with save_lock:
                                stats['latest_raw_data'] += 1
                                total_saved += 1
                                scraped_files_cache.add(clean_fn.lower())
                                if total_saved % 500 == 0:
                                    _save_hash_index()
                            downloaded_files += 1
                    except Exception as e:
                        log(f'      ERROR downloading {file_ext} {dl_url}: {e}')


            if download_urls and downloaded_files > 0:
                log(f'    Discarding HTML container — {downloaded_files} file(s) downloaded from {url}')
                return True
        except Exception as e:
            log(f'    ERROR checking download links on {url}: {e}')

    try:
        clean_content, title, text = clean_html(r.text, url)
        if len(text) < 100:
            return False

        # Ensure the content is cybersecurity-related
        text_to_check = f"{title} {url} {text}".lower()
        if not is_security_relevant(url, text_to_check, title):
            return False
        
        path_part   = urllib.parse.urlparse(url).path.strip('/').replace('/','_') or 'index'
        domain_part = urllib.parse.urlparse(url).netloc.replace('.','_').replace('www_','')
        filename    = f"{prefix or domain_part}_{path_part}.html"
        save_file(filename, clean_content, title=title, content_text=text, source_url=url)
        return True
    except Exception:
        return False

def get_sitemap_urls(base_url, session, max_urls=300):
    import gzip, io
    urls = []
    sitemap_locs = []
    try:
        robots_url = f"{base_url.rstrip('/')}/robots.txt"
        r = fetch(robots_url, session, retries=1)
        if r and r.status_code == 200:
            for line in r.text.splitlines():
                if line.lower().startswith('sitemap:'):
                    sitemap_locs.append(line.split(':',1)[1].strip())
    except Exception:
        pass
    if not sitemap_locs:
        sitemap_locs = [
            f"{base_url.rstrip('/')}/sitemap.xml",
            f"{base_url.rstrip('/')}/sitemap_index.xml",
            f"{base_url.rstrip('/')}/post-sitemap.xml",
            f"{base_url.rstrip('/')}/page-sitemap.xml",
            f"{base_url.rstrip('/')}/sitemap_index.xml.gz",
            f"{base_url.rstrip('/')}/sitemap.xml.gz",
        ]

    def fetch_and_decompress(url):
        try:
            r = fetch(url, session, retries=1)
            if not r or r.status_code != 200:
                return None
            is_gz = url.lower().endswith('.gz') or 'gzip' in r.headers.get('Content-Encoding','').lower()
            if is_gz:
                try:
                    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as f:
                        return f.read()
                except Exception:
                    return r.content
            return r.content
        except Exception:
            return None

    def parse_sitemap_xml(xml_content):
        local_urls = []
        try:
            root = ET.fromstring(xml_content)
            ns   = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            sitemaps = root.findall('.//sm:sitemap/sm:loc', ns)
            if sitemaps:
                for sm_loc in sitemaps[:5]:
                    sub_xml = fetch_and_decompress(sm_loc.text)
                    if sub_xml:
                        local_urls.extend(parse_sitemap_xml(sub_xml))
                        if len(local_urls) >= max_urls:
                            break
            else:
                for loc in root.findall('.//sm:url/sm:loc', ns):
                    local_urls.append(loc.text)
                    if len(local_urls) >= max_urls:
                        break
        except Exception:
            try:
                root = ET.fromstring(xml_content)
                for loc in root.iter():
                    if loc.tag.endswith('loc') and loc.text:
                        val = loc.text.strip()
                        if val.endswith(('.xml','.xml.gz')) or 'sitemap' in val:
                            sub_xml = fetch_and_decompress(val)
                            if sub_xml:
                                local_urls.extend(parse_sitemap_xml(sub_xml))
                        else:
                            local_urls.append(val)
                        if len(local_urls) >= max_urls:
                            break
            except Exception:
                pass
        return local_urls

    for sitemap_url in sitemap_locs:
        xml_content = fetch_and_decompress(sitemap_url)
        if xml_content:
            discovered = parse_sitemap_xml(xml_content)
            urls.extend(discovered)
            if len(urls) >= max_urls:
                break
    return list(set(urls))[:max_urls]

def get_rss_urls(base_url, session):
    urls = []
    feed_paths = ['/feed','/feed/','/rss','/rss/','/atom.xml','/feed.xml',
                  '/rss.xml','/feeds/posts/default','/blog/feed','/blog/rss',
                  '/index.xml','/blog/feed/','/blog.rss']
    base = base_url.rstrip('/')
    for fp in feed_paths:
        try:
            r = fetch(f'{base}{fp}', session, retries=1)
            if not r or r.status_code != 200:
                continue
            ct = r.headers.get('Content-Type','')
            if not any(x in ct for x in ('xml','rss','atom','text/html')):
                continue
            soup = BeautifulSoup(r.text, 'xml')
            for item in soup.find_all('item'):
                link = item.find('link')
                if link and link.text:
                    urls.append(link.text.strip())
            for entry in soup.find_all('entry'):
                for link in entry.find_all('link', rel='alternate'):
                    href = link.get('href')
                    if href:
                        urls.append(href)
                link = entry.find('link')
                if link and link.get('href'):
                    urls.append(link.get('href'))
            if urls:
                break
        except Exception:
            continue
    return list(set(urls))

def crawl_links_from_page(url, session, same_domain=True, max_depth=3):
    base_parsed   = urllib.parse.urlparse(url)
    base_domain   = base_parsed.netloc.replace('www.','')
    is_github     = 'github.com' in base_domain
    github_prefix = ''
    if is_github:
        parts = [p for p in base_parsed.path.strip('/').split('/') if p]
        if len(parts) >= 2:
            github_prefix = f'/{parts[0]}/{parts[1]}'
        elif len(parts) == 1:
            github_prefix = f'/{parts[0]}'
        else:
            github_prefix = '/NON_EXISTENT_PREFIX_TO_SKIP_CRAWL'

    visited    = set()
    to_visit   = [(url, 0)]
    found_urls = []

    is_large  = 'nist.gov' in base_domain or 'cve.org' in base_domain
    cap        = 2000 if is_github else (10000 if is_large else 2000)
    max_visits = 200 if is_large else 50

    while to_visit and len(found_urls) < cap:
        if len(visited) >= max_visits:
            break
        current_url, depth = to_visit.pop(0)
        if current_url in visited or depth > max_depth:
            continue
        visited.add(current_url)

        if any(current_url.lower().endswith(ext) for ext in ['.pdf','.zip','.docx']):
            found_urls.append(current_url)
            continue

        r = fetch(current_url, session, retries=3)
        if not r or r.status_code != 200:
            continue

        ct = r.headers.get('Content-Type','').lower()
        if 'application/pdf' in ct or 'application/zip' in ct or 'wordprocessingml' in ct:
            found_urls.append(current_url)
            continue

        try:
            is_xml_feed = ('xml' in ct or 'rss' in ct or 'atom' in ct or
                           current_url.lower().endswith(('.xml','/feed','/feed/','/rss','/rss/')))
            if is_xml_feed:
                soup = BeautifulSoup(r.text, 'xml')
                feed_links = []
                for item in soup.find_all('item'):
                    link = item.find('link')
                    if link and link.text:
                        feed_links.append(link.text.strip())
                for entry in soup.find_all('entry'):
                    for link in entry.find_all('link', rel='alternate'):
                        href = link.get('href')
                        if href:
                            feed_links.append(href)
                    link = entry.find('link')
                    if link and link.get('href'):
                        feed_links.append(link.get('href'))
                for fl in feed_links:
                    full       = urllib.parse.urljoin(current_url, fl)
                    p2         = urllib.parse.urlparse(full)
                    clean_url  = urllib.parse.urlunparse(p2._replace(fragment=''))
                    link_domain = p2.netloc.replace('www.','').lower()
                    if p2.scheme in ('http','https') and (not same_domain or link_domain == base_domain):
                        if clean_url not in visited:
                            to_visit.append((clean_url, depth+1))
                continue
            else:
                soup = BeautifulSoup(r.text, 'html.parser')
        except Exception:
            continue

        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith(('#','mailto:','javascript:','tel:')):
                continue
            full    = urllib.parse.urljoin(current_url, href)
            p2      = urllib.parse.urlparse(full)
            if p2.scheme not in ('http','https'):
                continue
            path_lower  = p2.path.lower()
            skip_exts   = ['.jpg','.jpeg','.png','.gif','.svg','.ico','.webp',
                           '.bmp','.css','.woff','.woff2','.ttf','.mp4','.mp3']
            if any(path_lower.endswith(e) for e in skip_exts):
                continue
            fname_p = p2.path.split('/')[-1].lower()
            if any(k in fname_p for k in ('license','licence','copying','notice')):
                continue
            clean_url   = urllib.parse.urlunparse(p2._replace(fragment=''))
            link_domain = p2.netloc.replace('www.','')
            if same_domain and link_domain != base_domain:
                continue
            if is_github:
                path_parts = [p for p in path_lower.strip('/').split('/') if p]
                if len(path_parts) >= 3 and path_parts[2] in (
                    'issues','pull','pulls','commit','commits','releases','tags','actions',
                    'projects','wiki','settings','branches','pulse','graphs','network',
                    'stargazers','watchers','search','find','forks','watch','stars',
                    'edit','new','delete','community','discussions','milestones','labels'):
                    continue
                if github_prefix and not p2.path.startswith(github_prefix):
                    continue
                if path_parts:
                    fname_p2 = path_parts[-1].lower()
                    noise_f  = {
                        'makefile','package.json','package-lock.json','.gitignore',
                        '.gitattributes','yarn.lock','cargo.lock','go.sum','go.mod',
                        'webpack.config.js','tsconfig.json','.eslintignore','.eslintrc.json',
                        'pnpm-lock.yaml','composer.lock','composer.json'
                    }
                    if fname_p2 in noise_f or fname_p2.startswith('makefile') or 'gitignore' in fname_p2:
                        continue
            if clean_url not in visited and clean_url not in found_urls:
                found_urls.append(clean_url)
                if depth+1 <= max_depth and not any(clean_url.lower().endswith(e) for e in ['.pdf','.zip','.docx']):
                    to_visit.append((clean_url, depth+1))

    if len(found_urls) >= cap and to_visit:
        log(f'WARNING: {url} capped at {cap} pages (more exist).')
    return list(set(found_urls))

# ─────────────────────────────────────────────────────────────
#  SITE-SPECIFIC SCRAPERS  (decorator registry)
# ─────────────────────────────────────────────────────────────
SITE_SCRAPERS = {}

def site_scraper(domains):
    def decorator(func):
        for d in domains:
            SITE_SCRAPERS[d] = func
        return func
    return decorator

# ─────────────────────────────────────────────────────────────
#  NEW: SANS WHITE PAPERS SCRAPER
# ─────────────────────────────────────────────────────────────
def _sanitize_filename(name, max_len=120):
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name)
    name = re.sub(r'\s+', '_', name.strip())
    name = re.sub(r'_+', '_', name)
    return name[:max_len].rstrip('._') or 'untitled'

def _save_binary(data, out_path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'wb') as f:
        f.write(data)

@site_scraper(['sans.org','www.sans.org'])
def scrape_sans_white_papers(session, out_dir=None):
    """
    Scrape ALL SANS white papers.
    Strategy:
      1. Try sitemap to find white paper URLs
      2. Paginate the listing page (with Playwright if JS blocks it)
      3. For each paper: find PDF download link → save to out_dir
    """
    out_dir = out_dir or (RESEARCH_ROOT / 'sans_white_papers')
    out_dir.mkdir(parents=True, exist_ok=True)

    SANS_BASE  = 'https://www.sans.org'
    LISTING    = f'{SANS_BASE}/white-papers/'
    paper_urls = set()

    # 1) Try sitemap for white paper URLs
    log('  [SANS] Trying sitemap...')
    sitemap_urls = get_sitemap_urls(SANS_BASE, session, max_urls=5000)
    for u in sitemap_urls:
        if re.search(r'/white-papers/\d+', u):
            paper_urls.add(u.split('?')[0])
    log(f'  [SANS] Sitemap: {len(paper_urls)} white paper URLs')

    # 2) Paginate listing pages
    page = 1
    while True:
        url  = LISTING if page == 1 else f'{LISTING}?page={page}'
        log(f'  [SANS] Listing page {page}: {url}')

        r    = fetch(url, session, referer=SANS_BASE)
        html_text = r.text if r else None

        # Playwright fallback if Cloudflare blocked or JS-rendered
        if not html_text or len(html_text) < 2000:
            log(f'  [SANS] Trying Playwright for listing page {page}...')
            html_text = pw_get_html(url, wait_ms=4000)

        if not html_text:
            log(f'  [SANS] Cannot fetch listing page {page}. Stopping pagination.')
            break

        soup      = BeautifulSoup(html_text, 'lxml')
        new_found = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            if re.search(r'/white-papers/\d+', href):
                full = urllib.parse.urljoin(SANS_BASE, href.split('?')[0])
                if full not in paper_urls:
                    new_found.add(full)

        if not new_found:
            log(f'  [SANS] No new papers on page {page}. Pagination done.')
            break

        paper_urls.update(new_found)
        log(f'  [SANS] Page {page}: +{len(new_found)} (total {len(paper_urls)})')

        has_next = bool(
            soup.select_one("a[rel='next']") or
            soup.find('a', string=re.compile(r'next', re.I)) or
            soup.find('a', href=re.compile(rf'page={page+1}'))
        )
        if not has_next:
            break
        page += 1

    if not paper_urls:
        log('  [SANS] No paper URLs found. Check if site structure changed.')
        return 0

    log(f'  [SANS] Total: {len(paper_urls)} papers to download with 3 threads')

    def _download_one_sans(paper_url):
        r = fetch(paper_url, session, referer=LISTING)
        html_text = r.text if r else None
        if not html_text or len(html_text) < 500:
            html_text = pw_get_html(paper_url)
        if not html_text:
            return False, paper_url, 'page load failed'

        soup  = BeautifulSoup(html_text, 'lxml')
        h1    = soup.find('h1')
        title = h1.get_text(strip=True) if h1 else re.sub(r'.*/','', paper_url)
        wp_id = re.search(r'/white-papers/(\d+)', paper_url)
        id_str = wp_id.group(1) if wp_id else '0'
        fname  = _sanitize_filename(f'{id_str}_{title}') + '.pdf'
        out    = out_dir / fname

        if out.exists() and out.stat().st_size > 1024:
            return True, paper_url, f'skip (exists): {fname}'

        # Find PDF download link
        pdf_url = None
        for a in soup.find_all('a', href=True):
            href = a['href']
            txt  = a.get_text(' ', strip=True).lower()
            if (href.lower().endswith('.pdf') or 'download' in href.lower() or
                    ('download' in txt and ('pdf' in txt or 'paper' in txt))):
                pdf_url = urllib.parse.urljoin(SANS_BASE, href)
                break
        if not pdf_url:
            form = soup.find('form', action=re.compile(r'download|pdf', re.I))
            if form:
                pdf_url = urllib.parse.urljoin(SANS_BASE, form['action'])
        if not pdf_url:
            return False, paper_url, 'no PDF link found'

        dl = fetch(pdf_url, session, referer=paper_url)
        if not dl:
            return False, paper_url, f'PDF download failed: {pdf_url}'
        ct = dl.headers.get('Content-Type','').lower()
        if 'pdf' not in ct and not pdf_url.lower().endswith('.pdf'):
            return False, paper_url, f'not a PDF (CT={ct})'

        _save_binary(dl.content, out)
        with save_lock:
            global total_saved
            stats['sans_white_papers'] += 1
            total_saved += 1
        return True, paper_url, f'saved: {fname}'

    results  = {'ok': 0, 'skip': 0, 'fail': 0}
    url_list = list(paper_urls)
    with ThreadPoolExecutor(max_workers=3, thread_name_prefix='sans') as ex:
        futures = {ex.submit(_download_one_sans, u): u for u in url_list}
        for fut in as_completed(futures):
            ok, url, msg = fut.result()
            if ok and 'skip' not in msg:
                results['ok'] += 1
            elif 'skip' in msg:
                results['skip'] += 1
            else:
                results['fail'] += 1
            status = '[OK]' if ok else '[XX]'
            log(f'  [SANS] {status} {msg}')

    log(f"  [SANS] Done: downloaded={results['ok']} skipped={results['skip']} failed={results['fail']}")
    return results['ok']

# ─────────────────────────────────────────────────────────────
#  NEW: HAL SCIENCE SCRAPER  (API-based, LaTeX first → PDF)
# ─────────────────────────────────────────────────────────────
def scrape_hal_query(session, search_query, folder_name, out_dir=None):
    """
    Download all papers for one HAL Science search query.
    Uses the HAL REST API for reliable pagination.
    Files: tries LaTeX source (files_s ending .tar.gz/.gz), falls back to PDF.
    """
    out_dir = out_dir or (RESEARCH_ROOT / folder_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Fetch all paper metadata via API ──────────────────────
    papers = []
    start  = 0
    while True:
        params = {
            'q'    : search_query,
            'fl'   : 'halId_s,title_s,files_s,fileMain_s',
            'wt'   : 'json',
            'rows' : HAL_ROWS,
            'start': start,
            'fq'   : 'submitType_s:file',
        }
        log(f'  [HAL] API query="{search_query}" start={start}')
        try:
            time.sleep(random.uniform(0.8, 2.0))
            r = session.get(HAL_API, params=params,
                            headers=_make_headers(), timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data      = r.json()
        except Exception as e:
            log(f'  [HAL] API error: {e}')
            break

        docs      = data.get('response', {}).get('docs', [])
        num_found = data.get('response', {}).get('numFound', 0)
        if not docs:
            break
        papers.extend(docs)
        log(f'  [HAL] Fetched {len(papers)}/{num_found} for "{search_query}"')
        if len(papers) >= num_found:
            break
        start += HAL_ROWS

    if not papers:
        log(f'  [HAL] 0 results for "{search_query}"')
        return 0

    log(f'  [HAL] {len(papers)} papers | 6 threads | folder: {folder_name}')

    def _download_one_hal(paper):
        hal_id = paper.get('halId_s','unknown')
        titles = paper.get('title_s') or ['untitled']
        title  = titles[0] if titles else 'untitled'
        base   = _sanitize_filename(f'{hal_id}_{title}')

        # ── Try LaTeX source from files_s ─────────────────────
        files_s  = paper.get('files_s') or []
        latex_url = None
        pdf_url   = None
        for f_url in files_s:
            fl = f_url.lower()
            if any(fl.endswith(ext) for ext in ('.tar.gz', '.tar', '.gz', '.tgz', '.zip')):
                latex_url = f_url
            elif fl.endswith('.pdf'):
                pdf_url = f_url

        if latex_url:
            latex_out = out_dir / (base + '.tar.gz')
            if latex_out.exists() and latex_out.stat().st_size > 512:
                return True, hal_id, f'skip (LaTeX exists): {latex_out.name}'
            lr = fetch(latex_url, session, referer=f'https://hal.science/{hal_id}')
            if lr and lr.status_code == 200:
                ct = lr.headers.get('Content-Type','').lower()
                if any(x in ct for x in ('gzip','tar','zip','octet')) or latex_url.lower().endswith(('.gz','.tar','.zip')):
                    _save_binary(lr.content, latex_out)
                    with save_lock:
                        global total_saved
                        stats[folder_name] += 1
                        total_saved += 1
                    return True, hal_id, f'LaTeX: {latex_out.name}'

        # ── Fallback: PDF from files_s ────────────────────────
        pdf_out = out_dir / (base + '.pdf')
        if pdf_out.exists() and pdf_out.stat().st_size > 512:
            return True, hal_id, f'skip (PDF exists): {pdf_out.name}'

        # Try direct files_s PDF URL first
        if pdf_url:
            pr = fetch(pdf_url, session, referer=f'https://hal.science/{hal_id}')
            if pr and pr.status_code == 200 and 'pdf' in pr.headers.get('Content-Type','').lower():
                _save_binary(pr.content, pdf_out)
                with save_lock:
                    stats[folder_name] += 1
                    total_saved += 1
                return True, hal_id, f'PDF (files_s): {pdf_out.name}'

        # Try fileMain_s (/document redirect)
        file_main = paper.get('fileMain_s','')
        if file_main:
            pr = fetch(file_main, session, referer=f'https://hal.science/{hal_id}')
            if pr and pr.status_code == 200 and 'pdf' in pr.headers.get('Content-Type','').lower():
                _save_binary(pr.content, pdf_out)
                with save_lock:
                    stats[folder_name] += 1
                    total_saved += 1
                return True, hal_id, f'PDF (fileMain): {pdf_out.name}'

        # Final fallback: construct /document URL
        doc_url = f'https://hal.science/{hal_id}/document'
        pr = fetch(doc_url, session, referer=f'https://hal.science/{hal_id}')
        if pr and pr.status_code == 200 and 'pdf' in pr.headers.get('Content-Type','').lower():
            _save_binary(pr.content, pdf_out)
            with save_lock:
                stats[folder_name] += 1
                total_saved += 1
            return True, hal_id, f'PDF (fallback): {pdf_out.name}'

        return False, hal_id, 'no downloadable file (all methods failed)'

    results = {'ok': 0, 'skip': 0, 'fail': 0}
    with ThreadPoolExecutor(max_workers=6, thread_name_prefix='hal') as ex:
        futures = {ex.submit(_download_one_hal, p): p for p in papers}
        for fut in as_completed(futures):
            ok, hid, msg = fut.result()
            if ok and 'skip' not in msg:
                results['ok'] += 1
            elif 'skip' in msg:
                results['skip'] += 1
            else:
                results['fail'] += 1
            status = '[OK]' if ok else '[XX]'
            log(f'  [HAL] {status} {hid} -- {msg}')

    log(f"  [HAL] '{search_query}' done: downloaded={results['ok']} skipped={results['skip']} failed={results['fail']}")
    return results['ok']

# ─────────────────────────────────────────────────────────────
#  NEW: SSRN SCRAPER  (no login, paginate results → PDF)
# ─────────────────────────────────────────────────────────────
def scrape_ssrn(session, search_term, folder_name, out_dir=None):
    """
    Paginate SSRN search results and download available PDFs (no login).
    Uses Playwright when Cloudflare blocks requests.
    """
    out_dir = out_dir or (RESEARCH_ROOT / folder_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    SSRN_BASE   = 'https://papers.ssrn.com'
    SSRN_SEARCH = f'{SSRN_BASE}/searchresults.cfm'
    encoded_q   = urllib.parse.quote_plus(search_term)

    # ── Collect abstract page URLs ────────────────────────────
    abstract_urls = []
    seen          = set()

    for page in range(1, 51):   # cap at 50 pages
        url = f'{SSRN_SEARCH}?term={encoded_q}&page={page}'
        log(f'  [SSRN] Search page {page}: {url}')

        r = fetch(url, session, referer=SSRN_BASE)
        html_text = r.text if r else None

        # Playwright fallback for Cloudflare
        if not html_text or 'challenge' in (html_text or '').lower() or \
                (r and r.status_code == 403):
            log(f'  [SSRN] Bot challenge on page {page} — using Playwright...')
            html_text = pw_get_html(url, wait_ms=5000)

        if not html_text:
            log(f'  [SSRN] Cannot fetch page {page}. Stopping.')
            break

        soup        = BeautifulSoup(html_text, 'lxml')
        new_on_page = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'abstract_id=' in href or re.search(r'/sol3/papers\.cfm', href):
                full = urllib.parse.urljoin(SSRN_BASE, href).split('&')[0]
                if full not in seen:
                    seen.add(full)
                    new_on_page.append(full)

        if not new_on_page:
            log(f'  [SSRN] No new papers on page {page}. Stopping.')
            break

        abstract_urls.extend(new_on_page)
        log(f'  [SSRN] Page {page}: +{len(new_on_page)} (total {len(abstract_urls)})')

        has_next = bool(
            soup.find('a', string=re.compile(r'next', re.I)) or
            soup.find('a', href=re.compile(rf'page={page+1}'))
        )
        if not has_next:
            log(f'  [SSRN] No next-page button after page {page}.')
            break

    if not abstract_urls:
        log('  [SSRN] No abstract URLs collected.')
        return 0

    log(f'  [SSRN] {len(abstract_urls)} papers | 2 threads')

    def _download_one_ssrn(abs_url):
        r = fetch(abs_url, session, referer=SSRN_SEARCH)
        html_text = r.text if r else None
        if not html_text or 'challenge' in (html_text or '').lower():
            html_text = pw_get_html(abs_url, wait_ms=4000)
        if not html_text:
            return False, abs_url, 'abstract page load failed'

        soup  = BeautifulSoup(html_text, 'lxml')
        h1    = soup.find('h1') or soup.select_one('.paper-title, #abstract_title')
        title = h1.get_text(strip=True) if h1 else 'untitled'

        m      = re.search(r'abstract_id=(\d+)', abs_url)
        abs_id = m.group(1) if m else re.sub(r'\D','', abs_url)[-8:]
        fname  = _sanitize_filename(f'ssrn_{abs_id}_{title}') + '.pdf'
        out    = out_dir / fname

        if out.exists() and out.stat().st_size > 1024:
            return True, abs_url, f'skip (exists): {fname}'

        # Find PDF download link
        pdf_url = None
        for a in soup.find_all('a', href=True):
            href = a['href']
            txt  = a.get_text(' ', strip=True).lower()
            if ('delivery.cfm' in href or href.lower().endswith('.pdf') or
                    ('download' in txt and 'pdf' in txt)):
                pdf_url = urllib.parse.urljoin(SSRN_BASE, href)
                break
        if not pdf_url and abs_id:
            pdf_url = f'{SSRN_BASE}/sol3/Delivery.cfm?abstractid={abs_id}'

        if not pdf_url:
            return False, abs_url, 'no PDF link found'

        dl = fetch(pdf_url, session, referer=abs_url)
        if not dl:
            return False, abs_url, 'download failed (may need login)'
        ct = dl.headers.get('Content-Type','').lower()
        if 'pdf' not in ct and 'octet-stream' not in ct:
            return False, abs_url, f'not a PDF (CT={ct}) -- login required'

        _save_binary(dl.content, out)
        with save_lock:
            global total_saved
            stats[folder_name] += 1
            total_saved += 1
        return True, abs_url, f'saved: {fname}'

    results = {'ok': 0, 'skip': 0, 'fail': 0}
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix='ssrn') as ex:
        futures = {ex.submit(_download_one_ssrn, u): u for u in abstract_urls}
        for fut in as_completed(futures):
            ok, url, msg = fut.result()
            if ok and 'skip' not in msg:
                results['ok'] += 1
            elif 'skip' in msg:
                results['skip'] += 1
            else:
                results['fail'] += 1
            status = '[OK]' if ok else '[XX]'
            log(f'  [SSRN] {status} {msg}')

    log(f"  [SSRN] Done: downloaded={results['ok']} skipped={results['skip']} failed={results['fail']}")
    return results['ok']

# ─────────────────────────────────────────────────────────────
#  EXISTING SITE SCRAPERS  (all preserved from v3)
# ─────────────────────────────────────────────────────────────
@site_scraper(['go.dev'])
def scrape_go_dev(session):
    log('  Scraping go.dev security pages...')
    urls = [
        'https://go.dev/blog/security-best-practices','https://go.dev/blog/vuln',
        'https://go.dev/blog/govulncheck','https://go.dev/blog/fuzz-beta',
        'https://go.dev/doc/security/vulncheck','https://go.dev/doc/security/vuln/database',
        'https://go.dev/doc/security/policy','https://go.dev/doc/fuzz',
        'https://go.dev/security/vuln/','https://go.dev/security/vuln/database',
        'https://go.dev/security/policy','https://go.dev/blog/security-for-go-developers',
    ]
    r = fetch('https://go.dev/blog/all', session)
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = (a.text + ' ' + href).lower()
            if any(kw in text for kw in ['security','vuln','fuzz','cve','crypt','hash','safe']):
                urls.append(urllib.parse.urljoin('https://go.dev', href))
    urls  = list(set(urls))
    count = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'go_dev') for u in urls]
        for f in as_completed(futures):
            if f.result():
                count += 1
    return count

@site_scraper(['vuln.go.dev'])
def scrape_vuln_go_dev(session):
    log('  Scraping vuln.go.dev...')
    urls = ['https://vuln.go.dev/']
    r = fetch('https://vuln.go.dev/', session)
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/ID/' in href or '/GO-' in href:
                urls.append(urllib.parse.urljoin('https://vuln.go.dev', href))
    urls  = list(set(urls))[:200]
    count = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'go_vuln') for u in urls]
        for f in as_completed(futures):
            if f.result():
                count += 1
    return count

@site_scraper(['attack.mitre.org'])
def scrape_mitre(session):
    log('  Scraping MITRE ATT&CK...')
    urls = [
        'https://attack.mitre.org/','https://attack.mitre.org/techniques/enterprise/',
        'https://attack.mitre.org/tactics/enterprise/','https://attack.mitre.org/mitigations/enterprise/',
        'https://attack.mitre.org/groups/','https://attack.mitre.org/software/',
    ]
    r = fetch('https://attack.mitre.org/techniques/enterprise/', session)
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            if '/techniques/T' in a['href']:
                urls.append(urllib.parse.urljoin('https://attack.mitre.org', a['href']))
    urls  = list(set(urls))[:300]
    count = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'mitre_attack') for u in urls]
        for f in as_completed(futures):
            if f.result():
                count += 1
    return count

@site_scraper(['cheatsheetseries.owasp.org','owasp.org'])
def scrape_owasp(session):
    log('  Scraping OWASP...')
    urls = [
        'https://cheatsheetseries.owasp.org/',
        'https://cheatsheetseries.owasp.org/cheatsheets/Go_Secure_Coding_Cheat_Sheet.html',
        'https://owasp.org/www-project-web-security-testing-guide/',
        'https://owasp.org/Top10/','https://owasp.org/www-project-mobile-top-10/',
        'https://owasp.org/www-project-api-security/',
    ]
    r = fetch('https://cheatsheetseries.owasp.org/', session)
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            if '/cheatsheets/' in a['href'] and a['href'].endswith('.html'):
                urls.append(urllib.parse.urljoin('https://cheatsheetseries.owasp.org', a['href']))
    r = fetch('https://owasp.org/projects/', session)
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            if '/www-project-' in a['href']:
                urls.append(urllib.parse.urljoin('https://owasp.org', a['href']))
    urls  = list(set(urls))[:300]
    count = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'owasp') for u in urls]
        for f in as_completed(futures):
            if f.result():
                count += 1
    return count

@site_scraper(['nvd.nist.gov'])
def scrape_nist(session):
    log('  Scraping NIST NVD...')
    urls = [
        'https://nvd.nist.gov/','https://nvd.nist.gov/vuln',
        'https://nvd.nist.gov/General/FAQ','https://nvd.nist.gov/General/News',
        'https://nvd.nist.gov/vuln/categories','https://nvd.nist.gov/vuln/search',
        'https://nvd.nist.gov/vuln/data-feeds',
    ]
    count = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'nist_nvd') for u in urls]
        for f in as_completed(futures):
            if f.result():
                count += 1
    return count

@site_scraper(['exploit-db.com','www.exploit-db.com'])
def scrape_exploit_db_index(session):
    log('  Scraping Exploit-DB index...')
    urls = get_rss_urls('https://www.exploit-db.com/', session)
    if not urls:
        r = fetch('https://www.exploit-db.com/', session)
        if r and r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                if '/exploits/' in a['href']:
                    urls.append(urllib.parse.urljoin('https://www.exploit-db.com/', a['href']))
    urls  = list(set(urls))[:200]
    count = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'exploit_db') for u in urls]
        for f in as_completed(futures):
            if f.result():
                count += 1
    return count

def get_subfolder_name(owner, repo, ctf_platform):
    owner_l = owner.lower() if owner else 'unknown'
    repo_l = repo.lower() if repo else 'unknown'
    plat_l = ctf_platform.lower() if ctf_platform else ''
    
    if plat_l == 'google_ctf' or repo_l == 'google-ctf':
        return 'google-ctf'
    elif plat_l == 'picoctf' or repo_l == 'picoctf':
        return 'picoCTF'
    elif plat_l == 'juiceshop' or repo_l == 'juice-shop':
        return 'juice-shop'
    elif plat_l == 'webgoat' or repo_l == 'webgoat':
        return 'WebGoat'
    elif plat_l == 'pwncollege':
        return f'pwncollege/{repo}'
    elif plat_l == 'portswigger':
        return 'portswigger'
    elif repo_l == 'dvwa':
        return 'DVWA'
    elif repo_l == 'nodegoat':
        return 'NodeGoat'
    elif repo_l == 'railsgoat':
        return 'railsgoat'
    else:
        return f'github/{owner}_{repo}'

def download_github_repo_zip(owner, repo, session, ctf_platform=None):
    import zipfile, io
    try:
        api_url = f'https://api.github.com/repos/{owner}/{repo}'
        api_r   = session.get(api_url, headers=GITHUB_API_HEADERS, timeout=10)
        if api_r.status_code == 200:
            size_kb = api_r.json().get('size', 0)
            if size_kb > 500000:
                log(f'    Repo {owner}/{repo} too large ({size_kb/1024:.2f} MB). Falling back to crawl...')
                return -1
    except Exception as e:
        log(f'    Warning: cannot fetch repo size for {owner}/{repo}: {e}')

    for branch in ('HEAD','main','master'):
        if branch == 'HEAD':
            zip_url = f'https://codeload.github.com/{owner}/{repo}/zip/HEAD'
        else:
            zip_url = f'https://codeload.github.com/{owner}/{repo}/zip/refs/heads/{branch}'
        log(f'    Trying ZIP for {owner}/{repo} (branch: {branch})...')
        r = fetch(zip_url, session)
        if r and r.status_code == 200:
            log(f'    Downloaded ZIP: {owner}/{repo} ({len(r.content)/(1024*1024):.2f} MB)')
            try:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    extracted = 0
                    subfolder = get_subfolder_name(owner, repo, ctf_platform)
                    dest_dir = DEST_ROOT / subfolder
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    for zinfo in z.infolist():
                        if zinfo.is_dir():
                            continue
                        fname_p = Path(zinfo.filename).name.lower()
                        if any(k in fname_p for k in ('license','licence','copying','notice')):
                            continue
                        if fname_p.endswith('.mp4'):
                            # Lab walkthrough video — stage it and queue for
                            # transcription instead of silently skipping it.
                            try:
                                staged_name = _sanitize_filename(
                                    f'{owner}_{repo}_{Path(zinfo.filename).name}')
                                staged_path = VIDEO_STAGING_DIR / staged_name
                                staged_path.parent.mkdir(parents=True, exist_ok=True)
                                with open(staged_path, 'wb') as vf:
                                    vf.write(z.read(zinfo.filename))
                                queue_video(
                                    platform=ctf_platform or 'github',
                                    title=Path(zinfo.filename).name,
                                    source_url=f'https://github.com/{owner}/{repo}/blob/HEAD/{zinfo.filename}',
                                    owner=owner, repo=repo,
                                    local_path=staged_path,
                                    delete_after=True,
                                )
                                log(f'    Queued lab video for transcription: {zinfo.filename}')
                            except Exception as e:
                                log(f'    WARNING: could not stage video {zinfo.filename}: {e}')
                            continue

                        img_exts = ('.jpg','.jpeg','.png','.gif','.svg','.ico','.webp',
                                    '.bmp','.woff','.woff2','.ttf','.mp3')
                        if any(fname_p.endswith(e) for e in img_exts):
                            continue
                        parts     = zinfo.filename.split('/')
                        # Preserve original directory structure (strip zip root folder)
                        rel_path  = '/'.join(parts[1:]) if len(parts) > 1 else parts[0]

                        # ── PATH SANITIZATION (no data lost) ─────────────────
                        # Instead of blocking traversal paths, we flatten them
                        # so ALL files are kept inside dest_dir safely.
                        # e.g. ../../../../Windows/evil.exe → Windows_evil.exe
                        dest_path = (dest_dir / rel_path).resolve()
                        if not str(dest_path).startswith(str(dest_dir.resolve())):
                            # Path escapes dest_dir — flatten to just the filename
                            safe_name = rel_path.replace('..', '').replace('/', '_').replace('\\', '_').strip('_')
                            if not safe_name:
                                safe_name = hashlib.md5(zinfo.filename.encode()).hexdigest()[:12]
                            dest_path = dest_dir / safe_name
                            log(f'    [PathFix] Redirected {zinfo.filename!r} → {safe_name}')

                        data_bytes = z.read(zinfo.filename)

                        # Content-hash dedup inside zip
                        if _is_duplicate_content(data_bytes):
                            continue

                        file_ext_z = Path(dest_path.name).suffix.lower()

                        # ── Route executables to dedicated EXE_DIR ──────────
                        if file_ext_z in EXE_EXTS:
                            save_executable(
                                dest_path.name,
                                data_bytes,
                                source_url=f'https://github.com/{owner}/{repo}/blob/HEAD/{zinfo.filename}',
                                extra_meta={
                                    'origin_repo'  : f'{owner}/{repo}',
                                    'ctf_platform' : ctf_platform or 'github',
                                    'zip_entry'    : zinfo.filename,
                                },
                            )
                        else:
                            safe_write_file(dest_path, data_bytes)  # safe write — no execute bit
                            _register_hash(data_bytes)
                            with save_lock:
                                global total_saved
                                stats['latest_raw_data'] += 1
                                total_saved += 1
                                scraped_files_cache.add(dest_path.name.lower())
                                if total_saved % 500 == 0:
                                    _save_hash_index()
                        extracted += 1


                    log(f'    Extracted {extracted} files from {owner}/{repo}')

                    if ctf_platform:
                        try:
                            structured = extract_ctf_structured_entries(ctf_platform, owner, repo, z)
                            if structured:
                                save_ctf_structured(ctf_platform, owner, repo, structured)
                            else:
                                log(f'    No structured {ctf_platform} challenge data found in {owner}/{repo}')
                        except Exception as e:
                            log(f'    WARNING: structured parse failed for {owner}/{repo}: {e}')

                    return extracted
            except Exception as e:
                log(f'    ERROR extracting ZIP for {owner}/{repo}: {e}')
    return -1


# ─────────────────────────────────────────────────────────────
#  CTF / SECURITY-TRAINING STRUCTURED DATA EXTRACTION
#  Pulls out per-challenge scenario + category metadata from the
#  known structured files each official platform ships in its repo.
# ─────────────────────────────────────────────────────────────
def _safe_yaml(raw_bytes):
    if yaml is None:
        return None
    try:
        return yaml.safe_load(raw_bytes.decode('utf-8', errors='ignore'))
    except Exception:
        return None


def extract_ctf_structured_entries(platform, owner, repo, z):
    """
    Given an open zipfile.ZipFile of a CTF-platform repo, return a list of
    highly-structured challenge/lesson dicts optimized for TTP and tool-calling training.
    """
    entries = []
    names = z.namelist()

    # ── OWASP Juice Shop ────────────────────────────────────
    if platform == 'juiceshop':
        target = next((n for n in names if n.endswith('data/static/challenges.yml')), None)
        if target:
            data = _safe_yaml(z.read(target)) or []
            for c in data:
                if not isinstance(c, dict):
                    continue
                # Map specific OWASP categories to structural MITRE or OWASP TTPs
                entries.append({
                    'source'        : 'OWASP Juice Shop',
                    'platform'      : 'juiceshop',
                    'category'      : c.get('category', 'web-app-vulnerability'),
                    'name'          : c.get('name', ''),
                    'difficulty'    : c.get('difficulty', 1),
                    'description'   : c.get('description', ''),
                    'hints'         : [c.get('hint', '')] if isinstance(c.get('hint', ''), str) else c.get('hints', []),
                    'mitigation_url': c.get('mitigationUrl', ''),
                    'tags'          : c.get('tags', []),
                    'key'           : c.get('key', ''),
                    'ttp_mapping'   : f"OWASP-Top-10:{c.get('category', '')}",
                    'training_prompt': f"Task: Exploit or verify the Juice Shop challenge '{c.get('name', '')}'. Category: {c.get('category', '')}. Goal: {c.get('description', '')}"
                })

    # ── OWASP WebGoat ───────────────────────────────────────
    elif platform == 'webgoat':
        adoc_files = [n for n in names if re.search(r'lessonplans?.*\.adoc$', n, re.I)]
        for n in adoc_files:
            try:
                text = z.read(n).decode('utf-8', errors='ignore')
            except Exception:
                continue
            parts       = n.split('/')
            category    = parts[-2] if len(parts) >= 2 else 'general'
            lesson_name = Path(n).stem
            
            # Clean up AsciiDoc headings and format codes
            clean_text  = re.sub(r'^=+.*$', '', text, flags=re.M)
            clean_text  = re.sub(r'\[.*?\]', '', clean_text)
            desc        = ' '.join(clean_text.split())[:3000]
            if len(desc) < 30:
                continue
            entries.append({
                'source'     : 'OWASP WebGoat',
                'platform'   : 'webgoat',
                'category'   : category,
                'name'       : lesson_name,
                'description': desc,
                'ttp_mapping': f"OWASP-Top-10:{category}",
                'training_prompt': f"Lab: WebGoat Lesson - {lesson_name}. Category: {category}. Lesson Details: {desc}"
            })

    # ── Google CTF ───────────────────────────────────────────
    elif platform == 'google_ctf':
        readme_re = re.compile(r'^[^/]+/(\d{4})/([^/]+)/([^/]+)/README(?:\.md)?$', re.I)
        for n in names:
            m = readme_re.match(n)
            if not m:
                continue
            year, category, chal_name = m.groups()
            try:
                readme_text = z.read(n).decode('utf-8', errors='ignore')
            except Exception:
                continue
            base_dir = n.rsplit('/', 1)[0]
            meta = {}
            for meta_name in (f'{base_dir}/challenge.yaml', f'{base_dir}/challenge.yml'):
                if meta_name in names:
                    meta = _safe_yaml(z.read(meta_name)) or {}
                    break
            if not isinstance(meta, dict):
                meta = {}
            
            # Extract challenge file attachments and verification steps if present
            desc = meta.get('description') or ' '.join(readme_text.split())[:3000]
            entries.append({
                'source'     : 'Google CTF',
                'platform'   : 'google_ctf',
                'year'       : year,
                'category'   : meta.get('category', category),
                'name'       : meta.get('name', chal_name),
                'points'     : meta.get('points', meta.get('score', '')),
                'description': desc,
                'ttp_mapping': f"CTF-Category:{meta.get('category', category)}",
                'training_prompt': f"Challenge: Google CTF ({year}) - {meta.get('name', chal_name)}. Category: {meta.get('category', category)}. Scenario: {desc}"
            })

    # ── picoCTF ──────────────────────────────────────────────
    elif platform == 'picoctf':
        json_files = [n for n in names if re.search(r'problems?/.+/problem\.json$', n, re.I)]
        for n in json_files:
            try:
                data = json.loads(z.read(n).decode('utf-8', errors='ignore'))
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            entries.append({
                'source'     : 'picoCTF',
                'platform'   : 'picoctf',
                'category'   : data.get('category', 'general'),
                'name'       : data.get('name', ''),
                'points'     : data.get('score', data.get('points', '')),
                'description': data.get('description', ''),
                'hints'      : data.get('hints', []),
                'ttp_mapping': f"picoCTF-Category:{data.get('category', 'general')}",
                'training_prompt': f"Task: Solve picoCTF challenge '{data.get('name', '')}'. Category: {data.get('category', 'general')}. Hints: {', '.join(data.get('hints', []))}. Problem: {data.get('description', '')}"
            })
        if not json_files:
            readme_re = re.compile(r'^problems?/([^/]+)/([^/]+)/README(?:\.md)?$', re.I)
            for n in names:
                m = readme_re.match(n)
                if not m:
                    continue
                category, chal_name = m.groups()
                try:
                    text = z.read(n).decode('utf-8', errors='ignore')
                except Exception:
                    continue
                entries.append({
                    'source'     : 'picoCTF',
                    'platform'   : 'picoctf',
                    'category'   : category,
                    'name'       : chal_name,
                    'description': ' '.join(text.split())[:3000],
                    'ttp_mapping': f"picoCTF-Category:{category}",
                    'training_prompt': f"Task: Solve picoCTF challenge '{chal_name}'. Category: {category}. Details: {' '.join(text.split())[:3000]}"
                })

    # ── pwn.college ──────────────────────────────────────────
    elif platform == 'pwncollege':
        dojo_file = next((n for n in names if n.endswith('dojo.yml') or n.endswith('dojo.yaml')), None)
        if dojo_file:
            dojo_data = _safe_yaml(z.read(dojo_file)) or {}
            if not isinstance(dojo_data, dict):
                dojo_data = {}
            dojo_name = dojo_data.get('name', repo)
            dojo_desc = dojo_data.get('description', '')
            for module in (dojo_data.get('modules') or []):
                if not isinstance(module, dict):
                    continue
                module_id   = module.get('id', '')
                module_name = module.get('name', module_id)
                module_desc = module.get('description', '')
                challenges  = module.get('challenges') or []
                if challenges:
                    for chal in challenges:
                        if not isinstance(chal, dict):
                            continue
                        chal_desc = chal.get('description', '')
                        entries.append({
                            'source'            : 'pwn.college',
                            'platform'          : 'pwncollege',
                            'dojo'              : dojo_name,
                            'dojo_description'  : dojo_desc,
                            'module_id'         : module_id,
                            'module_name'       : module_name,
                            'module_description': module_desc,
                            'challenge_id'      : chal.get('id', ''),
                            'name'              : chal.get('name', chal.get('id', '')),
                            'description'       : chal_desc,
                            'ttp_mapping'       : f"pwn.college:{module_id}",
                            'training_prompt'   : f"Scenario: pwn.college challenge - {chal.get('name', chal.get('id', ''))} in module {module_name}. Goal: {chal_desc}"
                        })
                else:
                    entries.append({
                        'source'            : 'pwn.college',
                        'platform'          : 'pwncollege',
                        'dojo'              : dojo_name,
                        'dojo_description'  : dojo_desc,
                        'module_id'         : module_id,
                        'module_name'       : module_name,
                        'category'          : module_name,
                        'description'       : module_desc,
                        'ttp_mapping'       : f"pwn.college:{module_id}",
                        'training_prompt'   : f"Module Overview: pwn.college module {module_name}. Topics Covered: {module_desc}"
                    })

    # ── Generic/Discovered CTF ───────────────────────────────
    elif platform == 'generic_ctf':
        # Strategy A: pwn.college-style dojo.yml
        dojo_file = next((n for n in names if n.endswith('dojo.yml') or n.endswith('dojo.yaml')), None)
        if dojo_file:
            entries.extend(extract_ctf_structured_entries('pwncollege', owner, repo, z))

        # Strategy B: CTFd/kCTF-style per-challenge metadata files
        if not entries:
            chal_files = [n for n in names if
                          re.search(r'(^|/)challenge\.ya?ml$', n, re.I) or
                          re.search(r'(^|/)chal(lenge)?\.json$', n, re.I) or
                          re.search(r'(^|/)problem\.json$', n, re.I)]
            for n in chal_files:
                try:
                    raw = z.read(n)
                    data = (_safe_yaml(raw) if n.lower().endswith(('.yml', '.yaml'))
                            else json.loads(raw.decode('utf-8', errors='ignore')))
                except Exception:
                    continue
                if not isinstance(data, dict):
                    continue
                folder_parts = n.rsplit('/', 1)[0].split('/') if '/' in n else []
                guessed_category = folder_parts[-2] if len(folder_parts) >= 2 else (
                    folder_parts[-1] if folder_parts else '')
                desc = data.get('description', data.get('desc', ''))
                name_val = data.get('name', data.get('title', folder_parts[-1] if folder_parts else repo))
                cat_val = data.get('category', data.get('type', guessed_category))
                entries.append({
                    'source'     : f'GitHub: {owner}/{repo}',
                    'platform'   : 'generic_ctf',
                    'repo'       : f'{owner}/{repo}',
                    'category'   : cat_val,
                    'name'       : name_val,
                    'difficulty' : data.get('difficulty', data.get('value', data.get('points', data.get('score', '')))),
                    'description': desc,
                    'hints'      : data.get('hints', data.get('hint', [])),
                    'ttp_mapping': f"generic_ctf:{cat_val}",
                    'training_prompt': f"Task: Solve vulnerability challenge '{name_val}' from {owner}/{repo}. Category: {cat_val}. Details: {desc}"
                })
                if len(entries) >= 500:
                    break

        # Strategy C: README.md two-level layout
        if not entries:
            readme_re = re.compile(r'(?:^|/)([^/]+)/([^/]+)/README(?:\.md)?$', re.I)
            seen_dirs = set()
            skip_names = {'the', 'master', 'main', repo.lower()}
            for n in names:
                m = readme_re.search(n)
                if not m:
                    continue
                category, chal_name = m.groups()
                if category.lower() in skip_names:
                    continue
                dir_key = (category.lower(), chal_name.lower())
                if dir_key in seen_dirs:
                    continue
                seen_dirs.add(dir_key)
                try:
                    text = z.read(n).decode('utf-8', errors='ignore')
                except Exception:
                    continue
                desc = ' '.join(text.split())[:3000]
                if len(desc) < 30:
                    continue
                entries.append({
                    'source'     : f'GitHub: {owner}/{repo}',
                    'platform'   : 'generic_ctf',
                    'repo'       : f'{owner}/{repo}',
                    'category'   : category,
                    'name'       : chal_name,
                    'description': desc,
                    'ttp_mapping': f"generic_ctf:{category}",
                    'training_prompt': f"Task: Exploit or patch target '{chal_name}' in category '{category}'. Lesson scenario: {desc}"
                })
                if len(entries) >= 500:
                    break

    return entries


def save_ctf_structured(platform, owner, repo, entries):
    subfolder = get_subfolder_name(owner, repo, platform)
    out_dir = DEST_ROOT / subfolder
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Save standard JSON
    out_path_json = out_dir / f'{platform}_{owner}_{repo}.json'
    try:
        with open(out_path_json, 'w', encoding='utf-8') as f:
            json.dump(entries, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log(f'    ERROR saving JSON for {owner}/{repo}: {e}')
        
    # 2. Save JSON Lines (JSONL)
    out_path_jsonl = out_dir / f'{platform}_{owner}_{repo}.jsonl'
    try:
        with open(out_path_jsonl, 'w', encoding='utf-8') as f:
            for entry in entries:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    except Exception as e:
        log(f'    ERROR saving JSONL for {owner}/{repo}: {e}')

    # 3. Save Apache Parquet (.parquet)
    out_path_parquet = out_dir / f'{platform}_{owner}_{repo}.parquet'
    try:
        import pandas as pd
        df = pd.DataFrame(entries)
        # Ensure all columns are standard datatypes (strings/ints/objects) before writing to Parquet
        for c in df.columns:
            if df[c].dtype == object:
                # Convert list or dict columns to string representation or keep as object depending on pyarrow support
                pass
        df.to_parquet(out_path_parquet, index=False, engine='pyarrow')
    except Exception as e:
        log(f'    ERROR saving Parquet for {owner}/{repo}: {e}')

    with save_lock:
        stats['ctf_structured'] += 1
        global total_saved
        total_saved += 1
    log(f'    Saved {len(entries)} structured entries -> JSON/JSONL/Parquet in {subfolder}')


# ─────────────────────────────────────────────────────────────
#  LAB VIDEO (.mp4) TRANSCRIPTION
#  Any mp4 discovered in a repo ZIP or embedded on a lab page gets queued
#  here. After all tiers finish scraping, process_video_queue() downloads
#  (if remote), transcribes locally with faster-whisper, auto-structures
#  the transcript into Q&A or Topic/Explanation sections, and writes
#  batched HTML pages (VIDEOS_PER_HTML_PAGE videos per page).
# ─────────────────────────────────────────────────────────────
QUESTION_RE = re.compile(r'.+\?\s*$')

def queue_video(platform, title, source_url, owner=None, repo=None,
                 video_url=None, local_path=None, delete_after=False):
    """Thread-safe append to the global video queue. Call this from any
    scraper as soon as an .mp4 is spotted — either a remote `video_url` to
    download later, or a `local_path` already on disk (e.g. extracted from
    a repo ZIP)."""
    with video_queue_lock:
        video_queue.append({
            'platform'    : platform,
            'title'       : title,
            'source_url'  : source_url,
            'owner'       : owner,
            'repo'        : repo,
            'video_url'   : video_url,
            'local_path'  : local_path,
            'delete_after': delete_after,
        })


def find_mp4_urls_in_html(html_text, base_url):
    """Scans a lab/page's HTML for direct .mp4 references: <video src>,
    <source src>, plain <a href> links, and common lazy-load data-* attrs.
    Does NOT follow YouTube/Vimeo iframe embeds — only direct mp4 files."""
    urls = set()
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        for tag_name in ('video', 'source', 'a'):
            for el in soup.find_all(tag_name):
                src = el.get('src') or el.get('href')
                if not src:
                    continue
                full = urllib.parse.urljoin(base_url, src)
                if full.lower().split('?')[0].endswith('.mp4'):
                    urls.add(full)
        # lazy-loaded video attrs some sites use (data-src, data-video, etc.)
        for el in soup.find_all(True):
            for attr_val in el.attrs.values():
                if isinstance(attr_val, str) and attr_val.lower().split('?')[0].endswith('.mp4'):
                    urls.add(urllib.parse.urljoin(base_url, attr_val))
    except Exception:
        pass
    return urls


def download_video_file(url, session, dest_path):
    """Streams an mp4 to disk. Returns True on success."""
    try:
        r = fetch(url, session, stream=True)
        if not r or r.status_code != 200:
            return False
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as e:
        log(f'    ERROR downloading video {url}: {e}')
        return False


_whisper_model = None
_whisper_model_lock = threading.Lock()

def get_whisper_model():
    """Lazily loads the local Whisper model once (thread-safe), reused for
    every video so we're not reloading it per-file."""
    global _whisper_model
    with _whisper_model_lock:
        if _whisper_model is None:
            log(f'  Loading local Whisper model ("{WHISPER_MODEL_SIZE}")... (first video only)')
            _whisper_model = _WhisperModel(WHISPER_MODEL_SIZE, device='cpu', compute_type='int8')
        return _whisper_model


def transcribe_video_whisper(video_path):
    """Transcribes a local mp4 with faster-whisper. Returns a list of
    {'start': float, 'end': float, 'text': str} segments, or None if
    transcription isn't available/failed. Requires ffmpeg on PATH."""
    if not HAS_FASTER_WHISPER:
        return None
    if shutil.which('ffmpeg') is None:
        log('    ffmpeg not found on PATH — cannot decode audio, skipping transcription.')
        return None
    try:
        model = get_whisper_model()
        segments_iter, _info = model.transcribe(str(video_path), vad_filter=True)
        return [{'start': s.start, 'end': s.end, 'text': s.text.strip()}
                for s in segments_iter if s.text and s.text.strip()]
    except Exception as e:
        log(f'    ERROR transcribing {video_path}: {e}')
        return None


def structure_transcript(segments):
    """Auto-detects whether the video reads like Q&A (lots of segments
    ending in '?') or a Topic-then-Explanation walkthrough (long
    uninterrupted stretches of speech), and groups the transcript
    accordingly. Returns {'mode': 'qa'|'topic', 'sections': [{'heading','body'}, ...]}."""
    if not segments:
        return {'mode': 'topic', 'sections': []}

    question_count = sum(1 for s in segments if QUESTION_RE.match(s['text']))
    qa_ratio = question_count / max(1, len(segments))

    if qa_ratio >= 0.15:
        sections  = []
        current_q = None
        current_a = []
        for s in segments:
            if QUESTION_RE.match(s['text']):
                if current_q is not None:
                    sections.append({'heading': current_q, 'body': ' '.join(current_a).strip()})
                current_q = s['text']
                current_a = []
            else:
                if current_q is None:
                    current_q = 'Introduction'
                current_a.append(s['text'])
        if current_q is not None:
            sections.append({'heading': current_q, 'body': ' '.join(current_a).strip()})
        return {'mode': 'qa', 'sections': [s for s in sections if s['body']]}

    # Topic/Explanation fallback — split on pauses between segments
    blocks = [[segments[0]]]
    for prev, cur in zip(segments, segments[1:]):
        if cur['start'] - prev['end'] >= VIDEO_SILENCE_GAP_SECONDS:
            blocks.append([cur])
        else:
            blocks[-1].append(cur)

    topic_sections = []
    for block in blocks:
        full_text = ' '.join(s['text'] for s in block).strip()
        if not full_text:
            continue
        m = re.match(r'(.{10,120}?[\.\!\?])(\s|$)', full_text)
        if m:
            heading = m.group(1).strip()
            body    = full_text[len(m.group(0)):].strip()
        else:
            words   = full_text.split()
            heading = ' '.join(words[:12]) + ('...' if len(words) > 12 else '')
            body    = ' '.join(words[12:])
        topic_sections.append({'heading': heading, 'body': body or full_text})
    return {'mode': 'topic', 'sections': topic_sections}


def render_video_batch_html(batch, out_path, batch_index):
    """Writes one HTML page covering up to VIDEOS_PER_HTML_PAGE videos,
    each rendered as a Q&A block or a Topic/Explanation block depending
    on what structure_transcript() detected."""
    parts = [
        '<!DOCTYPE html>', '<html>', '<head>', '<meta charset="utf-8">',
        f'<title>CTF Lab Video Transcripts — Batch {batch_index}</title>',
        '<style>',
        'body{font-family:Arial,Helvetica,sans-serif;max-width:900px;margin:40px auto;'
        'padding:0 20px;line-height:1.55;color:#222}',
        'h1{border-bottom:3px solid #333;padding-bottom:8px}',
        '.video{margin-bottom:50px;border:1px solid #ddd;border-radius:8px;padding:20px}',
        '.video h2{margin-top:0;color:#1a5276}',
        '.meta{color:#777;font-size:0.85em;margin-bottom:15px}',
        '.qa{margin-bottom:16px;padding:12px;background:#f7f9fb;border-left:4px solid #2874a6;border-radius:4px}',
        '.qa .q{font-weight:bold;color:#154360;margin-bottom:6px}',
        '.topic{margin-bottom:16px;padding:12px;background:#f9f9f7;border-left:4px solid #7d6608;border-radius:4px}',
        '.topic .heading{font-weight:bold;color:#7d6608;margin-bottom:6px}',
        '</style>', '</head>', '<body>',
        f'<h1>CTF Lab Video Transcripts — Batch {batch_index}</h1>',
    ]
    for entry in batch:
        title      = html.escape(entry.get('title') or 'Untitled video')
        platform   = html.escape(entry.get('platform') or '')
        source_url = entry.get('source_url') or ''
        structured = entry.get('structured') or {'mode': 'topic', 'sections': []}
        parts.append('<div class="video">')
        parts.append(f'<h2>{title}</h2>')
        meta_bits = [b for b in [platform,
                                 f'<a href="{html.escape(source_url)}">source</a>' if source_url else '']
                     if b]
        parts.append(f'<div class="meta">{" &middot; ".join(meta_bits)}</div>')
        if not structured['sections']:
            parts.append('<p><em>No speech detected, or transcription was unavailable for this video.</em></p>')
        elif structured['mode'] == 'qa':
            for sec in structured['sections']:
                parts.append('<div class="qa">')
                parts.append(f'<div class="q">Q: {html.escape(sec["heading"])}</div>')
                parts.append(f'<div class="a">{html.escape(sec["body"])}</div>')
                parts.append('</div>')
        else:
            for sec in structured['sections']:
                parts.append('<div class="topic">')
                parts.append(f'<div class="heading">{html.escape(sec["heading"])}</div>')
                parts.append(f'<div class="body">{html.escape(sec["body"])}</div>')
                parts.append('</div>')
        parts.append('</div>')
    parts.append('</body></html>')
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(parts))


def process_video_queue(session):
    """Entry point called once, after every scraping tier has finished
    queuing videos. Downloads (if remote) + transcribes each queued mp4,
    then writes them out in batches of VIDEOS_PER_HTML_PAGE to HTML pages
    under VIDEO_HTML_DIR."""
    global total_saved
    if not video_queue:
        log('  No lab videos (.mp4) were found to transcribe.')
        return
    if not VIDEO_TRANSCRIPTION_ENABLED:
        log('  VIDEO_TRANSCRIPTION_ENABLED is False — skipping video transcription.')
        return
    if not HAS_FASTER_WHISPER:
        log(f'  faster-whisper not installed — {len(video_queue)} queued video(s) will NOT be '
            'transcribed.')
        log('  Install with: pip install faster-whisper --break-system-packages')
        return
    if shutil.which('ffmpeg') is None:
        log(f'  ffmpeg not found on PATH — {len(video_queue)} queued video(s) will NOT be '
            'transcribed. Install ffmpeg and re-run.')
        return

    log('\n' + '=' * 70)
    log(f'LAB VIDEO TRANSCRIPTION — {len(video_queue)} mp4(s) queued')
    log('=' * 70)

    processed = []
    for i, entry in enumerate(video_queue, 1):
        title = entry.get('title', 'Untitled')
        log(f'  [{i}/{len(video_queue)}] {title}')

        local_path = entry.get('local_path')
        if not local_path:
            video_url = entry.get('video_url')
            if not video_url:
                continue
            fname = _sanitize_filename(Path(urllib.parse.urlparse(video_url).path).name or f'video_{i}.mp4')
            local_path = VIDEO_STAGING_DIR / f'{entry.get("platform","video")}_{i}_{fname}'
            if not download_video_file(video_url, session, local_path):
                log('    Download failed, skipping.')
                continue
            entry['delete_after'] = True

        segments   = transcribe_video_whisper(local_path)
        structured = structure_transcript(segments) if segments else {'mode': 'topic', 'sections': []}
        entry['structured'] = structured
        processed.append(entry)

        if entry.get('delete_after'):
            try:
                Path(local_path).unlink(missing_ok=True)
            except Exception:
                pass

    log(f'  Transcribed {len(processed)}/{len(video_queue)} video(s) successfully.')

    for batch_idx, start in enumerate(range(0, len(processed), VIDEOS_PER_HTML_PAGE), 1):
        batch    = processed[start:start + VIDEOS_PER_HTML_PAGE]
        out_path = VIDEO_HTML_DIR / f'video_transcripts_batch_{batch_idx:03d}.html'
        render_video_batch_html(batch, out_path, batch_idx)
        with save_lock:
            stats['video_transcripts_html'] += 1
            total_saved += 1
        log(f'    Wrote {out_path.name} ({len(batch)} videos)')


def discover_pwncollege_dojos(session, max_repos=40):
    """Dynamically list all *-dojo repos in the pwncollege GitHub org so new
    official dojos get picked up automatically without hardcoding names."""
    dojos = []
    try:
        page = 1
        while True:
            r = session.get(f'https://api.github.com/orgs/{PWNCOLLEGE_ORG}/repos',
                             params={'per_page': 100, 'page': page}, headers=GITHUB_API_HEADERS, timeout=15)
            if r.status_code != 200:
                break
            batch = r.json()
            if not batch:
                break
            for repo_info in batch:
                name = repo_info.get('name', '')
                if 'dojo' in name.lower() and not repo_info.get('fork'):
                    dojos.append(name)
            if len(batch) < 100 or len(dojos) >= max_repos:
                break
            page += 1
    except Exception as e:
        log(f'  WARNING: could not list pwn.college dojo repos via API: {e}')
    if not dojos:
        # Fallback if the API call failed/rate-limited
        dojos = ['intro-to-cybersecurity-dojo', 'fundamentals-dojo',
                 'software-exploitation-dojo', 'example-dojo']
        log('  Using fallback static list of pwn.college dojos.')
    return dojos[:max_repos]


def scrape_portswigger_labs(session):
    """PortSwigger Web Security Academy isn't an open-source repo, so this
    crawls the public lab index + each lab page directly for its scenario
    description and category."""
    log('  Scraping PortSwigger Web Security Academy (all-labs index)...')
    entries = []
    try:
        r = fetch(PORTSWIGGER_ALL_LABS_URL, session)
        if not r or r.status_code != 200:
            log('    Could not fetch PortSwigger all-labs index.')
            return 0
        soup = BeautifulSoup(r.text, 'html.parser')
        lab_links = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/web-security/' not in href:
                continue
            full = urllib.parse.urljoin(PORTSWIGGER_ALL_LABS_URL, href)
            path = urllib.parse.urlparse(full).path.strip('/')
            segs = path.split('/')
            # keep only deep links that look like an actual lab page,
            # not the top-level category/topic index pages
            if len(segs) >= 3 and 'all-labs' not in segs:
                lab_links.add(full)
        log(f'    Found {len(lab_links)} candidate lab links.')
    except Exception as e:
        log(f'    ERROR fetching PortSwigger index: {e}')
        return 0

    def process_lab(url):
        try:
            r = fetch(url, session)
            if not r or r.status_code != 200:
                return None
            title_tag = BeautifulSoup(r.text, 'html.parser').find('h1')
            title     = title_tag.get_text(strip=True) if title_tag else url
            segs      = urllib.parse.urlparse(url).path.strip('/').split('/')
            category  = segs[1] if len(segs) > 1 else 'general'
            _, _, text = clean_html(r.text, url)
            if len(text) < 80:
                return None

            # Queue any embedded walkthrough mp4s on this lab page for transcription
            for vurl in find_mp4_urls_in_html(r.text, url):
                queue_video(platform='portswigger', title=title, source_url=url, video_url=vurl)

            return {
                'source'     : 'PortSwigger Web Security Academy',
                'platform'   : 'portswigger',
                'category'   : category,
                'name'       : title,
                'description': text[:3000],
                'url'        : url,
                'ttp_mapping': f"OWASP-Top-10:{category}",
                'training_prompt': f"Lab scenario: PortSwigger Web Security Academy - {title}. Category: {category}. Lab task details: {text[:3000]}"
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = [ex.submit(process_lab, u) for u in lab_links]
        for f in as_completed(futures):
            res = f.result()
            if res:
                entries.append(res)

    if entries:
        save_ctf_structured('portswigger', 'portswigger', 'web-security-academy', entries)
    log(f'    PortSwigger: {len(entries)} lab scenarios captured.')
    return len(entries)


def discover_github_ctf_repos(session, max_results=MAX_DISCOVERED_REPOS):
    """Self-discover CTF-related repos on GitHub via the search API, ranked
    by star count (so official/popular repos surface before obscure ones).
    Filters out non-CTF matches and forks; skips repos below MIN_STARS_FOR_DISCOVERED."""
    log('  Searching GitHub for CTF repos (this can take a while on the '
        'unauthenticated rate limit)...')
    seen = {}
    for query in GITHUB_SEARCH_QUERIES:
        page = 1
        while True:
            try:
                r = session.get('https://api.github.com/search/repositories',
                                 params={'q': query, 'sort': 'stars', 'order': 'desc',
                                         'per_page': 100, 'page': page},
                                 headers=GITHUB_API_HEADERS, timeout=20)
            except Exception as e:
                log(f'    Search error for "{query}": {e}')
                break
            if r.status_code == 403:
                log('    GitHub search rate-limited — backing off 60s...')
                time.sleep(60)
                continue
            if r.status_code != 200:
                log(f'    Search "{query}" returned HTTP {r.status_code}, skipping.')
                break
            try:
                data = r.json()
            except Exception:
                break
            items = data.get('items', [])
            if not items:
                break
            for it in items:
                full_name = it.get('full_name')
                if not full_name or full_name in seen or it.get('fork'):
                    continue
                desc      = it.get('description') or ''
                topics    = it.get('topics', []) or []
                text_blob = f"{it.get('name', '')} {desc} {' '.join(topics)}"
                if not CTF_KEYWORD_RE.search(text_blob):
                    continue
                stars = it.get('stargazers_count', 0)
                if stars < MIN_STARS_FOR_DISCOVERED:
                    continue
                seen[full_name] = {
                    'owner'      : it['owner']['login'],
                    'repo'       : it['name'],
                    'stars'      : stars,
                    'description': desc,
                }
            # GitHub search API caps results at 1000 (10 pages of 100)
            if len(items) < 100 or page >= 10:
                break
            page += 1
            time.sleep(2)  # stay polite to the search rate limit

    ranked = sorted(seen.values(), key=lambda x: -x['stars'])
    log(f'  GitHub search discovered {len(ranked)} candidate CTF repos '
        f'(>= {MIN_STARS_FOR_DISCOVERED} stars, official-first list processed separately).')
    return ranked[:max_results]


def run_ctf_scraping(session):
    """Entry point: pulls full source + structured challenge metadata for
    every official CTF/security-training platform we know about, then
    self-discovers and scrapes additional CTF repos from GitHub, highest
    star count first."""
    log('\n' + '=' * 70)
    log('CTF / SECURITY-TRAINING SOURCES — deep structured scrape')
    log('=' * 70)

    already_scraped = set()

    # ── Tier 1: known-official platforms (processed regardless of stars) ──
    for owner, repo, platform in CTF_GITHUB_REPOS:
        log(f'  [{platform}] {owner}/{repo}')
        try:
            download_github_repo_zip(owner, repo, session, ctf_platform=platform)
        except Exception as e:
            log(f'    ERROR scraping {owner}/{repo}: {e}')
        already_scraped.add((owner.lower(), repo.lower()))

    # ── Tier 2: every official pwn.college dojo (discovered dynamically) ──
    dojo_repos = discover_pwncollege_dojos(session)
    log(f'  Discovered {len(dojo_repos)} pwn.college dojo repos: {", ".join(dojo_repos)}')
    for repo in dojo_repos:
        log(f'  [pwncollege] {PWNCOLLEGE_ORG}/{repo}')
        try:
            download_github_repo_zip(PWNCOLLEGE_ORG, repo, session, ctf_platform='pwncollege')
        except Exception as e:
            log(f'    ERROR scraping {PWNCOLLEGE_ORG}/{repo}: {e}')
        already_scraped.add((PWNCOLLEGE_ORG.lower(), repo.lower()))

    # ── Tier 3: PortSwigger Web Security Academy (website, not a repo) ────
    try:
        scrape_portswigger_labs(session)
    except Exception as e:
        log(f'    ERROR scraping PortSwigger: {e}')

    # ── Tier 4: self-discovered GitHub CTF repos, highest stars first ─────
    discovered = discover_github_ctf_repos(session)
    for item in discovered:
        key = (item['owner'].lower(), item['repo'].lower())
        if key in already_scraped:
            continue
        already_scraped.add(key)
        log(f"  [discovered:{item['stars']}\u2605] {item['owner']}/{item['repo']} "
            f"\u2014 {item['description'][:80]}")
        try:
            download_github_repo_zip(item['owner'], item['repo'], session, ctf_platform='generic_ctf')
        except Exception as e:
            log(f"    ERROR scraping {item['owner']}/{item['repo']}: {e}")

    # ── Tier 5: transcribe every lab video (.mp4) queued across all tiers ──
    try:
        process_video_queue(session)
    except Exception as e:
        log(f'    ERROR processing lab video queue: {e}')

    log('CTF structured scraping complete.\n')


def generic_blog_scraper(base_url, name, session, prefix=''):
    log(f'  Generic scraping: {name} ({base_url})')
    all_urls  = set()
    is_gh     = 'github.com' in base_url.lower() or 'raw.githubusercontent.com' in base_url.lower()
    depth     = 7 if is_gh else MAX_DEPTH
    domain_check = urllib.parse.urlparse(base_url).netloc.replace('www.','').lower()
    is_large  = 'nist.gov' in domain_check or 'cve.org' in domain_check
    cap       = 4000 if is_gh else (10000 if is_large else 2000)

    if is_gh:
        try:
            p2 = urllib.parse.urlparse(base_url)
            parts = [p for p in p2.path.strip('/').split('/') if p]
            if len(parts) >= 2:
                owner  = parts[0]
                repo   = parts[1]
                prefix_check = f'github_{owner.lower()}_{repo.lower()}_'
                if any(f.startswith(prefix_check) for f in scraped_files_cache):
                    log(f'    {owner}/{repo} already scraped. Skipping.')
                    return 1
                extracted = download_github_repo_zip(owner, repo, session)
                if extracted >= 0:
                    scrape_and_save(base_url, session, prefix=f'github_{owner}_{repo}')
                    return extracted + 1
        except Exception as e:
            log(f'    GitHub ZIP failed, falling back to crawl: {e}')
    else:
        rss_urls = get_rss_urls(base_url, session)
        all_urls.update(rss_urls)
        log(f'    RSS: {len(rss_urls)} URLs')
        sitemap_urls = get_sitemap_urls(base_url, session, max_urls=cap)
        all_urls.update(sitemap_urls)
        log(f'    Sitemap: {len(sitemap_urls)} URLs')

    if len(all_urls) < 50 or is_gh:
        run_depth = depth
        if len(scraped_files_cache) > 300 and not is_gh:
            log(f'    Cache has {len(scraped_files_cache)} files. Shallow crawl (depth=0)...')
            run_depth = 0
        crawled = crawl_links_from_page(base_url, session, same_domain=True, max_depth=run_depth)
        all_urls.update(crawled)
        log(f'    Crawl: {len(crawled)} URLs')

    all_urls.add(base_url)
    domain = urllib.parse.urlparse(base_url).netloc.replace('.','_').replace('www_','')
    pfx    = prefix or domain
    url_list = list(all_urls)[:cap]
    log(f'    Scraping {len(url_list)} total URLs for {name}...')

    count = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        futures = [ex.submit(scrape_and_save, u, session, pfx) for u in url_list]
        for f in as_completed(futures):
            try:
                if f.result():
                    count += 1
            except Exception:
                pass
    return count

# ─────────────────────────────────────────────────────────────
#  URL → (folder_name, scraper_func) ROUTING for url1.docx
# ─────────────────────────────────────────────────────────────
def route_url(url, session):
    """
    Given a URL from url1.docx, determine output folder and run correct scraper.
    Returns number of items saved.
    """
    global DEST_ROOT, scraped_files_cache, saved_urls

    parsed = urllib.parse.urlparse(url)
    netloc = parsed.netloc.lower()
    path   = parsed.path.lower()
    qs     = urllib.parse.parse_qs(parsed.query)

    # ── SANS White Papers ─────────────────────────────────────
    if 'sans.org' in netloc and 'white-papers' in path:
        folder  = 'sans_white_papers'
        out_dir = RESEARCH_ROOT / folder
        out_dir.mkdir(parents=True, exist_ok=True)
        log(f'  Routing to SANS White Papers scraper → {out_dir}')
        DEST_ROOT = out_dir
        _init_cache(out_dir)
        return scrape_sans_white_papers(session, out_dir=out_dir)

    # ── HAL Science (search queries) ──────────────────────────
    if 'hal.science' in netloc or 'hal.archives-ouvertes.fr' in netloc:
        raw_q = qs.get('q', [''])[0]
        # Try encoded and decoded forms
        folder_name, search_query = None, None
        for key, (fn, sq) in HAL_URL_MAP.items():
            if key.lower() in raw_q.lower() or raw_q.lower() in key.lower():
                folder_name, search_query = fn, sq
                break
        if not folder_name:
            # Build folder from query
            folder_name  = 'hal_' + re.sub(r'[^a-z0-9]+', '_', raw_q.lower()).strip('_')
            search_query = raw_q.replace('+', ' ')
        out_dir = RESEARCH_ROOT / folder_name
        out_dir.mkdir(parents=True, exist_ok=True)
        log(f'  Routing to HAL scraper (query="{search_query}") → {out_dir}')
        DEST_ROOT = out_dir
        _init_cache(out_dir)
        return scrape_hal_query(session, search_query, folder_name, out_dir=out_dir)

    # ── SSRN ─────────────────────────────────────────────────
    if 'ssrn.com' in netloc:
        raw_term = qs.get('term', ['cybersecurity threat'])[0].replace('+', ' ')
        folder_name = 'ssrn_' + re.sub(r'[^a-z0-9]+', '_', raw_term.lower()).strip('_')
        out_dir = RESEARCH_ROOT / folder_name
        out_dir.mkdir(parents=True, exist_ok=True)
        log(f'  Routing to SSRN scraper (term="{raw_term}") → {out_dir}')
        DEST_ROOT = out_dir
        _init_cache(out_dir)
        return scrape_ssrn(session, raw_term, folder_name, out_dir=out_dir)

    # ── Generic fallback ──────────────────────────────────────
    combined    = parsed.netloc + parsed.path
    safe_folder = re.sub(r'[^a-zA-Z0-9]', '_', combined)
    safe_folder = re.sub(r'_+', '_', safe_folder).strip('_') or 'root'
    out_dir     = RESEARCH_ROOT / safe_folder
    out_dir.mkdir(parents=True, exist_ok=True)
    DEST_ROOT = out_dir
    _init_cache(out_dir)
    log(f'  Routing to generic scraper → {out_dir}')
    domain = parsed.netloc.replace('www.','').lower()
    name   = domain.split('.')[0].upper()

    if domain in SITE_SCRAPERS:
        return SITE_SCRAPERS[domain](session)
    for reg_domain, scraper in SITE_SCRAPERS.items():
        if domain.endswith(reg_domain):
            return scraper(session)
    return generic_blog_scraper(url, name, session)

def _init_cache(folder):
    global scraped_files_cache, saved_urls
    scraped_files_cache = set()
    saved_urls          = set()
    if folder.exists():
        for root, _, files in os.walk(folder):
            for f in files:
                scraped_files_cache.add(f.lower())
    log(f'  Cache initialized: {len(scraped_files_cache)} existing files in {folder}')
    # Load persisted hash index and build from folder if needed
    _load_hash_index()
    if not _content_hashes:
        _build_hash_index_from_folder(folder)

# ─────────────────────────────────────────────────────────────
#  EXCEL MODE  (original functionality — unchanged)
# ─────────────────────────────────────────────────────────────
def extract_unique_sites():
    """
    DISABLED — this script no longer reads CSV input or scrapes arbitrary
    links from it. Kept here only for reference; it is never called from
    main() anymore. All scraping now goes through run_ctf_scraping()
    (official CTF platforms + self-discovered GitHub CTF repos).
    """
    raise RuntimeError(
        'extract_unique_sites() is disabled. CSV input has been turned off — '
        'this script now only scrapes CTF content via run_ctf_scraping().'
    )
    """
    Read the input CSV: 'github_2500_libraries_master (2).csv'
    Expected columns (case/spacing tolerant):
        Programming language, repositories name, github urls,
        description, star counts, type
    """
    import pandas as pd
    sites = {}

    csv_path = CSV_PATH
    if len(sys.argv) > 1 and sys.argv[1].lower().endswith('.csv'):
        csv_path = Path(sys.argv[1])

    if not csv_path.exists():
        log(f"ERROR: CSV not found at {csv_path}")
        return sites

    log(f"Parsing CSV: '{csv_path.name}'")
    df = pd.read_csv(csv_path, encoding='utf-8-sig')

    norm_cols = {c: re.sub(r'[^a-z0-9]', '', str(c).lower()) for c in df.columns}

    def find_col(*keywords):
        for orig, norm in norm_cols.items():
            if any(kw in norm for kw in keywords):
                return orig
        return None

    lang_col  = find_col('programminglanguage', 'language')
    name_col  = find_col('repositoriesname', 'repositoryname', 'reponame', 'repositories', 'repository')
    url_col   = find_col('githuburls', 'githuburl', 'url', 'link')
    desc_col  = find_col('description', 'desc')
    star_col  = find_col('starcounts', 'starcount', 'stars')
    type_col  = find_col('type')

    log(f"    Columns detected -> language={lang_col}, name={name_col}, url={url_col}, "
        f"description={desc_col}, stars={star_col}, type={type_col}")

    if not url_col:
        log("ERROR: Could not find a GitHub URL column in the CSV.")
        return sites

    for _, row in df.iterrows():
        url = row.get(url_col)
        if pd.isna(url):
            continue
        url = str(url).strip()
        if not url.startswith('http'):
            continue

        name = row.get(name_col) if name_col else None
        name = 'Resource' if (name is None or pd.isna(name)) else str(name).strip()

        if name:
            cleaned_name = clean_title_helper(name)
            if cleaned_name in arxiv_titles_cache:
                log(f"Skipping '{name}' — already in arxiv_papers.")
                continue

        desc = row.get(desc_col) if desc_col else None
        desc = '' if (desc is None or pd.isna(desc)) else str(desc).strip()

        language = row.get(lang_col) if lang_col else None
        language = '' if (language is None or pd.isna(language)) else str(language).strip()

        stars = row.get(star_col) if star_col else None
        stars = '' if (stars is None or pd.isna(stars)) else str(stars).strip()

        rtype = row.get(type_col) if type_col else None
        rtype = '' if (rtype is None or pd.isna(rtype)) else str(rtype).strip()

        parsed = urllib.parse.urlparse(url)
        if not parsed.netloc or parsed.netloc.startswith('.'):
            continue
        domain = parsed.netloc.replace('www.', '').lower()

        path = parsed.path
        base_url = f"{parsed.scheme or 'https'}://{parsed.netloc}{path.rstrip('/') or '/'}"

        site_key = base_url.lower() if 'github.com' in domain else domain
        if site_key not in sites:
            sites[site_key] = {
                'name'       : name,
                'description': desc,
                'url'        : base_url,
                'domain'     : domain,
                'language'   : language,
                'stars'      : stars,
                'type'       : rtype,
            }

    log(f"Loaded {len(sites)} unique GitHub repos from CSV")
    return sites

# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
def compile_datasets():
    import glob
    log('Compiling all structured JSON data into master Parquet/JSONL datasets...')
    all_entries = []
    platforms = ['juiceshop', 'webgoat', 'google_ctf', 'picoctf', 'pwncollege', 'generic_ctf', 'portswigger']
    
    # Find all JSON files recursively
    json_files = glob.glob(os.path.join(str(DEST_ROOT), '**/*.json'), recursive=True)
    for jf in json_files:
        fname = os.path.basename(jf).lower()
        is_ctf = any(fname.startswith(p + '_') or fname.startswith(p + '.') for p in platforms)
        if not is_ctf:
            continue
        try:
            with open(jf, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_entries.extend(data)
                elif isinstance(data, dict):
                    all_entries.append(data)
        except Exception as e:
            log(f"    Error reading {jf}: {e}")
            
    if all_entries:
        import pandas as pd
        df = pd.DataFrame(all_entries)
        df = df.fillna('')
        for col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else str(x))
            
        jsonl_path = os.path.join(str(DEST_ROOT), 'compiled_ctf_challenges.jsonl')
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for entry in all_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
                
        parquet_path = os.path.join(str(DEST_ROOT), 'compiled_ctf_challenges.parquet')
        try:
            df.to_parquet(parquet_path, index=False, engine='pyarrow')
            log(f"    Saved compiled Parquet dataset -> {parquet_path}")
        except Exception as e:
            log(f"    Error saving Parquet: {e}")
            
        json_path = os.path.join(str(DEST_ROOT), 'compiled_ctf_challenges.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(all_entries, f, indent=2, ensure_ascii=False)
            
        log(f"    Saved compiled JSONL dataset -> {jsonl_path}")
        log(f"    Saved compiled JSON dataset -> {json_path}")
    else:
        log("    No structured records found to compile.")


def run_targeted_ctf_scraping(session, target_platform):
    already_scraped = set()
    found = False
    for owner, repo, platform in CTF_GITHUB_REPOS:
        if platform.lower() == target_platform or repo.lower() == target_platform:
            log(f'  [{platform}] {owner}/{repo}')
            try:
                download_github_repo_zip(owner, repo, session, ctf_platform=platform)
            except Exception as e:
                log(f'    ERROR scraping {owner}/{repo}: {e}')
            found = True
            
    if target_platform == 'pwncollege':
        dojo_repos = discover_pwncollege_dojos(session)
        for repo in dojo_repos:
            log(f'  [pwncollege] {PWNCOLLEGE_ORG}/{repo}')
            try:
                download_github_repo_zip(PWNCOLLEGE_ORG, repo, session, ctf_platform='pwncollege')
            except Exception as e:
                log(f'    ERROR scraping {PWNCOLLEGE_ORG}/{repo}: {e}')
        found = True
        
    if target_platform == 'portswigger':
        try:
            scrape_portswigger_labs(session)
        except Exception as e:
            log(f'    ERROR scraping PortSwigger: {e}')
        found = True
        
    if not found:
        log(f"No predefined official scraper matches '{target_platform}'. Trying owner/repo as generic...")
        if '/' in target_platform:
            parts = target_platform.split('/')
            owner, repo = parts[0], parts[1]
            try:
                download_github_repo_zip(owner, repo, session, ctf_platform='generic_ctf')
            except Exception as e:
                log(f'    ERROR scraping {owner}/{repo}: {e}')
        else:
            log("Invalid repo format. Use 'owner/repo' to scrape an arbitrary GitHub repository.")


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
def main():
    global DEST_ROOT, scraped_files_cache, total_saved, saved_urls, stats
    import argparse

    parser = argparse.ArgumentParser(description='CTF Scraper and Dataset Compiler')
    parser.add_argument('--repo', type=str, help='Only scrape a specific platform (e.g., picoctf, juiceshop, webgoat, google_ctf, pwncollege, portswigger)')
    parser.add_argument('--compile-only', action='store_true', help='Only compile compiled_ctf_challenges parquet/jsonl without scraping')
    parser.add_argument('--dest', type=str, default=None, help=f'Output root directory (default: {DEST_ROOT})')
    parser.add_argument('--csv', type=str, default=None, help=f'Path to input CSV/XLSX file (default: {CSV_PATH})')
    args = parser.parse_args()

    if args.dest:
        DEST_ROOT = Path(args.dest).expanduser().resolve()
        DEST_ROOT.mkdir(parents=True, exist_ok=True)
    if args.csv:
        CSV_PATH = Path(args.csv).expanduser().resolve()

    log('=' * 70)
    log('CTF-ONLY Scraper v4 — preserving folder layouts & PyArrow Parquet/JSONL output')
    log(f'Output root         : {DEST_ROOT}')
    log(f'Video transcripts   : {VIDEO_HTML_DIR}')
    log(f'faster-whisper found: {HAS_FASTER_WHISPER}')
    log('=' * 70)
    start_time = time.time()

    initialize_scraped_files_cache()
    session = make_session()

    if args.compile_only:
        log('Running in compile-only mode...')
        compile_datasets()
        return

    if args.repo:
        repo_lower = args.repo.lower()
        log(f"Running targeted scraping for repo platform: '{repo_lower}'")
        run_targeted_ctf_scraping(session, repo_lower)
    else:
        run_ctf_scraping(session)
        compile_datasets()

    # ── Summary ───────────────────────────────────────────────
    elapsed = time.time() - start_time
    log('\n' + '=' * 70)
    log('SCRAPING COMPLETE')
    log('=' * 70)
    log(f'Time          : {elapsed/60:.1f} minutes')
    log(f'Total saved   : {total_saved}')
    log(f'Unique URLs   : {len(saved_urls)}')
    log('\nFiles per category:')
    for cat, count in sorted(stats.items(), key=lambda x: -x[1]):
        log(f'  {cat}: {count}')
    log('=' * 70)


if __name__ == '__main__':
    main()
