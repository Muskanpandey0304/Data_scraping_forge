# -*- coding: utf-8 -*-
"""
Workbook-driven resource scraper (refactored)
=============================================

Single source of truth: an Excel workbook (one sheet per category). Every
resource URL in the workbook is fetched and stored on disk grouped by
sheet, then by the row's Vendor and Product columns, with the file itself
named after the row's Resource Title:

    <DEST>/<Sheet Name>/<Vendor>/<Product>/<Resource Title>.<ext>

Two rows that legitimately share both a title and a product (e.g. a
"docs" link pinned to a live URL vs. a specific commit) don't overwrite
each other: byte-identical content anywhere in the tree is skipped as a
duplicate (full-content sha256), while different content landing on an
already-used filename gets an 8-char content-hash suffix instead.

There is NO hardcoded list of sites, repos, large datasets, or seed files.
Everything comes from the workbook, and every knob is an env var or CLI flag.

Routing per URL (decided automatically from the host):
  * github.com/<owner>/<repo>   -> whole repo downloaded as a tarball, unpacked,
                                   and every useful source file saved.
  * gitlab.com/<group>/<proj>   -> whole project archive downloaded and unpacked.
  * anything else               -> that exact URL is fetched once and cleaned to
                                   HTML/text (direct fetch only - no sitemap.xml
                                   probing, no RSS/Atom discovery, no crawling to
                                   other pages on the same site).

See the "WHAT GETS DOWNLOADED" note near the extension sets below.
"""

import os
import sys
import re
import time
import random
import hashlib
import threading
import html
import requests
import urllib.parse
import tarfile
import tempfile
import shutil
import argparse
import queue
import atexit
import collections
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup, Comment, Tag

try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

# ---- Soft dependencies -------------------------------------------------
# trafilatura: cleaner main-content extraction for HTML pages.
try:
    import trafilatura
    _HAS_TRAFILATURA = True
except Exception:
    _HAS_TRAFILATURA = False

# Playwright: headless browser for JS-rendered pages (fallback only).
try:
    from playwright.sync_api import sync_playwright
    _HAS_PLAYWRIGHT = True
except Exception:
    _HAS_PLAYWRIGHT = False

# openpyxl: required to read the workbook.
try:
    import openpyxl
    _HAS_OPENPYXL = True
except Exception:
    _HAS_OPENPYXL = False

# markdownify: turns cleaned article HTML into readable Markdown (no images).
try:
    from markdownify import markdownify as _markdownify
    _HAS_MARKDOWNIFY = True
except Exception:
    _HAS_MARKDOWNIFY = False

# weasyprint: renders cleaned article HTML (with its images) to a PDF.
try:
    from weasyprint import HTML as _WeasyHTML
    _HAS_WEASYPRINT = True
except Exception:
    _HAS_WEASYPRINT = False


SCRIPT_DIR = Path(__file__).parent.resolve()


def _env_path(name, default):
    v = os.environ.get(name, "").strip()
    return Path(v).expanduser() if v else Path(default)


def _env_int(name, default):
    v = os.environ.get(name, "").strip()
    try:
        return int(v) if v else default
    except ValueError:
        return default


def _env_bool(name, default):
    v = os.environ.get(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "on")


# =====================================================================
# CONFIGURATION  (env var in brackets; each is also a CLI flag in main())
# Nothing here points at a specific site, repo, or dataset. The only input
# is the workbook, and its path is overridable.
# =====================================================================
# [SCRAPER_EXCEL] The resources workbook. Defaults to the file next to this
# script; override with --excel or the env var.
EXCEL_PATH = _env_path("SCRAPER_EXCEL", SCRIPT_DIR / "Cyber_Defense_Direct_Links_Knowledge_Base.xlsx")

# [SCRAPER_DEST] Where output is written.
DEST_ROOT = _env_path("SCRAPER_DEST", Path.home() / "scraped_resources")

# [SCRAPER_PREV] A previously-scraped corpus consulted for skip/dedup. Anything
# whose filename or content-hash already exists here (or in DEST_ROOT) is skipped.
PART3_DIR = _env_path("SCRAPER_PREV", DEST_ROOT)

# Sheets to ignore entirely (banner/summary tabs with no URL column).
# [SCRAPER_SKIP_SHEETS] comma-separated, case-insensitive.
_skip_env = os.environ.get("SCRAPER_SKIP_SHEETS", "Overview")
SKIP_SHEETS = {s.strip().lower() for s in _skip_env.split(",") if s.strip()}

# ---------------------------------------------------------------------
# HARDCODED EXTRA SOURCES  (scraped IN ADDITION to the workbook)
# ---------------------------------------------------------------------
# These are deliberately hardcoded sites, not from the workbook. Each is
# fetched directly (that one URL only, no sitemap/rss/crawl discovery) and
# still runs through the SAME STRICT quality floor, so only substantial prose
# pages are saved (no stubs, no listing pages). Because you curate these by
# hand, the topic-keyword gate is bypassed by default ("relevance": False)
# so the site's real content isn't dropped for not matching the security
# vocabulary - the quality floor still guarantees the page is worth training
# on. Flip "relevance": True to also require the topic match. Add more dicts
# here to hardcode more sites.
#
# Output lands under: DEST_ROOT / <folder> / <resource> / <files>
EXTRA_SOURCES = [
    {
        "name": "APIVault",
        "url": "https://apivault.dev/",
        "folder": "Extra_Sources/apivault.dev",  # sheet-style folder
        "relevance": False,     # keep all quality content (bypass topic gate)
    },
]
RUN_EXTRA_SOURCES = _env_bool("SCRAPER_EXTRA_SOURCES", True)  # --skip-extra turns off

# State/log files live INSIDE DEST_ROOT (finalized in configure()).
LOG_FILE = _env_path("SCRAPER_LOG", DEST_ROOT / "scraper.log")
PROGRESS_FILE = _env_path("SCRAPER_PROGRESS", DEST_ROOT / "scraped_urls.txt")
MANIFEST_FILE = None          # provenance: one JSON line per saved file
FAILURES_FILE = None          # one JSON line per failed fetch/save
CONTENT_HASHES_FILE = None    # persistent sha256 index for content-level dedup

# Parallelism / politeness.
WORKERS = _env_int("SCRAPER_WORKERS", 60)                        # resource-level parallelism
PER_SOURCE_WORKERS = _env_int("SCRAPER_PER_SOURCE_WORKERS", 25)  # threads inside a site
PER_DOMAIN_MAX_CONCURRENCY = _env_int("SCRAPER_PER_DOMAIN", 6)   # cap per single host
PER_DOMAIN_OVERRIDES = {                                         # generic host tuning
    "github.com": 15,
    "raw.githubusercontent.com": 15,
    "codeload.github.com": 15,
}
REQUEST_TIMEOUT = _env_int("SCRAPER_TIMEOUT", 30)

# Every workbook URL is fetched directly, exactly once, as-is: no
# sitemap.xml probing, no /feed or /rss discovery, no same-domain crawl for
# neighboring pages. Only that single URL is saved, subject to the quality
# gate below.

# JavaScript rendering fallback.
JS_RENDER_WORKERS = _env_int("SCRAPER_JS_WORKERS", 2)
JS_RENDER_TIMEOUT = _env_int("SCRAPER_JS_TIMEOUT", 25)
MIN_CONTENT_CHARS = _env_int("SCRAPER_MIN_CONTENT_CHARS", 200)
JS_ENABLED = False   # finalized in main()

# ---- Article output format (the "blog / doc page has no direct download" case) ----
# Pages that ARE a direct download (pdf, docx, zip, raw .md/.yml, ...) are always
# saved as-is, byte for byte - never touched by this block.
# Everything else (an ordinary blog post / doc page reached via a link, not a
# file) is rendered from its cleaned article HTML into exactly one of:
#   * .pdf  - when the article contains real inline images (diagrams,
#             screenshots, architecture pictures...), so those images are not
#             lost. Requires the optional "weasyprint" package.
#   * .md   - when the article is image-free "clean" prose/reference text.
#             Requires the optional "markdownify" package.
# If the relevant optional package isn't installed, we fall back to saving the
# cleaned .html so nothing is ever silently dropped.
ARTICLE_FORMAT_MODE = os.environ.get("SCRAPER_ARTICLE_FORMAT", "auto").strip().lower()
# auto (default): image-aware -> pdf or md, per the rule above.
# md:   always markdown (images become normal markdown image links to their
#       original src URL, not embedded).
# pdf:  always render to pdf (even image-free pages).
# html: legacy behavior - always save the cleaned .html (old default).
MIN_ARTICLE_IMAGES = _env_int("SCRAPER_MIN_ARTICLE_IMAGES", 1)  # >=N real <img> -> "has images"

# ---- Corpus quality gates (applied to anything about to be SAVED) ----
# Quality floor: reject thin / boilerplate pages (stubs, listing pages).
# Measured on extracted MAIN text, so it reflects real prose.
MIN_CONTENT_WORDS = _env_int("SCRAPER_MIN_WORDS", 50)
# Max ratio of link-anchor characters to total text. Listing/index pages are
# mostly links; real articles are mostly prose. Above this -> treated as a
# navigation page (good for link discovery, not for saving).
MAX_LINK_TEXT_RATIO = float(os.environ.get("SCRAPER_MAX_LINK_RATIO", "0.5"))

# LANGUAGE GATE: only keep English language content; reject non-English URLs & content.
ENABLE_LANGUAGE_FILTER = _env_bool("SCRAPER_LANGUAGE_FILTER", True)

# KEYWORD GATE: (Disabled by default). When True, requires cybersecurity keywords.
ENABLE_KEYWORD_FILTER = _env_bool("SCRAPER_KEYWORD_FILTER", False)
RELEVANCE_MIN_HITS = _env_int("SCRAPER_RELEVANCE_MIN", 2)

# GitHub API token (optional but recommended). Read from env so it never lives
# in the file. Unauthenticated = 60 req/hr; authenticated = 5000.
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_API = "https://api.github.com"
GITHUB_REPO_MAX_FILE_MB = _env_int("SCRAPER_GH_MAX_FILE_MB", 25)

# Hosts that always bypass the (optional) keyword filter. Seeded empty and
# populated at load time from the workbook - curated resources are trusted.
TRUSTED_DOMAINS = set()

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

# Fallback UAs for file downloads behind naive WAFs.
_DL_UA_FALLBACKS = [
    {'User-Agent': 'curl/8.4.0', 'Accept': '*/*'},
    {'User-Agent': 'Wget/1.21.3', 'Accept': '*/*'},
    {'User-Agent': 'python-requests/2.31', 'Accept': '*/*'},
]

# ---------------------------------------------------------------------
# WHAT GETS DOWNLOADED  (file-type buckets, purely by extension).
# From ordinary web pages we save two things:
#   1. the cleaned HTML of each page we visit, and
#   2. any linked file whose extension is in BINARY_DOWNLOAD_EXTS
#      (code / data / archive / document types below).
# We never download the media / font / installer types in SKIP_EXTS.
# From GitHub/GitLab we keep files whose extension is in GITHUB_REPO_KEEP_EXTS.
# ---------------------------------------------------------------------
CODE_EXTS = {
    '.py', '.go', '.js', '.mjs', '.cjs', '.ts', '.tsx', '.jsx', '.java', '.c', '.h',
    '.cpp', '.cc', '.hpp', '.cs', '.rb', '.php', '.sh', '.bash', '.ps1', '.pl',
    '.rs', '.swift', '.kt', '.kts', '.scala', '.r', '.m', '.mm', '.lua', '.sql',
    '.ipynb', '.groovy', '.dart', '.vb', '.asm', '.s', '.f90', '.jl', '.ex', '.exs',
    '.erl', '.clj', '.hs', '.nim', '.zig', '.sol', '.proto', '.tf', '.dockerfile',
    '.vy', '.cairo', '.move', '.circom',   # Web3 / ZK languages
}
DATA_EXTS = {
    '.csv', '.tsv', '.json', '.jsonl', '.xml', '.yaml', '.yml', '.ndjson',
    '.parquet', '.sqlite', '.db', '.log', '.txt', '.md', '.ini', '.cfg', '.conf',
    '.toml', '.env', '.rst', '.adoc', '.markdown',
}
ARCHIVE_EXTS = {'.zip', '.tar', '.gz', '.tgz', '.bz2', '.7z', '.rar', '.xz'}
DOC_EXTS = {'.pdf', '.docx', '.doc', '.rtf', '.xlsx', '.xls', '.pptx', '.ppt', '.odt', '.ods'}

BINARY_DOWNLOAD_EXTS = CODE_EXTS | DATA_EXTS | ARCHIVE_EXTS | DOC_EXTS

SKIP_EXTS = {
    '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico', '.css', '.woff', '.woff2',
    '.ttf', '.eot', '.mp4', '.mp3', '.avi', '.mov', '.wav', '.iso', '.img',
    '.deb', '.sig', '.ova', '.torrent', '.pkg', '.dmg', '.bin', '.msi', '.exe',
}

# Repo files we keep when unpacking a GitHub/GitLab tarball.
# Strictly keep ONLY knowledge formats (.md, .pdf, .yaml, .yml) - no code/json/ui noise!
GITHUB_REPO_KEEP_EXTS = {'.md', '.markdown', '.pdf', '.docx', '.txt', '.yaml', '.yml'}
_GH_SKIP_DIRS = {'node_modules', '.git', '__pycache__', '.vscode', '.idea', 'coverage', 'dist', 'build'}
_GH_DOCTEXT_EXTS = {'.rst', '.adoc', '.markdown', '.md'}

# =====================================================================
# RELEVANCE VOCABULARY  (everything is under the cybersecurity umbrella)
# Each entry is one "concept"; relevance counts how many DISTINCT concepts a
# page hits, so the LENIENT gate can require e.g. 2+ before saving.
# Domains covered: general cyber, AI/ML security, OT/ICS/SCADA, IoT/embedded,
# GRC/audit/compliance, privacy/DLP, blockchain/smart-contract, API/DB/arch.
# =====================================================================
CYBER_KEYWORDS = [
    # ---- general cybersecurity ----
    r'secur(?:ity|ing)', r'vulnerab', r'exploit', r'\bthreat', r'\battack',
    r'malware', r'ransomware', r'spyware', r'trojan', r'rootkit', r'botnet',
    r'\bbreach', r'\bcve\b', r'\bcwe\b', r'\bcapec\b', r'\bapt\b', r'phish',
    r'penetration\s*test', r'pentest', r'red\s*team', r'blue\s*team',
    r'incident\s*response', r'\bsiem\b', r'\bsoc\b', r'\bedr\b', r'\bxdr\b',
    r'firewall', r'\bids\b', r'\bips\b', r'zero\s*trust', r'hardening',
    # ---- AI / ML security ----
    r'\bai\s*security', r'\bml\s*security', r'adversarial\s*(?:ml|example|attack)',
    r'prompt\s*injection', r'jailbreak', r'model\s*(?:poison|steal|extraction)',
    r'data\s*poison', r'llm\s*security', r'\bmlsecops\b', r'model\s*evasion',
    r'atlas', r'garak', r'\bpyrit\b', r'membership\s*inference',
    # ---- OT / ICS / SCADA ----
    r'\bics\b', r'\bscada\b', r'\bot\s*security', r'plc\b', r'modbus',
    r'\bdnp3\b', r'\bs7comm\b', r'purdue\s*model', r'industrial\s*control',
    r'operational\s*technology', r'800-82', r'\bhmi\b', r'field\s*device',
    # ---- IoT / embedded ----
    r'\biot\b', r'embedded\s*(?:security|device|firmware)', r'firmware',
    r'secure\s*boot', r'\brtos\b', r'\buart\b', r'\bjtag\b', r'\bmqtt\b',
    r'\bcoap\b', r'\bzigbee\b', r'\bble\b', r'side[- ]channel', r'op-?tee',
    # ---- GRC / audit / compliance ----
    r'\bgrc\b', r'compliance', r'\baudit', r'iso\s*2700\d', r'\bsoc\s*2\b',
    r'nist\s*csf', r'cybersecurity\s*framework', r'800-53', r'800-171',
    r'\bcis\s*(?:control|benchmark)', r'\boscal\b', r'risk\s*management',
    r'\bcmmc\b', r'\bpci\s*dss\b', r'\bstig\b', r'control\s*(?:mapping|baseline)',
    # ---- privacy engineering / data protection / DLP ----
    r'privacy', r'\bgdpr\b', r'\bccpa\b', r'data\s*protection', r'\bdlp\b',
    r'privacy\s*by\s*design', r'privacy\s*engineering', r'linddun',
    r'\bpii\b', r'data\s*minimi', r'consent', r'de-?identif', r'anonymi',
    r'pseudonymi', r'exfiltration',
    # ---- blockchain / smart-contract ----
    r'blockchain', r'web3', r'smart\s*contract', r'solidity', r'\bevm\b',
    r'reentran', r'\bdefi\b', r'wallet', r'crypto', r'\bkyc\b', r'sybil',
    r'cross-?chain', r'\bbridge', r'rug\s*pull', r'\bzk\b', r'zero-?knowledge',
    r'fuzz', r'flash\s*loan', r'oracle\s*manipulation', r'\berc-?20\b',
    # ---- API / database / secure architecture ----
    r'api\s*security', r'\basvs\b', r'threat\s*model', r'\bstride\b', r'\bpasta\b',
    r'secure\s*architecture', r'well-?architected', r'database\s*security',
    r'sql\s*injection', r'\boauth\b', r'\boidc\b', r'access\s*control',
    r'authentication', r'authorization', r'data\s*flow\s*diagram', r'\bdfd\b',
]
# Compile each concept separately so we can COUNT distinct hits (not just match).
_CYBER_PATS = [re.compile(r'(?i)' + kw) for kw in CYBER_KEYWORDS]
CYBER_PAT = re.compile(r'(?i)' + '|'.join(CYBER_KEYWORDS))   # any-hit convenience


def relevance_hits(text):
    """Number of DISTINCT domain concepts present in text (0..len)."""
    if not text:
        return 0
    return sum(1 for p in _CYBER_PATS if p.search(text))


# =====================================================================
# LOCKS / SHARED STATE
# =====================================================================
save_lock = threading.Lock()
log_lock = threading.Lock()
manifest_lock = threading.Lock()
failures_lock = threading.Lock()
hashes_lock = threading.Lock()
existing_index_lock = threading.Lock()

saved_urls = set()
existing_filenames = set()   # bare filenames, from PART3_DIR + DEST_ROOT (legacy/prev-corpus dedup)
existing_paths = set()       # "<site_folder>/<filename>" relative paths, DEST_ROOT only (this run's tree)
content_hashes = set()
stats = defaultdict(int)
total_saved = 0

html_to_pdf_count = 0
html_to_pdf_fallback_count = 0
html_to_md_count = 0
fallback_records = []
pdf_stats_lock = threading.Lock()


def log(msg):
    ts = time.strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with log_lock:
            with open(LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(line + '\n')
    except Exception:
        pass


# =====================================================================
# MANIFEST / FAILURES / CONTENT-HASH DEDUP
# =====================================================================
def write_manifest(record):
    if not MANIFEST_FILE:
        return
    try:
        import json
        with manifest_lock:
            with open(MANIFEST_FILE, 'a', encoding='utf-8') as f:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
    except Exception:
        pass


def record_failure(url, reason, context=None):
    if not FAILURES_FILE:
        return
    try:
        import json
        rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'url': url,
               'reason': str(reason)[:500]}
        if context:
            rec['context'] = context
        with failures_lock:
            with open(FAILURES_FILE, 'a', encoding='utf-8') as f:
                f.write(json.dumps(rec, ensure_ascii=False) + '\n')
    except Exception:
        pass


def load_content_hashes():
    hs = set()
    if CONTENT_HASHES_FILE and CONTENT_HASHES_FILE.exists():
        try:
            with open(CONTENT_HASHES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        hs.add(line)
        except Exception:
            pass
    return hs


def _hash_seen(h):
    with hashes_lock:
        return h in content_hashes


def _hash_remember(h):
    with hashes_lock:
        if h in content_hashes:
            return
        content_hashes.add(h)
    if CONTENT_HASHES_FILE:
        try:
            with hashes_lock:
                with open(CONTENT_HASHES_FILE, 'a', encoding='utf-8') as f:
                    f.write(h + '\n')
        except Exception:
            pass


# =====================================================================
# GITHUB SHARED RATE-LIMIT GATE
# =====================================================================
_gh_pause_lock = threading.Lock()
_gh_pause_until = 0.0


def _gh_wait_if_paused():
    while True:
        with _gh_pause_lock:
            wait = _gh_pause_until - time.time()
        if wait <= 0:
            return
        time.sleep(min(wait, 30) + 0.1)


def _gh_set_pause(until_ts):
    global _gh_pause_until
    with _gh_pause_lock:
        if until_ts > _gh_pause_until:
            _gh_pause_until = until_ts


# =====================================================================
# HEADLESS-BROWSER RENDER POOL (optional JS fallback)
# =====================================================================
_render_q = queue.Queue()
_render_threads = []
_render_lock = threading.Lock()
_render_started = False
_render_disabled = False


def _render_worker_loop(worker_id):
    global _render_disabled
    pw = browser = None
    try:
        pw = sync_playwright().start()
        browser = pw.chromium.launch(
            headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
    except Exception as e:
        _render_disabled = True
        log(f"  [js] renderer {worker_id}: could not launch browser ({e}). "
            f"JS rendering disabled - run 'playwright install chromium' to enable.")
        while True:
            job = _render_q.get()
            if job is None:
                _render_q.task_done()
                break
            _url, result = job
            result['done'].set()
            _render_q.task_done()
        return

    def _route(route):
        try:
            if route.request.resource_type in ('image', 'media', 'font'):
                route.abort()
            else:
                route.continue_()
        except Exception:
            try:
                route.continue_()
            except Exception:
                pass

    while True:
        job = _render_q.get()
        if job is None:
            _render_q.task_done()
            break
        url, result = job
        try:
            page = browser.new_page(user_agent=HEADERS.get('User-Agent'))
            try:
                page.route('**/*', _route)
            except Exception:
                pass
            try:
                page.goto(url, wait_until='networkidle', timeout=JS_RENDER_TIMEOUT * 1000)
            except Exception:
                pass
            try:
                result['html'] = page.content()
            except Exception as e:
                result['error'] = str(e)
            try:
                page.close()
            except Exception:
                pass
        except Exception as e:
            result['error'] = str(e)
        finally:
            result['done'].set()
            _render_q.task_done()
    try:
        browser.close()
        pw.stop()
    except Exception:
        pass


def start_js_renderer():
    global _render_started
    with _render_lock:
        if _render_started:
            return
        _render_started = True
        for i in range(max(1, JS_RENDER_WORKERS)):
            t = threading.Thread(target=_render_worker_loop, args=(i,), daemon=True)
            t.start()
            _render_threads.append(t)


def render_js(url):
    if not JS_ENABLED or _render_disabled:
        return None
    start_js_renderer()
    if _render_disabled:
        return None
    ev = threading.Event()
    result = {'done': ev}
    _render_q.put((url, result))
    if not ev.wait(timeout=JS_RENDER_TIMEOUT + 15):
        return None
    return result.get('html')


def stop_js_renderer():
    if not _render_started:
        return
    for _ in _render_threads:
        _render_q.put(None)


atexit.register(stop_js_renderer)


# =====================================================================
# PER-DOMAIN THROTTLING
# =====================================================================
_domain_semaphores = {}
_domain_sem_lock = threading.Lock()


def _get_domain_semaphore(url):
    domain = urllib.parse.urlparse(url).netloc.lower()
    max_conc = PER_DOMAIN_OVERRIDES.get(domain, PER_DOMAIN_MAX_CONCURRENCY)
    with _domain_sem_lock:
        sem = _domain_semaphores.get(domain)
        if sem is None:
            sem = threading.Semaphore(max_conc)
            _domain_semaphores[domain] = sem
        return sem


# =====================================================================
# NAMING / STORAGE HELPERS
#
# STORAGE MODEL (the key behaviour): every saved file lands at
#     DEST_ROOT / <sheet folder> / <resource folder> / <filename>
# `site_folder` carries "<sheet>/<resource>" and is preserved as a nested
# path by sanitize_name(). save_file/save_binary_file write straight into it -
# there is no extra per-extension sub-folder.
# =====================================================================
def sanitize_name(name):
    """Sanitize a name, preserving '/' so callers can pass a nested
    "<sheet>/<resource>" path; each segment is cleaned independently."""
    parts = []
    for seg in str(name).replace('\\', '/').split('/'):
        clean = "".join(c if c.isalnum() or c in "._- " else "_" for c in seg).strip('_ ')
        if clean:
            parts.append(clean)
    return "/".join(parts) or "resource"


def _clean_filename(filename, default_ext='.html'):
    clean_fn = "".join(c if c.isalnum() or c in "._- " else "_" for c in str(filename))
    if len(clean_fn) > 200:
        ext = Path(clean_fn).suffix or default_ext
        clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + ext
    return clean_fn


def _sanitize_title_base(title):
    """Sanitize a workbook Resource Title into a bare filename base (no
    extension, no path separators - '/' is escaped like any other unsafe
    char). Returns None if there's no usable title, so callers can fall
    back to a URL-derived name."""
    if not title:
        return None
    base = "".join(c if c.isalnum() or c in "._- " else "_" for c in str(title)).strip('_ ')
    return base or None


def ext_stat_key(filename):
    ext = Path(filename).suffix.lower().lstrip('.')
    ext = "".join(c for c in ext if c.isalnum())
    return ext if ext else 'no_ext'


def bucket_for_ext(ext):
    ext = ext.lower()
    if ext in ARCHIVE_EXTS:
        return 'archives'
    if ext in DOC_EXTS:
        return 'documents'
    if ext in CODE_EXTS:
        return 'code'
    if ext in DATA_EXTS:
        return 'data'
    return 'html'


def site_folder_for(prefix, url):
    """Folder (under DEST_ROOT) for this resource. With the workbook driver the
    prefix is always "<sheet>/<vendor>/<product>"; for a bare URL we fall
    back to host."""
    if prefix:
        return sanitize_name(prefix)
    domain = urllib.parse.urlparse(url).netloc.replace('www.', '')
    return sanitize_name(domain)


def load_saved_urls():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def _index_existing_filenames(root):
    found = set()
    if root and root.exists():
        for p in root.rglob('*'):
            if p.is_file():
                found.add(p.name)
    return found


def _index_existing_paths(root):
    """Index files under `root` as paths relative to it (POSIX-style), e.g.
    "Sheet/Vendor/Product/Title.pdf". Used to scope resumability checks to
    THIS run's own tree, since with title-based filenames the same bare
    name can legitimately recur across different Vendor/Product folders."""
    found = set()
    if root and root.exists():
        for p in root.rglob('*'):
            if p.is_file():
                try:
                    found.add(p.relative_to(root).as_posix())
                except ValueError:
                    pass
    return found


def build_existing_filenames_index():
    """Bare-filename index (legacy behavior) spans PART3_DIR + DEST_ROOT -
    used as a best-effort check against a previous, possibly differently
    organized corpus. Also rebuilds `existing_paths`, a DEST_ROOT-only,
    path-scoped index used for accurate same-run resumability."""
    global existing_paths
    idx = set()
    idx |= _index_existing_filenames(PART3_DIR)
    idx |= _index_existing_filenames(DEST_ROOT)
    existing_paths = _index_existing_paths(DEST_ROOT)
    return idx


def is_already_present(site_folder, clean_fn):
    """True if this exact file (by path, anywhere in THIS run's tree -
    including anything saved earlier in this same run) already exists, OR
    its bare filename was present at start-up in the legacy/prev-corpus
    index (a previously-scraped, possibly differently organized tree).
    The path check is authoritative for this run and keeps title-based
    filenames that legitimately recur across different Vendor/Product
    folders from being treated as duplicates of each other; the bare-name
    check is only a coarse, best-effort resume signal against older data
    and is never updated after start-up (see _reserve_output_path)."""
    rel = f"{site_folder or 'misc'}/{clean_fn}"
    with existing_index_lock:
        return rel in existing_paths or clean_fn in existing_filenames


def _reserve_output_path(site_folder, clean_fn, sha, out_dir):
    """Atomically claim a filename for this content within site_folder. If
    that exact filename is already claimed *in this same folder* (by
    different content - the caller has already ruled out identical content
    via the sha256 dedup check), disambiguate with an 8-char content-hash
    suffix rather than overwriting. Returns (final_clean_fn, final_out_path).

    Only updates `existing_paths` (this run's live, path-scoped index) -
    deliberately never touches the legacy bare-name `existing_filenames`
    set, which must stay a static start-of-run snapshot. Otherwise every
    save in this run would pollute that global set, and two different
    resources in different Vendor/Product folders that happen to share a
    title would start looking like duplicates of each other again."""
    with existing_index_lock:
        out_path = out_dir / clean_fn
        rel = f"{site_folder or 'misc'}/{clean_fn}"
        if rel in existing_paths or out_path.exists():
            stem, ext = os.path.splitext(clean_fn)
            clean_fn = f"{stem}_{sha[:8]}{ext}"
            out_path = out_dir / clean_fn
            rel = f"{site_folder or 'misc'}/{clean_fn}"
        existing_paths.add(rel)
    return clean_fn, out_path


# =====================================================================
# SAVE
# =====================================================================
def save_file(filename, content, bucket='html', site_folder=None,
              source_url=None, title=None, content_type=None, extra=None):
    """Save text to DEST_ROOT/<site_folder>/<filename>. Dedups by sha256 of
    the full content (identical content anywhere in the tree -> skip). A
    different-content file that lands on an already-used filename in the
    same folder gets an 8-char content-hash suffix instead of overwriting."""
    global total_saved
    data = content.encode('utf-8', errors='ignore') if isinstance(content, str) else content
    sha = hashlib.sha256(data).hexdigest()
    clean_fn = _clean_filename(filename, '.html')

    if _hash_seen(sha):
        rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'saved': False,
               'duplicate_of_sha256': sha, 'filename': clean_fn, 'bucket': bucket,
               'site_folder': site_folder, 'source_url': source_url, 'title': title}
        if extra:
            rec.update(extra)
        write_manifest(rec)
        return True

    out_dir = DEST_ROOT / (site_folder or 'misc')
    out_dir.mkdir(parents=True, exist_ok=True)
    clean_fn, out_path = _reserve_output_path(site_folder, clean_fn, sha, out_dir)
    try:
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(content if isinstance(content, str) else data.decode('utf-8', 'ignore'))
        with save_lock:
            stats[ext_stat_key(clean_fn)] += 1
            total_saved += 1
        _hash_remember(sha)
        rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'saved': True, 'sha256': sha,
               'filename': clean_fn, 'bucket': bucket, 'site_folder': site_folder,
               'path': str(out_path), 'bytes': len(data), 'source_url': source_url,
               'title': title, 'content_type': content_type}
        if extra:
            rec.update(extra)
        write_manifest(rec)
        return True
    except Exception as e:
        record_failure(source_url or clean_fn, f"save_file: {e}")
        return False


def save_binary_file(filename, content_bytes, bucket, site_folder=None,
                     source_url=None, title=None, content_type=None, extra=None):
    """Save raw bytes to DEST_ROOT/<site_folder>/<filename>. Dedups by sha256
    of the full content (identical content anywhere in the tree -> skip). A
    different-content file that lands on an already-used filename in the
    same folder gets an 8-char content-hash suffix instead of overwriting."""
    global total_saved
    sha = hashlib.sha256(content_bytes).hexdigest()
    clean_fn = _clean_filename(filename, Path(str(filename)).suffix or '')

    if _hash_seen(sha):
        rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'saved': False,
               'duplicate_of_sha256': sha, 'filename': clean_fn, 'bucket': bucket,
               'site_folder': site_folder, 'source_url': source_url, 'title': title}
        if extra:
            rec.update(extra)
        write_manifest(rec)
        return True

    out_dir = DEST_ROOT / (site_folder or 'misc')
    out_dir.mkdir(parents=True, exist_ok=True)
    clean_fn, out_path = _reserve_output_path(site_folder, clean_fn, sha, out_dir)
    try:
        with open(out_path, 'wb') as f:
            f.write(content_bytes)
        with save_lock:
            stats[ext_stat_key(clean_fn)] += 1
            total_saved += 1
        _hash_remember(sha)
        rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'saved': True, 'sha256': sha,
               'filename': clean_fn, 'bucket': bucket, 'site_folder': site_folder,
               'path': str(out_path), 'bytes': len(content_bytes), 'source_url': source_url,
               'title': title, 'content_type': content_type}
        if extra:
            rec.update(extra)
        write_manifest(rec)
        return True
    except Exception as e:
        record_failure(source_url or clean_fn, f"save_binary_file: {e}")
        return False


# =====================================================================
# FETCH
# =====================================================================
def fetch(url, session, retries=4):
    """Per-domain throttled GET with exponential backoff + jitter."""
    sem = _get_domain_semaphore(url)
    with sem:
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(0.05, 0.3))
                r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                if r.status_code == 429 or r.status_code in (502, 503, 504):
                    retry_after = r.headers.get('Retry-After')
                    try:
                        wait = float(retry_after) if retry_after else (2 ** attempt) + random.uniform(0, 1)
                    except ValueError:
                        wait = (2 ** attempt) + random.uniform(0, 1)
                    if attempt < retries - 1:
                        log(f"  [{r.status_code}] backing off {wait:.1f}s for "
                            f"{urllib.parse.urlparse(url).netloc}")
                        time.sleep(min(wait, 60))
                        continue
                return r
            except Exception:
                if attempt < retries - 1:
                    time.sleep(min((2 ** attempt) + random.uniform(0, 1), 30))
    return None


def fetch_download(url, session, retries=3):
    """Fetch a file URL, retrying with curl/wget UAs if the browser UA is blocked."""
    r = fetch(url, session, retries=retries)
    if r is not None and r.status_code == 200:
        return r
    sem = _get_domain_semaphore(url)
    for hdrs in _DL_UA_FALLBACKS:
        with sem:
            try:
                time.sleep(random.uniform(0.05, 0.3))
                r2 = session.get(url, headers=hdrs, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            except Exception:
                r2 = None
        if r2 is not None and r2.status_code == 200:
            return r2
        if r2 is not None:
            r = r2
    return r


# =====================================================================
# LANGUAGE FILTER (ENGLISH ONLY) + RELEVANCE / QUALITY GATES
# =====================================================================
NON_EN_PATH_PAT = re.compile(
    r'/(?:zh-cn|zh-tw|zh-hans|zh-hant|zh|ja-jp|ja|ko-kr|ko|de-de|de|fr-fr|fr|es-es|es-mx|es|it-it|it|pt-br|pt-pt|pt|ru-ru|ru|nl-nl|nl|pl-pl|pl|ar-sa|ar|he-il|he|tr-tr|tr|id-id|id|vi-vn|vi|th-th|th|sv-se|sv|da-dk|da|fi-fi|fi|nb-no|no|cs-cz|cs|hu-hu|hu|ro-ro|ro|uk-ua|uk)(?:/|$)',
    re.IGNORECASE
)

# Common structural English words
ENGLISH_STOPWORDS = {
    'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'it',
    'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at', 'this',
    'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her', 'she', 'or',
    'an', 'will', 'my', 'one', 'all', 'would', 'there', 'their', 'what',
    'so', 'up', 'out', 'if', 'about', 'who', 'get', 'which', 'go', 'me',
    'when', 'make', 'can', 'like', 'time', 'no', 'just', 'him', 'know',
    'take', 'into', 'your', 'good', 'some', 'could', 'them', 'see', 'other',
    'than', 'then', 'now', 'look', 'only', 'its', 'over', 'think', 'also',
    'back', 'after', 'use', 'how', 'our', 'work', 'first', 'well', 'way',
    'even', 'new', 'want', 'because', 'any', 'these', 'give', 'most', 'us',
    'is', 'are', 'was', 'were', 'been', 'has', 'had', 'does', 'did', 'set',
    'using', 'used', 'such', 'more', 'both', 'between', 'each', 'under'
}

# Common non-English stop words
FOREIGN_STOPWORDS = {
    # French
    'le', 'la', 'les', 'de', 'des', 'du', 'un', 'une', 'et', 'est', 'en',
    'que', 'qui', 'dans', 'au', 'aux', 'ce', 'ces', 'son', 'sa', 'ses',
    'pour', 'par', 'sur', 'pas', 'plus', 'avec', 'se', 'ne', 'mais', 'ou',
    'décrit', 'cette', 'sont', 'nous', 'vous', 'leur', 'leurs',
    # German
    'der', 'die', 'das', 'und', 'in', 'zu', 'den', 'nicht', 'von', 'sie',
    'ist', 'des', 'sich', 'mit', 'dem', 'dass', 'er', 'es', 'ein', 'ich',
    'auf', 'eine', 'auch', 'als', 'an', 'nach', 'wie', 'im', 'für', 'oder',
    'beschreibt', 'diese', 'dieser', 'dieses', 'sind', 'über', 'unter',
    # Spanish
    'el', 'los', 'las', 'del', 'unos', 'unas', 'por',
    'para', 'con', 'su', 'sus', 'al', 'lo', 'como', 'más', 'pero',
    'este', 'esta', 'estos', 'estas', 'guía', 'proporciona', 'información',
    'cómo', 'configurar', 'políticas', 'seguridad', 'permisos',
    # Italian
    'il', 'gli', 'della', 'dei', 'delle', 'ed', 'per', 'su', 'tra', 'fra', 'questo', 'questa',
    # Portuguese
    'uma', 'uns', 'umas', 'dos', 'das', 'no', 'na', 'nos', 'nas', 'pelo', 'pela', 'pelos', 'pelas'
}

# Non-Latin character blocks: CJK, Japanese Kana, Korean Hangul, Cyrillic, Arabic, Hebrew, Thai, Devanagari
NON_LATIN_SCRIPTS = re.compile(r'[\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af\u0400-\u04ff\u0600-\u06ff\u0590-\u05ff\u0e00-\u0e7f\u0900-\u097f]')


def is_non_english_url(url):
    """Pre-fetch check: detects if URL specifies a non-English locale (e.g. ?hl=zh-tw, /de-de/, /fr/)."""
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    for k in ('hl', 'lang', 'language', 'locale', 'in_lang'):
        if k in qs:
            for val in qs[k]:
                v = val.lower().strip()
                if v and not (v == 'en' or v.startswith('en-') or v.startswith('en_')):
                    return True, f"url_query({k}={val})"
    m = NON_EN_PATH_PAT.search(parsed.path)
    if m:
        return True, f"url_path({m.group(0)})"
    return False, None


def is_english_content(text, clean_html_doc=None):
    """Detect if content is in English:
    1. HTML <html lang="..."> tag check
    2. Non-Latin alphabet ratio check
    3. English vs foreign stop words frequency analysis"""
    if not ENABLE_LANGUAGE_FILTER:
        return True, "ok"

    if clean_html_doc:
        try:
            soup = BeautifulSoup(clean_html_doc, 'html.parser')
            html_tag = soup.find('html')
            if html_tag and html_tag.get('lang'):
                hl = html_tag.get('lang').lower().strip()
                if not (hl.startswith('en') or hl.startswith('en-') or hl.startswith('en_')):
                    if any(hl.startswith(prefix) for prefix in (
                            'zh', 'ja', 'ko', 'de', 'fr', 'es', 'it', 'ru', 'pt', 'nl', 'pl',
                            'ar', 'he', 'tr', 'id', 'vi', 'th', 'sv', 'da', 'fi', 'no', 'cs',
                            'hu', 'ro', 'uk')):
                        return False, f"html_lang({hl})"
        except Exception:
            pass

    if not text:
        return True, "ok"

    total_alpha = sum(1 for c in text if c.isalpha())
    if total_alpha >= 20:
        non_latin_count = len(NON_LATIN_SCRIPTS.findall(text))
        if non_latin_count / total_alpha > 0.15:
            return False, f"non_latin_script({non_latin_count/total_alpha:.1%})"

    words = [w.lower() for w in re.findall(r'\b[^\W\d_]{2,}\b', text, re.UNICODE)]
    if len(words) >= 10:
        en_hits = sum(1 for w in words if w in ENGLISH_STOPWORDS)
        foreign_hits = sum(1 for w in words if w in FOREIGN_STOPWORDS)
        if foreign_hits > en_hits and foreign_hits >= 2:
            return False, f"foreign_text(en={en_hits}, foreign={foreign_hits})"
        if en_hits == 0 and foreign_hits >= 2:
            return False, f"foreign_text(en=0, foreign={foreign_hits})"

    return True, "ok"


def is_relevant(url, text=None):
    """Topic gate (disabled by default). If ENABLE_KEYWORD_FILTER is False, all resources pass."""
    if not ENABLE_KEYWORD_FILTER:
        return True
    host = urllib.parse.urlparse(url).netloc.lower()
    if host in TRUSTED_DOMAINS or host.replace('www.', '') in TRUSTED_DOMAINS:
        return True
    if CYBER_PAT.search(url):
        return True
    if text is None:
        return True
    return relevance_hits(text) >= RELEVANCE_MIN_HITS


def passes_quality(text, clean_html_doc=None, raw_html=None):
    """Quality and language floor for anything about to be SAVED.
    Rejects non-English, thin stubs, and link-heavy navigation menus/directories."""
    if ENABLE_LANGUAGE_FILTER:
        is_en, en_why = is_english_content(text, clean_html_doc)
        if not is_en:
            return False, f"non_english:{en_why}"

    words = len((text or "").split())
    if words < MIN_CONTENT_WORDS:
        return False, f"too_short({words}w<{MIN_CONTENT_WORDS})"

    # Link-heavy navigation/listing/directory pages: check anchor text ratio on raw/clean HTML
    html_to_check = raw_html or clean_html_doc
    if html_to_check:
        try:
            soup = BeautifulSoup(html_to_check, 'html.parser')
            for tag in soup(['script', 'style', 'noscript', 'header', 'footer', 'nav', 'aside']):
                tag.decompose()
            anchor_chars = sum(len(a.get_text() or "") for a in soup.find_all('a'))
            total_chars = max(1, len(soup.get_text() or ""))
            ratio = anchor_chars / total_chars
            if ratio > MAX_LINK_TEXT_RATIO:
                return False, f"link_heavy_menu({ratio:.2f}>{MAX_LINK_TEXT_RATIO})"
        except Exception:
            pass

    return True, "ok"


# =====================================================================
# HTML CLEANING
# =====================================================================
# Headings that mark a trailing boilerplate section we don't want in a saved
# article: reference/citation lists, "related posts" link-dumps, feedback
# widgets, etc. Everything from a matching heading to the next heading of the
# same-or-higher level is dropped.
_REF_HEADING_PAT = re.compile(
    r'(?i)^\s*(references?|citations?|footnotes?|endnotes?|bibliography|'
    r'sources?|see\s*also|related\s*(?:articles?|posts?|links?|content|topics?|'
    r'resources?)|further\s*reading|additional\s*resources?|read\s*(?:more|next)|'
    r'you\s*(?:might|may)\s*also\s*like|recommended\s*(?:for\s*you|reading|articles?)|'
    r"what.?s\s*next|next\s*steps?|in\s*this\s*article|table\s*of\s*contents|"
    r"was\s*this\s*(?:page|article|helpful)\W*|feedback|leave\s*a\s*reply|"
    r'\d*\s*comments?|share\s*this\s*(?:article|post)?)\s*:?\s*$'
)

# Containers that are reference/citation/related-content boilerplate by
# class or id, whether or not there's a heading in front of them (merges the
# original nav/footer/social noise set with reference-specific terms).
_BOILERPLATE_CLASS_ID_PAT = re.compile(
    r'(?i)\b(nav|navigation|sidebar|left-sidebar|right-sidebar|menu|footer|header|breadcrumb|breadcrumbs|related|social|'
    r'share|cookie|cookie-banner|cookie-notice|advertisement|ads|promo|widget|toc\b|table-of-contents|'
    r'comments?|comment-section|comment-form|disqus|subscribe|newsletter|popup|'
    r'modal|tag-cloud|categories|archives|pagination|pager|toolbar|topbar|'
    r'masthead|site-header|site-footer|footnotes?|endnotes?|citations?|'
    r'cite-list|bibliograph|reference-?list|references?-?section|see-also|'
    r'read-next|next-steps?|feedback-widget|was-this-helpful|feedback|survey|'
    r'author-bio|byline|chat-widget|intercom|drift-widget|zendesk|search-container|search-box|search-bar)\b')

# Footnote-marker text left over once its wrapping <a>/<sup> is unwrapped:
# bare numbers, bracketed/parenthesized numbers, carets, up-arrows, or
# unicode superscript digits - never real prose on its own.
_FOOTNOTE_MARKER_TEXT_PAT = re.compile(
    r'^[\[\(]?\s*\d{1,3}\s*[\]\)]?$|^[\^\*\u2191\u21a9\u00b9\u00b2\u00b3'
    r'\u2070\u2074-\u2079]+$')

# Inline bracket-style citation markers embedded in normal prose, e.g.
# "zero trust architectures [12][13]" or "as shown previously [3]".
_INLINE_CITATION_PAT = re.compile(r'\s?\[\d{1,3}\](?:\[\d{1,3}\])*')


def _strip_reference_noise(main):
    """In-place cleanup of an extracted article container so a converted
    PDF/Markdown file ends with real content, not a References/Footnotes/
    Related-posts link-dump or orphaned citation markers. Safe to call on
    output from either trafilatura or the bs4 fallback."""
    if main is None:
        return main
    try:
        for tag in main(['script', 'style', 'noscript', 'iframe']):
            tag.decompose()
        for c in main.find_all(string=lambda t: isinstance(t, Comment)):
            c.extract()
    except Exception:
        pass

    # 1) Heading-triggered trailing sections (References, See also, Related
    #    posts, Was this page helpful?, ...): drop the heading and every
    #    sibling up to the next heading of equal-or-higher level.
    try:
        for h in list(main.find_all(re.compile(r'^h[1-6]$'))):
            if not h.parent:
                continue  # already removed as part of an earlier section
            if not _REF_HEADING_PAT.match(h.get_text(' ', strip=True)):
                continue
            level = int(h.name[1])
            sib = h.find_next_sibling()
            while sib is not None:
                nxt = sib.find_next_sibling()
                if isinstance(sib, Tag) and re.match(r'^h[1-6]$', sib.name or '') \
                        and int(sib.name[1]) <= level:
                    break
                try:
                    sib.decompose()
                except Exception:
                    pass
                sib = nxt
            try:
                h.decompose()
            except Exception:
                pass
    except Exception:
        pass

    # 2) Reference/footnote/related-content containers by class or id, with
    #    or without a heading in front of them.
    try:
        for el in list(main.find_all(True)):
            if not isinstance(el, Tag) or not el.parent:
                continue
            classes = ' '.join(el.get('class', []) or [])
            el_id = el.get('id', '') or ''
            if _BOILERPLATE_CLASS_ID_PAT.search(classes) or _BOILERPLATE_CLASS_ID_PAT.search(el_id):
                try:
                    el.decompose()
                except Exception:
                    pass
    except Exception:
        pass

    # 3) Superscript footnote markers - almost always a citation number, not
    #    prose (e.g. "...zero trust<sup><a href='#fn3'>3</a></sup>").
    try:
        for sup in main.find_all('sup'):
            try:
                sup.decompose()
            except Exception:
                pass
    except Exception:
        pass

    # 4) In-page anchor links (href="#..."): footnote backrefs / "back to
    #    top" / TOC jumps. Drop pure markers outright; unwrap anything else
    #    so the sentence stays intact but the dead internal link is gone.
    try:
        for a in list(main.find_all('a', href=True)):
            href = (a.get('href') or '').strip()
            if not href.startswith('#'):
                continue
            txt = a.get_text(strip=True)
            try:
                if not txt or _FOOTNOTE_MARKER_TEXT_PAT.match(txt):
                    a.decompose()
                else:
                    a.replace_with(txt)
            except Exception:
                pass
    except Exception:
        pass

    # 5) Leftover bracket-style citation markers in plain prose.
    try:
        for node in main.find_all(string=True):
            if isinstance(node, Comment):
                continue
            new_text = _INLINE_CITATION_PAT.sub('', str(node))
            if new_text != str(node):
                node.replace_with(new_text)
    except Exception:
        pass

    return main


def clean_html(raw_html, source_url):
    if _HAS_TRAFILATURA and '0xdf.gitlab.io' not in source_url:
        try:
            text = trafilatura.extract(raw_html, url=source_url, include_comments=False,
                                       include_tables=True, include_images=True, favor_recall=True)
            body = trafilatura.extract(raw_html, url=source_url, include_comments=False,
                                       include_tables=True, include_images=True, favor_recall=True,
                                       output_format='html')
            if text and len(text) >= 50:
                title = None
                try:
                    meta = trafilatura.extract_metadata(raw_html)
                    if meta and getattr(meta, 'title', None):
                        title = meta.title
                except Exception:
                    title = None
                if not title:
                    ts = BeautifulSoup(raw_html, 'html.parser').find('title')
                    title = ts.string.strip() if ts and ts.string else "Scraped Resource"
                body_html = body if body else f"<pre>{html.escape(text)}</pre>"
                # Run the same reference/footnote/boilerplate scrub trafilatura's
                # own output gets as the bs4 fallback does, then re-derive the
                # plain-text `text` from the SAME cleaned fragment so the
                # quality/relevance gates downstream see what's actually kept.
                try:
                    frag = BeautifulSoup(body_html, 'html.parser')
                    _strip_reference_noise(frag)
                    cleaned_text = frag.get_text(' ', strip=True)
                    if len(cleaned_text) >= 50:  # guard against over-aggressive scrub
                        body_html = str(frag)
                        text = cleaned_text
                except Exception:
                    pass
                clean_doc = (
                    '<!DOCTYPE html>\n<html>\n<head><meta charset="utf-8">'
                    f'<title>{html.escape(title)}</title></head>\n<body>\n'
                    f'<p>Source: <a href="{source_url}">{source_url}</a></p><hr>\n'
                    f'{body_html}\n</body>\n</html>')
                return clean_doc, title, text
        except Exception:
            pass
    return _clean_html_bs4(raw_html, source_url)


def _clean_html_bs4(raw_html, source_url):
    soup = BeautifulSoup(raw_html, 'html.parser')
    for tag in soup(['script', 'style', 'noscript', 'iframe', 'svg']):
        tag.decompose()
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()
    for tag_name in {'header', 'footer', 'nav', 'aside'}:
        for el in soup.find_all(tag_name):
            try:
                el.decompose()
            except Exception:
                pass
    for el in list(soup.find_all(True)):
        if not isinstance(el, Tag):
            continue
        try:
            classes = ' '.join(el.get('class', []) or [])
            el_id = el.get('id', '') or ''
        except Exception:
            continue
        if _BOILERPLATE_CLASS_ID_PAT.search(classes) or _BOILERPLATE_CLASS_ID_PAT.search(el_id):
            try:
                el.decompose()
            except Exception:
                pass
    main = None
    # If this is 0xdf or standard tech blog, ensure specific tags, author widgets, and disqus are purged
    if '0xdf.gitlab.io' in source_url:
        for selector in ['.tags', '.post-tags', '.share-box', '.disqus', '#disqus_thread', '.related', '.author-box']:
            for el in soup.select(selector):
                try:
                    el.decompose()
                except Exception:
                    pass

    for sel in ['article', 'main', '[role="main"]', '.post-content', '.entry-content',
                '.article-body', '.content', '.post', '#content', '#main', '.blog-post']:
        found = soup.select_one(sel)
        if found:
            main = found
            break
    if not main:
        main = soup.body if soup.body else soup

    # 0xdf specific: Strip 'Box Info' metadata block and all banner/radar/badge images before 'Recon'
    if '0xdf.gitlab.io' in source_url and main:
        try:
            recon_h = None
            for h in main.find_all(['h2', 'h1']):
                if 'recon' in h.get_text().lower():
                    recon_h = h
                    break
            if recon_h:
                curr = recon_h.previous_sibling
                while curr:
                    prev = curr.previous_sibling
                    if isinstance(curr, Tag):
                        txt = curr.get_text(strip=True).lower()
                        if 'box info' in txt or curr.name in ['picture', 'img'] or curr.find(['picture', 'img']):
                            curr.decompose()
                    curr = prev
        except Exception:
            pass
    # References/footnotes/related-posts sections + citation markers, before
    # the blanket "turn every remaining <a> into plain text" pass below.
    _strip_reference_noise(main)
    # Ensure images inside main have absolute URLs and proper responsive styling
    for img_tag in main.find_all('img', src=True):
        img_tag['src'] = urllib.parse.urljoin(source_url, img_tag['src'])
        img_tag['style'] = 'max-width: 100%; height: auto; display: block; margin: 15px auto; border: 1px solid #ddd; border-radius: 4px;'
    for a_tag in list(main.find_all('a')):
        try:
            a_tag.replace_with(a_tag.get_text())
        except Exception:
            pass
    title_tag = soup.find('title')
    title = title_tag.string.strip() if title_tag and title_tag.string else "Scraped Resource"
    text = main.get_text(separator=' ', strip=True)
    clean_doc = (f'<!DOCTYPE html>\n<html>\n<head><meta charset="utf-8">'
                 f'<title>{title}</title></head>\n<body>\n'
                 f'{main}\n</body>\n</html>')
    return clean_doc, title, text


# =====================================================================
# ARTICLE OUTPUT: image-aware PDF-vs-Markdown branching
# =====================================================================
# Tiny/spacer/tracking-pixel images and known icon/logo/avatar paths don't
# count as "the article has images" - they're noise, not content.
_NOISE_IMG_PAT = re.compile(
    r'(?i)(?:^|/)(?:pixel|spacer|blank|1x1|tracking|beacon|avatar|gravatar|'
    r'logo|icon|favicon|sprite|badge|button)[^/]*\.(?:png|gif|jpe?g|svg|webp)')


def _article_images(article_html, base_url):
    """Return the list of real (non-noise) absolute image URLs in the cleaned
    article HTML. Used both to decide the output format and, for the PDF
    path, so WeasyPrint can resolve them against base_url."""
    try:
        soup = BeautifulSoup(article_html, 'html.parser')
    except Exception:
        return []
    imgs = []
    for img in soup.find_all('img'):
        src = (img.get('src') or img.get('data-src') or '').strip()
        if not src or src.startswith('data:'):
            continue
        try:
            w = int(re.sub(r'[^\d]', '', str(img.get('width') or '')) or 0)
            h = int(re.sub(r'[^\d]', '', str(img.get('height') or '')) or 0)
            if (w and w <= 2) or (h and h <= 2):
                continue  # 1x1 tracking pixel
        except Exception:
            pass
        if _NOISE_IMG_PAT.search(src):
            continue
        imgs.append(urllib.parse.urljoin(base_url, src))
    # de-dupe, keep order
    return list(dict.fromkeys(imgs))


def _html_to_markdown(article_html, url, title):
    """Cleaned article HTML -> readable Markdown with a single clean source header."""
    body_html = article_html
    try:
        soup = BeautifulSoup(article_html, 'html.parser')
        for p in soup.find_all('p'):
            if p.get_text().strip().startswith('Source:'):
                p.decompose()
        for hr in soup.find_all('hr'):
            hr.decompose()
        body_html = str(soup.body) if soup.body else str(soup)
    except Exception:
        pass
    if _HAS_MARKDOWNIFY:
        try:
            body_md = _markdownify(body_html, heading_style="ATX", strip=['script', 'style'])
        except Exception:
            body_md = BeautifulSoup(body_html, 'html.parser').get_text('\n', strip=True)
    else:
        body_md = BeautifulSoup(body_html, 'html.parser').get_text('\n', strip=True)
    body_md = re.sub(r'\n{3,}', '\n\n', body_md).strip()
    header = f"# {title or 'Scraped Resource'}\n\n"
    return header + body_md + "\n"


_pdf_render_lock = threading.Lock()

def _html_to_pdf_bytes(article_html, url, title):
    """Cleaned article HTML (images resolved against url) -> PDF bytes.
    Uses WeasyPrint if available; otherwise falls back to Playwright (Chromium)
    with thread-locking to prevent concurrent C++ driver collisions."""
    doc = (f'<!DOCTYPE html><html><head><meta charset="utf-8">'
           f'<title>{html.escape(title or "Scraped Resource")}</title>'
           f'<style>body{{font-family:sans-serif;max-width:800px;margin:2em auto;'
           f'line-height:1.45}} img{{max-width:100%}} '
           f'</style></head>'
           f'<body><h1>{html.escape(title or "Scraped Resource")}</h1>'
           f'{article_html}</body></html>')

    if _HAS_WEASYPRINT:
        try:
            return _WeasyHTML(string=doc, base_url=url).write_pdf()
        except Exception:
            if not _HAS_PLAYWRIGHT:
                raise

    if _HAS_PLAYWRIGHT:
        with _pdf_render_lock:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
                )
                try:
                    page = browser.new_page()
                    page.set_content(doc, wait_until='load')
                    try:
                        page.wait_for_load_state('networkidle', timeout=3000)
                    except Exception:
                        pass
                    pdf_bytes = page.pdf(
                        format="A4",
                        print_background=True,
                        margin={"top": "15mm", "bottom": "15mm", "left": "15mm", "right": "15mm"}
                    )
                    return pdf_bytes
                finally:
                    try:
                        browser.close()
                    except Exception:
                        pass

    raise RuntimeError("Neither WeasyPrint nor Playwright Chromium is available for PDF conversion")


def save_article_smart(article_html, url, title, site_folder, prefix_path,
                       resource_title=None, extra=None):
    """Save one already-cleaned article (blog post / doc page with no direct
    download link) as EITHER .pdf (has real images) or .md (clean text-only),
    per ARTICLE_FORMAT_MODE. Falls back to .html if the needed optional
    package is missing, so content is never lost.

    Filename base: the workbook's Resource Title (resource_title) when
    available, else the page's own extracted <title> (title), else a
    URL-path-derived fallback."""
    path_part = (_sanitize_title_base(resource_title) or _sanitize_title_base(title) or
                urllib.parse.urlparse(url).path.strip('/').replace('/', '_') or 'index')
    imgs = _article_images(article_html, url)
    has_images = len(imgs) >= MIN_ARTICLE_IMAGES
    mode = ARTICLE_FORMAT_MODE
    is_htb_or_thm = ('0xdf.gitlab.io' in url) or ('tryhackme' in url.lower()) or ('try hack me' in str(site_folder).lower())
    want_pdf = (mode == 'pdf') or is_htb_or_thm or (mode == 'auto' and has_images)

    if want_pdf:
        try:
            pdf_bytes = _html_to_pdf_bytes(article_html, url, title)
            rec_extra = dict(extra or {})
            rec_extra.update({'format': 'pdf', 'image_count': len(imgs), 'pdf_engine': 'playwright/weasyprint'})
            log(f"    [HTML->PDF SUCCESS] Converted article with {len(imgs)} image(s) -> {path_part}.pdf")
            saved = save_binary_file(f"{path_part}.pdf", pdf_bytes, bucket='documents',
                                     site_folder=site_folder, source_url=url,
                                     title=title, extra=rec_extra)
            if saved:
                with pdf_stats_lock:
                    global html_to_pdf_count
                    html_to_pdf_count += 1
            return saved
        except Exception as e:
            with pdf_stats_lock:
                global html_to_pdf_fallback_count
                html_to_pdf_fallback_count += 1
                fallback_records.append({
                    'url': url,
                    'title': title or resource_title or path_part,
                    'image_count': len(imgs),
                    'error': str(e)
                })
            log(f"    [HTML->PDF FAILED] ⚠️ Could not convert {url} ({e}) -> FALLING BACK to Markdown (.md)")
            # fall through to markdown below

    if mode == 'html' or (not _HAS_MARKDOWNIFY and not _HAS_WEASYPRINT and not _HAS_PLAYWRIGHT):
        log(f"    [HTML ONLY] Saved raw HTML -> {path_part}.html")
        return save_file(f"{path_part}.html", article_html, bucket='html',
                         site_folder=site_folder, source_url=url, title=title,
                         extra=extra)

    md_text = _html_to_markdown(article_html, url, title)
    rec_extra = dict(extra or {})
    rec_extra.update({'format': 'markdown', 'image_count': len(imgs)})
    log(f"    [HTML->MD] Saved text article ({len(imgs)} images) -> {path_part}.md")
    saved_md = save_file(f"{path_part}.md", md_text, bucket='markdown',
                         site_folder=site_folder, source_url=url, title=title,
                         extra=rec_extra)
    if saved_md and not has_images:
        with pdf_stats_lock:
            global html_to_md_count
            html_to_md_count += 1
    return saved_md


def _get_response_text(r):
    """Extract clean UTF-8 text from response, preventing Latin-1 mojibake encoding corruption."""
    if r is None:
        return ""
    if not r.encoding or r.encoding.lower() in ('iso-8859-1', 'latin-1'):
        r.encoding = r.apparent_encoding or 'utf-8'
    try:
        return r.text
    except Exception:
        try:
            return r.content.decode('utf-8', errors='replace')
        except Exception:
            return ""


# =====================================================================
# GENERIC PAGE / FILE SCRAPE
# =====================================================================
def scrape_and_save(url, session, prefix="", require_relevance=True, resource_title=None):
    """`resource_title` is the workbook's Resource Title (or an equivalent
    display name for hardcoded extra sources). When present it becomes the
    filename base everywhere a filename would otherwise be derived from the
    URL, so files read cleanly inside their Vendor/Product folder. Falls
    back to the old URL-derived naming when no title is available."""
    with save_lock:
        if url in saved_urls:
            return False
        saved_urls.add(url)
        try:
            with open(PROGRESS_FILE, 'a', encoding='utf-8') as _pf:
                _pf.write(url + '\n')
        except Exception:
            pass

    url_lower = url.lower()
    if any(url_lower.endswith(ext) for ext in SKIP_EXTS):
        return False

    if ENABLE_LANGUAGE_FILTER:
        is_non_en, why_non_en = is_non_english_url(url)
        if is_non_en:
            log(f"  SKIP (non-english:{why_non_en}): {url}")
            return False

    parsed = urllib.parse.urlparse(url)
    is_github = 'github.com' in parsed.netloc or 'raw.githubusercontent.com' in parsed.netloc
    path_ext = Path(parsed.path).suffix.lower()
    site_folder = site_folder_for(prefix, url)
    title_base = _sanitize_title_base(resource_title)

    def _basename(fallback, ext):
        return f"{title_base}{ext}" if title_base else f"{fallback}{ext}"

    # Resumability: skip if we already have a file that would match this URL.
    try:
        if is_github:
            path_parts = [p for p in parsed.path.strip('/').split('/') if p]
            if len(path_parts) >= 3 and path_parts[2] in (
                    'issues', 'pull', 'commit', 'commits', 'releases', 'tags',
                    'actions', 'projects', 'wiki', 'settings', 'branches', 'pulse',
                    'graphs', 'network', 'stargazers', 'watchers', 'tree'):
                return False
            if len(path_parts) >= 4 and path_parts[2] in ('blob', 'raw'):
                owner, repo = path_parts[0], path_parts[1]
                flat = '_'.join(path_parts[4:])
                file_ext = Path('/'.join(path_parts[4:])).suffix.lower()
                ext = file_ext if file_ext in BINARY_DOWNLOAD_EXTS else '.html'
                clean_fn = _basename(f"github_{owner}_{repo}_{flat}", ext)
            elif len(path_parts) >= 2:
                clean_fn = _basename(f"github_{path_parts[0]}_{path_parts[1]}", '.html')
            else:
                clean_fn = _basename(f"github_{'_'.join(path_parts) or 'index'}", '.html')
        else:
            if path_ext in BINARY_DOWNLOAD_EXTS:
                path_part = parsed.path.strip('/').split('/')[-1] or f'document{path_ext}'
                fallback_stem = urllib.parse.unquote(Path(path_part).stem) or 'document'
                clean_fn = _basename(fallback_stem, path_ext)
            else:
                path_part = parsed.path.strip('/').replace('/', '_') or 'index'
                clean_fn = _basename(path_part, '.html')
        clean_fn = _clean_filename(clean_fn, '.html')
        if is_already_present(site_folder, clean_fn):
            return True
    except Exception:
        pass

    # GitHub blob/raw or raw.githubusercontent.com -> fetch the raw file directly.
    if is_github or 'raw.githubusercontent.com' in parsed.netloc:
        try:
            path_parts = [p for p in parsed.path.strip('/').split('/') if p]
            raw_url = None
            if 'raw.githubusercontent.com' in parsed.netloc:
                raw_url = url
                file_path_str = '/'.join(path_parts[2:]) if len(path_parts) >= 3 else '/'.join(path_parts)
                owner = path_parts[0] if len(path_parts) >= 1 else 'github'
                repo = path_parts[1] if len(path_parts) >= 2 else 'repo'
            elif len(path_parts) >= 4 and path_parts[2] in ('blob', 'raw'):
                owner, repo, branch = path_parts[0], path_parts[1], path_parts[3]
                file_path_str = '/'.join(path_parts[4:])
                raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path_str}"

            if raw_url:
                raw_r = fetch(raw_url, session)
                if raw_r and raw_r.status_code == 200:
                    file_ext = Path(file_path_str).suffix.lower()
                    flat = '_'.join(path_parts[4:]) if len(path_parts) >= 4 else Path(file_path_str).stem
                    if file_ext in BINARY_DOWNLOAD_EXTS:
                        fn = _basename(f"github_{owner}_{repo}_{flat}", file_ext)
                        save_binary_file(fn, raw_r.content,
                                         bucket_for_ext(file_ext), site_folder=site_folder,
                                         source_url=url)
                    elif file_ext in ('.md', '.markdown'):
                        # Process Markdown: resolve relative image URLs to raw GitHub URLs
                        md_text = raw_r.text
                        
                        # Clean THM Room information, Information, TOC navigation, and References boilerplate
                        if 'tryhackme' in url.lower() or 'cajac' in raw_url.lower():
                            md_text = re.sub(r'(?s)##\s+Room\s+information.*?(?=\n##\s+|\Z)', '', md_text, flags=re.IGNORECASE)
                            md_text = re.sub(r'(?s)##\s+Information.*?(?=\n##\s+|\Z)', '', md_text, flags=re.IGNORECASE)
                            md_text = re.sub(r'-\s+\[Room information\]\(#[^\)]+\)\s*\n?', '', md_text, flags=re.IGNORECASE)
                            md_text = re.sub(r'-\s+\[Information\]\(#[^\)]+\)\s*\n?', '', md_text, flags=re.IGNORECASE)
                            md_text = re.sub(r'-\s+\[Solution\]\(#[^\)]+\)\s*\n?', '', md_text, flags=re.IGNORECASE)
                            md_text = re.sub(r'-\s+\[References\]\(#[^\)]+\)\s*\n?', '', md_text, flags=re.IGNORECASE)
                            md_text = re.sub(r'-\s+\[Included rooms\]\(#[^\)]+\)\s*\n?', '', md_text, flags=re.IGNORECASE)
                            md_text = re.sub(r'(?i)^Room\s+link:\s*.*$', '', md_text, flags=re.MULTILINE)
                            md_text = re.sub(r'(?i)^Subscription\s+type:\s*.*$', '', md_text, flags=re.MULTILINE)
                            md_text = re.sub(r'(?i)^Meta\s+Tags:\s*.*$', '', md_text, flags=re.MULTILINE)
                            md_text = re.sub(r'(?i)^Tags:\s*.*$', '', md_text, flags=re.MULTILINE)
                            md_text = re.sub(r'(?s)##\s+References.*$', '', md_text, flags=re.IGNORECASE)

                        base_raw_folder = raw_url.rsplit('/', 1)[0] + '/'
                        def _resolve_gh_img(m):
                            alt = m.group(1)
                            src = m.group(2).strip()
                            if not src.startswith('http://') and not src.startswith('https://'):
                                clean_rel = src.replace('../', '')
                                src = urllib.parse.urljoin(base_raw_folder, clean_rel)
                            return f'![{alt}]({src})'
                        resolved_md = re.sub(r'!\[(.*?)\]\((.*?)\)', _resolve_gh_img, md_text)
                        
                        # Extract title from markdown
                        title_m = re.search(r'^#\s+(.+)$', resolved_md, re.MULTILINE)
                        md_title = title_m.group(1).strip() if title_m else (resource_title or Path(file_path_str).stem.replace('_', ' '))
                        
                        # Convert markdown to clean HTML for save_article_smart
                        # Split by lines and wrap properly
                        html_lines = []
                        in_code = False
                        for l in resolved_md.splitlines():
                            if l.strip().startswith('```'):
                                in_code = not in_code
                                html_lines.append('<pre><code>' if in_code else '</code></pre>')
                                continue
                            if in_code:
                                html_lines.append(html.escape(l))
                            elif l.startswith('# '):
                                html_lines.append(f'<h1>{l[2:].strip()}</h1>')
                            elif l.startswith('## '):
                                html_lines.append(f'<h2>{l[3:].strip()}</h2>')
                            elif l.startswith('### '):
                                html_lines.append(f'<h3>{l[4:].strip()}</h3>')
                            elif l.startswith('#### '):
                                html_lines.append(f'<h4>{l[5:].strip()}</h4>')
                            else:
                                l_clean = re.sub(r'!\[(.*?)\]\((.*?)\)', r'<img src="\2" alt="\1" style="max-width:100%;margin:12px 0;border:1px solid #ddd;border-radius:4px;display:block;">', l)
                                l_clean = re.sub(r'`(.*?)`', r'<code>\1</code>', l_clean)
                                if l_clean.strip():
                                    html_lines.append(f'<p>{l_clean}</p>')
                        
                        article_html = '\n'.join(html_lines)
                        save_article_smart(article_html, url, md_title, site_folder, prefix,
                                           resource_title=resource_title,
                                           extra={'github_raw': True, 'content_type': 'text/markdown'})
                    else:
                        page_title = f"GitHub - {owner}/{repo}: {file_path_str}"
                        doc = (f"<!DOCTYPE html>\n<html>\n<head><meta charset='utf-8'>"
                               f"<title>{page_title}</title></head>\n<body>\n"
                               f"<pre><code>{html.escape(raw_r.text)}</code></pre>\n"
                               f"</body>\n</html>")
                        fn = _basename(f"github_{owner}_{repo}_{flat}", '.html')
                        save_file(fn, doc, bucket='code',
                                  site_folder=site_folder, source_url=url, title=page_title)
                    return True
                return False
        except Exception as e:
            log(f"  ERROR processing GitHub file {url}: {e}")
            return False

    r = fetch(url, session)
    if not r or r.status_code != 200:
        record_failure(url, f"fetch status {getattr(r, 'status_code', 'no-response')}",
                       context=prefix or None)
        return False

    ct = r.headers.get('Content-Type', '').lower()
    ct_binary_map = [
        ('application/pdf', '.pdf'), ('application/zip', '.zip'),
        ('application/x-zip-compressed', '.zip'), ('application/gzip', '.gz'),
        ('application/x-tar', '.tar'), ('application/x-7z-compressed', '.7z'),
        ('application/x-rar-compressed', '.rar'),
        ('application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx'),
        ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.xlsx'),
        ('application/vnd.openxmlformats-officedocument.presentationml.presentation', '.pptx'),
        ('application/msword', '.doc'), ('application/vnd.ms-excel', '.xls'),
        ('application/vnd.ms-powerpoint', '.ppt'),
        ('application/octet-stream', path_ext if path_ext else '.bin'),
    ]

    resolved_ext = path_ext
    is_binary_download = resolved_ext in BINARY_DOWNLOAD_EXTS
    if not is_binary_download:
        for ct_frag, ext_guess in ct_binary_map:
            if ct_frag in ct:
                resolved_ext = ext_guess
                is_binary_download = ext_guess in BINARY_DOWNLOAD_EXTS or \
                    ct_frag != 'application/octet-stream'
                break

    if is_binary_download and resolved_ext in (ARCHIVE_EXTS | DOC_EXTS | CODE_EXTS | DATA_EXTS):
        try:
            if title_base:
                clean_fn = f"{title_base}{resolved_ext}"
            else:
                path_part = parsed.path.strip('/').split('/')[-1] or f'document{resolved_ext}'
                clean_fn = urllib.parse.unquote(path_part)
                if not clean_fn.lower().endswith(resolved_ext):
                    clean_fn += resolved_ext
            clean_fn = _clean_filename(clean_fn, resolved_ext)
            save_binary_file(clean_fn, r.content, bucket_for_ext(resolved_ext),
                             site_folder=site_folder, source_url=url, content_type=ct)
            return True
        except Exception as e:
            record_failure(url, f"save binary: {e}")
            return False

    if 'text/html' not in ct and 'application/xhtml' not in ct:
        text_like = ['application/json', 'application/xml', 'text/xml', 'text/csv',
                     'text/plain', 'application/x-yaml', 'text/x-python', 'application/javascript']
        if any(frag in ct for frag in text_like):
            try:
                guess = '.json' if 'json' in ct else ('.xml' if 'xml' in ct else
                        ('.csv' if 'csv' in ct else '.txt'))
                fallback = parsed.path.strip('/').replace('/', '_') or 'index'
                clean_fn = _basename(fallback, guess)
                save_binary_file(clean_fn, r.content, bucket_for_ext(guess),
                                 site_folder=site_folder, source_url=url, content_type=ct)
                return True
            except Exception:
                pass
        return False

    try:
        raw_html = _get_response_text(r)
        clean_content, page_title, text = clean_html(raw_html, url)
        used_render = False
        if len(text) < MIN_CONTENT_CHARS and JS_ENABLED:
            rendered = render_js(url)
            if rendered:
                c2, t2, x2 = clean_html(rendered, url)
                if x2 and len(x2) > len(text):
                    clean_content, page_title, text = c2, (t2 or page_title), x2
                    used_render = True
                    raw_html = rendered
        # Quality floor: reject non-English, thin pages, and menu directories.
        ok, why = passes_quality(text, clean_content, raw_html=raw_html)
        if not ok:
            log(f"  SKIP (quality:{why}): {url}")
            return False
        save_article_smart(clean_content, url, page_title, site_folder, prefix,
                           resource_title=resource_title,
                           extra={'rendered': used_render, 'content_type': ct})
        return True
    except Exception:
        return False


# =====================================================================
# GITHUB / GITLAB REPO SCRAPERS
# =====================================================================
def _github_headers():
    h = {"Accept": "application/vnd.github+json", "User-Agent": HEADERS['User-Agent'],
         "X-GitHub-Api-Version": "2022-11-28"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


def _github_get(url, session, params=None, stream=False, retries=4):
    for attempt in range(retries):
        _gh_wait_if_paused()
        try:
            r = session.get(url, headers=_github_headers(), params=params,
                            timeout=REQUEST_TIMEOUT, stream=stream, allow_redirects=True)
        except Exception:
            time.sleep(min((2 ** attempt) + random.uniform(0, 1), 30))
            continue
        if r.status_code == 403 and r.headers.get("X-RateLimit-Remaining") == "0":
            reset = r.headers.get("X-RateLimit-Reset")
            try:
                reset_ts = int(reset)
                wait = max(0, reset_ts - int(time.time())) + 2
            except (TypeError, ValueError):
                reset_ts = time.time() + 60
                wait = 60
            log(f"  [github] rate limit hit; pausing all workers ~{min(wait, 900):.0f}s")
            _gh_set_pause(min(reset_ts + 2, time.time() + 900))
            _gh_wait_if_paused()
            continue
        if r.status_code in (429, 502, 503, 504):
            ra = r.headers.get("Retry-After")
            wait = float(ra) if (ra and ra.isdigit()) else (2 ** attempt) + random.uniform(0, 1)
            time.sleep(min(wait, 60))
            continue
        return r
    return None


def _safe_tar_members(tar, dest):
    dest = os.path.realpath(dest)
    for m in tar.getmembers():
        mp = os.path.realpath(os.path.join(dest, m.name))
        if (mp == dest or mp.startswith(dest + os.sep)) and m.isreg():
            yield m


def _process_repo_tarfile(tar_path, site_folder, source_base, keep_exts=None,
                          path_include=None, cap_files=None):
    keep_exts = keep_exts or GITHUB_REPO_KEEP_EXTS
    count = 0
    extract_dir = tempfile.mkdtemp(prefix="rex_")
    try:
        with tarfile.open(tar_path, "r:gz") as tar:
            members = list(_safe_tar_members(tar, extract_dir))
            try:
                tar.extractall(extract_dir, members=members, filter='data')
            except TypeError:
                tar.extractall(extract_dir, members=members)
        subdirs = [os.path.join(extract_dir, d) for d in os.listdir(extract_dir)]
        subdirs = [d for d in subdirs if os.path.isdir(d)]
        base = subdirs[0] if subdirs else extract_dir
        max_bytes = GITHUB_REPO_MAX_FILE_MB * 1024 * 1024
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in _GH_SKIP_DIRS]
            for fn in filenames:
                fp = os.path.join(dirpath, fn)
                rel = os.path.relpath(fp, base).replace(os.sep, '/')
                if path_include and path_include not in rel:
                    continue
                ext = Path(fn).suffix.lower()
                # Skip build configs, package locks, and UI component tests
                fn_lower = fn.lower()
                if any(fn_lower.endswith(sfx) for sfx in ['.spec.ts', '.test.ts', '.component.ts', '.spec.js', '.test.js', '.mjs', '.cjs']):
                    continue
                if fn_lower in ('package.json', 'package-lock.json', 'tsconfig.json', 'angular.json', 'yarn.lock', 'pnpm-lock.yaml'):
                    continue
                if ext in SKIP_EXTS:
                    continue
                if ext and ext not in keep_exts:
                    continue
                if not ext and fn.lower() not in ('readme', 'license', 'dockerfile',
                                                  'makefile', 'changelog'):
                    continue
                try:
                    if os.path.getsize(fp) > max_bytes:
                        continue
                except OSError:
                    continue
                clean_fn = _clean_filename(rel.replace('/', '_'), ext)
                if is_already_present(site_folder, clean_fn):
                    continue
                try:
                    with open(fp, "rb") as bf:
                        data = bf.read()
                except OSError:
                    continue
                bucket = 'data' if (not ext or ext in _GH_DOCTEXT_EXTS) else bucket_for_ext(ext)
                src = f"{source_base}/blob/-/{rel}" if source_base else None
                if save_binary_file(clean_fn, data, bucket, site_folder=site_folder, source_url=src):
                    count += 1
                if cap_files and count >= cap_files:
                    return count
    except Exception as e:
        log(f"    [repo-unpack] error for {site_folder}: {e}")
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)
    return count


def github_repo_scraper(owner, repo, session, ref=None, prefix=None,
                        keep_exts=None, path_include=None, cap_files=None):
    site_folder = sanitize_name(prefix or f"github_{owner}_{repo}")
    log(f"  [github-repo] {owner}/{repo} -> {site_folder}")
    if not ref:
        meta = _github_get(f"{GITHUB_API}/repos/{owner}/{repo}", session)
        if meta is not None and meta.status_code == 200:
            try:
                ref = meta.json().get("default_branch") or "main"
            except Exception:
                ref = "main"
        elif meta is not None and meta.status_code == 404:
            log(f"    [github-repo] {owner}/{repo} not found (404) - skipping")
            record_failure(f"{owner}/{repo}", "github repo 404")
            return 0
        else:
            ref = "main"
    r = _github_get(f"{GITHUB_API}/repos/{owner}/{repo}/tarball/{ref}", session, stream=True)
    if r is None or r.status_code != 200:
        log(f"    [github-repo] tarball failed for {owner}/{repo} "
            f"(status {getattr(r, 'status_code', 'n/a')})")
        record_failure(f"{owner}/{repo}", f"github tarball status {getattr(r, 'status_code', 'n/a')}")
        return 0
    tmpdir = tempfile.mkdtemp(prefix="ghrepo_")
    count = 0
    try:
        tar_path = os.path.join(tmpdir, "repo.tar.gz")
        with open(tar_path, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 16):
                if chunk:
                    fh.write(chunk)
        count = _process_repo_tarfile(tar_path, site_folder,
                                      f"https://github.com/{owner}/{repo}",
                                      keep_exts=keep_exts, path_include=path_include,
                                      cap_files=cap_files)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    log(f"    [github-repo] {owner}/{repo}: saved {count} files")
    return count


def gitlab_repo_scraper(full_path, session, prefix=None, keep_exts=None, path_include=None):
    site_folder = sanitize_name(prefix or f"gitlab_{full_path}")
    log(f"  [gitlab-repo] {full_path} -> {site_folder}")
    enc = urllib.parse.quote(full_path, safe='')
    ref = None
    try:
        r = session.get(f"https://gitlab.com/api/v4/projects/{enc}",
                        headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            ref = r.json().get('default_branch')
    except Exception:
        pass
    proj = full_path.rstrip('/').split('/')[-1]
    branches = [ref] if ref else []
    branches += [b for b in ('main', 'master') if b not in branches]
    resp = None
    for b in branches:
        arch = f"https://gitlab.com/{full_path}/-/archive/{b}/{proj}-{b}.tar.gz"
        resp = fetch_download(arch, session)
        if resp is not None and resp.status_code == 200:
            break
    if resp is None or resp.status_code != 200:
        record_failure(full_path, f"gitlab archive status {getattr(resp, 'status_code', 'n/a')}")
        log(f"    [gitlab-repo] archive failed for {full_path}")
        return 0
    tmpdir = tempfile.mkdtemp(prefix="glrepo_")
    count = 0
    try:
        tar_path = os.path.join(tmpdir, "repo.tar.gz")
        with open(tar_path, "wb") as fh:
            fh.write(resp.content)
        count = _process_repo_tarfile(tar_path, site_folder,
                                      f"https://gitlab.com/{full_path}",
                                      keep_exts=keep_exts, path_include=path_include)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    log(f"    [gitlab-repo] {full_path}: saved {count} files")
    return count


# =====================================================================
# URL ROUTING
# =====================================================================
def _parse_github_url(url):
    p = urllib.parse.urlparse(url)
    if 'github.com' not in p.netloc.lower():
        return None
    parts = [x for x in p.path.strip('/').split('/') if x]
    if len(parts) < 2:
        return None
    owner, repo = parts[0], parts[1].replace('.git', '')
    path_include = None
    if len(parts) >= 5 and parts[2] in ('tree', 'blob'):
        sub = parts[4:]
        if sub:
            path_include = urllib.parse.unquote('/'.join(sub))
    return owner, repo, path_include


def _parse_gitlab_url(url):
    p = urllib.parse.urlparse(url)
    if 'gitlab.com' not in p.netloc.lower():
        return None
    raw = p.path.strip('/')
    if not raw:
        return None
    path_include = None
    if '/-/' in raw:
        base, rest = raw.split('/-/', 1)
        segs = [s for s in rest.split('/') if s]
        if segs and segs[0] in ('tree', 'blob') and len(segs) >= 3:
            sub = segs[2:]
            if sub:
                path_include = urllib.parse.unquote('/'.join(sub))
        raw = base
    raw = raw.rstrip('/')
    if raw.count('/') < 1:
        return None
    return raw, path_include


def scrape_one_resource(sheet, vendor, product, title, url, typ, session):
    """Route one workbook resource to the right handler by host. `typ` (the
    workbook's own 'Link Type' column) is accepted for signature compat with
    the caller but no longer changes routing - direct-only mode fetches
    every non-repo URL the same way. Files land in
    DEST_ROOT/<sheet>/<vendor>/<product>/ regardless of handler; `title`
    (the workbook's Resource Title) becomes the filename for direct-fetch
    saves (repo dumps keep their own in-repo file names)."""
    vendor_seg = sanitize_name(vendor) if vendor else 'Unknown Vendor'
    product_seg = sanitize_name(product) if product else 'Unknown Product'
    prefix = f"{sanitize_name(sheet)}/{vendor_seg}/{product_seg}"
    host = urllib.parse.urlparse(url).netloc.lower()
    try:
        gh = _parse_github_url(url)
        if gh and 'raw.githubusercontent' not in host:
            owner, repo, pinc = gh
            return github_repo_scraper(owner, repo, session, prefix=prefix, path_include=pinc)
        gl = _parse_gitlab_url(url)
        if gl:
            full_path, pinc = gl
            return gitlab_repo_scraper(full_path, session, prefix=prefix, path_include=pinc)
        # Every other URL (direct-download types, 'Direct Doc / Blog Link',
        # 'Primary Doc Hub', unlabeled, or anything else) is fetched exactly
        # as-is: no sitemap.xml probe, no /feed or /rss lookup, no
        # same-domain crawl for other pages. Only this one row's URL is
        # saved (still subject to the quality gate in passes_quality()).
        return 1 if scrape_and_save(url, session, prefix=prefix, resource_title=title) else 0
    except Exception as e:
        record_failure(url, f"resource error: {e}", context=sheet)
        return 0


# =====================================================================
# WORKBOOK LOADER
# =====================================================================
def _find_header_row(rows):
    """Return (index, [lowercased header per column]) for the first row that
    contains a cell whose text is/contains 'url' - matches a plain 'URL'
    column as well as e.g. 'Direct Link / Download URL'."""
    for i, r in enumerate(rows):
        if not r:
            continue
        cells = [(str(c).strip() if c is not None else "") for c in r]
        low = [c.lower() for c in cells]
        if any('url' in c for c in low if c):
            return i, low
    return None, None


def _find_col(headers, *keywords, exact=None):
    """First column index whose header matches. `exact` is checked first
    (whole-header equality), then `keywords` (substring match, in order)."""
    if exact:
        for j, h in enumerate(headers):
            if h in exact:
                return j
    for kw in keywords:
        for j, h in enumerate(headers):
            if kw in h:
                return j
    return None


def _cell(row, idx):
    if idx is None or idx >= len(row):
        return ""
    v = row[idx]
    return str(v).strip() if v is not None else ""


def load_excel_resources(path=None):
    """Read every category sheet. Detects each sheet's header row dynamically
    and maps columns by NAME (URL, Title, Category, Sub-Category, Format), so it
    is robust to banner rows and extra columns. Returns a list of dicts."""
    path = Path(path or EXCEL_PATH)
    if not _HAS_OPENPYXL:
        log("  [excel] openpyxl not installed - run: pip install openpyxl")
        return []
    if not path.exists():
        log(f"  [excel] workbook not found: {path}")
        return []
    try:
        wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    except Exception as e:
        log(f"  [excel] could not open workbook: {e}")
        return []

    resources = []
    for ws in wb.worksheets:
        sheet = ws.title
        if sheet.strip().lower() in SKIP_SHEETS:
            log(f"  [excel] skipping sheet '{sheet}' (in SKIP_SHEETS)")
            continue
        rows = list(ws.iter_rows(values_only=True))
        hdr_idx, headers = _find_header_row(rows)
        if hdr_idx is None:
            log(f"  [excel] sheet '{sheet}': no header row with a 'URL' column - skipping")
            continue
        # Substring-matched so this works whether the sheet uses simple names
        # ('URL', 'Title', 'Type') or this workbook's longer ones
        # ('Direct Link / Download URL', 'Resource Title', 'Link Type').
        ui = _find_col(headers, 'url', 'direct resource url', 'direct raw markdown url', 'direct link')
        ti = _find_col(headers, 'resource title / module', 'room / writeup title', 'resource title', 'title', 'name')
        idi = _find_col(headers, 'resource id', 'id', exact={'#', 'id'})
        cati = _find_col(headers, 'track / category', 'category', 'focus area')
        domi = _find_col(headers, 'section / topic', 'domain', 'focus area')
        subi = _find_col(headers, 'sub-category', 'subcategory')
        lti = _find_col(headers, 'link type', 'resource classification', 'format', 'type of data', 'type')
        vendi = _find_col(headers, 'provider', 'vendor', 'platform')
        prodi = _find_col(headers, 'program / platform', 'track / module heading', 'product', 'program')

        for r in rows[hdr_idx + 1:]:
            if not r:
                continue
            url = _cell(r, ui)
            if not url.lower().startswith(('http://', 'https://')):
                continue
            title = _cell(r, ti)
            rid = _cell(r, idi)
            vendor = _cell(r, vendi)
            product = _cell(r, prodi)
            link_type = _cell(r, lti)
            name = (title or (f"{vendor} {product}".strip() if (vendor or product) else '')
                    or urllib.parse.urlparse(url).netloc.replace('www.', '') or 'resource')
            resources.append({
                'sheet': sheet, 'name': name, 'title': title or name, 'id': rid,
                'url': url, 'category': _cell(r, cati) or _cell(r, domi),
                'subcategory': _cell(r, subi), 'type': link_type,
                'link_type': link_type, 'vendor': vendor or 'General', 'product': product or 'Resources',
            })
    return resources


def scrape_extra_sources(session):
    """Scrape the hardcoded EXTRA_SOURCES (in addition to the workbook). Each
    source is fully isolated: any error is logged and the next source still
    runs. Files land under DEST_ROOT/<folder>/<name>/."""
    if not RUN_EXTRA_SOURCES or not EXTRA_SOURCES:
        return 0
    log("=" * 70)
    log(f"HARDCODED EXTRA SOURCES ({len(EXTRA_SOURCES)})")
    log("=" * 70)
    total = 0
    for src in EXTRA_SOURCES:
        name = src.get("name") or urllib.parse.urlparse(src.get("url", "")).netloc
        url = src.get("url", "").strip()
        if not url.lower().startswith(("http://", "https://")):
            log(f"  [extra] {name}: bad/missing url - skipping")
            continue
        folder = src.get("folder") or f"Extra_Sources/{urllib.parse.urlparse(url).netloc}"
        prefix = f"{sanitize_name(folder)}/{sanitize_name(name)}"
        # Trust the host so any binary/text downloads under it aren't filtered.
        host = urllib.parse.urlparse(url).netloc.lower()
        TRUSTED_DOMAINS.add(host)
        TRUSTED_DOMAINS.add(host.replace('www.', ''))
        require_rel = bool(src.get("relevance", False))
        log(f"  [extra] {name} -> {DEST_ROOT / sanitize_name(folder) / sanitize_name(name)} "
            f"(relevance gate: {'on' if require_rel else 'off'})")
        try:
            # Direct fetch only - no sitemap/rss/crawl discovery for this
            # source either, just the one URL configured above.
            n = 1 if scrape_and_save(url, session, prefix=prefix,
                                      require_relevance=require_rel,
                                      resource_title=name) else 0
            total += n or 0
            log(f"  [extra] {name}: saved {n} files")
        except Exception as e:
            log(f"  [extra] {name}: ERROR {e} - continuing")
            record_failure(url, f"extra source error: {e}", context=name)
    log(f"EXTRA SOURCES COMPLETE -> {total} files saved")
    return total


def scrape_excel_resources(session):
    resources = load_excel_resources()
    if not resources:
        log("  [excel] no resources to scrape")
        return 0

    # De-dup by URL and trust every workbook host (curated -> bypass filter).
    seen, tasks = set(), []
    per_sheet = collections.Counter()
    for res in resources:
        host = urllib.parse.urlparse(res['url']).netloc.lower()
        if host:
            TRUSTED_DOMAINS.add(host.replace('www.', ''))
            TRUSTED_DOMAINS.add(host)
        if res['url'] in seen:
            continue
        seen.add(res['url'])
        tasks.append(res)
        per_sheet[res['sheet']] += 1

    log(f"  [excel] {len(tasks)} unique resources across {len(per_sheet)} sheets:")
    for s, c in per_sheet.items():
        log(f"          {s}: {c}")

    total = 0

    def _do(res):
        return scrape_one_resource(res['sheet'], res['vendor'], res['product'],
                                   res['title'], res['url'], res['type'], session)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(_do, t): t for t in tasks}
        done = 0
        for f in as_completed(futs):
            done += 1
            try:
                total += f.result() or 0
            except Exception as e:
                log(f"  [excel] {futs[f]['url']} error: {e}")
            if done % 25 == 0:
                log(f"  [excel] progress: {done}/{len(tasks)} resources, {total} files saved")
    return total


# =====================================================================
# CLI / ORCHESTRATION
# =====================================================================
def configure():
    global MANIFEST_FILE, FAILURES_FILE, CONTENT_HASHES_FILE
    global content_hashes, LOG_FILE, PROGRESS_FILE
    DEST_ROOT.mkdir(parents=True, exist_ok=True)
    if not os.environ.get("SCRAPER_LOG", "").strip():
        LOG_FILE = DEST_ROOT / "scraper.log"
    if not os.environ.get("SCRAPER_PROGRESS", "").strip():
        PROGRESS_FILE = DEST_ROOT / "scraped_urls.txt"
    MANIFEST_FILE = DEST_ROOT / "manifest.jsonl"
    FAILURES_FILE = DEST_ROOT / "failures.jsonl"
    CONTENT_HASHES_FILE = DEST_ROOT / "content_hashes.txt"
    content_hashes = load_content_hashes()


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Workbook-driven resource scraper (no hardcoded sources).")
    p.add_argument('--excel', help="Path to the resources workbook (env SCRAPER_EXCEL)")
    p.add_argument('--dest', help="Output directory (env SCRAPER_DEST)")
    p.add_argument('--prev', help="Previously-scraped dir for skip/dedup (env SCRAPER_PREV)")
    p.add_argument('--skip-sheets', help="Comma-separated sheet names to ignore "
                                         "(env SCRAPER_SKIP_SHEETS; default 'Overview')")
    p.add_argument('--workers', type=int, help="Resource-level parallelism (env SCRAPER_WORKERS)")
    p.add_argument('--min-words', type=int,
                   help="Quality floor: min words of main text to save a page "
                        "(env SCRAPER_MIN_WORDS, default 50)")
    p.add_argument('--no-language-filter', action='store_true',
                   help="Disable the English language filter (keep non-English content). "
                        "Language filter is ON by default.")
    p.add_argument('--enable-keyword-filter', action='store_true',
                   help="Enable the cybersecurity keyword relevance gate (off by default).")
    p.add_argument('--relevance-min', type=int,
                   help="LENIENT topic gate: min distinct domain keywords when keyword filter is enabled "
                        "(env SCRAPER_RELEVANCE_MIN, default 2)")
    p.add_argument('--no-keyword-filter', action='store_true',
                   help="Legacy flag (keyword filter is already off by default).")
    p.add_argument('--no-js', action='store_true',
                   help="Disable the headless-browser fallback for JS-rendered pages.")
    p.add_argument('--js-workers', type=int, help="Headless browsers for JS rendering.")
    p.add_argument('--skip-extra', action='store_true',
                   help="Skip the hardcoded EXTRA_SOURCES (e.g. apivault.dev).")
    p.add_argument('--github-token', '--token', help="GitHub Personal Access Token (or set GITHUB_TOKEN env var)")
    p.add_argument('--article-format', choices=['auto', 'md', 'pdf', 'html'],
                   help="How to save a blog/doc page that has NO direct download "
                        "link (env SCRAPER_ARTICLE_FORMAT, default 'auto': pdf if "
                        "the article has real images, else clean markdown).")
    p.add_argument('--list', action='store_true',
                   help="Only parse the workbook and print the resource plan, then exit.")
    return p.parse_args(argv)


def apply_args(args):
    global EXCEL_PATH, DEST_ROOT, PART3_DIR, WORKERS
    global SKIP_SHEETS, ENABLE_KEYWORD_FILTER, ENABLE_LANGUAGE_FILTER
    global MIN_CONTENT_WORDS, RELEVANCE_MIN_HITS, RUN_EXTRA_SOURCES
    global ARTICLE_FORMAT_MODE, GITHUB_TOKEN
    if getattr(args, 'github_token', None):
        GITHUB_TOKEN = args.github_token.strip()
    if args.excel:
        EXCEL_PATH = Path(args.excel).expanduser()
    if args.dest:
        DEST_ROOT = Path(args.dest).expanduser()
    if args.prev:
        PART3_DIR = Path(args.prev).expanduser()
    elif args.dest:
        PART3_DIR = DEST_ROOT
    if args.skip_sheets:
        SKIP_SHEETS = {s.strip().lower() for s in args.skip_sheets.split(",") if s.strip()}
    if args.workers:
        WORKERS = args.workers
    if getattr(args, 'min_words', None) is not None:
        MIN_CONTENT_WORDS = args.min_words
    if getattr(args, 'no_language_filter', False):
        ENABLE_LANGUAGE_FILTER = False
    if getattr(args, 'enable_keyword_filter', False):
        ENABLE_KEYWORD_FILTER = True
    if getattr(args, 'no_keyword_filter', False):
        ENABLE_KEYWORD_FILTER = False
    if getattr(args, 'relevance_min', None) is not None:
        RELEVANCE_MIN_HITS = args.relevance_min
    if getattr(args, 'skip_extra', False):
        RUN_EXTRA_SOURCES = False
    if getattr(args, 'article_format', None):
        ARTICLE_FORMAT_MODE = args.article_format
    if getattr(args, 'js_workers', None):
        globals()['JS_RENDER_WORKERS'] = args.js_workers


def main(argv=None):
    """Crash-proof entry point: any unexpected error is logged, not raised, so a
    single failure never kills the run. Ctrl-C still stops cleanly."""
    try:
        _run(argv)
    except KeyboardInterrupt:
        log("\nInterrupted by user (Ctrl-C). Partial output is saved; "
            "re-run to resume from scraped_urls.txt.")
    except SystemExit:
        raise
    except Exception as e:
        import traceback
        log(f"FATAL (caught, not crashing): {e}")
        try:
            log(traceback.format_exc())
        except Exception:
            pass


def _run(argv=None):
    global saved_urls, existing_filenames, JS_ENABLED
    args = parse_args(argv)
    apply_args(args)
    configure()
    JS_ENABLED = _HAS_PLAYWRIGHT and not args.no_js

    log("=" * 70)
    log("Workbook-driven resource scraper")
    log("=" * 70)
    log(f"  WORKBOOK : {EXCEL_PATH}  (openpyxl: {'yes' if _HAS_OPENPYXL else 'NO'})")
    log(f"  DEST     : {DEST_ROOT}")
    log(f"  PREV     : {PART3_DIR}")
    log(f"  HTML extractor: {'trafilatura' if _HAS_TRAFILATURA else 'builtin-bs4'}  "
        f"GitHub token: {'yes' if GITHUB_TOKEN else 'NO'}")
    log("  Discovery: direct-only - each workbook URL is fetched as-is, "
        "no sitemap/rss/crawl.")
    log(f"  Language filter: {'ON (English only)' if ENABLE_LANGUAGE_FILTER else 'off (all languages allowed)'}")
    log(f"  Quality gate: >= {MIN_CONTENT_WORDS} words, link-ratio < {MAX_LINK_TEXT_RATIO}")
    log(f"  Relevance keyword gate: {'ON' if ENABLE_KEYWORD_FILTER else 'off (disabled - all sheet topics kept)'}")
    pdf_engine = "weasyprint" if _HAS_WEASYPRINT else ("playwright-chromium (no sudo)" if _HAS_PLAYWRIGHT else "NO (install weasyprint or playwright)")
    log(f"  Article format (blog/doc pages with no direct download): "
        f"{ARTICLE_FORMAT_MODE}  "
        f"[markdownify: {'yes' if _HAS_MARKDOWNIFY else 'NO'}, "
        f"PDF Engine: {pdf_engine}]")
    if JS_ENABLED:
        log(f"  JS rendering: playwright ({JS_RENDER_WORKERS} browser(s))")
    else:
        log("  JS rendering: disabled")
    if RUN_EXTRA_SOURCES and EXTRA_SOURCES:
        log(f"  Extra hardcoded sources: {', '.join(s.get('name', s.get('url', '?')) for s in EXTRA_SOURCES)}")
        if not JS_ENABLED:
            log("    NOTE: some sites (e.g. apivault.dev) are JS apps - install "
                "playwright (playwright install chromium) or they may yield little.")

    # --list: print the plan and exit (no network).
    if args.list:
        resources = load_excel_resources()
        per_sheet = collections.Counter(r['sheet'] for r in resources)
        log(f"\nPlan: {len(resources)} resources across {len(per_sheet)} sheets")
        for s, c in per_sheet.items():
            log(f"  {s}: {c}  ->  {DEST_ROOT / sanitize_name(s)}")
        return

    if not GITHUB_TOKEN:
        log("  WARNING: GITHUB_TOKEN not set - GitHub API is limited to 60 req/hr. "
            "Set it to avoid stalls if the workbook has many GitHub URLs.")

    saved_urls = load_saved_urls()
    log(f"Resuming: {len(saved_urls)} URLs already done")
    log("Indexing already-scraped files (PREV + DEST)...")
    existing_filenames = build_existing_filenames_index()
    log(f"  Found {len(existing_filenames)} existing files - these will be skipped")

    session = requests.Session()
    session.headers.update(HEADERS)
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=WORKERS * PER_SOURCE_WORKERS,
        pool_maxsize=WORKERS * PER_SOURCE_WORKERS, max_retries=0)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    start_time = time.time()
    log("=" * 70)
    log("SCRAPING WORKBOOK RESOURCES")
    log("=" * 70)
    try:
        scrape_excel_resources(session)
    except Exception as e:
        log(f"  [excel] phase error (continuing): {e}")

    # Hardcoded extra sources - fully isolated; failure here never stops the run.
    try:
        scrape_extra_sources(session)
    except Exception as e:
        log(f"  [extra] phase error (continuing): {e}")

    elapsed = time.time() - start_time
    log("\n" + "=" * 70)
    log("SCRAPING COMPLETE & FINAL FORMAT SUMMARY")
    log("=" * 70)
    log(f"Time Elapsed:           {elapsed / 60:.1f} minutes")
    log(f"Total Files Saved:      {total_saved}")
    log(f"Unique URLs Processed:  {len(saved_urls)}")
    log(f"Content Hashes Tracked: {len(content_hashes)}")
    log("\n--- HTML Article Conversion Breakdown ---")
    log(f"  • HTML -> PDF Converted (Articles with images):  {html_to_pdf_count}")
    log(f"  • HTML -> PDF Fallbacks (Failed PDF -> Saved MD): {html_to_pdf_fallback_count}")
    log(f"  • HTML -> Markdown (Text-only articles):          {html_to_md_count}")
    log("\n--- All Saved Files by Extension ---")
    for cat, count in sorted(stats.items(), key=lambda x: -x[1]):
        log(f"  {cat}: {count}")

    if html_to_pdf_fallback_count > 0:
        fb_file = DEST_ROOT / "pdf_fallbacks.jsonl"
        log(f"\n⚠️ NOTE: {html_to_pdf_fallback_count} article(s) had images but fell back to Markdown (.md).")
        log(f"   Detailed fallback log saved to: {fb_file}")
        try:
            import json
            with open(fb_file, 'w', encoding='utf-8') as ff:
                for r in fallback_records:
                    ff.write(json.dumps(r, ensure_ascii=False) + '\n')
        except Exception:
            pass
    else:
        log("\n✅ ZERO Fallbacks: All articles requiring PDF were successfully converted to PDF!")

    log(f"\nProvenance manifest: {MANIFEST_FILE}")
    log(f"Failure log:         {FAILURES_FILE}")
    log("=" * 70)


if __name__ == "__main__":
    main()
