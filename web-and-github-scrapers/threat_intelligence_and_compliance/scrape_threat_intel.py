# -*- coding: utf-8 -*-
import os
import sys
import re
import json
import time
import random
import hashlib
import threading
import html
import requests
import urllib.parse
import urllib.robotparser as robotparser
import zipfile
import io
import tarfile
import tempfile
import shutil
import argparse
import queue
import atexit
from pathlib import Path
from bs4 import BeautifulSoup, Comment, Tag
from concurrent.futures import ThreadPoolExecutor, as_completed
import collections
from collections import defaultdict
import xml.etree.ElementTree as ET
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

# Optional: trafilatura gives much cleaner main-content / boilerplate removal
# for HTML pages. If it's not installed the crawler falls back to the built-in
# BeautifulSoup heuristic - so it's a soft dependency, never required.
try:
    import trafilatura
    _HAS_TRAFILATURA = True
except Exception:
    _HAS_TRAFILATURA = False

# Optional: Playwright drives a headless browser to render JavaScript-heavy
# pages (SPAs, JS-injected content) that static HTML extraction can't see.
# It only fires as a fallback when static extraction returns almost nothing.
# Soft dependency: if it (or its browser) isn't installed, the crawler simply
# skips JS rendering. Enable with:  pip install playwright && playwright install chromium
try:
    from playwright.sync_api import sync_playwright
    _HAS_PLAYWRIGHT = True
except Exception:
    _HAS_PLAYWRIGHT = False

# =====================================================================
# NOTE: This version has NO classification step (no Llama / keyword
# classifier). Every file is saved into DEST_ROOT, bucketed only by a
# simple, cheap "file type" folder (html / pdf / zip / docs / code /
# data / other) so the pipeline stays fast and dependency-free.
# =====================================================================

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

def _env_list(name, default):
    v = os.environ.get(name, "").strip()
    if not v:
        return list(default)
    return [x.strip().lower() for x in v.split(",") if x.strip()]

# ---------------------------------------------------------------------
# Configuration. Nothing is hardcoded to a Windows path any more: every
# path/knob comes from an environment variable (shown in brackets) with a
# Linux/SSH-friendly default, and can be overridden again by CLI flags in
# main(). Defaults live next to the script.
# ---------------------------------------------------------------------
# [SCRAPER_URLS_DOCX] Word doc containing the seed URLs.
URLS_DOCX_PATH = _env_path("SCRAPER_URLS_DOCX", SCRIPT_DIR / "Sites Search.docx")
# [SCRAPER_DEST] Where scraped output is written.
DEST_ROOT      = _env_path("SCRAPER_DEST",      SCRIPT_DIR / "Gap_Data")
# [SCRAPER_PREV] Previously-scraped corpus consulted for skip/dedup. Anything
# whose filename or content-hash already exists here (or in DEST_ROOT) is
# treated as done and skipped. Defaults to DEST_ROOT itself.
PART3_DIR      = _env_path("SCRAPER_PREV",      DEST_ROOT)

LOG_FILE      = SCRIPT_DIR / "scraper_new.log"
PROGRESS_FILE = SCRIPT_DIR / "scraped_urls_new.txt"
# Output-index files derived from DEST_ROOT once paths are finalized (configure()).
MANIFEST_FILE       = None   # provenance: one JSON line per saved file
FAILURES_FILE       = None   # one JSON line per failed fetch/save
CONTENT_HASHES_FILE = None   # persistent sha256 index for content-level dedup
REPO_PROGRESS_FILE  = None   # one JSON line per repo-level start/progress/done event

WORKERS            = _env_int("SCRAPER_WORKERS", 60)             # site-level parallelism
PER_SOURCE_WORKERS = _env_int("SCRAPER_PER_SOURCE_WORKERS", 25)  # threads inside a site
PER_DOMAIN_MAX_CONCURRENCY = _env_int("SCRAPER_PER_DOMAIN", 6)   # cap per single host
# Domain-specific overrides - raise limit for high-capacity hosts (e.g. GitHub).
PER_DOMAIN_OVERRIDES = {
    "github.com": 15,
    "raw.githubusercontent.com": 15,
    "codeload.github.com": 15,
}
REQUEST_TIMEOUT = _env_int("SCRAPER_TIMEOUT", 30)
MAX_DEPTH       = _env_int("SCRAPER_MAX_DEPTH", 6)

# ---------------------------------------------------------------------
# URL-discovery ceiling.
#   MAX_URLS_PER_SOURCE = 0  -> UNLIMITED (recommended: don't miss data).
#   MAX_URLS_PER_SOURCE > 0  -> soft cap; when hit we LOG A WARNING and record
#                               it, so truncation is never silent.
# HARD_URL_CEILING bounds memory even in unlimited mode. EFFECTIVE_URL_CAP is
# the number actually used for slicing (finalized in configure()).
# [SCRAPER_MAX_URLS] / [SCRAPER_HARD_CEILING]
# ---------------------------------------------------------------------
MAX_URLS_PER_SOURCE = _env_int("SCRAPER_MAX_URLS", 0)
HARD_URL_CEILING    = _env_int("SCRAPER_HARD_CEILING", 100000)
EFFECTIVE_URL_CAP   = HARD_URL_CEILING  # recomputed in configure()

# ---------------------------------------------------------------------
# JavaScript rendering fallback (headless browser via Playwright).
#   * Only triggers when static extraction of a page yields fewer than
#     MIN_CONTENT_CHARS characters (the tell-tale sign of a JS-rendered page).
#   * Renders through a small BOUNDED pool of browsers (JS_RENDER_WORKERS) so
#     it never scales with the crawler's thread count or exhausts memory.
#   * Auto-enabled when Playwright + a browser are installed; disable with --no-js.
# [SCRAPER_JS_WORKERS] / [SCRAPER_JS_TIMEOUT] / [SCRAPER_MIN_CONTENT_CHARS]
# ---------------------------------------------------------------------
JS_RENDER_WORKERS = _env_int("SCRAPER_JS_WORKERS", 2)     # concurrent browsers
JS_RENDER_TIMEOUT = _env_int("SCRAPER_JS_TIMEOUT", 25)    # seconds per page
MIN_CONTENT_CHARS = _env_int("SCRAPER_MIN_CONTENT_CHARS", 200)
JS_ENABLED = False   # finalized in main() after CLI parsing

# ---------------------------------------------------------------------
# Whether to save cleaned HTML pages at all (the 'html' bucket).
#   * True  (default): HTML pages are cleaned + saved like any other file.
#   * False: HTML pages are still FETCHED and CRAWLED for links (so PDFs,
#     docs, code, etc. reachable from them are still discovered/saved) -
#     only the page's own .html file is skipped. No html/ folders are
#     created at all in that mode.
# [SCRAPER_SAVE_HTML=0] or CLI --no-html
# ---------------------------------------------------------------------
SAVE_HTML = os.environ.get("SCRAPER_SAVE_HTML", "1").strip().lower() not in ("0", "false", "no")

# =====================================================================
# v7 GAP-FILL EXTENSION
# ---------------------------------------------------------------------
# Adds curated, high-value sources for four target domains that the
# generic blog/keyword crawler cannot reach reliably:
#   * AI/ML security       (MITRE ATLAS, OWASP ML/LLM Top 10, NIST AI RMF,
#                           adversarial-ML arXiv papers, garak/PyRIT/ART/Counterfit)
#   * OT/ICS/SCADA         (ATT&CK for ICS, NIST SP 800-82, CISA ICS
#                           advisories, Modbus/DNP3/S7 refs, GRFICS, Conpot)
#   * Blockchain           (Code4rena, Immunefi, Trail of Bits, SWC registry,
#                           Slither, Mythril, Ethernaut, Damn Vulnerable DeFi)
#   * IoT / embedded       (OWASP IoT Top 10, FACT, Firmadyne, IoTGoat,
#                           OP-TEE / secure-boot references)
# Retrieval is matched to each source: GitHub tarball, arXiv API, direct
# PDF download, bounded HTML scrape, docs sitemap, or CISA advisory walk.
# =====================================================================

# =====================================================================
# CURATED SOURCES
# ---------------------------------------------------------------------
# Two hardcoded sources, reaped in full (see LARGE_EXTERNAL_DOWNLOADS below):
#   - github.com/EbookFoundation/free-programming-books (whole repo tarball)
#   - git-stars.org/en/languages (full same-domain crawl)
# =====================================================================

# GitHub API token (optional but strongly recommended). Read from env so it
# never lives in the file. Unauthenticated = 60 req/hr; authenticated = 5000.
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_API = "https://api.github.com"
GITHUB_REPO_MAX_FILE_MB = 25       # skip individual repo files larger than this
GITHUB_ORG_REPO_WORKERS = 6        # concurrent repos when sweeping an org

# Hardcoded curated lists are removed - kept as empty for backward-compat with
# helper functions that accept them as defaults.
CURATED_GITHUB_REPOS = []
ARXIV_QUERIES = []
ARXIV_PER_QUERY = 0
DIRECT_DOWNLOADS = []
CURATED_DOC_SITES = []

# ---------------------------------------------------------------------
# Curated sources to reap in full.
#   kind 'fpb_repo_links'      -> see scrape_free_programming_books() below.
#   kind 'git_stars_languages' -> scrapes git-stars.org/en/languages ONLY -
#                                 no other source is substituted in. If a
#                                 page looks JS-gated/thin, it's re-fetched
#                                 through the existing headless-browser
#                                 renderer (render_js(), already used
#                                 elsewhere in this file) and simply given
#                                 time to finish loading - no CAPTCHA
#                                 solving, no fingerprint spoofing, just
#                                 waiting for the page like a real browser
#                                 would. See scrape_git_stars_by_language()
#                                 below.
# ---------------------------------------------------------------------
# Previous curated list (free-programming-books, git-stars.org) removed per
# request - this pipeline is now dedicated to Indian regulatory / critical-
# infrastructure security sources instead.
LARGE_EXTERNAL_DOWNLOADS = []
RUN_LARGE_DOWNLOADS = False   # nothing left in LARGE_EXTERNAL_DOWNLOADS to run

# ---------------------------------------------------------------------
# REGULATORY / CII SEED SITES (replaces the old hardcoded curated list).
# Each entry is crawled with generic_blog_scraper() (sitemap + RSS + link
# crawl, same_domain-scoped, recursive to MAX_DEPTH), which will pick up
# every PDF/DOC/circular/notification it can discover on-site.
#
# NOTE: verify these URLs before a full run - Indian government sites
# restructure fairly often, and some (e.g. NCSC/NSCS) do not publish a
# public circular repository at all, only policy-level pages.
# ---------------------------------------------------------------------
REGULATORY_SEED_SITES = [
    # --- RBI ONLY (for now) - every major content index, not just the
    # homepage, so the crawl starts already inside each section instead
    # of having to discover it by chance from nav links. ---
    {"name": "RBI-Notifications",    "url": "https://www.rbi.org.in/Scripts/NotificationUser.aspx"},
    {"name": "RBI-CircularIndex",    "url": "https://www.rbi.org.in/Scripts/BS_CircularIndexDisplay.aspx"},
    {"name": "RBI-MasterCirculars",  "url": "https://www.rbi.org.in/Scripts/BS_ViewMasterCirculars.aspx"},
    {"name": "RBI-MasterDirections", "url": "https://www.rbi.org.in/Scripts/BS_ViewMasDirections.aspx"},
    {"name": "RBI-PressReleases",    "url": "https://www.rbi.org.in/scripts/BS_PressReleaseDisplay.aspx"},
    {"name": "RBI-Publications",     "url": "https://www.rbi.org.in/Scripts/Publications.aspx"},
    {"name": "RBI-Reports",          "url": "https://www.rbi.org.in/scripts/bs_viewpublicationreport.aspx"},
    {"name": "RBI-AnnualReport",     "url": "https://www.rbi.org.in/Scripts/AnnualReportMainDisplay.aspx"},
    {"name": "RBI-WorkingPapers",    "url": "https://www.rbi.org.in/Scripts/OccasionalPublications.aspx?head=Working+Papers"},
    {"name": "RBI-OccasionalPapers", "url": "https://www.rbi.org.in/SCRIPTs/OccasionalPapers.aspx"},
    {"name": "RBI-Speeches",         "url": "https://www.rbi.org.in/Scripts/BS_ViewSpeeches.aspx"},
    {"name": "RBI-Home",             "url": "https://www.rbi.org.in"},  # safety-net catch-all
]

REGULATORY_DOMAINS = {
    urllib.parse.urlparse(e['url']).netloc.replace('www.', '').lower()
    for e in REGULATORY_SEED_SITES
}

# Domains that always pass the keyword gate (no content check needed - these
# are code-hosting infra, not regulator sites, so "is this page relevant"
# doesn't apply the same way).
# NOTE: the 8 regulatory seed domains (RBI/SEBI/IRDAI/NSCS/CEA/PowerMin/
# IndianRailways/CRIS) are DELIBERATELY NOT in this set any more. They used
# to bypass the keyword filter entirely (pulling every circular, including
# plain banking/insurance/railway-ops content). Now they're filtered like
# any other site through is_cyber_related() / CYBER_PAT below, so only
# cyber/IT/OT/SCADA/ICS/AI-ML/infosec-related pages and documents from these
# sites are kept - general-purpose regulatory content is rejected.
# CERT-In and NCIIPC are intentionally left in CYBER_DOMAINS (below) instead:
# both are pure cybersecurity/critical-infra bodies, so nothing meaningful
# is lost by keeping their full-bypass behavior.
TRUSTED_DOMAINS = {
    'github.com', 'raw.githubusercontent.com', 'githubusercontent.com',
    'gitlab.com', 'git-stars.org', 'api.github.com',
}

# The site's own language pages show real counts like 15,997 repos for
# Python - "all of them" means these need to be high enough to not cut
# pagination short. Lower via env if you want a capped test run first.
GIT_STARS_MAX_REPOS_PER_LANGUAGE = _env_int("SCRAPER_GIT_STARS_MAX_REPOS", 50000)
GIT_STARS_MAX_ISSUES_PER_REPO   = _env_int("SCRAPER_GIT_STARS_MAX_ISSUES", 200)
GIT_STARS_MAX_PAGES_PER_LANGUAGE = _env_int("SCRAPER_GIT_STARS_MAX_PAGES", 2000)  # pagination cap
# Test-run scope: only these language slugs (as they appear in the URL,
# e.g. /en/languages/shell -> "shell") get scraped. Clear this list (set
# SCRAPER_GIT_STARS_LANGUAGES="") once you're ready to run every language.
GIT_STARS_LANGUAGE_FILTER = _env_list("SCRAPER_GIT_STARS_LANGUAGES", ["shell"])




save_lock = threading.Lock()

# ---------------------------------------------------------------------
# Provenance manifest (JSONL). One line per saved file recording where it
# came from, its hash, bucket, and metadata - the backbone for attribution,
# licensing, incremental re-crawls, and auditing what was actually collected.
# ---------------------------------------------------------------------
manifest_lock = threading.Lock()

def write_manifest(record):
    if not MANIFEST_FILE:
        return
    try:
        with manifest_lock:
            with open(MANIFEST_FILE, 'a', encoding='utf-8') as f:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
    except Exception:
        pass

# ---------------------------------------------------------------------
# Failure log (JSONL). Every failed fetch/save is recorded so you can see
# and retry what broke instead of it vanishing into the console.
# ---------------------------------------------------------------------
failures_lock = threading.Lock()

def record_failure(url, reason, context=None):
    if not FAILURES_FILE:
        return
    rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'url': url, 'reason': str(reason)[:500]}
    if context:
        rec['context'] = context
    try:
        with failures_lock:
            with open(FAILURES_FILE, 'a', encoding='utf-8') as f:
                f.write(json.dumps(rec, ensure_ascii=False) + '\n')
    except Exception:
        pass

# ---------------------------------------------------------------------
# Content-level dedup by sha256. Filename dedup misses the same document
# fetched via two different URLs (common: a NIST PDF, a mirrored advisory,
# an identical finding copied across Code4rena repos). A persistent hash
# index catches those. Loaded at startup; appended as we save.
# ---------------------------------------------------------------------
content_hashes = set()
content_hash_lock = threading.Lock()

def load_content_hashes():
    s = set()
    if CONTENT_HASHES_FILE and CONTENT_HASHES_FILE.exists():
        try:
            with open(CONTENT_HASHES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        s.add(line)
        except Exception:
            pass
    return s

def _hash_seen(h):
    with content_hash_lock:
        return h in content_hashes

def _hash_remember(h):
    with content_hash_lock:
        if h in content_hashes:
            return False
        content_hashes.add(h)
    try:
        with open(CONTENT_HASHES_FILE, 'a', encoding='utf-8') as f:
            f.write(h + '\n')
    except Exception:
        pass
    return True

# ---------------------------------------------------------------------
# Shared GitHub rate-limit gate. When ANY worker hits GitHub's limit it sets
# a global "paused until" timestamp; every other worker checks it before
# calling the API, so the whole pool backs off together instead of each
# thread independently slamming into the same 403.
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Headless-browser render pool for JS-rendered pages.
#
# Playwright's sync objects are bound to the thread that created them, so we
# can't share one browser across the crawler's many worker threads. Instead a
# small, fixed number of DEDICATED render threads each own their own browser
# and pull jobs off a queue. Worker threads submit a URL + an Event and block
# on that Event, so at most JS_RENDER_WORKERS browsers ever exist regardless of
# how many crawl threads are running. If the browser can't launch (Playwright
# installed but no browser binary), rendering disables itself gracefully and
# the crawler carries on with static extraction only.
# ---------------------------------------------------------------------
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
        # Drain any queued jobs so their waiters don't hang forever.
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
        # Skip heavy assets we never keep, to speed up rendering.
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
                pass  # timeout/partial load - still grab whatever rendered
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
    """Return fully-rendered HTML for a JS page, or None if rendering is
    unavailable/failed. Blocks the calling worker until the render completes."""
    if not JS_ENABLED or _render_disabled:
        return None
    start_js_renderer()
    if _render_disabled:
        return None
    ev = threading.Event()
    result = {'done': ev}
    _render_q.put((url, result))
    if not ev.wait(timeout=JS_RENDER_TIMEOUT + 15):
        return None  # renderer stuck; don't hang the crawl
    return result.get('html')

def stop_js_renderer():
    if not _render_started:
        return
    for _ in _render_threads:
        _render_q.put(None)

atexit.register(stop_js_renderer)

# ---------------------------------------------------------------------
# Per-domain throttling. Total thread count can be high (many domains in
# flight at once) while still being polite to any single host - this is
# what actually avoids bans, not the raw thread count.
# ---------------------------------------------------------------------
_domain_semaphores = {}
_domain_sem_lock = threading.Lock()

def _get_domain_semaphore(url):
    domain = urllib.parse.urlparse(url).netloc.lower()
    # Apply per‑domain override if defined
    max_conc = PER_DOMAIN_OVERRIDES.get(domain, PER_DOMAIN_MAX_CONCURRENCY)
    with _domain_sem_lock:
        sem = _domain_semaphores.get(domain)
        if sem is None:
            sem = threading.Semaphore(max_conc)
            _domain_semaphores[domain] = sem
        return sem

# ---------------------------------------------------------------------
def load_content_hashes():
    s = set()
    if CONTENT_HASHES_FILE and CONTENT_HASHES_FILE.exists():
        try:
            with open(CONTENT_HASHES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        s.add(line)
        except Exception:
            pass
    return s

def _hash_seen(h):
    with content_hash_lock:
        return h in content_hashes

def _hash_remember(h):
    with content_hash_lock:
        if h in content_hashes:
            return False
        content_hashes.add(h)
    try:
        with open(CONTENT_HASHES_FILE, 'a', encoding='utf-8') as f:
            f.write(h + '\n')
    except Exception:
        pass
    return True

# ---------------------------------------------------------------------
# Shared GitHub rate-limit gate. When ANY worker hits GitHub's limit it sets
# a global "paused until" timestamp; every other worker checks it before
# calling the API, so the whole pool backs off together instead of each
# thread independently slamming into the same 403.
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Headless-browser render pool for JS-rendered pages.
#
# Playwright's sync objects are bound to the thread that created them, so we
# can't share one browser across the crawler's many worker threads. Instead a
# small, fixed number of DEDICATED render threads each own their own browser
# and pull jobs off a queue. Worker threads submit a URL + an Event and block
# on that Event, so at most JS_RENDER_WORKERS browsers ever exist regardless of
# how many crawl threads are running. If the browser can't launch (Playwright
# installed but no browser binary), rendering disables itself gracefully and
# the crawler carries on with static extraction only.
# ---------------------------------------------------------------------
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
        # Drain any queued jobs so their waiters don't hang forever.
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
        # Skip heavy assets we never keep, to speed up rendering.
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
                pass  # timeout/partial load - still grab whatever rendered
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
    """Return fully-rendered HTML for a JS page, or None if rendering is
    unavailable/failed. Blocks the calling worker until the render completes."""
    if not JS_ENABLED or _render_disabled:
        return None
    start_js_renderer()
    if _render_disabled:
        return None
    ev = threading.Event()
    result = {'done': ev}
    _render_q.put((url, result))
    if not ev.wait(timeout=JS_RENDER_TIMEOUT + 15):
        return None  # renderer stuck; don't hang the crawl
    return result.get('html')

def stop_js_renderer():
    if not _render_started:
        return
    for _ in _render_threads:
        _render_q.put(None)

atexit.register(stop_js_renderer)

# ---------------------------------------------------------------------
# Per-domain throttling. Total thread count can be high (many domains in
# flight at once) while still being polite to any single host - this is
# what actually avoids bans, not the raw thread count.
# ---------------------------------------------------------------------
_domain_semaphores = {}
_domain_sem_lock = threading.Lock()

def _get_domain_semaphore(url):
    domain = urllib.parse.urlparse(url).netloc.lower()
    # Apply per‑domain override if defined
    max_conc = PER_DOMAIN_OVERRIDES.get(domain, PER_DOMAIN_MAX_CONCURRENCY)
    with _domain_sem_lock:
        sem = _domain_semaphores.get(domain)
        if sem is None:
            sem = threading.Semaphore(max_conc)
            _domain_semaphores[domain] = sem
        return sem

# ---------------------------------------------------------------------
# robots.txt compliance. One RobotFileParser per domain, fetched once and
# cached. If robots.txt is unreachable/missing we fail OPEN (treat as
# allow-all) rather than blocking the whole crawl on a transient error -
# that mirrors how real crawlers behave, and matches what the sitemap
# discovery already assumes (_robots_sitemaps below).
# ---------------------------------------------------------------------
RESPECT_ROBOTS = False   # Set default to False, so we don't get blocked by robots.txt
_robots_cache = {}
_robots_lock = threading.Lock()

def _get_robots_parser(url, session):
    p = urllib.parse.urlparse(url)
    domain_key = f"{p.scheme}://{p.netloc}"
    with _robots_lock:
        cached = _robots_cache.get(domain_key)
        if cached is not None:
            return cached
    rp = robotparser.RobotFileParser()
    robots_url = f"{domain_key}/robots.txt"
    try:
        resp = session.get(robots_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            rp.parse(resp.text.splitlines())
        else:
            rp.parse([])  # 404/403/etc on robots.txt itself -> treat as allow-all
    except Exception as e:
        log(f"  Error fetching robots.txt from {robots_url}: {e}")
        rp.parse([])  # unreachable -> fail open, don't block the crawl
    with _robots_lock:
        _robots_cache[domain_key] = rp
    return rp

def is_allowed_by_robots(url, session):
    if not RESPECT_ROBOTS:
        return True
    try:
        rp = _get_robots_parser(url, session)
        return rp.can_fetch(HEADERS.get('User-Agent', '*'), url)
    except Exception:
        return True  # a parser bug should never be the reason we stop crawling

# ---------------------------------------------------------------------
# Per-domain request spacing ("don't look like an attack"). Independent of
# the concurrency semaphore above: this enforces a real minimum WALL-CLOCK
# gap between the START of consecutive requests to the same host, no
# matter how many threads are hitting it. Each thread "reserves" the next
# free slot under a lock, then sleeps until its turn - so N threads queued
# on one domain fire ~SCRAPER_MIN_DELAY-SCRAPER_MAX_DELAY seconds apart,
# never in a burst.
# ---------------------------------------------------------------------
DOMAIN_MIN_DELAY = float(os.environ.get("SCRAPER_MIN_DELAY", "2.0"))
DOMAIN_MAX_DELAY = float(os.environ.get("SCRAPER_MAX_DELAY", "3.0"))
_domain_next_slot = {}
_domain_throttle_lock = threading.Lock()

def _throttle_domain(url):
    domain = urllib.parse.urlparse(url).netloc.replace('www.', '').lower()
    delay = random.uniform(DOMAIN_MIN_DELAY, DOMAIN_MAX_DELAY)
    with _domain_throttle_lock:
        now = time.time()
        next_free = _domain_next_slot.get(domain, now)
        my_slot = max(now, next_free)
        _domain_next_slot[domain] = my_slot + delay
    wait = my_slot - now
    if wait > 0:
        time.sleep(wait)

# ---------------------------------------------------------------------
# File-type buckets (replaces the old LLM classifier). Purely extension
# / content-type based, no content inspection needed.
# ---------------------------------------------------------------------
CODE_EXTS = {
    '.py', '.go', '.js', '.mjs', '.cjs', '.ts', '.tsx', '.jsx', '.java', '.c', '.h',
    '.cpp', '.cc', '.hpp', '.cs', '.rb', '.php', '.sh', '.bash', '.ps1', '.pl',
    '.rs', '.swift', '.kt', '.kts', '.scala', '.r', '.m', '.mm', '.lua', '.sql',
    '.ipynb', '.groovy', '.dart', '.vb', '.asm', '.s', '.f90', '.jl', '.ex', '.exs',
    '.erl', '.clj', '.hs', '.nim', '.zig', '.sol', '.proto', '.tf', '.dockerfile',
}
DATA_EXTS = {
    '.csv', '.tsv', '.json', '.jsonl', '.xml', '.yaml', '.yml', '.ndjson',
    '.parquet', '.sqlite', '.db', '.log', '.txt', '.md', '.ini', '.cfg', '.conf',
    '.toml', '.env',
}
ARCHIVE_EXTS = {'.zip', '.tar', '.gz', '.tgz', '.bz2', '.7z', '.rar', '.xz'}
DOC_EXTS = {'.pdf', '.docx', '.doc', '.rtf', '.xlsx', '.xls', '.pptx', '.ppt', '.odt', '.ods'}

# Extensions we will always attempt to download as raw binary/text (no HTML parse)
BINARY_DOWNLOAD_EXTS = CODE_EXTS | DATA_EXTS | ARCHIVE_EXTS | DOC_EXTS

# Extensions we never want (media/fonts/installers - not useful as data/text/code)
SKIP_EXTS = {
    '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico', '.css', '.woff', '.woff2',
    '.ttf', '.eot', '.mp4', '.mp3', '.avi', '.mov', '.wav', '.iso', '.img',
    '.deb', '.sig', '.ova', '.torrent', '.pkg', '.dmg', '.bin', '.msi', '.exe',
}

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

def ext_folder(filename):
    """Leaf folder name based on the file's EXTENSION (e.g. pdf, md, json, sol),
    not a coarse category. Files with no extension go under 'no_ext'."""
    ext = Path(filename).suffix.lower().lstrip('.')
    # keep it filesystem-safe and tidy
    ext = "".join(c for c in ext if c.isalnum())
    return ext if ext else 'no_ext'

def load_saved_urls():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def mark_url_saved(url):
    with save_lock:
        saved_urls.add(url)
        with open(PROGRESS_FILE, 'a', encoding='utf-8') as f:
            f.write(url + '\n')

def sanitize_name(name):
    # Preserve '/' as a path separator so callers can nest sub-datasets under a
    # single resource folder (e.g. "code4rena/<contest>"); each path segment is
    # sanitized independently.
    parts = []
    for seg in str(name).replace('\\', '/').split('/'):
        clean = "".join([c if c.isalnum() or c in "._- " else "_" for c in seg]).strip('_ ')
        if clean:
            parts.append(clean)
    return "/".join(parts) or "site"

def site_folder_for(prefix, url):
    """Folder name (under DEST_ROOT) that all data for this source URL goes into."""
    if prefix:
        return sanitize_name(prefix)
    domain = urllib.parse.urlparse(url).netloc.replace('www.', '')
    return sanitize_name(domain)

# ---------------------------------------------------------------------
# Content-type folder naming for the regulatory seed sites.
# NOT a hardcoded list any more - the folder name is derived from each
# site's OWN url path, so whatever categories a given regulator actually
# uses (Circulars, Notifications, Press_Releases, Publications, Statistics,
# Advisories, Forms, Master_Directions, Speeches, ... or anything else) show
# up as their own folder, automatically, with no fixed ceiling on how many.
#
# How: take the url path, drop the filename/extension and query string,
# drop numeric/date/id-looking segments and generic boilerplate words
# (en, web, home, index, site, pages, aspx, php...), and use the first
# remaining meaningful segment - title-cased with underscores, matching
# the look of the old hardcoded names (e.g. "press-releases" ->
# "Press_Releases"). If nothing usable is left, falls back to Other_Misc.
# ---------------------------------------------------------------------
_BOILERPLATE_SEGMENTS = {
    'en', 'web', 'www', 'home', 'index', 'default', 'site', 'sites',
    'page', 'pages', 'content', 'content-web', 'scripts', 'script',
    'doc', 'docs', 'view', 'viewpage', 'display', 'show', 'main', 'root',
    'servlet', 'list', 'details', 'detail', 'guest', 'welcome', 'portal',
}
_FILEISH_EXT_RE = re.compile(r'\.(aspx|php|html?|jsp|do|action|cshtml)$', re.I)
_CAMEL_SPLIT_RE = re.compile(r'(?<=[a-z0-9])(?=[A-Z])')

def _words_from_segment(seg_clean):
    """Split a path/filename segment into candidate words: camelCase and
    -/_ boundaries both count, e.g. 'BS_CircularIndexDisplay' ->
    ['bs','circular','index','display']. Drops boilerplate/numeric/short
    tokens. Returns [] if nothing meaningful survives."""
    seg_clean = _CAMEL_SPLIT_RE.sub('_', seg_clean)
    words = [w for w in re.split(r'[-_\s]+', seg_clean) if w]
    words = [w for w in words
             if w.lower() not in _BOILERPLATE_SEGMENTS
             and not re.fullmatch(r'[\d.]+', w)
             and len(w) > 2]
    return words

def classify_content_type(url, title=""):
    """Derive a folder name for this URL from the site's own path structure
    (see module docstring above). Falls back to 'Other_Misc' if the path
    has nothing usable, or the file extension makes the type obvious
    (e.g. spreadsheets/CSVs always go to Data_Statistics)."""
    parsed_path = Path(urllib.parse.urlparse(url).path)
    path_ext = parsed_path.suffix.lower()
    if path_ext in ('.xlsx', '.xls', '.csv'):
        # A spreadsheet/CSV on a regulator site is almost always tabular
        # data/statistics, regardless of what the surrounding path says.
        return "Data_Statistics"

    raw_segments = [urllib.parse.unquote(s).strip() for s in
                     urllib.parse.urlparse(url).path.split('/') if s]
    # If the last segment looks like a filename, use its stem (not the raw
    # segment) as the final candidate, so e.g. ".../BS_CircularIndexDisplay.aspx"
    # still yields "Circular_Index_Display" instead of being dropped outright.
    if raw_segments:
        last = raw_segments[-1]
        if Path(last).suffix or _FILEISH_EXT_RE.search(last):
            raw_segments[-1] = Path(last).stem

    for seg_clean in raw_segments:
        if re.fullmatch(r'[\d_.\-]+', seg_clean):   # purely numeric/id/date
            continue
        words = _words_from_segment(seg_clean)
        if words:
            return "_".join(w.capitalize() for w in words)

    return "Other_Misc"

# ---------------------------------------------------------------------
# E1/E5/E8/E9/E11 tagging (Governance, Engineering & Assurance subset that
# actually has real content on these regulator sites - see conversation:
# E2/E4/E6/E7/E12 are skipped, there's essentially nothing to tag there).
# This is stored as metadata in the manifest line (e_tag field), NOT as a
# folder, so we don't create near-empty directories.
# ---------------------------------------------------------------------
E_TAG_RULES = [
    ("E9",  re.compile(r'data[-_ ]?protection|data[-_ ]?localis|data[-_ ]?localiz|encrypt|'
                        r'\bdlp\b|breach[-_ ]?notif|privacy', re.I)),
    ("E8",  re.compile(r'third[-_ ]?party|vendor|outsourc|supply[-_ ]?chain|service[-_ ]?provider[-_ ]?risk', re.I)),
    ("E5",  re.compile(r'\bmfa\b|\b2fa\b|multi[-_ ]?factor|authentic|\bsso\b|identity[-_ ]?(and|&)?[-_ ]?access|'
                        r'privileged[-_ ]?access|\bpam\b', re.I)),
    ("E11", re.compile(r'physical[-_ ]?security|data[-_ ]?cent(er|re)|surveillance|access[-_ ]?control[-_ ]?system|'
                        r'facility[-_ ]?security|tamper', re.I)),
    ("E1",  re.compile(r'\bgrc\b|risk[-_ ]?management|risk[-_ ]?assessment|\baudit\b|compliance|'
                        r'governance|control[-_ ]?framework', re.I)),
]

def classify_e_tag(url, title=""):
    """Return 'E1'/'E5'/'E8'/'E9'/'E11' if the URL/title matches, else None.
    Checked most-specific-first so e.g. a 'data protection audit' circular
    tags E9 (its more specific hit) rather than falling through to the very
    broad E1 patterns."""
    hay = f"{urllib.parse.urlparse(url).path} {title or ''}"
    for tag, pat in E_TAG_RULES:
        if pat.search(hay):
            return tag
    return None

# ---------------------------------------------------------------------
# Raw-file integrity check ("except for the raw file which cannot be
# open"). Cheap magic-byte / sanity check run on binary downloads BEFORE
# they're saved, so a corrupted download or an HTML error page served with
# a misleading .pdf/.docx extension never lands in the corpus as a fake,
# unopenable file. Failures are logged to FAILURES_FILE, not silently
# dropped.
# ---------------------------------------------------------------------
_MAGIC_BYTES = {
    '.pdf':  (b'%PDF',),
    '.docx': (b'PK\x03\x04', b'PK\x05\x06'),
    '.xlsx': (b'PK\x03\x04', b'PK\x05\x06'),
    '.pptx': (b'PK\x03\x04', b'PK\x05\x06'),
    '.zip':  (b'PK\x03\x04', b'PK\x05\x06'),
    '.doc':  (b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1', b'PK\x03\x04'),  # old OLE or modern zip-based
    '.xls':  (b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1', b'PK\x03\x04'),
    '.ppt':  (b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1', b'PK\x03\x04'),
}

def is_valid_binary(ext, content_bytes):
    """True if content_bytes looks like a genuine, openable file of type
    ext. Returns False (and the reason) for: empty/near-empty responses,
    HTML error pages saved under a document extension, and files whose
    magic bytes don't match their extension."""
    ext = ext.lower()
    if not content_bytes or len(content_bytes) < 100:
        return False, "empty or near-empty (likely a failed/blocked download)"
    head = content_bytes[:512].lstrip()
    if head[:1] in (b'<',) and (b'<html' in head.lower() or b'<!doctype html' in head.lower()):
        return False, "looks like an HTML error/placeholder page, not a real document"
    expected = _MAGIC_BYTES.get(ext)
    if expected and not any(content_bytes.startswith(sig) for sig in expected):
        return False, f"magic bytes don't match expected {ext} signature"
    return True, None

def is_regulatory_domain(url):
    """True if url belongs to one of the 9 hardcoded regulatory sources
    (RBI/SEBI/IRDAI/NCIIPC/CERT-In/NSCS/PowerSector/Railway/FFIEC)."""
    domain = urllib.parse.urlparse(url).netloc.replace('www.', '').lower()
    return any(domain == d or domain.endswith('.' + d) for d in REGULATORY_DOMAINS)

def regulatory_site_folder(site_folder, url, title=""):
    """For regulatory-domain URLs, nest the content-type folder under the
    site folder (e.g. 'rbi.org.in/Circulars_Notifications'). For every
    other domain, returns site_folder unchanged."""
    if is_regulatory_domain(url):
        return f"{site_folder}/{classify_content_type(url, title)}"
    return site_folder

# ---------------------------------------------------------------------
# Cross-run / cross-folder dedup index.
# Populated at startup from both PART3_DIR (previous partial scrape) and
# DEST_ROOT (this run's output so far, if resuming) so we never re-fetch
# or re-save something we already have, no matter which folder it's in.
# ---------------------------------------------------------------------
existing_filenames = set()
existing_index_lock = threading.Lock()

def _index_existing_filenames(root):
    found = set()
    if root and root.exists():
        for p in root.rglob('*'):
            if p.is_file():
                found.add(p.name)
    return found

def build_existing_filenames_index():
    idx = set()
    idx |= _index_existing_filenames(PART3_DIR)
    idx |= _index_existing_filenames(DEST_ROOT)
    return idx

def is_already_present(clean_fn):
    with existing_index_lock:
        return clean_fn in existing_filenames

def mark_filename_present(clean_fn):
    with existing_index_lock:
        existing_filenames.add(clean_fn)

saved_urls = set()  # Populated at start of main()
stats = defaultdict(int)
total_saved = 0

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

log_lock = threading.Lock()

def log(msg):
    ts = time.strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with log_lock:
            with open(LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(line + '\n')
    except:
        pass

CYBER_DOMAINS = {
    'cert-in.org.in', 'cert.org.in', 'nciipc.gov.in', 'ncsc.gov.in',
    'cisa.gov', 'owasp.org', 'mitre.org', 'nvd.nist.gov', 'csrc.nist.gov'
}

CYBER_KEYWORDS = [
    r'cyber',
    # ---- explicit coverage for the three main topics requested: ------
    # information security / network security / cybersecurity, in any
    # publication/report/circular/bulletin/advisory/policy form, in
    # either word order ("cybersecurity report" or "report on cyber
    # security"), plus common abbreviations/synonyms.
    r'cyber[-\s_]*security', r'information[-\s_]*security', r'network[-\s_]*security',
    r'\binfosec\b', r'\bnetsec\b', r'information[-\s_]*assurance',
    r'(?:cyber|information|info|network|it)[-\s_]*security[-\s_]*(?:report|reports|circular|circulars|publication|publications|advisory|advisories|policy|policies|framework|frameworks|guideline|guidelines|standard|standards|notification|notifications|bulletin|bulletins|directive|directives|assessment|strategy|regulation|regulations)',
    r'(?:report|reports|circular|circulars|publication|publications|advisory|advisories|notification|notifications|bulletin|bulletins|directive|directives|guideline|guidelines|policy|policies|framework|frameworks)[-\s_]*(?:on|for|regarding)?[-\s_]*(?:cyber|information|info|network)[-\s_]*security',
    r'\b(?:information|info|network|data|computer|it|digital|internet|cloud|system|application|endpoint|mobile|web|infrastructure|physical|operational|telecom|port|aviation|railway|power|critical)[-\s_]*secur',
    r'\bsecur(?:ity|ing)[-\s_]+(?:breach|incident|audit|control|patch|standard|policy|guideline|measure|threat|alert|advisory|flaw|weakness|vulnerability|assessment|architecture|framework|operation|compliance|officer|guard|force)',
    r'\bsecur(?:ity|ities)[-\s_]+(?:advisory|advisories|alert|alerts|bulletin|bulletins|incident|incidents|patch|patches|vulnerab)',
    r'malware', r'ransomware', r'spyware', r'adware', r'trojan', r'rootkit', r'bootkit', r'keylogger', r'backdoor', r'botnet', r'wiper', r'logic[-\s_]*bomb',
    r'vulnerab', r'exploit', r'threat', r'intrusion', r'intruder',
    r'obfuscate', r'privilege[-\s_]*escalation',
    r'\bc2\b', r'\bc&c\b', r'\bcnc\b', r'\bcommand[-\s_]*and[-\s_]*control\b',
    r'owasp', r'\bcve\b', r'\bcwe\b', r'\bcapec\b',
    r'firewall', r'\bwaf\b', r'\bips\b', r'\bids\b',
    r'cryptograph', r'encrypt', r'decrypt', r'cipher',
    r'penetration[-\s_]*test', r'pentest', r'red[-\s_]*team', r'blue[-\s_]*team',
    r'access[-\s_]*control', r'\bmfa\b', r'\b2fa\b', r'authentication',
    r'zero[-\s_]*trust', r'data[-\s_]*loss[-\s_]*prevention', r'\bdlp\b', r'exfiltration',
    r'leakage', r'cert-in', r'nciipc', r'ncsc', r'phish', r'\bapt\b',
    r'forensics', r'dfir',
    r'antivirus', r'anti-malware', r'antimalware', r'\bedr\b', r'\bxdr\b', r'\bsiem\b', r'\bsoc\b',
    r'incident[-\s_]*response',
    # ---- domain vocabulary (GRC / privacy / API-DB-architecture) ------
    # GRC / audit / compliance
    r'\bgrc\b', r'control[-\s_]*mapping', r'control[-\s_]*catalog',
    r'iso[-\s_]*2700\d', r'\bsoc[-\s_]*2\b', r'nist[-\s_]*csf', r'cybersecurity[-\s_]*framework',
    r'800-53', r'800-171', r'\bcis[-\s_]*control', r'\bcis[-\s_]*benchmark', r'\boscal\b',
    r'risk[-\s_]*management', r'control[-\s_]*baseline', r'\bstig\b', r'\bcmmc\b', r'\bpci[-\s_]*dss\b',
    # Privacy engineering / data protection / DLP
    r'\bgdpr\b', r'\bccpa\b', r'data[-\s_]*protection', r'\bdlp\b',
    r'privacy[-\s_]*framework', r'privacy[-\s_]*by[-\s_]*design', r'privacy[-\s_]*engineering',
    r'privacy[-\s_]*pattern', r'linddun', r'\bpii\b', r'data[-\s_]*minimi',
    r'de-?identif', r'anonymi', r'pseudonymi',
    # API / database / secure architecture
    r'api[-\s_]*security', r'\basvs\b', r'threat[-\s_]*model', r'\bstride\b', r'\bpasta\b',
    r'well-?architected', r'secure[-\s_]*architecture', r'security[-\s_]*pillar',
    r'database[-\s_]*security', r'\bsql[-\s_]*injection',
    r'access[-\s_]*control', r'authentication', r'authorization', r'\boauth\b', r'\boidc\b',
    r'data[-\s_]*flow[-\s_]*diagram', r'\bdfd\b',
    # ---- OT / ICS / SCADA -------------------------------------------
    r'\bscada\b', r'\bics\b', r'industrial[-\s_]*control', r'operational[-\s_]*technology',
    r'\bot[-\s_]*(?:secur|environment|network|asset|system)', r'\bplc\b', r'\brtu\b',
    r'\bdcs\b', r'\bhmi\b', r'\bmodbus\b', r'\bdnp3\b', r'\bs7[-\s_]*protocol\b',
    r'critical[-\s_]*infrastructure', r'critical[-\s_]*information[-\s_]*infrastructure',
    # ---- AI / ML -----------------------------------------------------
    r'artificial[-\s_]*intelligence', r'machine[-\s_]*learning', r'\bai\/ml\b', r'\bml\/ai\b',
    r'generative[-\s_]*ai', r'\bgenai\b', r'large[-\s_]*language[-\s_]*model', r'\bllm\b',
    r'neural[-\s_]*network', r'deep[-\s_]*learning', r'foundation[-\s_]*model',
    r'\bai[-\s_]*(?:governance|risk|security|framework|model|regulation|policy|guideline|ethics)',
    r'algorithmic[-\s_]*(?:bias|governance|accountability)', r'model[-\s_]*risk[-\s_]*management',
]

# ---------------------------------------------------------------------
# WEAK/generic keywords: words that show up plenty in NON-security text
# too (a banking "advisory committee", a "compliance" deadline for filing
# taxes, a railway "incident" report, a "warning" about train delays...).
# On their OWN, one hit isn't good evidence of real security content, so
# these require >=2 total hits (of weak words) to pass - unless a STRONG
# keyword above is also present, in which case one hit is enough as before.
# ---------------------------------------------------------------------
WEAK_KEYWORDS = [
    r'attack', r'compromise', r'breach', r'hack', r'incident',
    r'advisory', r'advisories', r'bulletin', r'warning', r'patch', r'hotfix',
    r'compliance', r'\baudit', r'defense', r'defence', r'privacy', r'consent',
    r'hardening', r'benchmark', r'framework', r'guideline', r'guidelines',
    r'policy', r'policies', r'standard', r'standards', r'control',
]

CYBER_PAT = re.compile(r'(?i)' + '|'.join(CYBER_KEYWORDS))
WEAK_PAT = re.compile(r'(?i)' + '|'.join(WEAK_KEYWORDS))

def is_cyber_related(url, text=None):
    domain = urllib.parse.urlparse(url).netloc.lower()

    # v7: curated/known-relevant domains bypass keyword filtering entirely,
    # so hand-picked gap-fill sources are never dropped as "not cyber".
    for d in TRUSTED_DOMAINS:
        if d in domain:
            return True
    for d in CYBER_DOMAINS:
        if d in domain:
            return True

    haystacks = [url] + ([text] if text else [])

    # A single STRONG-keyword hit anywhere is enough, same as before.
    for hay in haystacks:
        if CYBER_PAT.search(hay):
            return True

    # No strong hit: fall back to WEAK keywords, but require >=2 total
    # hits (across url+text combined) before treating it as cyber-related.
    weak_hits = 0
    for hay in haystacks:
        weak_hits += len(WEAK_PAT.findall(hay))
        if weak_hits >= 2:
            return True

    return False


def clean_html(raw_html, source_url):
    """Return (clean_html_doc, title, text). Uses trafilatura for main-content
    extraction when available (much better boilerplate removal), and falls back
    to the BeautifulSoup heuristic otherwise."""
    if _HAS_TRAFILATURA:
        try:
            text = trafilatura.extract(
                raw_html, url=source_url, include_comments=False,
                include_tables=True, favor_recall=True)
            body = trafilatura.extract(
                raw_html, url=source_url, include_comments=False,
                include_tables=True, favor_recall=True, output_format='html')
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
                clean_doc = (
                    '<!DOCTYPE html>\n<html>\n<head><meta charset="utf-8">'
                    f'<title>{html.escape(title)}</title></head>\n<body>\n'
                    f'<p>Source: <a href="{source_url}">{source_url}</a></p><hr>\n'
                    f'{body_html}\n</body>\n</html>')
                return clean_doc, title, text
        except Exception:
            pass  # fall through to the BeautifulSoup path
    return _clean_html_bs4(raw_html, source_url)

def _clean_html_bs4(raw_html, source_url):
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
            except:
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
            el_id = el.get('id', '') or ''
        except:
            continue
        if noise_patterns.search(classes) or noise_patterns.search(el_id):
            try:
                el.decompose()
            except:
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
        except:
            pass

    title_tag = soup.find('title')
    title = title_tag.string.strip() if title_tag and title_tag.string else "Scraped Resource"
    text = main.get_text(separator=' ', strip=True)

    clean_doc = f'<!DOCTYPE html>\n<html>\n<head><meta charset="utf-8"><title>{title}</title></head>\n<body>\n'
    clean_doc += f'<p>Source: <a href="{source_url}">{source_url}</a></p><hr>\n'
    clean_doc += str(main)
    clean_doc += '\n</body>\n</html>'

    return clean_doc, title, text

def _atomic_write_bytes(out_path, data_bytes):
    """Write via a temp file + atomic rename. A crash between open() and
    write() finishing can never leave a truncated/empty file sitting under
    the REAL filename - worst case is an orphaned .tmp file, which is
    obviously incomplete and never mistaken for a genuinely saved file."""
    out_path = Path(out_path)
    tmp_path = out_path.with_name(f".{out_path.name}.tmp{os.getpid()}")
    with open(tmp_path, 'wb') as f:
        f.write(data_bytes)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, out_path)  # atomic on both POSIX and Windows

def save_file(filename, content, bucket='html', site_folder=None,
              source_url=None, title=None, content_type=None, extra=None):
    """Save text content to DEST_ROOT/<site_folder>/<bucket>/<filename>.
    Deduplicates by sha256 content hash and records a manifest line."""
    global total_saved
    data = content.encode('utf-8', errors='ignore') if isinstance(content, str) else content
    sha = hashlib.sha256(data).hexdigest()

    clean_fn = "".join([c if c.isalnum() or c in "._- " else "_" for c in filename])
    if len(clean_fn) > 200:
        clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + '.html'

    # Content-level duplicate: don't rewrite, but keep provenance.
    if _hash_seen(sha):
        rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'saved': False,
               'duplicate_of_sha256': sha, 'filename': clean_fn,
               'bucket': bucket, 'site_folder': site_folder,
               'source_url': source_url, 'title': title}
        if extra:
            rec.update(extra)
        write_manifest(rec)
        return True

    out_dir = DEST_ROOT / (site_folder or 'misc') / ext_folder(clean_fn)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / clean_fn
    try:
        _atomic_write_bytes(out_path, data)
        with save_lock:
            stats[ext_folder(clean_fn)] += 1
            total_saved += 1
        _hash_remember(sha)
        mark_filename_present(clean_fn)
        rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'saved': True,
               'sha256': sha, 'filename': clean_fn, 'bucket': bucket,
               'site_folder': site_folder, 'path': str(out_path),
               'bytes': len(data), 'source_url': source_url,
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
    """Save raw bytes. Deduplicates by sha256 and records a manifest line."""
    global total_saved
    sha = hashlib.sha256(content_bytes).hexdigest()

    clean_fn = "".join([c if c.isalnum() or c in "._- " else "_" for c in filename])
    if len(clean_fn) > 200:
        ext = Path(clean_fn).suffix
        clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + ext

    if _hash_seen(sha):
        rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'saved': False,
               'duplicate_of_sha256': sha, 'filename': clean_fn,
               'bucket': bucket, 'site_folder': site_folder,
               'source_url': source_url, 'title': title}
        if extra:
            rec.update(extra)
        write_manifest(rec)
        return True

    out_dir = DEST_ROOT / (site_folder or 'misc') / ext_folder(clean_fn)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / clean_fn
    try:
        _atomic_write_bytes(out_path, content_bytes)
        with save_lock:
            stats[ext_folder(clean_fn)] += 1
            total_saved += 1
        _hash_remember(sha)
        mark_filename_present(clean_fn)
        rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'saved': True,
               'sha256': sha, 'filename': clean_fn, 'bucket': bucket,
               'site_folder': site_folder, 'path': str(out_path),
               'bytes': len(content_bytes), 'source_url': source_url,
               'title': title, 'content_type': content_type}
        if extra:
            rec.update(extra)
        write_manifest(rec)
        return True
    except Exception as e:
        record_failure(source_url or clean_fn, f"save_binary_file: {e}")
        return False

def fetch(url, session, retries=4):
    """
    Per-domain throttled fetch with exponential backoff + jitter.
    - robots.txt is checked first; disallowed URLs are never requested.
    - The semaphore caps concurrent in-flight requests to a single host,
      regardless of how many total worker threads exist.
    - _throttle_domain() additionally enforces a real ~2-3s minimum gap
      between requests to the same host (see definition above) so the
      crawl reads as a slow, polite client rather than a burst/attack.
    - 429/503 (and connection errors) trigger backoff instead of hammering
      the host again immediately - this is what actually prevents bans.
    """
    if not is_allowed_by_robots(url, session):
        log(f"  SKIP (robots.txt disallow): {url}")
        record_failure(url, "blocked by robots.txt")
        return None

    sem = _get_domain_semaphore(url)
    with sem:
        for attempt in range(retries):
            try:
                _throttle_domain(url)
                r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)

                if r.status_code == 429 or r.status_code in (502, 503, 504):
                    retry_after = r.headers.get('Retry-After')
                    if retry_after:
                        try:
                            wait = float(retry_after)
                        except ValueError:
                            wait = (2 ** attempt) + random.uniform(0, 1)
                    else:
                        wait = (2 ** attempt) + random.uniform(0, 1)
                    if attempt < retries - 1:
                        log(f"  [{r.status_code}] backing off {wait:.1f}s for {urllib.parse.urlparse(url).netloc}")
                        time.sleep(min(wait, 60))
                        continue
                return r
            except Exception as e:
                log(f"  Connection/SSL error fetching {url} on attempt {attempt+1}: {e}")
                if attempt < retries - 1:
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(min(wait, 30))
    return None

# Simple, "honest" client User-Agents used as fallbacks for file downloads.
# Some document CDNs (NIST/Akamai, modbus.org) return 404/403 to a browser
# User-Agent that lacks a full browser fingerprint, but serve the file fine to
# curl/wget. We try the normal headers first, then these.
_DL_UA_FALLBACKS = [
    {'User-Agent': 'curl/8.4.0', 'Accept': '*/*'},
    {'User-Agent': 'Wget/1.21.3', 'Accept': '*/*'},
    {'User-Agent': 'python-requests/2.31', 'Accept': '*/*'},
]

def fetch_download(url, session, retries=3):
    """Fetch a file URL, retrying with curl/wget-style User-Agents if the
    default browser UA gets WAF-blocked (404/403). Returns the response or None."""
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
            r = r2  # keep latest for status reporting
    return r

def scrape_and_save(url, session, prefix=""):
    """Wrapper: claims the URL in-memory immediately (stops other threads in
    THIS run from double-fetching it), but only persists it to PROGRESS_FILE
    once _scrape_and_save_body() has actually finished - saved, confirmed
    duplicate, or a deterministic skip. If the process is killed anywhere
    inside the body, nothing gets written here, so a resumed run retries
    this URL instead of trusting a save that never completed."""
    with save_lock:
        if url in saved_urls:
            return False
        saved_urls.add(url)  # in-memory only, for this run's dedup

    try:
        result = _scrape_and_save_body(url, session, prefix)
    except Exception as e:
        record_failure(url, f"scrape_and_save crashed: {e}", context=prefix or None)
        with save_lock:
            saved_urls.discard(url)  # let a future run retry it
        return False

    with save_lock:
        with open(PROGRESS_FILE, 'a', encoding='utf-8') as _pf:
            _pf.write(url + '\n')
    return result

def _scrape_and_save_body(url, session, prefix=""):
    global total_saved

    url_lower = url.lower()
    if any(url_lower.endswith(ext) for ext in SKIP_EXTS):
        return False

    parsed = urllib.parse.urlparse(url)
    is_github = 'github.com' in parsed.netloc or 'raw.githubusercontent.com' in parsed.netloc
    path_ext = Path(parsed.path).suffix.lower()
    site_folder = site_folder_for(prefix, url)

    # Skip fetching if the file is already scraped (resumability check).
    # Checks against: (a) previously scraped partial data in PART3_DIR,
    # (b) anything already saved in DEST_ROOT (this run or a prior one).
    try:
        if is_github:
            path_parts = [p for p in parsed.path.strip('/').split('/') if p]
            if len(path_parts) >= 3 and path_parts[2] in ('issues', 'pull', 'commit', 'commits', 'releases', 'tags', 'actions', 'projects', 'wiki', 'settings', 'branches', 'pulse', 'graphs', 'network', 'stargazers', 'watchers'):
                return False
            if len(path_parts) >= 3 and path_parts[2] == 'tree':
                return False

            if len(path_parts) >= 4 and path_parts[2] in ('blob', 'raw'):
                owner = path_parts[0]
                repo = path_parts[1]
                file_path = '_'.join(path_parts[4:])
                clean_fn = f"github_{owner}_{repo}_{file_path}.html"
            elif len(path_parts) >= 2:
                clean_fn = f"github_{path_parts[0]}_{path_parts[1]}.html"
            else:
                clean_fn = f"github_{'_'.join(path_parts) or 'index'}.html"
        else:
            if path_ext in BINARY_DOWNLOAD_EXTS:
                path_part = parsed.path.strip('/').split('/')[-1] or f'document{path_ext}'
                clean_fn = urllib.parse.unquote(path_part)
                if ('github.com' in parsed.netloc or 'codeload.github.com' in parsed.netloc) and path_ext == '.zip':
                    parts = parsed.path.strip('/').split('/')
                    if len(parts) >= 2:
                        clean_fn = f"{parts[0]}_{parts[1]}{path_ext}"
            else:
                path_part = parsed.path.strip('/').replace('/', '_') or 'index'
                domain_part = parsed.netloc.replace('.', '_').replace('www_', '')
                clean_fn = f"{prefix or domain_part}_{path_part}.html"

        clean_fn = "".join([c if c.isalnum() or c in "._- " else "_" for c in clean_fn])
        if len(clean_fn) > 200:
            ext_tail = Path(clean_fn).suffix or '.html'
            clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + ext_tail

        if is_already_present(clean_fn):
            return True
    except:
        pass

    # Exploit-db handler for scraping raw payloads safely
    if 'exploit-db.com' in url.lower():
        try:
            match = re.search(r'/exploits/(\d+)', url)
            if match:
                exploit_id = match.group(1)
                download_url = f"https://www.exploit-db.com/download/{exploit_id}"
                raw_r = fetch(download_url, session)
                if raw_r and raw_r.status_code == 200:
                    content_text = raw_r.text
                    title = f"Exploit DB {exploit_id}"
                    clean_doc = f"<!DOCTYPE html>\n<html>\n<head><meta charset='utf-8'><title>{title}</title></head>\n<body>\n"
                    clean_doc += f"<p>Source: <a href='{url}'>{url}</a></p><hr>\n"
                    clean_doc += f"<pre><code>{html.escape(content_text)}</code></pre>\n"
                    clean_doc += "</body>\n</html>"
                    filename = f"exploit_db_{exploit_id}.html"
                    save_file(filename, clean_doc, bucket='code', site_folder=site_folder,
                              source_url=url, title=title)
                    return True
        except Exception as e:
            log(f"  ERROR processing Exploit-DB {url}: {e}")

    # GitHub handler for raw file download
    if is_github:
        try:
            path_parts = [p for p in parsed.path.strip('/').split('/') if p]
            if len(path_parts) >= 4 and path_parts[2] in ('blob', 'raw'):
                owner = path_parts[0]
                repo = path_parts[1]
                branch = path_parts[3]
                file_path_str = '/'.join(path_parts[4:])
                raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path_str}"

                raw_r = fetch(raw_url, session)
                if raw_r and raw_r.status_code == 200:
                    file_ext = Path(file_path_str).suffix.lower()
                    file_path_flat = '_'.join(path_parts[4:])
                    if file_ext in BINARY_DOWNLOAD_EXTS:
                        filename = f"github_{owner}_{repo}_{file_path_flat}"
                        save_binary_file(filename, raw_r.content, bucket_for_ext(file_ext),
                                         site_folder=site_folder, source_url=url)
                    else:
                        content_text = raw_r.text
                        title = f"GitHub - {owner}/{repo}: {file_path_str}"
                        clean_doc = f"<!DOCTYPE html>\n<html>\n<head><meta charset='utf-8'><title>{title}</title></head>\n<body>\n"
                        clean_doc += f"<p>Source: <a href='{url}'>{url}</a></p><hr>\n"
                        clean_doc += f"<pre><code>{html.escape(content_text)}</code></pre>\n"
                        clean_doc += "</body>\n</html>"
                        filename = f"github_{owner}_{repo}_{file_path_flat}.html"
                        save_file(filename, clean_doc, bucket='code', site_folder=site_folder,
                                  source_url=url, title=title)
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

    # Determine if this is a binary/data/code/archive/document download by
    # extension OR content-type.
    ct_binary_map = [
        ('application/pdf', '.pdf'),
        ('application/zip', '.zip'),
        ('application/x-zip-compressed', '.zip'),
        ('application/gzip', '.gz'),
        ('application/x-tar', '.tar'),
        ('application/x-7z-compressed', '.7z'),
        ('application/x-rar-compressed', '.rar'),
        ('application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx'),
        ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.xlsx'),
        ('application/vnd.openxmlformats-officedocument.presentationml.presentation', '.pptx'),
        ('application/msword', '.doc'),
        ('application/vnd.ms-excel', '.xls'),
        ('application/vnd.ms-powerpoint', '.ppt'),
        ('application/octet-stream', path_ext if path_ext else '.bin'),
    ]

    resolved_ext = path_ext
    is_binary_download = resolved_ext in BINARY_DOWNLOAD_EXTS
    if not is_binary_download:
        for ct_frag, ext_guess in ct_binary_map:
            if ct_frag in ct:
                resolved_ext = ext_guess
                is_binary_download = ext_guess in BINARY_DOWNLOAD_EXTS or ct_frag not in ('application/octet-stream',)
                break

    if is_binary_download and resolved_ext in (ARCHIVE_EXTS | DOC_EXTS | CODE_EXTS | DATA_EXTS):
        # Apply rich keyword filtering on binary URL
        if not is_cyber_related(url):
            log(f"  SKIP (binary not cyber-related by URL): {url}")
            return False

        try:
            path_part = parsed.path.strip('/').split('/')[-1] or f'document{resolved_ext}'
            clean_fn = urllib.parse.unquote(path_part)
            if not clean_fn.lower().endswith(resolved_ext):
                clean_fn += resolved_ext

            if ('github.com' in parsed.netloc or 'codeload.github.com' in parsed.netloc) and resolved_ext == '.zip':
                parts = parsed.path.strip('/').split('/')
                if len(parts) >= 2:
                    clean_fn = f"{parts[0]}_{parts[1]}{resolved_ext}"

            clean_fn = "".join([c if c.isalnum() or c in "._- " else "_" for c in clean_fn])
            if len(clean_fn) > 200:
                clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + resolved_ext

            # Raw-file integrity check: never save a corrupted/unopenable
            # download (empty response, HTML error page under a .pdf/.docx
            # extension, or bad magic bytes).
            ok, reason = is_valid_binary(resolved_ext, r.content)
            if not ok:
                log(f"  SKIP (unopenable {resolved_ext} file): {url} - {reason}")
                record_failure(url, f"invalid binary content: {reason}")
                return False

            dest_folder = regulatory_site_folder(site_folder, url)
            e_tag = classify_e_tag(url) if is_regulatory_domain(url) else None
            save_binary_file(clean_fn, r.content, bucket_for_ext(resolved_ext),
                             site_folder=dest_folder, source_url=url, content_type=ct,
                             extra=({'e_tag': e_tag} if e_tag else None))
            return True
        except Exception as e:
            log(f"  ERROR saving binary file {url}: {e}")
            record_failure(url, f"save binary: {e}")
            return False

    if 'text/html' not in ct and 'application/xhtml' not in ct:
        # Plain-text/code/data content types served without a matching extension
        text_like_frags = ['application/json', 'application/xml', 'text/xml', 'text/csv',
                            'text/plain', 'application/x-yaml', 'text/x-python', 'application/javascript']
        if any(frag in ct for frag in text_like_frags):
            try:
                # Apply content-level keyword filtering on plain text files
                content_text = r.text
                if not is_cyber_related(url, content_text):
                    log(f"  SKIP (text content not cyber-related): {url}")
                    return False

                domain_part = parsed.netloc.replace('.', '_').replace('www_', '')
                path_part = parsed.path.strip('/').replace('/', '_') or 'index'
                guess_ext = '.json' if 'json' in ct else ('.xml' if 'xml' in ct else ('.csv' if 'csv' in ct else '.txt'))
                filename = f"{prefix or domain_part}_{path_part}{guess_ext}"
                dest_folder = regulatory_site_folder(site_folder, url)
                e_tag = classify_e_tag(url) if is_regulatory_domain(url) else None
                save_binary_file(filename, r.content, bucket_for_ext(guess_ext),
                                 site_folder=dest_folder, source_url=url, content_type=ct,
                                 extra=({'e_tag': e_tag} if e_tag else None))
                return True
            except:
                pass
        return False

    if not SAVE_HTML:
        # HTML saving disabled: don't clean/save the page's own .html file.
        # (Link discovery/crawling happens separately in crawl_links_from_page,
        # so PDFs/docs/etc. linked from this page are still found and saved.)
        log(f"  SKIP (HTML saving disabled): {url}")
        return False

    try:
        clean_content, title, text = clean_html(r.text, url)
        used_render = False
        # Near-empty static extraction is the signature of a JS-rendered page.
        # Re-fetch through the headless browser (if enabled) and extract from
        # the fully rendered DOM instead.
        if len(text) < MIN_CONTENT_CHARS and JS_ENABLED:
            rendered = render_js(url)
            if rendered:
                c2, t2, x2 = clean_html(rendered, url)
                if x2 and len(x2) > len(text):
                    clean_content, title, text = c2, (t2 or title), x2
                    used_render = True

        if len(text) < 100:
            return False

        # Apply rich content-level keyword filtering on HTML pages (using both URL and page body + title)
        if not is_cyber_related(url, text + " " + (title or "")):
            log(f"  SKIP (HTML not cyber-related): {url}")
            return False

        path_part = parsed.path.strip('/').replace('/', '_') or 'index'
        domain_part = parsed.netloc.replace('.', '_').replace('www_', '')
        filename = f"{prefix or domain_part}_{path_part}.html"
        dest_folder = regulatory_site_folder(site_folder, url, title)
        e_tag = classify_e_tag(url, title) if is_regulatory_domain(url) else None

        save_file(filename, clean_content, bucket='html', site_folder=dest_folder,
                  source_url=url, title=title, content_type=ct,
                  extra={'rendered': used_render, 'e_tag': e_tag} if e_tag else {'rendered': used_render})
        return True
    except:
        return False

def _robots_sitemaps(base_url, session):
    """Sitemap URLs declared in robots.txt (the authoritative source)."""
    out = []
    try:
        p = urllib.parse.urlparse(base_url)
        robots = f"{p.scheme}://{p.netloc}/robots.txt"
        r = fetch(robots, session, retries=1)
        if r and r.status_code == 200:
            for line in r.text.splitlines():
                if line.lower().startswith('sitemap:'):
                    out.append(line.split(':', 1)[1].strip())
    except Exception:
        pass
    return out

def get_sitemap_urls(base_url, session, max_urls=None):
    """Discover a site's own URLs from its sitemap(s).

    A sitemap is the site's authoritative, self-declared list of pages, so
    following it fully is both COMPLETE (we don't miss data) and inherently
    ON-SITE (it only lists that site's URLs). We therefore:
      * read robots.txt for declared sitemaps + probe common locations,
      * recurse through EVERY sub-sitemap in an index (not just the first 20),
      * only stop at HARD_URL_CEILING, logging a loud warning if we do.
    """
    ceiling = max_urls if (max_urls and max_urls > 0) else HARD_URL_CEILING
    ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls = []
    seen_maps = set()

    candidates = list(_robots_sitemaps(base_url, session)) + [
        f"{base_url.rstrip('/')}/sitemap.xml",
        f"{base_url.rstrip('/')}/sitemap_index.xml",
        f"{base_url.rstrip('/')}/sitemap-index.xml",
        f"{base_url.rstrip('/')}/post-sitemap.xml",
        f"{base_url.rstrip('/')}/page-sitemap.xml",
    ]
    queue = list(dict.fromkeys(candidates))  # de-dup, preserve order
    truncated = False

    while queue:
        sm_url = queue.pop(0)
        if sm_url in seen_maps:
            continue
        seen_maps.add(sm_url)
        try:
            r = fetch(sm_url, session, retries=1)
            if not r or r.status_code != 200:
                continue
            root = ET.fromstring(r.text)
        except Exception:
            continue

        child_maps = root.findall('.//sm:sitemap/sm:loc', ns)
        if child_maps:
            # Sitemap index -> enqueue ALL children (bounded by seen_maps).
            for c in child_maps:
                if c.text and c.text not in seen_maps:
                    queue.append(c.text.strip())
        for loc in root.findall('.//sm:url/sm:loc', ns):
            if loc.text:
                urls.append(loc.text.strip())
                if len(urls) >= ceiling:
                    truncated = True
                    break
        if truncated:
            break

    if truncated:
        log(f"    [sitemap] WARNING: hit hard ceiling {ceiling} for {base_url} "
            f"- output may be incomplete. Raise SCRAPER_HARD_CEILING to capture all.")
    return urls

def get_rss_urls(base_url, session):
    urls = []
    feed_paths = [
        '/feed', '/feed/', '/rss', '/rss/', '/atom.xml', '/feed.xml',
        '/rss.xml', '/feeds/posts/default', '/blog/feed', '/blog/rss',
        '/index.xml', '/blog/feed/', '/blog.rss'
    ]

    base = base_url.rstrip('/')
    for fp in feed_paths:
        try:
            r = fetch(f"{base}{fp}", session, retries=1)
            if not r or r.status_code != 200:
                continue
            ct = r.headers.get('Content-Type', '')
            if 'xml' not in ct and 'rss' not in ct and 'atom' not in ct and 'text/html' not in ct:
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
        except:
            continue

    return list(set(urls))

def crawl_links_from_page(url, session, same_domain=True, max_depth=MAX_DEPTH,
                          path_scope=False):
    """Breadth-first, in-scope link crawl.

    Scope guarantees (so the crawler never wanders off the target site):
      * same_domain=True  -> only follow links on the SAME host (strict netloc
        match after stripping www).
      * path_scope=True   -> in addition, only follow links UNDER the seed's
        path prefix (e.g. seed .../PyRIT/ stays under /PyRIT/, never the rest
        of the shared host). Use this for docs hosted on big shared domains.
    Bounded by HARD_URL_CEILING (memory guard), which is logged if hit.
    """
    base_parsed = urllib.parse.urlparse(url)
    base_domain = base_parsed.netloc.replace('www.', '')
    # Path prefix = the seed's directory (drop a trailing filename segment).
    base_path = base_parsed.path
    if not base_path.endswith('/'):
        base_path = base_path.rsplit('/', 1)[0] + '/' if '/' in base_path.strip('/') else '/'
    base_path_prefix = base_path if base_path else '/'

    is_github = 'github.com' in base_domain
    github_prefix = ""
    if is_github:
        parts = [p for p in base_parsed.path.strip('/').split('/') if p]
        if len(parts) >= 2:
            github_prefix = f"/{parts[0]}/{parts[1]}"
        elif len(parts) == 1:
            github_prefix = f"/{parts[0]}"
        else:
            github_prefix = "/NON_EXISTENT_PREFIX_TO_SKIP_CRAWL"

    visited = set()
    to_visit = [(url, 0)]
    found_urls = []

    while to_visit and len(found_urls) < EFFECTIVE_URL_CAP:
        current_url, depth = to_visit.pop(0)
        if current_url in visited or depth > max_depth:
            continue
        visited.add(current_url)

        current_ext = Path(urllib.parse.urlparse(current_url).path).suffix.lower()
        if current_ext in BINARY_DOWNLOAD_EXTS:
            found_urls.append(current_url)
            continue

        r = fetch(current_url, session, retries=1)
        if not r or r.status_code != 200:
            continue

        ct = r.headers.get('Content-Type', '').lower()
        if 'application/pdf' in ct or 'zip' in ct or 'wordprocessingml' in ct or 'octet-stream' in ct:
            found_urls.append(current_url)
            continue

        try:
            soup = BeautifulSoup(r.text, 'html.parser')
        except:
            continue

        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith(('#', 'mailto:', 'javascript:', 'tel:')):
                continue
            full = urllib.parse.urljoin(current_url, href)
            parsed = urllib.parse.urlparse(full)
            if parsed.scheme not in ('http', 'https'):
                continue

            path_lower = parsed.path.lower()
            if any(path_lower.endswith(e) for e in SKIP_EXTS):
                continue

            clean_url = urllib.parse.urlunparse(parsed._replace(fragment=''))
            link_domain = parsed.netloc.replace('www.', '')

            if same_domain and link_domain != base_domain:
                continue

            # Path scoping: stay under the seed's directory prefix.
            if path_scope and not parsed.path.startswith(base_path_prefix):
                continue

            if is_github:
                path_parts = [p for p in path_lower.strip('/').split('/') if p]
                if len(path_parts) >= 3 and path_parts[2] in ('issues', 'pull', 'commit', 'commits', 'releases', 'tags', 'actions', 'projects', 'wiki', 'settings', 'branches', 'pulse', 'graphs', 'network', 'stargazers', 'watchers'):
                    continue
                if github_prefix and not parsed.path.startswith(github_prefix):
                    continue

            if clean_url not in visited and clean_url not in found_urls:
                found_urls.append(clean_url)
                link_ext = Path(parsed.path).suffix.lower()
                if depth + 1 <= max_depth and link_ext not in BINARY_DOWNLOAD_EXTS:
                    to_visit.append((clean_url, depth + 1))

    if len(found_urls) >= EFFECTIVE_URL_CAP and to_visit:
        log(f"    [crawl] WARNING: hit ceiling {EFFECTIVE_URL_CAP} for {url} "
            f"with links still queued - output may be incomplete. "
            f"Raise SCRAPER_MAX_URLS / SCRAPER_HARD_CEILING to capture all.")
    return list(set(found_urls))

SITE_SCRAPERS = {}

def site_scraper(domains):
    def decorator(func):
        for d in domains:
            SITE_SCRAPERS[d] = func
        return func
    return decorator

@site_scraper(['go.dev'])
def scrape_go_dev(session):
    log("  Scraping go.dev security pages...")
    urls = [
        'https://go.dev/blog/security-best-practices',
        'https://go.dev/blog/vuln',
        'https://go.dev/blog/govulncheck',
        'https://go.dev/blog/fuzz-beta',
        'https://go.dev/doc/security/vulncheck',
        'https://go.dev/doc/security/vuln/database',
        'https://go.dev/doc/security/policy',
        'https://go.dev/doc/fuzz',
        'https://go.dev/security/vuln/',
        'https://go.dev/security/vuln/database',
        'https://go.dev/security/policy',
        'https://go.dev/blog/security-for-go-developers',
    ]
    r = fetch('https://go.dev/blog/all', session)
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = (a.text + ' ' + href).lower()
            if any(kw in text for kw in ['security', 'vuln', 'fuzz', 'cve', 'crypt', 'hash', 'safe']):
                full = urllib.parse.urljoin('https://go.dev', href)
                urls.append(full)

    urls = list(set(urls))
    count = 0
    with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'go_dev') for u in urls]
        for f in as_completed(futures):
            try:
                if f.result():
                    count += 1
            except Exception as e:
                log(f"  ERROR in future: {e}")
    return count

@site_scraper(['vuln.go.dev'])
def scrape_vuln_go_dev(session):
    log("  Scraping vuln.go.dev...")
    urls = ['https://vuln.go.dev/']
    r = fetch('https://vuln.go.dev/', session)
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/ID/' in href or '/GO-' in href:
                urls.append(urllib.parse.urljoin('https://vuln.go.dev', href))

    urls = list(set(urls))[:EFFECTIVE_URL_CAP]
    count = 0
    with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'go_vuln') for u in urls]
        for f in as_completed(futures):
            try:
                if f.result():
                    count += 1
            except Exception as e:
                log(f"  ERROR in future: {e}")
    return count

@site_scraper(['attack.mitre.org'])
def scrape_mitre(session):
    log("  Scraping MITRE ATT&CK...")
    urls = [
        'https://attack.mitre.org/',
        'https://attack.mitre.org/techniques/enterprise/',
        'https://attack.mitre.org/tactics/enterprise/',
        'https://attack.mitre.org/mitigations/enterprise/',
        'https://attack.mitre.org/groups/',
        'https://attack.mitre.org/software/',
    ]

    for index_url in ['https://attack.mitre.org/techniques/enterprise/']:
        r = fetch(index_url, session)
        if r and r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href']
                if '/techniques/T' in href:
                    urls.append(urllib.parse.urljoin('https://attack.mitre.org', href))

    urls = list(set(urls))[:EFFECTIVE_URL_CAP]
    count = 0
    with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'mitre_attack') for u in urls]
        for f in as_completed(futures):
            try:
                if f.result():
                    count += 1
            except Exception as e:
                log(f"  ERROR in future: {e}")
    return count

@site_scraper(['cheatsheetseries.owasp.org', 'owasp.org'])
def scrape_owasp(session):
    log("  Scraping OWASP...")
    urls = [
        'https://cheatsheetseries.owasp.org/',
        'https://cheatsheetseries.owasp.org/cheatsheets/Go_Secure_Coding_Cheat_Sheet.html',
        'https://owasp.org/www-project-web-security-testing-guide/',
        'https://owasp.org/Top10/',
        'https://owasp.org/www-project-mobile-top-10/',
        'https://owasp.org/www-project-api-security/',
    ]

    r = fetch('https://cheatsheetseries.owasp.org/', session)
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/cheatsheets/' in href and href.endswith('.html'):
                urls.append(urllib.parse.urljoin('https://cheatsheetseries.owasp.org', href))

    r = fetch('https://owasp.org/projects/', session)
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/www-project-' in href:
                urls.append(urllib.parse.urljoin('https://owasp.org', href))

    urls = list(set(urls))[:EFFECTIVE_URL_CAP]
    count = 0
    with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'owasp') for u in urls]
        for f in as_completed(futures):
            try:
                if f.result():
                    count += 1
            except Exception as e:
                log(f"  ERROR in future: {e}")
    return count

@site_scraper(['nvd.nist.gov'])
def scrape_nist(session):
    log("  Scraping NIST NVD...")
    urls = [
        'https://nvd.nist.gov/',
        'https://nvd.nist.gov/vuln',
        'https://nvd.nist.gov/General/FAQ',
        'https://nvd.nist.gov/General/News',
        'https://nvd.nist.gov/vuln/categories',
        'https://nvd.nist.gov/vuln/search',
    ]
    count = 0
    with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'nist_nvd') for u in urls]
        for f in as_completed(futures):
            try:
                if f.result():
                    count += 1
            except Exception as e:
                log(f"  ERROR in future: {e}")
    return count

@site_scraper(['exploit-db.com', 'www.exploit-db.com'])
def scrape_exploit_db_index(session):
    log("  Scraping Exploit-DB database index...")
    urls = get_rss_urls('https://www.exploit-db.com/', session)
    if not urls:
        r = fetch('https://www.exploit-db.com/', session)
        if r and r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href']
                if '/exploits/' in href:
                    urls.append(urllib.parse.urljoin('https://www.exploit-db.com/', href))

    urls = list(set(urls))[:EFFECTIVE_URL_CAP]
    count = 0
    with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
        futures = [ex.submit(scrape_and_save, u, session, 'exploit_db') for u in urls]
        for f in as_completed(futures):
            try:
                if f.result():
                    count += 1
            except Exception as e:
                log(f"  ERROR in future: {e}")
    return count

def _in_scope(u, base_netloc, base_path_prefix, path_scope):
    """True if URL u belongs to the target site (and section, if path_scope)."""
    try:
        p = urllib.parse.urlparse(u)
    except Exception:
        return False
    if p.scheme not in ('http', 'https'):
        return False
    if p.netloc.replace('www.', '') != base_netloc:
        return False
    if path_scope and not p.path.startswith(base_path_prefix):
        return False
    return True

def generic_blog_scraper(base_url, name, session, prefix="", path_scope=False):
    """Discover and scrape a whole site (or one section, if path_scope=True).

    Completeness vs. staying on-site is handled explicitly:
      * discovery prefers the sitemap (authoritative + on-site + complete),
      * every discovered URL is filtered through _in_scope so we never leave
        the target host (or, with path_scope, the target section),
      * we LOG discovered-vs-scraped counts and warn loudly on any truncation,
        so missing data is never silent.
    """
    log(f"  Generic scraping: {name} ({base_url}) [path_scope={path_scope}]")
    bp = urllib.parse.urlparse(base_url)
    base_netloc = bp.netloc.replace('www.', '')
    base_path_prefix = bp.path if bp.path.endswith('/') else (
        (bp.path.rsplit('/', 1)[0] + '/') if '/' in bp.path.strip('/') else '/')

    all_urls = set()

    rss_urls = get_rss_urls(base_url, session)
    all_urls.update(rss_urls)
    log(f"    RSS: found {len(rss_urls)} URLs")

    sitemap_urls = get_sitemap_urls(base_url, session)
    all_urls.update(sitemap_urls)
    log(f"    Sitemap: found {len(sitemap_urls)} URLs")

    # Crawl fallback when feeds/sitemaps are thin. Pass scope flags through so
    # the crawl itself never leaves the target site/section.
    if len(all_urls) < 50:
        crawled = crawl_links_from_page(base_url, session, same_domain=True,
                                        max_depth=MAX_DEPTH, path_scope=path_scope)
        all_urls.update(crawled)
        log(f"    Crawl: found {len(crawled)} URLs")

    all_urls.add(base_url)

    # Enforce scope on the merged set (sitemaps can list cross-section URLs).
    discovered = len(all_urls)
    in_scope = [u for u in all_urls if _in_scope(u, base_netloc, base_path_prefix, path_scope)]
    dropped = discovered - len(in_scope)
    if dropped > 0:
        log(f"    Scope: kept {len(in_scope)}/{discovered} on-site "
            f"({dropped} off-site/off-section URLs dropped)")

    domain = urllib.parse.urlparse(base_url).netloc.replace('.', '_').replace('www_', '')
    pfx = prefix or domain

    if len(in_scope) > EFFECTIVE_URL_CAP:
        log(f"    WARNING: {len(in_scope)} in-scope URLs exceeds cap {EFFECTIVE_URL_CAP} "
            f"for {name} - TRUNCATING. Set SCRAPER_MAX_URLS=0 (unlimited) or raise "
            f"SCRAPER_HARD_CEILING to capture everything.")
    url_list = in_scope[:EFFECTIVE_URL_CAP]
    log(f"    Scraping {len(url_list)} in-scope URLs for {name}...")

    count = 0
    with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
        futures = [ex.submit(scrape_and_save, u, session, pfx) for u in url_list]
        for f in as_completed(futures):
            try:
                if f.result():
                    count += 1
            except Exception as e:
                log(f"  ERROR in future: {e}")
    return count

def id_sweep_scraper(name, url_template, start_id, end_id, session, prefix=""):
    """For pages that are just '?Id=NNNN' (or similarly-named) query params
    on a fixed script (RBI Notifications/Speeches/Press Releases etc.),
    discovering every ID by following links is unreliable - older years can
    sit behind dropdown/postback pagination a static crawler can't click.
    Since the IDs themselves are sequential and predictable, we generate
    and fetch the whole range directly instead of discovering it. Invalid/
    retired IDs just come back empty or non-200 and get skipped for free -
    scrape_and_save() already handles that.
    """
    log(f"  ID sweep: {name} -> {url_template} for Id={start_id}..{end_id} "
        f"({end_id - start_id + 1} candidate URLs)")
    pfx = prefix or name
    url_list = [url_template.format(id=i) for i in range(start_id, end_id + 1)]

    count = 0
    with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
        futures = [ex.submit(scrape_and_save, u, session, pfx) for u in url_list]
        for f in as_completed(futures):
            try:
                if f.result():
                    count += 1
            except Exception as e:
                log(f"  ERROR in future (id sweep {name}): {e}")
    log(f"  ID sweep {name} complete -> {count} saved out of {len(url_list)} candidates")
    return count

# ---------------------------------------------------------------------
# ID-sweep targets: RBI pages that are just a fixed script + sequential
# ?Id=/?prid= query param. Ranges below are generous upper bounds set
# above the highest known-live ID as of mid-2026 (RBI never reuses IDs,
# it only keeps incrementing) - invalid IDs in the range are cheap to
# skip, so overshooting the end is safe; undershooting would silently
# miss newer documents, so err high. Override per-target via env vars
# if RBI's numbering has moved on by the time you run this.
# ---------------------------------------------------------------------
ID_SWEEP_TARGETS = [
    {
        "name": "RBI-Notifications-Sweep",
        "url_template": "https://www.rbi.org.in/Scripts/NotificationUser.aspx?Id={id}&Mode=0",
        "start": _env_int("SCRAPER_RBI_NOTIF_START", 1),
        "end":   _env_int("SCRAPER_RBI_NOTIF_END", 14000),
    },
    {
        "name": "RBI-PressReleases-Sweep",
        "url_template": "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx?prid={id}",
        "start": _env_int("SCRAPER_RBI_PRID_START", 1),
        "end":   _env_int("SCRAPER_RBI_PRID_END", 65000),
    },
    {
        "name": "RBI-Speeches-Sweep",
        "url_template": "https://www.rbi.org.in/Scripts/BS_SpeechesView.aspx?Id={id}",
        "start": _env_int("SCRAPER_RBI_SPEECH_START", 1),
        "end":   _env_int("SCRAPER_RBI_SPEECH_END", 1600),
    },
]


URL_REGEX = re.compile(r'https?://[^\s<>"\')\]]+')

def _extract_docx_urls_via_python_docx(path):
    urls = set()
    try:
        import docx
        doc = docx.Document(str(path))

        # Paragraph text (regex any bare URLs typed as plain text)
        for para in doc.paragraphs:
            for m in URL_REGEX.finditer(para.text):
                urls.add(m.group(0))

        # Table cell text
        for table in doc.tables:
            for row in table.rows:
                for cell in row.cells:
                    for m in URL_REGEX.finditer(cell.text):
                        urls.add(m.group(0))

        # Actual hyperlink relationships (covers "click here"-style links
        # where the visible text isn't the URL itself)
        try:
            rels = doc.part.rels
            for rel in rels.values():
                if "hyperlink" in rel.reltype and rel.target_ref.startswith('http'):
                    urls.add(rel.target_ref)
        except Exception:
            pass

        return urls
    except Exception as e:
        log(f"  python-docx extraction failed: {e}")
        return urls

def _extract_docx_urls_via_zip(path):
    """Fallback raw-XML scan in case python-docx isn't available/fails."""
    urls = set()
    try:
        with zipfile.ZipFile(path) as z:
            for name in z.namelist():
                if name.endswith('.xml'):
                    try:
                        raw = z.read(name).decode('utf-8', errors='ignore')
                    except Exception:
                        continue
                    for m in URL_REGEX.finditer(raw):
                        urls.add(m.group(0).rstrip('"<'))
    except Exception as e:
        log(f"  zip-based docx extraction failed: {e}")
    return urls

def build_regulatory_sites():
    """Turn REGULATORY_SEED_SITES into the same {site_key: info} shape that
    extract_unique_sites() returns, so both feed the same process_site() /
    generic_blog_scraper() pipeline in main()."""
    sites = {}
    for entry in REGULATORY_SEED_SITES:
        url = entry["url"].rstrip("/")
        parsed = urllib.parse.urlparse(url)
        domain = parsed.netloc.replace("www.", "").lower()
        site_key = url.lower()
        sites[site_key] = {
            "name": entry["name"],
            "description": "",
            "url": url,
            "domain": domain,
        }
    return sites

def extract_unique_sites():
    """Parse Urls.docx and return a dict of unique site targets."""
    sites = {}

    if not URLS_DOCX_PATH.exists():
        log(f"ERROR: Urls.docx not found at {URLS_DOCX_PATH}")
        return sites

    log(f"Parsing URL list from: {URLS_DOCX_PATH.name}")

    raw_urls = _extract_docx_urls_via_python_docx(URLS_DOCX_PATH)
    if not raw_urls:
        raw_urls = _extract_docx_urls_via_zip(URLS_DOCX_PATH)

    log(f"  Found {len(raw_urls)} raw URLs in document")

    for raw_url in raw_urls:
        url = raw_url.strip().rstrip('.,;)')
        if not url.startswith('http'):
            continue

        parsed = urllib.parse.urlparse(url)
        if not parsed.netloc or parsed.netloc.startswith('.'):
            continue

        domain = parsed.netloc.replace('www.', '').lower()
        if any(d in domain for d in ['twitter.com', 'x.com', 'youtube.com', 'reddit.com', 'linkedin.com', 'facebook.com', 'instagram.com']):
            continue

        path = parsed.path
        path = re.sub(r'/page/\d+$', '', path)
        path = re.sub(r'/node-\d+$', '', path)

        base_url = f"{parsed.scheme or 'https'}://{parsed.netloc}{path.rstrip('/') or '/'}"

        site_key = base_url.lower() if 'github.com' in domain else domain
        if site_key not in sites:
            sites[site_key] = {
                'name': domain,
                'description': '',
                'url': base_url,
                'domain': domain,
            }

    return sites

# =====================================================================
# v7 GAP-FILL SCRAPERS
# =====================================================================

# ---- GitHub API helpers ---------------------------------------------
def _github_headers():
    h = {
        "Accept": "application/vnd.github+json",
        "User-Agent": HEADERS['User-Agent'],
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

def _github_get(url, session, params=None, stream=False, retries=4):
    """GET against the GitHub API with primary/secondary rate-limit handling.
    Honors the SHARED pause gate so the whole worker pool backs off together
    when any thread hits the limit."""
    for attempt in range(retries):
        _gh_wait_if_paused()  # respect a limit already hit by another worker
        try:
            r = session.get(url, headers=_github_headers(), params=params,
                            timeout=REQUEST_TIMEOUT, stream=stream, allow_redirects=True)
        except Exception:
            time.sleep(min((2 ** attempt) + random.uniform(0, 1), 30))
            continue
        # Primary rate limit exhausted -> publish a shared pause until reset.
        if r.status_code == 403 and r.headers.get("X-RateLimit-Remaining") == "0":
            reset = r.headers.get("X-RateLimit-Reset")
            try:
                reset_ts = int(reset)
                wait = max(0, reset_ts - int(time.time())) + 2
            except (TypeError, ValueError):
                reset_ts = time.time() + 60
                wait = 60
            log(f"  [github] rate limit hit; pausing all workers ~{min(wait,900):.0f}s")
            _gh_set_pause(min(reset_ts + 2, time.time() + 900))
            _gh_wait_if_paused()
            continue
        # Secondary rate limit / transient server errors.
        if r.status_code in (429, 502, 503, 504):
            ra = r.headers.get("Retry-After")
            wait = float(ra) if (ra and ra.isdigit()) else (2 ** attempt) + random.uniform(0, 1)
            time.sleep(min(wait, 60))
            continue
        return r
    return None

GITHUB_REPO_KEEP_EXTS = (
    CODE_EXTS | DATA_EXTS | DOC_EXTS |
    {'.rst', '.adoc', '.markdown', '.sol', '.vy', '.cairo', '.move', '.circom'}
)
# NOTE: do NOT skip dist/build/out here - MITRE ATLAS data (dist/ATLAS.yaml)
# and ATT&CK STIX bundles live under dist/. Skip only genuine junk dirs.
_GH_SKIP_DIRS = {'node_modules', '.git', '__pycache__', '.vscode', '.idea', 'coverage'}
# Doc-text extensions that should land in the 'data' bucket, not 'html'.
_GH_DOCTEXT_EXTS = {'.rst', '.adoc', '.markdown'}

def _safe_tar_members(tar, dest):
    dest = os.path.realpath(dest)
    for m in tar.getmembers():
        mp = os.path.realpath(os.path.join(dest, m.name))
        if (mp == dest or mp.startswith(dest + os.sep)) and m.isreg():
            yield m

# ---------------------------------------------------------------------
# Per-repo progress. Separate from manifest.jsonl (which only ever records
# a file AFTER it's saved, one line per file - useless for "what repo did
# we stop in the middle of"). This tracks the REPO itself:
#   - repo_progress.jsonl (DEST_ROOT-level): one line per start/progress/
#     done event - "how many files, how many done" at a glance, per repo.
#   - <repo folder>/_repo_state/summary.json + done_files.txt: lives INSIDE
#     that repo's own output folder, so a resumed run can (a) skip a repo
#     entirely if it's already fully done - no re-download, no re-extract -
#     and (b) for a repo that only got partway through, skip re-saving the
#     files it already finished instead of re-writing everything.
# ---------------------------------------------------------------------
repo_progress_lock = threading.Lock()

def _log_repo_progress(event, owner, repo, **fields):
    if not REPO_PROGRESS_FILE:
        return
    rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'event': event,
           'owner': owner, 'repo': repo}
    rec.update(fields)
    try:
        with repo_progress_lock:
            with open(REPO_PROGRESS_FILE, 'a', encoding='utf-8') as f:
                f.write(json.dumps(rec, ensure_ascii=False) + '\n')
    except Exception:
        pass

def _repo_state_paths(site_folder):
    """State lives under one top-level DEST_ROOT/repo_state/ folder, mirroring
    the same path each repo's actual content is saved under (e.g.
    repo_state/git-stars_languages/shell/torvalds_linux/) - so it's easy to
    scan all repo progress in one place, keyed by the exact repo path, without
    it cluttering the repo's own output folder."""
    state_dir = DEST_ROOT / "repo_state" / site_folder
    return state_dir, state_dir / "summary.json", state_dir / "done_files.txt"

def _repo_state_load(site_folder):
    """Returns (summary_dict_or_None, set_of_already_done_relative_paths)."""
    _, summary_path, done_path = _repo_state_paths(site_folder)
    summary = None
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding='utf-8'))
        except Exception:
            summary = None
    done = set()
    if done_path.exists():
        try:
            with open(done_path, 'r', encoding='utf-8') as f:
                done = set(line.strip() for line in f if line.strip())
        except Exception:
            pass
    return summary, done

def _repo_state_write_summary(site_folder, summary):
    state_dir, summary_path, _ = _repo_state_paths(site_folder)
    state_dir.mkdir(parents=True, exist_ok=True)
    try:
        _atomic_write_bytes(summary_path, json.dumps(summary, ensure_ascii=False).encode('utf-8'))
    except Exception:
        pass

def _repo_state_mark_file_done(site_folder, rel_path):
    """Append-only, like PROGRESS_FILE - only called AFTER save_binary_file
    confirms the write succeeded, so this can never mark a file 'done' that
    isn't actually sitting on disk."""
    _, _, done_path = _repo_state_paths(site_folder)
    try:
        with open(done_path, 'a', encoding='utf-8') as f:
            f.write(rel_path + '\n')
    except Exception:
        pass

def _process_repo_tarfile(tar_path, site_folder, source_base, keep_exts=None,
                          path_include=None, cap_files=None, owner=None, repo=None):
    """Extract a repo tarball and save every useful file. Shared by the GitHub
    and GitLab scrapers. source_base is like https://github.com/owner/repo.
    When owner/repo are given, progress is logged and tracked per-file so a
    resumed run skips files already saved instead of redoing all of them."""
    keep_exts = keep_exts or GITHUB_REPO_KEEP_EXTS
    count = 0
    extract_dir = tempfile.mkdtemp(prefix="rex_")
    _, done_files = _repo_state_load(site_folder) if (owner and repo) else (None, set())
    try:
        with tarfile.open(tar_path, "r:gz") as tar:
            members = list(_safe_tar_members(tar, extract_dir))
            try:
                tar.extractall(extract_dir, members=members, filter='data')
            except TypeError:
                tar.extractall(extract_dir, members=members)

        # Archives wrap everything in a single top-level dir.
        subdirs = [os.path.join(extract_dir, d) for d in os.listdir(extract_dir)]
        subdirs = [d for d in subdirs if os.path.isdir(d)]
        base = subdirs[0] if subdirs else extract_dir

        max_bytes = GITHUB_REPO_MAX_FILE_MB * 1024 * 1024

        # Pass 1: figure out exactly which files pass the filters, so the
        # true total is known and logged BEFORE any saving starts.
        candidates = []
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in _GH_SKIP_DIRS]
            for fn in filenames:
                fp = os.path.join(dirpath, fn)
                rel = os.path.relpath(fp, base).replace(os.sep, '/')
                if path_include and path_include not in rel:
                    continue
                ext = Path(fn).suffix.lower()
                if ext in SKIP_EXTS:
                    continue
                if ext and ext not in keep_exts:
                    continue
                if not ext and fn.lower() not in ('readme', 'license', 'dockerfile', 'makefile', 'changelog'):
                    continue
                try:
                    if os.path.getsize(fp) > max_bytes:
                        continue
                except OSError:
                    continue
                candidates.append((rel, fp, ext))

        total = len(candidates)
        already_done = len(done_files)
        if owner and repo:
            if already_done:
                log(f"    [github-repo] {owner}/{repo}: resuming - "
                    f"{already_done}/{total} files already done")
            _log_repo_progress('repo_start', owner, repo, total_files=total,
                               already_done=already_done)

        # Pass 2: save whatever isn't already done.
        for rel, fp, ext in candidates:
            if rel in done_files:
                continue  # saved on a previous, interrupted pass

            clean_fn = "".join([c if c.isalnum() or c in "._- " else "_"
                                for c in rel.replace('/', '_')])
            if len(clean_fn) > 200:
                tail = Path(clean_fn).suffix or ''
                clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + tail
            if is_already_present(clean_fn):
                if owner and repo:
                    _repo_state_mark_file_done(site_folder, rel)
                continue
            try:
                with open(fp, "rb") as bf:
                    data = bf.read()
            except OSError:
                continue
            if not ext or ext in _GH_DOCTEXT_EXTS:
                bucket = 'data'
            else:
                bucket = bucket_for_ext(ext)
            src = f"{source_base}/blob/-/{rel}" if source_base else None
            if save_binary_file(clean_fn, data, bucket, site_folder=site_folder, source_url=src):
                count += 1
                if owner and repo:
                    # Only recorded as done AFTER save_binary_file confirms the
                    # atomic write succeeded - never for a mid-write crash.
                    _repo_state_mark_file_done(site_folder, rel)
                    done_now = already_done + count
                    if count % 25 == 0 or done_now == total:
                        log(f"    [github-repo] {owner}/{repo}: {done_now}/{total} files done")
                        _log_repo_progress('repo_progress', owner, repo,
                                           done=done_now, total=total)
            if cap_files and count >= cap_files:
                break
    except Exception as e:
        log(f"    [repo-unpack] error for {site_folder}: {e}")
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)

    if owner and repo:
        final_done = already_done + count
        # total_files may be 0 for an interrupted-before-candidates-listed
        # crash - only that combination should never be marked 'done'.
        status = 'done' if (total is not None and final_done >= total and total >= already_done) else 'partial'
        _repo_state_write_summary(site_folder, {
            'owner': owner, 'repo': repo, 'total_files': total,
            'done_files': final_done, 'status': status,
            'updated_ts': time.strftime('%Y-%m-%dT%H:%M:%S'),
        })
        _log_repo_progress('repo_done' if status == 'done' else 'repo_interrupted',
                           owner, repo, done=final_done, total=total)
    return count

def github_repo_scraper(owner, repo, session, ref=None, prefix=None,
                        keep_exts=None, path_include=None, cap_files=None):
    """Download a whole repo as a tarball (one API call), unpack, and save
    every useful file. Far more complete/reliable than crawling GitHub HTML.
    If a previous run already finished this exact repo, the tarball isn't
    re-downloaded at all - see _repo_state_load()."""
    site_folder = sanitize_name(prefix or f"github_{owner}_{repo}")

    prev_summary, _ = _repo_state_load(site_folder)
    if prev_summary and not cap_files:
        if prev_summary.get('status') == 'done':
            n = prev_summary.get('done_files', 0)
            log(f"  [github-repo] {owner}/{repo}: already fully scraped "
                f"({n} files) - skipping, no re-download")
            return 0
        log(f"  [github-repo] {owner}/{repo}: resuming a partial scrape "
            f"({prev_summary.get('done_files', 0)}/{prev_summary.get('total_files', '?')} done)")

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
        record_failure(f"{owner}/{repo}", f"github tarball status {getattr(r,'status_code','n/a')}")
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
                                      cap_files=cap_files, owner=owner, repo=repo)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    log(f"    [github-repo] {owner}/{repo}: saved {count} new files this run")
    return count


def gitlab_repo_scraper(full_path, session, prefix=None, keep_exts=None, path_include=None):
    """Download a whole GitLab project as an archive and save useful files.
    full_path is the project path after gitlab.com (may include nested groups)."""
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
        record_failure(full_path, f"gitlab archive status {getattr(resp,'status_code','n/a')}")
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

def github_org_scraper(org, session, name_filter=None, cap=None,
                       prefix_fn=None, keep_exts=None, path_include=None,
                       repo_workers=None):
    """Enumerate every public repo in an org (paginated) and repo-scrape the
    ones matching name_filter. Used for the full code-423n4 findings sweep."""
    repo_workers = repo_workers or GITHUB_ORG_REPO_WORKERS
    log(f"  [github-org] enumerating {org} ...")
    repos, page = [], 1
    while True:
        r = _github_get(f"{GITHUB_API}/orgs/{org}/repos", session,
                        params={"per_page": 100, "page": page, "type": "public",
                                "sort": "created", "direction": "desc"})
        if r is None or r.status_code != 200:
            r = _github_get(f"{GITHUB_API}/users/{org}/repos", session,
                            params={"per_page": 100, "page": page})
            if r is None or r.status_code != 200:
                break
        try:
            batch = r.json()
        except Exception:
            break
        if not isinstance(batch, list) or not batch:
            break
        for repo in batch:
            name = repo.get("name", "")
            if name_filter and not name_filter(name):
                continue
            repos.append((repo["owner"]["login"], name, repo.get("default_branch")))
        if len(batch) < 100:
            break
        page += 1
        if cap and len(repos) >= cap:
            repos = repos[:cap]
            break
    log(f"  [github-org] {org}: {len(repos)} repos matched")

    total = 0
    def _do(item):
        o, n, br = item
        pfx = prefix_fn(o, n) if prefix_fn else f"github_{o}_{n}"
        return github_repo_scraper(o, n, session, ref=br, prefix=pfx,
                                   keep_exts=keep_exts, path_include=path_include)
    with ThreadPoolExecutor(max_workers=repo_workers) as ex:
        futs = [ex.submit(_do, it) for it in repos]
        for f in as_completed(futs):
            try:
                total += f.result() or 0
            except Exception as e:
                log(f"    [github-org] repo error: {e}")
    return total

# ---- arXiv adversarial-ML paper corpus ------------------------------
def arxiv_scraper(session, queries=None, per_query=None, page_size=100):
    queries = queries or ARXIV_QUERIES
    per_query = per_query or ARXIV_PER_QUERY
    site_folder = "arxiv_adversarial_ml"
    ns = {'atom': 'http://www.w3.org/2005/Atom'}
    total = 0
    log(f"  [arxiv] {len(queries)} queries, up to {per_query} PDFs each")
    for q in queries:
        start = 0
        while start < per_query:
            api = ("http://export.arxiv.org/api/query"
                   f"?search_query={urllib.parse.quote(q)}"
                   f"&start={start}&max_results={min(page_size, per_query - start)}"
                   "&sortBy=submittedDate&sortOrder=descending")
            try:
                r = session.get(api, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            except Exception as e:
                log(f"    [arxiv] request error for '{q}': {e}")
                break
            if r.status_code != 200:
                break
            try:
                root = ET.fromstring(r.text)
            except Exception:
                break
            entries = root.findall('atom:entry', ns)
            if not entries:
                break
            for e in entries:
                aid = (e.findtext('atom:id', default='', namespaces=ns) or '').strip()
                m = re.search(r'arxiv\.org/abs/(.+)$', aid)
                arxiv_id = m.group(1) if m else aid.split('/')[-1]
                if not arxiv_id:
                    continue
                pdf_url = None
                for link in e.findall('atom:link', ns):
                    if link.get('title') == 'pdf' or link.get('type') == 'application/pdf':
                        pdf_url = link.get('href')
                        break
                if not pdf_url:
                    pdf_url = f"http://arxiv.org/pdf/{arxiv_id}"
                clean_fn = f"arxiv_{arxiv_id.replace('/', '_')}.pdf"
                clean_fn = "".join([c if c.isalnum() or c in "._- " else "_" for c in clean_fn])
                if is_already_present(clean_fn):
                    continue
                try:
                    pr = session.get(pdf_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                    if pr.status_code == 200 and pr.content[:4] == b'%PDF':
                        abs_url = f"https://arxiv.org/abs/{arxiv_id}"
                        if save_binary_file(clean_fn, pr.content, 'documents',
                                            site_folder=site_folder, source_url=abs_url,
                                            content_type='application/pdf'):
                            total += 1
                except Exception:
                    pass
                time.sleep(random.uniform(0.3, 0.8))
            start += page_size
            time.sleep(3.0)  # arXiv API politeness (>= 3s between calls)
    log(f"    [arxiv] saved {total} PDFs")
    return total

# ---- Direct standards / spec downloads ------------------------------
def direct_downloads_scraper(session, items=None):
    items = items or DIRECT_DOWNLOADS
    total = 0
    log(f"  [direct] {len(items)} curated documents")
    for url, folder, fname in items:
        try:
            r = fetch_download(url, session)
            if not r or r.status_code != 200:
                log(f"    [direct] failed ({getattr(r,'status_code','n/a')}): {url}")
                record_failure(url, f"direct download status {getattr(r,'status_code','n/a')}")
                continue
            name = fname or (urllib.parse.unquote(
                urllib.parse.urlparse(url).path.strip('/').split('/')[-1]) or 'document.pdf')
            ext = Path(name).suffix.lower() or '.pdf'
            clean_fn = "".join([c if c.isalnum() or c in "._- " else "_" for c in name])
            if is_already_present(clean_fn):
                total += 1
                continue
            if save_binary_file(clean_fn, r.content, bucket_for_ext(ext),
                                site_folder=sanitize_name(folder), source_url=url,
                                content_type=r.headers.get('Content-Type')):
                total += 1
        except Exception as e:
            log(f"    [direct] error {url}: {e}")
    log(f"    [direct] saved {total}")
    return total

# ---- CISA ICS advisories --------------------------------------------
def cisa_ics_scraper(session, max_pages=80):
    log("  [cisa-ics] collecting ICS advisories ...")
    base = "https://www.cisa.gov"
    listing = base + "/news-events/cybersecurity-advisories"
    # advisory_type 95 = ICS Advisory, 96 = ICS Medical Advisory.
    seeds = [
        listing + "?f%5B0%5D=advisory_type%3A95",
        listing + "?f%5B0%5D=advisory_type%3A96",
    ]
    found = set()
    for seed in seeds:
        for page in range(0, max_pages):
            url = f"{seed}&page={page}"
            r = fetch(url, session, retries=2)
            if not r or r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, 'html.parser')
            new_here = 0
            for a in soup.find_all('a', href=True):
                href = a['href']
                if ('/news-events/ics-advisories/' in href or
                        '/news-events/ics-medical-advisories/' in href or
                        '/news-events/alerts/' in href):
                    full = urllib.parse.urljoin(base, href)
                    if full not in found:
                        found.add(full)
                        new_here += 1
            if new_here == 0:
                break
    log(f"  [cisa-ics] {len(found)} advisory URLs")
    count = 0
    with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
        futs = [ex.submit(scrape_and_save, u, session, 'cisa_ics') for u in found]
        for f in as_completed(futs):
            try:
                if f.result():
                    count += 1
            except Exception:
                pass
    return count

# ---- MITRE ATT&CK for ICS (HTML technique pages) --------------------
def scrape_attack_ics(session):
    log("  [attack-ics] MITRE ATT&CK for ICS ...")
    base = "https://attack.mitre.org"
    urls = [
        f"{base}/matrices/ics/",
        f"{base}/tactics/ics/",
        f"{base}/mitigations/ics/",
        f"{base}/datasources/",
        f"{base}/assets/",
    ]
    for idx in [f"{base}/techniques/ics/", f"{base}/matrices/ics/"]:
        r = fetch(idx, session)
        if r and r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href']
                if '/techniques/T' in href:
                    urls.append(urllib.parse.urljoin(base, href))
    urls = list(set(urls))[:EFFECTIVE_URL_CAP]
    count = 0
    with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
        futs = [ex.submit(scrape_and_save, u, session, 'mitre_attack_ics') for u in urls]
        for f in as_completed(futs):
            try:
                if f.result():
                    count += 1
            except Exception:
                pass
    return count

# ---- Docs sites via sitemap/RSS -------------------------------------
def scrape_curated_doc_sites(session):
    total = 0
    for url, pfx in CURATED_DOC_SITES:
        try:
            # path_scope=True: these live on shared hosts (azure.github.io,
            # owasp.org, *.readthedocs.io) so we must stay under the project's
            # own path and not crawl the entire host.
            total += generic_blog_scraper(url, pfx, session, prefix=pfx, path_scope=True) or 0
        except Exception as e:
            log(f"    [docs] {url}: {e}")
    return total

# ---- Code4rena: ALL findings repos ----------------------------------
def scrape_code4rena(session):
    return github_org_scraper(
        "code-423n4", session,
        name_filter=lambda n: n.endswith("-findings"),
        cap=None,  # user requested the entire findings history
        prefix_fn=lambda o, n: f"Code4rena/{n}",
        keep_exts={'.md', '.json', '.txt', '.csv', '.yaml', '.yml', '.sol', '.pdf'},
    )

# =====================================================================
# free-programming-books: reap each LINKED resource, not the repo's own
# markdown pages.
# ---------------------------------------------------------------------
# Repo layout: books/<lang>.md, courses/<lang>.md, casts/<lang>.md
# (screencasts/podcasts - video/audio, SKIPPED entirely), more/... (not
# requested - skipped).
#
# For every link found inside a books/*.md or courses/*.md file:
#   - a direct ebook file (pdf/epub/mobi/...)         -> downloaded as-is
#   - a course link on a video/audio host or file       -> skipped entirely
#   - a course link to a normal page                    -> page fetched once,
#                                                          any PDF links on it
#                                                          are downloaded
#                                                          (no video/audio)
#   - a book link to a normal page (e.g. an online book
#     like openbook.rheinwerk-verlag.de)                -> the landing page is
#                                                          checked for a direct
#                                                          ebook download first
#                                                          (used if found); only
#                                                          books WITHOUT one are
#                                                          crawled, and only 2
#                                                          link-hops deep from
#                                                          the landing page
#                                                          (its own chapters),
#                                                          not the whole site
#
# Disk layout: <folder>/books/<md-file-stem>/<resource-slug>/...
#              <folder>/courses/<md-file-stem>/<resource-slug>/...
# =====================================================================
VIDEO_AUDIO_HOSTS = {
    'youtube.com', 'youtu.be', 'vimeo.com', 'soundcloud.com', 'twitch.tv',
    'dailymotion.com', 'wistia.com', 'ted.com', 'spotify.com', 'podcasts.apple.com',
}
VIDEO_AUDIO_EXTS = {'.mp4', '.mp3', '.m4a', '.mov', '.avi', '.mkv', '.webm',
                     '.wav', '.ogg', '.flac', '.m3u8', '.aac', '.wmv'}
EBOOK_FILE_EXTS = {'.pdf', '.epub', '.mobi', '.azw3', '.azw', '.djvu', '.djv',
                    '.chm', '.zip'}
_MD_LINK_RE = re.compile(r'\[([^\]]*)\]\((https?://[^\s)]+)\)')

def _fpb_repo_md_files(owner, repo, session, top_dirs=('books', 'courses')):
    """List every .md file under the given top-level repo dirs via the git
    trees API (recursive). Anything outside top_dirs - in particular
    casts/ (screencasts/podcasts) and more/ - is never listed."""
    branch = 'main'
    meta = _github_get(f"{GITHUB_API}/repos/{owner}/{repo}", session)
    if meta is not None and meta.status_code == 200:
        try:
            branch = meta.json().get('default_branch') or 'main'
        except Exception:
            pass
    tree_r = _github_get(f"{GITHUB_API}/repos/{owner}/{repo}/git/trees/{branch}",
                         session, params={'recursive': '1'})
    if tree_r is None or tree_r.status_code != 200:
        record_failure(f"{owner}/{repo}", f"fpb tree listing status {getattr(tree_r,'status_code','n/a')}")
        return branch, []
    try:
        tree = tree_r.json().get('tree', [])
    except Exception:
        return branch, []
    out = []
    for entry in tree:
        path = entry.get('path', '')
        if entry.get('type') != 'blob' or not path.endswith('.md'):
            continue
        if path.split('/', 1)[0] not in top_dirs:
            continue
        out.append(path)
    return branch, out

def _fpb_extract_links(md_text, owner, repo):
    """Pull outbound resource links out of a books/courses markdown file,
    dropping badges/self-links that aren't actual resources."""
    seen, links = set(), []
    self_ref = f'github.com/{owner.lower()}/{repo.lower()}'
    skip_hosts = {'shields.io', 'img.shields.io', 'creativecommons.org',
                  'opensource.org', 'ebookfoundation.github.io'}
    for _label, url in _MD_LINK_RE.findall(md_text):
        url = url.rstrip('.,)>')
        if url in seen:
            continue
        host = urllib.parse.urlparse(url).netloc.lower().replace('www.', '')
        if not host or self_ref in url.lower() or host in skip_hosts:
            continue
        seen.add(url)
        links.append(url)
    return links

def _fpb_resource_slug(url):
    p = urllib.parse.urlparse(url)
    host = p.netloc.replace('www.', '')
    segs = [s for s in p.path.strip('/').split('/') if s]
    slug = f"{host}_{segs[-1]}" if segs else host
    slug = sanitize_name(slug)
    if len(slug) > 80:
        slug = slug[:60] + '_' + hashlib.md5(url.encode()).hexdigest()[:8]
    return slug or hashlib.md5(url.encode()).hexdigest()[:12]

def _fpb_download_direct(url, dest_prefix, session):
    """Direct-download a single ebook/pdf file into dest_prefix."""
    r = fetch_download(url, session)
    if not r or r.status_code != 200:
        record_failure(url, f"fpb direct download status {getattr(r,'status_code','n/a')}")
        return False
    up = urllib.parse.urlparse(url)
    ext = Path(up.path).suffix.lower() or '.pdf'
    fname = urllib.parse.unquote(Path(up.path).name) or f"download{ext}"
    site_folder = site_folder_for(dest_prefix, url)
    return save_binary_file(fname, r.content, bucket_for_ext(ext),
                            site_folder=site_folder, source_url=url)

def _fpb_scrape_course_page(url, dest_prefix, session):
    """Courses: never follow video/audio, download PDFs directly, and for a
    normal page fetch it ONCE and grab any PDF links found on it."""
    host = urllib.parse.urlparse(url).netloc.lower().replace('www.', '')
    ext = Path(urllib.parse.urlparse(url).path).suffix.lower()
    if host in VIDEO_AUDIO_HOSTS or ext in VIDEO_AUDIO_EXTS:
        return 0
    if ext == '.pdf':
        return 1 if _fpb_download_direct(url, dest_prefix, session) else 0

    r = fetch(url, session, retries=1)
    if not r or r.status_code != 200 or 'text/html' not in r.headers.get('Content-Type', ''):
        return 0
    got = 0
    try:
        soup = BeautifulSoup(r.text, 'html.parser')
        pdf_links = set()
        for a in soup.find_all('a', href=True):
            href = urllib.parse.urljoin(url, a['href'])
            if href.lower().split('?')[0].endswith('.pdf'):
                pdf_links.add(href)
        for pl in pdf_links:
            if _fpb_download_direct(pl, dest_prefix, session):
                got += 1
    except Exception:
        pass
    return got

def _fpb_find_direct_download(url, session):
    """Check a book's landing page for a direct ebook download link (pdf/
    epub/mobi/azw3/djvu/...). Returns the download URL if one is found."""
    r = fetch(url, session, retries=1)
    if not r or r.status_code != 200 or 'text/html' not in r.headers.get('Content-Type', ''):
        return None
    try:
        soup = BeautifulSoup(r.text, 'html.parser')
    except Exception:
        return None
    for a in soup.find_all('a', href=True):
        href = urllib.parse.urljoin(url, a['href'])
        href_l = href.lower().split('?')[0]
        if any(href_l.endswith(e) for e in EBOOK_FILE_EXTS):
            return href
    for a in soup.find_all('a', href=True):
        href = urllib.parse.urljoin(url, a['href'])
        text = (a.get_text() or '').strip().lower()
        if 'download' in text and any(k in text for k in ('pdf', 'epub', 'mobi', 'ebook', 'book')):
            return href
    return None

def _fpb_crawl_book_depth2(url, dest_prefix, session, depth=2):
    """Crawl a book's own chapter pages up to `depth` link-hops from the
    landing page (same domain + same path prefix only) - deep enough for a
    typical table-of-contents -> chapter book, without an unrestricted
    whole-site crawl. Only used for books that don't have a direct download."""
    try:
        found = crawl_links_from_page(url, session, same_domain=True,
                                      max_depth=depth, path_scope=True)
    except Exception as e:
        record_failure(url, f"fpb book crawl error: {e}")
        found = []
    urls = list(dict.fromkeys(found + [url]))
    count = 0
    with ThreadPoolExecutor(max_workers=min(PER_SOURCE_WORKERS, 10)) as ex:
        futs = [ex.submit(scrape_and_save, u, session, dest_prefix) for u in urls]
        for f in as_completed(futs):
            try:
                if f.result():
                    count += 1
            except Exception:
                pass
    return count

def _fpb_scrape_book_link(url, dest_prefix, session):
    """Books: prefer a direct download whenever one exists (the link itself
    IS the file, or the landing page offers one) - that's the whole book in
    one clean file. Only when no direct download is available does the book
    "require" a crawl, and that crawl goes 2 link-hops deep from the landing
    page (its own chapters), not an unrestricted whole-site crawl."""
    ext = Path(urllib.parse.urlparse(url).path).suffix.lower()
    if ext in EBOOK_FILE_EXTS:
        return 1 if _fpb_download_direct(url, dest_prefix, session) else 0

    direct = _fpb_find_direct_download(url, session)
    if direct:
        if _fpb_download_direct(direct, dest_prefix, session):
            return 1
        # direct link was found but failed to fetch - fall through to crawl.

    return _fpb_crawl_book_depth2(url, dest_prefix, session, depth=2)

def scrape_free_programming_books(owner, repo, session, folder):
    """Reap free-programming-books PROPERLY: read every books/*.md and
    courses/*.md file, then visit EACH linked resource and scrape/download
    the real content behind it - never just the repo's own markdown."""
    branch, md_paths = _fpb_repo_md_files(owner, repo, session, top_dirs=('books', 'courses'))
    if not md_paths:
        log(f"  [fpb] could not list books/courses .md files for {owner}/{repo}")
        return 0
    log(f"  [fpb] {len(md_paths)} md files across books/ + courses/ (casts/ skipped - video/audio)")

    tasks = []  # (category, md_stem, url)
    for path in md_paths:
        category = path.split('/', 1)[0]  # 'books' or 'courses'
        md_stem = Path(path).stem
        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
        r = fetch(raw_url, session, retries=2)
        if not r or r.status_code != 200:
            record_failure(raw_url, f"fpb raw fetch status {getattr(r,'status_code','n/a')}")
            continue
        for link in _fpb_extract_links(r.text, owner, repo):
            tasks.append((category, md_stem, link))

    log(f"  [fpb] {len(tasks)} resource links found - visiting each one...")

    def _do(t):
        category, md_stem, url = t
        slug = _fpb_resource_slug(url)
        dest_prefix = f"{folder}/{category}/{md_stem}/{slug}"
        if category == 'courses':
            return _fpb_scrape_course_page(url, dest_prefix, session)
        return _fpb_scrape_book_link(url, dest_prefix, session)

    total = done = 0
    with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
        futs = {ex.submit(_do, t): t for t in tasks}
        for f in as_completed(futs):
            done += 1
            try:
                total += f.result() or 0
            except Exception as e:
                log(f"  [fpb] {futs[f][2]} error: {e}")
            if done % 100 == 0:
                log(f"  [fpb] progress: {done}/{len(tasks)} resources visited, {total} files saved")
    log(f"  [fpb] done -> {total} files saved from {len(tasks)} resources")
    return total

# =====================================================================
# git-stars.org - scraped directly, nothing substituted in.
# ---------------------------------------------------------------------
# Every page (the languages index, each language's repo list, and any
# internal repo-detail page) is fetched normally first; if the static HTML
# looks thin/JS-gated, it's re-fetched through the render_js() headless
# browser already used elsewhere in this file, which simply loads the page
# and WAITS for it to settle (networkidle) - no CAPTCHA solving, no
# fingerprint spoofing, just patience for a JS-heavy page.
#
# Flow: languages index -> each language link becomes a subfolder -> every
# repo link on that language's page (paginated) -> resolved to its
# github.com owner/repo -> repo content + issues pulled via the GitHub API
# (needed regardless, since that's where the actual file/issue data lives).
#
# Disk layout: git-stars_languages/<language>/<owner>_<repo>/... (code)
#              git-stars_languages/<language>/<owner>_<repo>/issues/issues.md
# =====================================================================
def _git_stars_get_html(url, session, wait_label=""):
    """Fetch a page; if it looks JS-gated/thin, wait for it to render via
    the existing headless-browser pool instead of trusting the static HTML."""
    try:
        r = fetch(url, session, retries=2)
    except Exception:
        r = None
    static_text = ''
    if r and r.status_code == 200 and 'text/html' in r.headers.get('Content-Type', ''):
        static_text = r.text

    if len(static_text) >= MIN_CONTENT_CHARS and '<a ' in static_text.lower():
        return static_text

    if not JS_ENABLED:
        log(f"  [git-stars] {wait_label}: page looks thin and JS rendering is "
            f"off (playwright not installed) - using whatever static HTML came back")
        return static_text

    log(f"  [git-stars] {wait_label}: page looks JS-rendered - waiting for it to load...")
    rendered = render_js(url)
    return rendered or static_text

def _git_stars_discover_languages(url, session):
    """Every language link on the languages index page (cards like 'Python
    #1 - 15,997 repos - 53.5M stars'), following the index's OWN pagination
    if it has any - the first screen isn't necessarily all of them. Logs
    each language's stated repo count so scale is visible up front."""
    seen, links = set(), []
    page_url = url
    for page_num in range(1, GIT_STARS_MAX_PAGES_PER_LANGUAGE + 1):
        html = _git_stars_get_html(page_url, session, f"languages index page {page_num}")
        if not html:
            break
        soup = BeautifulSoup(html, 'html.parser')
        new_this_page = 0
        for a in soup.find_all('a', href=True):
            href = urllib.parse.urljoin(page_url, a['href'])
            if '/en/languages/' not in href or href.rstrip('/') == url.rstrip('/'):
                continue
            if href in seen:
                continue
            seen.add(href)
            links.append(href)
            new_this_page += 1
            card_text = ' '.join(a.get_text(' ', strip=True).split())
            m = re.search(r'([\d,]+)\s*repos', card_text)
            repo_count = m.group(1) if m else '?'
            log(f"  [git-stars] found language: {card_text[:40]!r} -> {repo_count} repos listed")
        nxt = _git_stars_next_page_url(html, page_url)
        if not nxt or nxt == page_url or new_this_page == 0:
            break
        page_url = nxt
    log(f"  [git-stars] {len(links)} language links found")
    return links

def _git_stars_next_page_url(html, current_url):
    """Follow the site's own pagination (rel=next, or a 'Next' link/button),
    if it has any - no scrolling/inference, just what the page provides."""
    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception:
        return None
    nxt = soup.find('a', rel=lambda v: v and 'next' in v)
    if nxt and nxt.get('href'):
        return urllib.parse.urljoin(current_url, nxt['href'])
    for a in soup.find_all('a', href=True):
        text = (a.get_text() or '').strip().lower()
        if text in ('next', 'next »', '»', 'load more'):
            return urllib.parse.urljoin(current_url, a['href'])
    return None

def _git_stars_collect_repo_links(lang_url, session):
    """Every repo link on a language's page, following that page's own
    pagination up to GIT_STARS_MAX_PAGES_PER_LANGUAGE."""
    seen, links = set(), []
    url = lang_url
    for page_num in range(1, GIT_STARS_MAX_PAGES_PER_LANGUAGE + 1):
        html = _git_stars_get_html(url, session, f"page {page_num}")
        if not html:
            break
        soup = BeautifulSoup(html, 'html.parser')
        new_this_page = 0
        for a in soup.find_all('a', href=True):
            href = urllib.parse.urljoin(url, a['href'])
            if href in seen:
                continue
            if 'github.com/' in href or '/repo/' in href or '/repos/' in href:
                seen.add(href)
                links.append(href)
                new_this_page += 1
            if len(links) >= GIT_STARS_MAX_REPOS_PER_LANGUAGE:
                break
        if len(links) >= GIT_STARS_MAX_REPOS_PER_LANGUAGE:
            break
        nxt = _git_stars_next_page_url(html, url)
        if not nxt or nxt == url or new_this_page == 0:
            break
        url = nxt
    return links

def _git_stars_resolve_github_repo(link, session):
    """Return (owner, repo) for a link - directly if it's already a
    github.com URL, otherwise by loading the internal git-stars.org page and
    finding the outbound GitHub link on it."""
    if 'github.com/' in link:
        parts = [x for x in urllib.parse.urlparse(link).path.strip('/').split('/') if x]
        if len(parts) >= 2:
            return parts[0], parts[1].replace('.git', '')
        return None
    html = _git_stars_get_html(link, session, "repo detail page")
    if not html:
        return None
    soup = BeautifulSoup(html, 'html.parser')
    for a in soup.find_all('a', href=True):
        if 'github.com/' in a['href']:
            parts = [x for x in urllib.parse.urlparse(a['href']).path.strip('/').split('/') if x]
            if len(parts) >= 2:
                return parts[0], parts[1].replace('.git', '')
    return None

def _github_fetch_issues(owner, repo, session, max_issues=None):
    """All issues (state=all), pull requests excluded - the /issues endpoint
    returns both, so PRs are filtered out here."""
    max_issues = max_issues or GIT_STARS_MAX_ISSUES_PER_REPO
    issues, page, per_page = [], 1, 100
    while len(issues) < max_issues:
        r = _github_get(f"{GITHUB_API}/repos/{owner}/{repo}/issues", session, params={
            'state': 'all', 'per_page': per_page, 'page': page,
        })
        if r is None or r.status_code != 200:
            break
        try:
            batch = r.json()
        except Exception:
            break
        if not batch:
            break
        issues.extend(i for i in batch if 'pull_request' not in i)
        if len(batch) < per_page:
            break
        page += 1
    return issues[:max_issues]

def _save_repo_issues(owner, repo, issues, dest_prefix, session):
    if not issues:
        return 0
    lines = [f"# Issues - {owner}/{repo}\n"]
    for it in issues:
        labels = ', '.join(l.get('name', '') for l in it.get('labels', []) or [])
        lines.append(f"## #{it.get('number')} {it.get('title', '')}")
        lines.append(f"State: {it.get('state')}  |  Labels: {labels}  |  "
                     f"Comments: {it.get('comments', 0)}")
        lines.append(f"URL: {it.get('html_url', '')}")
        body = (it.get('body') or '').strip()
        if body:
            lines.append("")
            lines.append(body)
        lines.append("\n---\n")
    site_folder = site_folder_for(f"{dest_prefix}/issues", f"https://github.com/{owner}/{repo}")
    ok = save_file("issues.md", "\n".join(lines), 'data', site_folder=site_folder,
                   source_url=f"https://github.com/{owner}/{repo}/issues")
    return 1 if ok else 0

def scrape_git_stars_by_language(languages_url, session, folder):
    """Scrape git-stars.org/en/languages ONLY: its own language links, its
    own repo links (paginated per that page's own pagination), resolved to
    GitHub repos - then repo content + issues pulled for each one."""
    lang_links = _git_stars_discover_languages(languages_url, session)
    if not lang_links:
        log("  [git-stars] no language links found even after waiting for JS "
            "to render - nothing to scrape this run")
        return 0

    if GIT_STARS_LANGUAGE_FILTER:
        before = len(lang_links)
        lang_links = [u for u in lang_links
                     if sanitize_name(u.rstrip('/').split('/')[-1]).lower() in GIT_STARS_LANGUAGE_FILTER]
        log(f"  [git-stars] language filter {GIT_STARS_LANGUAGE_FILTER}: "
            f"{len(lang_links)}/{before} language pages match")
        if not lang_links:
            log("  [git-stars] none of the discovered languages match the filter - nothing to scrape")
            return 0

    total = 0
    for lang_url in lang_links:
        lang_name = sanitize_name(lang_url.rstrip('/').split('/')[-1]) or 'unknown'
        # Every repo pulled for this language comes ONLY from this one
        # language's own page (and its own pagination) - never mixed with
        # links found on any other language's page.
        repo_links = _git_stars_collect_repo_links(lang_url, session)
        log(f"  [git-stars] {lang_name}: {len(repo_links)} repo links found on the site")
        if not repo_links:
            continue

        resolved = []
        for link in repo_links:
            gh = _git_stars_resolve_github_repo(link, session)
            if gh:
                resolved.append(gh)
        resolved = list(dict.fromkeys(resolved))
        log(f"  [git-stars] {lang_name}: {len(resolved)} resolved to GitHub repos")

        def _do(gh):
            owner, repo = gh
            prefix = f"{folder}/{lang_name}/{sanitize_name(owner)}_{sanitize_name(repo)}"
            n = 0
            try:
                n += github_repo_scraper(owner, repo, session, prefix=prefix) or 0
            except Exception as e:
                record_failure(f"{owner}/{repo}", f"git-stars repo error: {e}")
            try:
                issues = _github_fetch_issues(owner, repo, session)
                n += _save_repo_issues(owner, repo, issues, prefix, session)
            except Exception as e:
                record_failure(f"{owner}/{repo}", f"git-stars issues error: {e}")
            return n

        done = 0
        with ThreadPoolExecutor(max_workers=PER_SOURCE_WORKERS) as ex:
            futs = {ex.submit(_do, gh): gh for gh in resolved}
            for f in as_completed(futs):
                done += 1
                try:
                    total += f.result() or 0
                except Exception as e:
                    log(f"  [git-stars] {futs[f]} error: {e}")
                if done % 50 == 0:
                    log(f"  [git-stars] {lang_name}: {done}/{len(resolved)} repos done, {total} files saved so far")
    log(f"  [git-stars] done -> {total} files saved across {len(lang_links)} language pages")
    return total

# =====================================================================
# LARGE EXTERNAL DATASETS - streaming + resumable
# =====================================================================
def _download_resumable(url, out_path, session, max_attempts=12):
    """Stream a (possibly huge) file to disk in 1 MB chunks, resuming with HTTP
    Range if the connection drops. Never loads the file into memory."""
    out_path = str(out_path)
    total = None
    try:
        _hh = dict(HEADERS); _hh['Accept-Encoding'] = 'identity'
        h = session.head(url, headers=_hh, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if h.status_code == 200 and h.headers.get('Content-Length'):
            total = int(h.headers['Content-Length'])
    except Exception:
        pass

    for attempt in range(max_attempts):
        have = os.path.getsize(out_path) if os.path.exists(out_path) else 0
        if total and have >= total:
            return True
        headers = dict(HEADERS)
        # Force identity so the server doesn't gzip the transfer - otherwise a
        # ranged request returns a slice of a gzip stream that can't be decoded,
        # and we want the true raw bytes of these archive files anyway.
        headers['Accept-Encoding'] = 'identity'
        if have > 0:
            headers['Range'] = f'bytes={have}-'
        try:
            r = session.get(url, headers=headers, stream=True,
                            timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if have and r.status_code == 200:
                have = 0            # server ignored Range -> restart file
                mode = 'wb'
            elif r.status_code in (200, 206):
                mode = 'ab' if have else 'wb'
            else:
                time.sleep(min(2 ** attempt, 60))
                continue
            if total is None:
                cr = r.headers.get('Content-Range')
                if cr and '/' in cr:
                    try: total = int(cr.split('/')[-1])
                    except ValueError: pass
                elif r.headers.get('Content-Length') and not have:
                    try: total = int(r.headers['Content-Length'])
                    except ValueError: pass
            downloaded = have
            last = time.time()
            with open(out_path, mode) as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if time.time() - last > 30:
                            pct = f"{100*downloaded/total:.1f}%" if total else "?"
                            log(f"    [large] {os.path.basename(out_path)}: "
                                f"{downloaded/1e9:.2f} GB ({pct})")
                            last = time.time()
            if total is None:
                return True         # can't verify size; assume single clean pass
        except Exception as e:
            log(f"    [large] stream error ({e}); resuming in a moment...")
            time.sleep(min(2 ** attempt, 60))
    have = os.path.getsize(out_path) if os.path.exists(out_path) else 0
    return bool(total and have >= total)

def _download_gdrive(file_id, out_dir):
    """Download a Google Drive file by ID via gdown (handles big-file confirm
    tokens + resume). Returns the downloaded path or None."""
    try:
        import gdown
    except Exception:
        log("    [large] gdown not installed - run: pip install gdown")
        record_failure(f"gdrive:{file_id}", "gdown not installed")
        return None
    os.makedirs(out_dir, exist_ok=True)
    try:
        out = gdown.download(id=file_id, output=out_dir + os.sep, quiet=False, resume=True)
        if not out:
            record_failure(f"gdrive:{file_id}", "gdown returned no file")
        return out
    except Exception as e:
        record_failure(f"gdrive:{file_id}", f"gdown error: {e}")
        return None

def _extract_archive(path, dest_dir):
    """Extract .zip / .tar.gz / .tgz / .tar / .gz to dest_dir. Streams per-member,
    so a 59 GB zip does not go through memory."""
    p = str(path).lower()
    os.makedirs(dest_dir, exist_ok=True)
    try:
        if p.endswith('.zip'):
            with zipfile.ZipFile(path) as z:
                z.extractall(dest_dir)
        elif p.endswith(('.tar.gz', '.tgz', '.tar')):
            with tarfile.open(path) as t:
                try: t.extractall(dest_dir, filter='data')
                except TypeError: t.extractall(dest_dir)
        elif p.endswith('.gz'):
            import gzip
            out = os.path.join(dest_dir, os.path.basename(path)[:-3])
            with gzip.open(path, 'rb') as fi, open(out, 'wb') as fo:
                shutil.copyfileobj(fi, fo, length=1 << 20)
        else:
            log(f"    [large] not an archive, skipping extract: {os.path.basename(path)}")
            return False
        log(f"    [large] extracted -> {dest_dir}")
        return True
    except Exception as e:
        log(f"    [large] extract failed for {os.path.basename(str(path))}: {e}")
        record_failure(str(path), f"extract failed: {e}")
        return False

def scrape_large_external(session):
    if not LARGE_EXTERNAL_DOWNLOADS:
        return 0
    log("=" * 70)
    log("LARGE EXTERNAL DATASETS")
    log("=" * 70)
    saved = 0
    for item in LARGE_EXTERNAL_DOWNLOADS:
        name = item['name']
        folder = DEST_ROOT / item['folder']
        folder.mkdir(parents=True, exist_ok=True)
        log(f"  [large] {name} ({item['kind']}) -> {folder}")
        out_path = None

        if item['kind'] == 'http':
            out_path = folder / item.get('filename', item['url'].split('/')[-1])
            if not _download_resumable(item['url'], out_path, session):
                log(f"    [large] {name}: download incomplete (will resume next run)")
                continue
        elif item['kind'] == 'gdrive':
            got = _download_gdrive(item['gdrive_id'], str(folder))
            if not got:
                log(f"    [large] {name}: Google Drive download failed")
                continue
            out_path = Path(got)
        elif item['kind'] == 'git_archive':
            # Download the WHOLE repo tarball and keep every file (no filter/cap).
            owner, repo = item['owner'], item['repo']
            ref = 'main'
            meta = _github_get(f"{GITHUB_API}/repos/{owner}/{repo}", session)
            if meta is not None and meta.status_code == 200:
                try:
                    ref = meta.json().get('default_branch') or 'main'
                except Exception:
                    pass
            r = _github_get(f"{GITHUB_API}/repos/{owner}/{repo}/tarball/{ref}", session, stream=True)
            if r is None or r.status_code != 200:
                record_failure(f"{owner}/{repo}", f"git_archive status {getattr(r,'status_code','n/a')}")
                log(f"    [large] {name}: repo archive download failed")
                continue
            out_path = folder / f"{repo}.tar.gz"
            with open(out_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)
        elif item['kind'] == 'fpb_repo_links':
            # Don't just pull the repo's own markdown - parse books/*.md and
            # courses/*.md for outbound links and scrape/download the ACTUAL
            # resource behind each one. casts/ (screencasts) is skipped
            # entirely (video/audio); courses/ only keep PDFs.
            n = scrape_free_programming_books(item['owner'], item['repo'], session, item['folder'])
            log(f"    [large] {name}: {n} files")
            saved += 1 if n else 0
            continue
        elif item['kind'] == 'github_repo':
            # Normal repo pull (keeps the useful/index files). For the awesome
            # index this yields the markdown that links out to datasets.
            n = github_repo_scraper(item['owner'], item['repo'], session, prefix=item['folder'])
            log(f"    [large] {name}: {n} files")
            saved += 1 if n else 0
            continue
        elif item['kind'] == 'git_stars_languages':
            n = scrape_git_stars_by_language(item['url'], session, item['folder'])
            log(f"    [large] {name}: {n} files")
            saved += 1 if n else 0
            continue
        else:
            continue

        saved += 1
        try:
            write_manifest({'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'saved': True,
                            'large_dataset': name, 'path': str(out_path),
                            'source_url': item.get('url') or f"gdrive:{item.get('gdrive_id')}",
                            'bytes': os.path.getsize(out_path) if out_path and os.path.exists(out_path) else None})
        except Exception:
            pass

        if item.get('unzip') and out_path and str(out_path).lower().endswith(
                ('.zip', '.tar.gz', '.tgz', '.tar', '.gz')):
            log(f"    [large] extracting {os.path.basename(str(out_path))} (this can take a while)...")
            _extract_archive(out_path, folder / 'extracted')

    log(f"LARGE DATASETS COMPLETE -> {saved} downloaded")
    return saved

# ---- Orchestrator ---------------------------------------------------
def run_curated_sources(session):
    log("=" * 70)
    log("CURATED SOURCES")
    log("=" * 70)
    if not GITHUB_TOKEN:
        log("  WARNING: GITHUB_TOKEN not set - GitHub API is limited to 60 req/hr.")
        log("           This WILL stall the free-programming-books pull. Set the token.")
    grand = 0

    if RUN_LARGE_DOWNLOADS:
        try:
            grand += scrape_large_external(session) or 0
        except Exception as e:
            log(f"  [large] phase error: {e}")
    else:
        log("  Skipping curated sources (--skip-large).")

    log(f"CURATED SOURCES COMPLETE -> {grand} items saved")
    return grand

# ---- Diagnostic (--audit-resume): find "marked visited, nothing saved" gaps
# left by past runs, without changing anything ---------------------------
def audit_resumability():
    log("=" * 70)
    log("AUDIT MODE - checking for the 'marked visited but nothing saved' gap")
    log("=" * 70)

    # 1. Zero-byte files: unambiguous evidence of a write interrupted between
    #    open() (which creates the file) and the content actually landing.
    #    (Runs made AFTER this fix can't produce these any more - save_file/
    #    save_binary_file now write atomically - but past runs still can.)
    zero_byte = []
    if DEST_ROOT.exists():
        for p in DEST_ROOT.rglob('*'):
            if p.is_file() and p.stat().st_size == 0:
                zero_byte.append(p)
    log(f"Zero-byte files on disk: {len(zero_byte)}")
    for p in zero_byte[:25]:
        log(f"  EMPTY: {p}")
    if len(zero_byte) > 25:
        log(f"  ... and {len(zero_byte) - 25} more")

    # 2. Progress-file vs manifest gap. CAVEAT: not every URL in the progress
    #    file has a manifest line by design - deterministic skips (extension
    #    filters, github issues/pulls/etc., already-present files) legitimately
    #    return without writing to the manifest. So this list is a superset of
    #    real "crashed mid-save" cases, not a precise one - treat it as
    #    "worth a second look", not "definitely broken".
    progress_urls = load_saved_urls()
    manifest_urls = set()
    if MANIFEST_FILE and MANIFEST_FILE.exists():
        try:
            with open(MANIFEST_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    su = rec.get('source_url')
                    if su:
                        manifest_urls.add(su)
        except Exception as e:
            log(f"  could not read manifest: {e}")

    gap = progress_urls - manifest_urls
    log(f"URLs marked visited with no matching manifest entry: {len(gap)} "
        f"(of {len(progress_urls)} total visited)")
    log("  NOTE: this includes deliberate skips (filtered extensions, github "
        "issues/pulls, already-downloaded files) as well as genuine gaps - "
        "it's a starting point for review, not a precise failure count.")
    gap_file = DEST_ROOT / "audit_progress_manifest_gap.txt"
    try:
        with open(gap_file, 'w', encoding='utf-8') as f:
            for u in sorted(gap):
                f.write(u + '\n')
        log(f"  full list written to: {gap_file}")
    except Exception as e:
        log(f"  could not write gap file: {e}")

    log("=" * 70)
    log("AUDIT DONE - nothing was changed. To force a retry of specific URLs, "
        "remove them from scraped_urls_new.txt and rerun.")
    log("=" * 70)

# ---- Pre-flight validation (--validate): check sources without scraping ----
def validate_curated_sources(session):
    log("=" * 70)
    log("VALIDATION MODE - checking curated sources (no data scraped)")
    log("=" * 70)

    if not LARGE_EXTERNAL_DOWNLOADS:
        log("  No curated sources configured.")
        log("=" * 70)
        return

    for item in LARGE_EXTERNAL_DOWNLOADS:
        name, kind = item['name'], item['kind']
        owner, repo = item.get('owner'), item.get('repo')
        if kind in ('git_archive', 'fpb_repo_links', 'github_repo'):
            url = f"{GITHUB_API}/repos/{owner}/{repo}"
            try:
                r = _github_get(url, session)
                status = getattr(r, 'status_code', 'ERR')
            except Exception as e:
                status = f"ERR:{e}"
            label = f"{owner}/{repo}"
        else:
            url = item.get('url', '')
            try:
                r = fetch(url, session, retries=1)
                status = getattr(r, 'status_code', 'ERR')
            except Exception as e:
                status = f"ERR:{e}"
            label = url
        mark = 'OK' if status == 200 else 'CHECK'
        log(f"  [{kind:16s}] {mark:6s} {name} ({label}) -> {status}")

    log("=" * 70)
    log("VALIDATION DONE. If it looks right, run without --validate.")
    log("=" * 70)

def configure():
    """Finalize derived paths and caps after CLI/env resolution."""
    global MANIFEST_FILE, FAILURES_FILE, CONTENT_HASHES_FILE, EFFECTIVE_URL_CAP
    global content_hashes, REPO_PROGRESS_FILE
    DEST_ROOT.mkdir(parents=True, exist_ok=True)
    MANIFEST_FILE       = DEST_ROOT / "manifest.jsonl"
    FAILURES_FILE       = DEST_ROOT / "failures.jsonl"
    CONTENT_HASHES_FILE = DEST_ROOT / "content_hashes.txt"
    REPO_PROGRESS_FILE  = DEST_ROOT / "repo_progress.jsonl"
    EFFECTIVE_URL_CAP = MAX_URLS_PER_SOURCE if MAX_URLS_PER_SOURCE > 0 else HARD_URL_CEILING
    content_hashes = load_content_hashes()

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Cyber resource scraper (v7) - docx sites + curated gap-fill sources.")
    p.add_argument('--urls-docx', help="Path to the seed-URL .docx (env SCRAPER_URLS_DOCX)")
    p.add_argument('--dest', help="Output directory (env SCRAPER_DEST)")
    p.add_argument('--prev', help="Previously-scraped dir for skip/dedup (env SCRAPER_PREV)")
    p.add_argument('--workers', type=int, help="Site-level parallelism (env SCRAPER_WORKERS)")
    p.add_argument('--max-urls', type=int,
                   help="Per-source URL cap; 0 = unlimited (env SCRAPER_MAX_URLS)")
    p.add_argument('--validate', action='store_true',
                   help="Pre-flight: check curated sources are reachable, then exit.")
    p.add_argument('--audit-resume', action='store_true',
                   help="Check a previous run's output for the 'marked visited "
                        "but nothing saved' gap (zero-byte files + progress/"
                        "manifest mismatches), then exit. No changes are made.")
    p.add_argument('--skip-docx', action='store_true', help="Skip the docx-driven sites.")
    p.add_argument('--with-docx', action='store_true',
                   help="Re-enable the docx seed-site phase. OFF by default right now "
                        "(no docx file in use) - only the hardcoded regulatory + curated "
                        "sources run unless this is passed.")
    p.add_argument('--skip-curated', action='store_true', help="Skip the curated gap-fill sources.")
    p.add_argument('--curated-only', action='store_true',
                   help="Scrape ONLY the hardcoded curated sources (no docx needed).")
    p.add_argument('--docx-only', action='store_true',
                   help="Scrape ONLY the docx seed sites (skip curated sources).")
    p.add_argument('--no-js', action='store_true',
                   help="Disable the headless-browser fallback for JS-rendered pages.")
    p.add_argument('--no-html', action='store_true',
                   help="Don't save cleaned HTML pages (env SCRAPER_SAVE_HTML=0). "
                        "Pages are still fetched/crawled for links, so PDFs/docs/code/data "
                        "reachable from them are still discovered and saved; only the "
                        "page's own .html file is skipped and no html/ folders are made.")
    p.add_argument('--js-workers', type=int,
                   help="Number of headless browsers for JS rendering (env SCRAPER_JS_WORKERS).")
    p.add_argument('--skip-large', action='store_true',
                   help="Skip the curated sources (free-programming-books, git-stars.org).")
    p.add_argument('--only-large', action='store_true',
                   help="Download ONLY the curated sources, nothing else.")
    p.add_argument('--skip-regulatory', action='store_true',
                   help="Skip the hardcoded regulatory seed sites (RBI/SEBI/IRDAI/"
                        "NCIIPC/CERT-In/NSCS/PowerSector/Railway).")
    p.add_argument('--skip-id-sweep', action='store_true',
                   help="Skip the ID-range sweep for RBI's Id=/prid= script pages "
                        "(NotificationUser.aspx, BS_PressReleaseDisplay.aspx, "
                        "BS_SpeechesView.aspx). On by default - this is what "
                        "guarantees full coverage on pages with no sitemap/RSS "
                        "and possible postback-only pagination for older years.")
    p.add_argument('--respect-robots', action='store_true',
                   help="Respect robots.txt (ignored by default).")
    return p.parse_args(argv)

def apply_args(args):
    global URLS_DOCX_PATH, DEST_ROOT, PART3_DIR, WORKERS, MAX_URLS_PER_SOURCE, RESPECT_ROBOTS
    if getattr(args, 'respect_robots', False):
        RESPECT_ROBOTS = True
    if args.urls_docx:
        URLS_DOCX_PATH = Path(args.urls_docx).expanduser()
    if args.dest:
        DEST_ROOT = Path(args.dest).expanduser()
    if args.prev:
        PART3_DIR = Path(args.prev).expanduser()
    elif not args.dest:
        pass  # PART3_DIR keeps its env/default
    if args.dest and not args.prev:
        PART3_DIR = DEST_ROOT
    if args.workers:
        WORKERS = args.workers
    if args.max_urls is not None:
        MAX_URLS_PER_SOURCE = args.max_urls
    if getattr(args, 'js_workers', None):
        globals()['JS_RENDER_WORKERS'] = args.js_workers
    if getattr(args, 'no_html', False):
        globals()['SAVE_HTML'] = False

def _resolve_phase_flags(args):
    """--curated-only / --docx-only / --only-large are conveniences over the flags."""
    global RUN_LARGE_DOWNLOADS
    # Docx phase is OFF by default right now (no docx file in use) - only
    # --with-docx or --docx-only turns it back on.
    if not (getattr(args, 'with_docx', False) or args.docx_only):
        args.skip_docx = True
    if args.curated_only:
        args.skip_docx = True
    if args.docx_only:
        args.skip_curated = True
    if getattr(args, 'skip_large', False):
        RUN_LARGE_DOWNLOADS = False
    if getattr(args, 'only_large', False):
        # Only the curated sources: skip docx entirely.
        args.skip_docx = True
        args.skip_curated = False
        RUN_LARGE_DOWNLOADS = True
    return args

def main(argv=None):
    global saved_urls, existing_filenames
    args = parse_args(argv)
    apply_args(args)
    _resolve_phase_flags(args)
    configure()

    global JS_ENABLED
    JS_ENABLED = _HAS_PLAYWRIGHT and not args.no_js

    log("=" * 70)
    log("Resource Scraper v7 (docx sites + curated gap-fill)")
    log("=" * 70)
    log(f"  DEST={DEST_ROOT}  PREV={PART3_DIR}")
    log(f"  URL cap: {'UNLIMITED' if MAX_URLS_PER_SOURCE == 0 else MAX_URLS_PER_SOURCE} "
        f"(hard ceiling {HARD_URL_CEILING})")
    log(f"  HTML extractor: {'trafilatura' if _HAS_TRAFILATURA else 'builtin-bs4'}  "
        f"GitHub token: {'yes' if GITHUB_TOKEN else 'NO'}")
    log(f"  Save HTML pages: {'YES' if SAVE_HTML else 'NO (--no-html) - pages still crawled for links, only .html files skipped'}")
    log(f"  Regulatory seed sites: {', '.join(i['name'] for i in REGULATORY_SEED_SITES)}")

    saved_urls = load_saved_urls()
    log(f"Resuming: {len(saved_urls)} URLs already done")

    log(f"Indexing already-scraped files (PART3_DIR + DEST_ROOT)...")
    existing_filenames = build_existing_filenames_index()
    log(f"  Found {len(existing_filenames)} existing files - these will be skipped")

    # The seed-URL .docx is ONLY needed for the docx-driven phase. When that
    # phase is skipped (curated-only run) we never touch it, so no file is
    # required. If it's needed but missing, warn and continue rather than crash.
    sites = {}
    if not args.skip_docx:
        try:
            sites = extract_unique_sites()
            log(f"Extracted {len(sites)} unique targets from {URLS_DOCX_PATH.name}\n")
        except Exception as e:
            log(f"WARNING: could not read seed URLs from {URLS_DOCX_PATH} ({e}).")
            log("         Docx phase will be skipped; continuing with curated sources.")
            sites = {}
        if not sites:
            log(f"NOTE: no seed URLs found (is {URLS_DOCX_PATH} present?). "
                f"Docx phase has nothing to do.")

    # Always merge in the hardcoded regulatory/CII seed sites (RBI, SEBI,
    # IRDAI, NCIIPC, CERT-In, NSCS/PMO, power sector, railway security) -
    # these don't depend on the docx file at all.
    if not getattr(args, 'skip_regulatory', False):
        reg_sites = build_regulatory_sites()
        added = 0
        for k, v in reg_sites.items():
            if k not in sites:
                sites[k] = v
                added += 1
        log(f"Added {added} hardcoded regulatory seed sites "
            f"(RBI/SEBI/IRDAI/NCIIPC/CERT-In/NSCS/PowerSector/Railway/FFIEC)\n")

    session = requests.Session()
    session.verify = False
    session.headers.update(HEADERS)
    # Size the connection pool to match our real thread count, otherwise
    # requests silently queues connections and high WORKERS does nothing.
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=WORKERS * PER_SOURCE_WORKERS,
        pool_maxsize=WORKERS * PER_SOURCE_WORKERS,
        max_retries=0,  # we handle retries ourselves in fetch()
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    def process_site(domain, info):
        try:
            if domain in SITE_SCRAPERS:
                return SITE_SCRAPERS[domain](session)

            for registered_domain, scraper in SITE_SCRAPERS.items():
                if domain.endswith(registered_domain):
                    return scraper(session)

            return generic_blog_scraper(info['url'], info['name'], session)
        except Exception as e:
            log(f"  ERROR on {domain}: {e}")
            return 0

    if args.skip_docx:
        log("Docx phase skipped (--skip-docx / default) - only hardcoded sites in play.")

    if not sites:
        log("No sites to process (docx skipped/empty and no regulatory sites).")
    else:
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {}
            for site_key, info in sites.items():
                f = executor.submit(process_site, info['domain'], info)
                futures[f] = (info['domain'], info['name'])

            for future in as_completed(futures):
                domain, name = futures[future]
                try:
                    count = future.result()
                    log(f"DONE: {name} ({domain}) -> {count} pages saved")
                except Exception as e:
                    log(f"FAIL: {name} ({domain}) -> {e}")

    # ---- ID-range sweep for RBI's Id=/prid= script pages - these are
    # skipped by default when the sweep would just duplicate the same URLs
    # generic_blog_scraper already reached via the -Sweep name suffix
    # existing in `sites`, but since the sweep is address-space-based (not
    # link-discovery-based) it's the only way to guarantee full coverage
    # on a site with no sitemap/RSS and possible postback-only pagination.
    if getattr(args, 'skip_id_sweep', False):
        log("Skipping ID-range sweep (--skip-id-sweep).")
    else:
        for target in ID_SWEEP_TARGETS:
            try:
                count = id_sweep_scraper(
                    target['name'], target['url_template'],
                    target['start'], target['end'], session)
                log(f"DONE: {target['name']} -> {count} pages saved")
            except Exception as e:
                log(f"FAIL: {target['name']} -> {e}")

    # ---- v7: curated gap-fill sources (AI/ML, OT/ICS, blockchain, IoT) ----
    if args.skip_curated:
        log("Skipping curated gap-fill sources (--skip-curated).")
    else:
        try:
            run_curated_sources(session)
        except Exception as e:
            log(f"Curated sources phase error: {e}")

    elapsed = time.time() - start_time
    log("\n" + "=" * 70)
    log("SCRAPING COMPLETE")
    log("=" * 70)
    log(f"Time: {elapsed/60:.1f} minutes")
    log(f"Total items saved: {total_saved}")
    log(f"Unique URLs: {len(saved_urls)}")
    log(f"Content hashes tracked: {len(content_hashes)}")
    log("\nFiles per extension:")
    for cat, count in sorted(stats.items(), key=lambda x: -x[1]):
        log(f"  {cat}: {count}")
    log(f"\nProvenance manifest: {MANIFEST_FILE}")
    log(f"Failure log:         {FAILURES_FILE}")
    log("=" * 70)

if __name__ == "__main__":
    main()
