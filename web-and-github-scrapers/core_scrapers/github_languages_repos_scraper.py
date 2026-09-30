# -*- coding: utf-8 -*-
"""
GitHub repo scraper - xlsx-driven, no hardcoded sources.

Reads a spreadsheet (default: shell_repositories.xlsx) and downloads a full
tarball of every GitHub repo it lists (code, docs, READMEs, data files -
whatever passes GITHUB_REPO_KEEP_EXTS). Nothing else is scraped: no docx
seed sites, no curated doc sites, no arXiv/CISA/git-stars/free-programming-
books gap-fill sources. Those phases from the old script are gone, not just
disabled.

Repo list resolution, per row:
  1. Use the "Full Name" column if present (e.g. "owner/repo").
  2. Otherwise parse "GitHub URL" (e.g. "https://github.com/owner/repo").
Rows that yield neither are skipped and logged.

Usage:
    python scrape_github_repos_from_xlsx.py --shell --xlsx shell_repositories.xlsx --dest ./Gap_Data
    python scrape_github_repos_from_xlsx.py --python --xlsx shell_repositories.xlsx --dest ./Gap_Data
    python scrape_github_repos_from_xlsx.py --sheet PowerShell --xlsx shell_repositories.xlsx
    GITHUB_TOKEN=ghp_xxx python scrape_github_repos_from_xlsx.py --shell   # strongly recommended

Language flags:
    Each key in LANGUAGE_SHEETS (near the top of this file) becomes its own
    "--<language>" flag that picks that language's worksheet, e.g. --shell,
    --python, --powershell. To support a new language, add one entry to
    LANGUAGE_SHEETS - no other code needs to change. --sheet <name> still
    works directly for anything not (yet) in that dict.

Env vars (all optional):
    SCRAPER_XLSX          Path to the input spreadsheet.
    SCRAPER_DEST          Output directory.
    SCRAPER_REPO_WORKERS  Concurrent repo downloads (default 180). This is
                           I/O-bound work (network waits, gzip decompression,
                           hashing all release Python's GIL), so thread count
                           can safely exceed CPU core count - it does not need
                           to equal your core count 1:1. On a big box, raise
                           this to use more concurrency; on a small/shared
                           box or flaky network, lower it. Also raise
                           `ulimit -n` (open file descriptor limit) before
                           running with a high worker count, e.g.:
                               ulimit -n 65536
                           GitHub may also apply per-IP secondary/abuse rate
                           limiting independent of your token's quota if you
                           push concurrency very high - if you see repeated
                           403s or connection resets under load, dial back
                           SCRAPER_REPO_WORKERS.
    GITHUB_TOKEN          GitHub API token. Unauthenticated = 60 req/hr,
                           authenticated = 5000 req/hr. With thousands of
                           repos, set this or you will stall on rate limits.
"""
import os
import sys
import re
import json
import time
import random
import hashlib
import threading
import urllib.parse
import tarfile
import tempfile
import shutil
import argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import requests

try:
    from openpyxl import load_workbook
except ImportError:
    sys.exit("Missing dependency: pip install openpyxl")

try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

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


# ---------------------------------------------------------------------
# Configuration - everything comes from the xlsx + env/CLI, nothing else.
# ---------------------------------------------------------------------
XLSX_PATH  = _env_path("SCRAPER_XLSX", SCRIPT_DIR / "shell_repositories.xlsx")
DEST_ROOT  = _env_path("SCRAPER_DEST", SCRIPT_DIR / "Gap_Data")

# ---------------------------------------------------------------------
# Language -> worksheet-name map. This is the ONLY place you touch to add
# a new per-language CLI flag (e.g. --shell, --python, --powershell).
# Each key here automatically becomes:
#   - a "--<key>" boolean flag (e.g. "python" -> --python)
#   - resolved to the given sheet name (matched case-insensitively
#     against the actual worksheet titles in the xlsx, so exact casing
#     here doesn't matter)
# To add a new language, just add one line, e.g. "javascript": "javascript".
# Nothing else in the script needs to change.
# ---------------------------------------------------------------------
LANGUAGE_SHEETS = {
    'python':       'Python',
    'javascript':   'JavaScript',
    'typescript':   'TypeScript',
    'go':           'Go',
    'cpp':          'C++',
    'java':         'Java',
    'rust':         'Rust',
    'c':            'C',
    'csharp':       'C#',
    'shell':        'Shell',
    'php':          'PHP',
    'html':         'HTML',
    'jupyter':      'Jupyter Notebook',
    'swift':        'Swift',
    'kotlin':       'Kotlin',
    'ruby':         'Ruby',
    'dart':         'Dart',
    'vue':          'Vue',
    'lua':          'Lua',
    'css':          'CSS',
    'objectivec':   'Objective-C',
    'powershell':   'PowerShell',
    'scala':        'Scala',
    'vimscript':    'Vim Script',
    'emacslisp':    'Emacs Lisp',
    'elixir':       'Elixir',
    'r':            'R',
    'tex':          'TeX',
    'clojure':      'Clojure',
    'scss':         'SCSS',
    'dockerfile':   'Dockerfile',
    'haskell':      'Haskell',
    'makefile':     'Makefile',
    'gdscript':     'GDScript',
    'nix':          'Nix',
    'perl':         'Perl',
    'svelte':       'Svelte',
    'julia':        'Julia',
    'mdx':          'MDX',
    'assembly':     'Assembly',
    'hcl':          'HCL',
    'zig':          'Zig',
    'solidity':     'Solidity',
    'ocaml':        'OCaml',
    'batchfile':    'Batchfile',
    'astro':        'Astro',
    'markdown':     'Markdown',
    'cuda':         'Cuda',
    'pascal':       'Pascal',
    'erlang':       'Erlang',
}
DEFAULT_SHEET = 'Python'

LOG_FILE = None            # finalized in configure()
MANIFEST_FILE = None
FAILURES_FILE = None
CONTENT_HASHES_FILE = None
REPO_PROGRESS_FILE = None

REPO_WORKERS = _env_int("SCRAPER_REPO_WORKERS", 180)
REQUEST_TIMEOUT = _env_int("SCRAPER_TIMEOUT", 30)

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_API = "https://api.github.com"
GITHUB_REPO_MAX_FILE_MB = 200

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
}

# ---------------------------------------------------------------------
# File-type filters (kept from the original script) - controls which files
# inside a downloaded repo get saved. Docs (README, .md, .rst, .pdf, etc.)
# are included by default; this is a file-type filter, not a source list.
# ---------------------------------------------------------------------
CODE_EXTS = {
    '.py', '.pyi', '.pyx', '.pxd',  # .pyi = type stubs, .pyx/.pxd = Cython (perf-critical crypto/parsing code)
    '.go', '.js', '.mjs', '.cjs', '.ts', '.tsx', '.jsx', '.java', '.c', '.h',
    '.cpp', '.cc', '.hpp', '.cs', '.rb', '.php', '.sh', '.bash', '.ps1', '.pl',
    '.ksh', '.csh', '.tcsh', '.fish', '.bat',
    '.rs', '.swift', '.kt', '.kts', '.scala', '.r', '.m', '.mm', '.lua', '.sql',
    '.ipynb', '.groovy', '.dart', '.vb', '.asm', '.s', '.f90', '.jl', '.ex', '.exs',
    '.erl', '.clj', '.hs', '.nim', '.zig', '.sol', '.proto', '.tf',
    '.zsh', '.zsh-theme', '.bats', '.vue', '.cmd', '.exp', '.mod', '.sum',
}
DATA_EXTS = {
    '.csv', '.tsv', '.json', '.jsonl', '.xml', '.yaml', '.yml', '.ndjson',
    '.parquet', '.sqlite', '.db', '.log', '.txt', '.md', '.ini', '.cfg', '.conf',
    '.toml', '.env',
}
DOC_EXTS = {'.pdf', '.docx', '.doc', '.rtf', '.xlsx', '.xls', '.pptx', '.ppt', '.odt', '.ods'}
SKIP_EXTS = {
    '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico', '.css', '.woff', '.woff2',
    '.ttf', '.eot', '.mp4', '.mp3', '.avi', '.mov', '.wav', '.iso', '.img',
    '.deb', '.sig', '.ova', '.torrent', '.pkg', '.dmg', '.bin', '.msi', '.exe',
    '.dockerfile',
}
# Filenames (matched by prefix, case-insensitive) to always exclude, regardless
# of extension - catches LICENSE, LICENSE.md, LICENSE.txt, LICENSE-MIT,
# Dockerfile, Dockerfile.dev, dockerfile.prod, etc.
SKIP_FILENAME_PREFIXES = ('license', 'dockerfile', 'changelog', 'pipfile')
# ---------------------------------------------------------------------
# Security-specific extensions.
# Added on top of the generic code/data/doc buckets because they carry
# dense, structured *security* signal that a generic filter misses - each
# one is plain text, small, and directly useful for a cybersecurity SLM
# (detection logic, exploit/recon tooling, forensic artifacts). We
# deliberately do NOT add binary/opaque formats (.p12, .pfx, .der, .bin,
# raw malware samples, .sig detached signatures) - they're unparseable by
# a text tokenizer and add noise/risk (e.g. genuinely embedded key
# material) rather than learnable signal - i.e. the "garbage" to avoid.
# ---------------------------------------------------------------------
SECURITY_EXTS = {
    '.yar', '.yara',        # YARA rules - malware family/pattern detection signatures
    '.rules',                # Snort/Suricata IDS/IPS signature rules
    '.sigma',                # Sigma rules - SIEM-agnostic detection logic
    '.nse',                  # Nmap Scripting Engine - recon/vuln-check scripts
    '.vbs', '.vbe',          # VBScript - dominant language in malicious macro droppers
    '.psm1', '.psd1',        # PowerShell modules/manifests
    '.ps1xml',               # PowerShell format/type-extension XML (Format.ps1xml, Types.ps1xml)
    '.reg',                  # Windows registry export - forensic/persistence artifacts
    '.pem', '.crt', '.csr',  # Text-based (PEM/base64) cert material - PKI/TLS security tooling
    '.bpf',                  # eBPF source (e.g. probe.bpf) - runtime/kernel security tooling
                              # (Falco, Tracee, Cilium). Note: files like probe.bpf.c are already
                              # caught by '.c' above - Path.suffix only reads the last dot segment.
}

GITHUB_REPO_KEEP_EXTS = (
    CODE_EXTS | DATA_EXTS | DOC_EXTS | SECURITY_EXTS |
    {'.rst', '.adoc', '.markdown', '.sol', '.vy', '.cairo', '.move', '.circom'}
)
_GH_SKIP_DIRS = {'node_modules', '.git', '__pycache__', '.vscode', '.idea', 'coverage'}
_GH_DOCTEXT_EXTS = {'.rst', '.adoc', '.markdown'}


def bucket_for_ext(ext):
    ext = ext.lower()
    if ext in DOC_EXTS:
        return 'documents'
    if ext in CODE_EXTS:
        return 'code'
    if ext in DATA_EXTS:
        return 'data'
    return 'html'


def ext_folder(filename):
    ext = Path(filename).suffix.lower().lstrip('.')
    ext = "".join(c for c in ext if c.isalnum())
    return ext if ext else 'no_ext'


def sanitize_name(name):
    parts = []
    for seg in str(name).replace('\\', '/').split('/'):
        clean = "".join([c if c.isalnum() or c in "._- " else "_" for c in seg]).strip('_ ')
        if clean:
            parts.append(clean)
    return "/".join(parts) or "repo"


log_lock = threading.Lock()


def _atomic_append_line(path, text):
    """Append one line to `path` in a way that's safe across BOTH threads
    and separate OS processes. A Python threading.Lock only protects
    against other threads in the SAME process - it does nothing for other
    processes (each has its own independent copy of the lock object after
    fork/spawn). Real cross-process safety here comes from the OS: a
    single os.write() of data under PIPE_BUF (~4096 bytes on Linux) to a
    file opened with O_APPEND is guaranteed atomic by the kernel, so
    concurrent writers (threads or processes) can never interleave
    mid-line, no explicit lock required.
    """
    if not path:
        return
    data = (text if text.endswith('\n') else text + '\n').encode('utf-8')
    try:
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            os.write(fd, data)
        finally:
            os.close(fd)
    except Exception:
        pass


def log(msg):
    ts = time.strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    _atomic_append_line(LOG_FILE, line)


manifest_lock = threading.Lock()


def write_manifest(record):
    if not MANIFEST_FILE:
        return
    _atomic_append_line(MANIFEST_FILE, json.dumps(record, ensure_ascii=False))


failures_lock = threading.Lock()


def record_failure(url, reason):
    if not FAILURES_FILE:
        return
    rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'url': url, 'reason': str(reason)[:500]}
    _atomic_append_line(FAILURES_FILE, json.dumps(rec, ensure_ascii=False))


content_hashes = set()
content_hash_lock = threading.Lock()
# NOTE (multiprocessing): each worker PROCESS gets its own independent copy
# of `content_hashes`, loaded once at process start via load_content_hashes()
# (see the pool initializer in main()). This correctly avoids re-saving
# content that existed before this run started, and avoids duplicates
# within a single worker process's lifetime. It does NOT give perfectly
# synced real-time dedup *across* different worker processes during the
# same run (that would need a multiprocessing.Manager, adding IPC overhead
# on every single file). Practical effect: on a fresh run, if two different
# repos assigned to two different processes both contain a byte-identical
# file, both copies get saved instead of one - a minor storage cost, not a
# correctness problem, and a deliberate tradeoff for full-core throughput.


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
    _atomic_append_line(CONTENT_HASHES_FILE, h)
    return True


existing_filenames = set()
existing_index_lock = threading.Lock()


def build_existing_filenames_index():
    idx = set()
    if DEST_ROOT.exists():
        for p in DEST_ROOT.rglob('*'):
            if p.is_file():
                idx.add(p.name)
    return idx


def is_already_present(clean_fn):
    with existing_index_lock:
        return clean_fn in existing_filenames


def mark_filename_present(clean_fn):
    with existing_index_lock:
        existing_filenames.add(clean_fn)


save_lock = threading.Lock()
# NOTE (multiprocessing): `stats`/`total_saved` are no longer relied on for
# the final summary, since a plain dict/int can't be safely shared across
# separate OS processes without a multiprocessing.Manager (added IPC cost
# per file). Every save is already durably recorded in MANIFEST_FILE via
# write_manifest() below, so main() derives the final "files saved" /
# "files per extension" summary by reading that file once, after all
# worker processes finish - simpler and exactly as accurate.
stats = {}
total_saved = 0


def _atomic_write_bytes(out_path, data_bytes):
    out_path = Path(out_path)
    tmp_path = out_path.with_name(f".{out_path.name}.tmp{os.getpid()}")
    with open(tmp_path, 'wb') as f:
        f.write(data_bytes)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, out_path)


def save_binary_file(filename, content_bytes, bucket, site_folder=None, source_url=None):
    global total_saved
    sha = hashlib.sha256(content_bytes).hexdigest()

    clean_fn = "".join([c if c.isalnum() or c in "._- " else "_" for c in filename])
    if len(clean_fn) > 200:
        ext = Path(clean_fn).suffix
        clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + ext

    if _hash_seen(sha):
        write_manifest({'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'saved': False,
                         'duplicate_of_sha256': sha, 'filename': clean_fn,
                         'bucket': bucket, 'site_folder': site_folder, 'source_url': source_url})
        return True

    out_dir = DEST_ROOT / (site_folder or 'misc') / ext_folder(clean_fn)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / clean_fn
    try:
        _atomic_write_bytes(out_path, content_bytes)
        _hash_remember(sha)
        mark_filename_present(clean_fn)
        write_manifest({'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'saved': True,
                         'sha256': sha, 'filename': clean_fn, 'bucket': bucket,
                         'site_folder': site_folder, 'path': str(out_path),
                         'bytes': len(content_bytes), 'source_url': source_url})
        return True
    except Exception as e:
        record_failure(source_url or clean_fn, f"save_binary_file: {e}")
        return False


# ---------------------------------------------------------------------
# GitHub API + rate-limit handling
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


# ---------------------------------------------------------------------
# Per-repo resume state (lets a killed run pick back up without re-downloading
# repos that already finished, and without re-saving files already written).
# ---------------------------------------------------------------------
repo_progress_lock = threading.Lock()


def _log_repo_progress(event, owner, repo, **fields):
    if not REPO_PROGRESS_FILE:
        return
    rec = {'ts': time.strftime('%Y-%m-%dT%H:%M:%S'), 'event': event, 'owner': owner, 'repo': repo}
    rec.update(fields)
    _atomic_append_line(REPO_PROGRESS_FILE, json.dumps(rec, ensure_ascii=False))


def _repo_state_paths(site_folder):
    state_dir = DEST_ROOT / "repo_state" / site_folder
    return state_dir, state_dir / "summary.json", state_dir / "done_files.txt"


def _repo_state_load(site_folder):
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
    _, _, done_path = _repo_state_paths(site_folder)
    try:
        with open(done_path, 'a', encoding='utf-8') as f:
            f.write(rel_path + '\n')
    except Exception:
        pass


def _process_repo_tarfile(tar_path, site_folder, source_base, owner, repo):
    keep_exts = GITHUB_REPO_KEEP_EXTS
    count = 0
    extract_dir = tempfile.mkdtemp(prefix="rex_")
    _, done_files = _repo_state_load(site_folder)
    total = 0
    already_done = 0  # recomputed below, once `candidates` (current-filter matches) is known
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

        candidates = []
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in _GH_SKIP_DIRS]
            for fn in filenames:
                fp = os.path.join(dirpath, fn)
                rel = os.path.relpath(fp, base).replace(os.sep, '/')
                ext = Path(fn).suffix.lower()
                if fn.lower().startswith(SKIP_FILENAME_PREFIXES):
                    continue
                if ext in SKIP_EXTS:
                    continue
                if ext and ext not in keep_exts:
                    continue
                if not ext and fn.lower() not in (
                    'readme', 'makefile',
                ):
                    continue
                try:
                    if os.path.getsize(fp) > max_bytes:
                        continue
                except OSError:
                    continue
                candidates.append((rel, fp, ext))

        total = len(candidates)
        # Only count previously-done files that STILL match the current
        # keep-extension rules. Without this, re-running after the filter
        # list changes (extensions added/removed) makes `already_done`
        # (from an older, differently-scoped run) inconsistent with the
        # freshly recomputed `total`, incorrectly flagging fully-covered
        # repos as 'partial'.
        candidate_rels = {rel for rel, _, _ in candidates}
        already_done = len(done_files & candidate_rels)
        if already_done:
            log(f"    [github-repo] {owner}/{repo}: resuming - {already_done}/{total} files already done")
        _log_repo_progress('repo_start', owner, repo, total_files=total, already_done=already_done)

        for rel, fp, ext in candidates:
            if rel in done_files:
                continue
            clean_fn = "".join([c if c.isalnum() or c in "._- " else "_" for c in rel.replace('/', '_')])
            if len(clean_fn) > 200:
                tail = Path(clean_fn).suffix or ''
                clean_fn = clean_fn[:180] + '_' + hashlib.md5(clean_fn.encode()).hexdigest()[:8] + tail
            # NOTE: no filename-only "already present" pre-check here on purpose.
            # Across thousands of different repos, many legitimately share common
            # root-level filenames (README.md, install.sh, run.sh, Makefile, ...).
            # A global filename-only index would wrongly treat those as duplicates
            # of each other and silently drop real, distinct per-repo content.
            # True duplicate detection is handled correctly inside
            # save_binary_file() via SHA-256 content hashing, which only skips
            # files whose actual bytes are identical - not just same name.
            try:
                with open(fp, "rb") as bf:
                    data = bf.read()
            except OSError:
                continue
            bucket = 'data' if (not ext or ext in _GH_DOCTEXT_EXTS) else bucket_for_ext(ext)
            src = f"{source_base}/blob/-/{rel}" if source_base else None
            if save_binary_file(clean_fn, data, bucket, site_folder=site_folder, source_url=src):
                count += 1
                _repo_state_mark_file_done(site_folder, rel)
                done_now = already_done + count
                if count % 25 == 0 or done_now == total:
                    log(f"    [github-repo] {owner}/{repo}: {done_now}/{total} files done")
                    _log_repo_progress('repo_progress', owner, repo, done=done_now, total=total)
    except Exception as e:
        log(f"    [repo-unpack] error for {site_folder}: {e}")
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)

    final_done = already_done + count
    status = 'done' if (final_done >= total and total >= already_done) else 'partial'
    _repo_state_write_summary(site_folder, {
        'owner': owner, 'repo': repo, 'total_files': total,
        'done_files': final_done, 'status': status,
        'updated_ts': time.strftime('%Y-%m-%dT%H:%M:%S'),
    })
    _log_repo_progress('repo_done' if status == 'done' else 'repo_interrupted',
                        owner, repo, done=final_done, total=total)
    return count


_worker_session = None  # set once per worker PROCESS by _worker_init()


def _worker_init(dest_root, xlsx_path, github_token, repo_workers, request_timeout, headers):
    """Runs once in EACH worker process when the ProcessPoolExecutor starts
    it (not once per repo/task). Sets up everything that used to be a
    plain module-level global in the single-process/threaded version, so
    it works correctly regardless of the OS's multiprocessing start method
    (fork on Linux inherits parent globals automatically; spawn - the
    default on Windows/macOS - starts a fresh interpreter with none of
    that, so we set it explicitly here to be portable either way).
    """
    global DEST_ROOT, XLSX_PATH, GITHUB_TOKEN, REPO_WORKERS, REQUEST_TIMEOUT, HEADERS
    global LOG_FILE, MANIFEST_FILE, FAILURES_FILE, CONTENT_HASHES_FILE, REPO_PROGRESS_FILE
    global content_hashes, _worker_session

    DEST_ROOT = Path(dest_root)
    XLSX_PATH = Path(xlsx_path)
    GITHUB_TOKEN = github_token
    REPO_WORKERS = repo_workers
    REQUEST_TIMEOUT = request_timeout
    HEADERS = headers

    LOG_FILE            = DEST_ROOT / "scraper.log"
    MANIFEST_FILE       = DEST_ROOT / "manifest.jsonl"
    FAILURES_FILE       = DEST_ROOT / "failures.jsonl"
    CONTENT_HASHES_FILE = DEST_ROOT / "content_hashes.txt"
    REPO_PROGRESS_FILE  = DEST_ROOT / "repo_progress.jsonl"

    # Per-process content-hash dedup, seeded from whatever was already on
    # disk before this run started (see the NOTE above the module-level
    # `content_hashes` declaration for what this does and doesn't cover).
    content_hashes = load_content_hashes()

    # One persistent requests.Session per worker PROCESS, reused across
    # every repo that process handles for its whole lifetime (a
    # ProcessPoolExecutor worker stays alive across many submitted tasks,
    # it isn't re-spawned per repo) - this keeps HTTP connection reuse
    # working the same way it did in the single-session threaded version.
    _worker_session = requests.Session()
    _worker_session.headers.update(HEADERS)
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=4, pool_maxsize=4, max_retries=0,
    )
    _worker_session.mount('http://', adapter)
    _worker_session.mount('https://', adapter)


def github_repo_scraper(owner, repo):
    """Download a whole repo as a tarball (one API call), unpack, and save
    every file that passes GITHUB_REPO_KEEP_EXTS (code + docs + data).
    Uses this worker process's own persistent Session (see _worker_init)."""
    session = _worker_session
    site_folder = sanitize_name(f"github_{owner}_{repo}")

    prev_summary, _ = _repo_state_load(site_folder)
    if prev_summary and prev_summary.get('status') == 'done':
        n = prev_summary.get('done_files', 0)
        log(f"  [github-repo] {owner}/{repo}: already fully scraped ({n} files) - skipping")
        return 0

    log(f"  [github-repo] {owner}/{repo} -> {site_folder}")

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
        count = _process_repo_tarfile(tar_path, site_folder, f"https://github.com/{owner}/{repo}",
                                       owner, repo)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    log(f"    [github-repo] {owner}/{repo}: saved {count} new files this run")
    return count


# ---------------------------------------------------------------------
# xlsx -> list of (owner, repo), read from the "python" sheet only, sorted
# by that sheet's own "Stars" column (highest stars first). No GitHub API
# calls needed for this - the sheet already has stars.
# ---------------------------------------------------------------------
_GH_URL_RE = re.compile(r'github\.com/([^/\s]+)/([^/\s#?]+)', re.IGNORECASE)


def _to_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return 0


def read_repos_from_xlsx(xlsx_path, sheet_name='python'):
    wb = load_workbook(xlsx_path, data_only=True, read_only=True)

    ws = None
    for candidate in wb.worksheets:
        if candidate.title.strip().lower() == sheet_name.strip().lower():
            ws = candidate
            break
    if ws is None:
        available = ", ".join(w.title for w in wb.worksheets)
        log(f"ERROR: no sheet named '{sheet_name}' found in {xlsx_path}. "
            f"Available sheets: {available}")
        return []

    rows = ws.iter_rows(values_only=True)
    try:
        header = next(rows)
    except StopIteration:
        return []
    header = [str(h).strip() if h is not None else "" for h in header]
    col_idx = {h.lower(): i for i, h in enumerate(header)}

    full_name_col = col_idx.get('full name')
    url_col = col_idx.get('github url') or col_idx.get('url') or col_idx.get('repo url')
    stars_col = col_idx.get('stars')

    entries = []
    seen = set()
    skipped = 0

    for row in rows:
        owner = repo = None
        if full_name_col is not None and full_name_col < len(row) and row[full_name_col]:
            fn = str(row[full_name_col]).strip().strip('/')
            if '/' in fn:
                owner, repo = fn.split('/', 1)
        if not (owner and repo) and url_col is not None and url_col < len(row) and row[url_col]:
            m = _GH_URL_RE.search(str(row[url_col]))
            if m:
                owner, repo = m.group(1), m.group(2)
        if not (owner and repo):
            skipped += 1
            continue
        repo = repo[:-4] if repo.endswith('.git') else repo
        key = (owner.lower(), repo.lower())
        if key in seen:
            continue
        seen.add(key)
        stars = _to_int(row[stars_col]) if (stars_col is not None and stars_col < len(row)) else 0
        entries.append((owner, repo, stars))

    if skipped:
        log(f"  {skipped} row(s) in sheet '{ws.title}' had no usable GitHub owner/repo - skipped")

    entries.sort(key=lambda t: t[2], reverse=True)
    if entries:
        top = entries[:5]
        log("  Top by stars: " + ", ".join(f"{o}/{r} ({s}★)" for o, r, s in top))

    return [(o, r) for o, r, s in entries]



def configure():
    global LOG_FILE, MANIFEST_FILE, FAILURES_FILE, CONTENT_HASHES_FILE, REPO_PROGRESS_FILE
    global content_hashes
    DEST_ROOT.mkdir(parents=True, exist_ok=True)
    LOG_FILE             = DEST_ROOT / "scraper.log"
    MANIFEST_FILE        = DEST_ROOT / "manifest.jsonl"
    FAILURES_FILE        = DEST_ROOT / "failures.jsonl"
    CONTENT_HASHES_FILE  = DEST_ROOT / "content_hashes.txt"
    REPO_PROGRESS_FILE   = DEST_ROOT / "repo_progress.jsonl"
    content_hashes = load_content_hashes()


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Download every GitHub repo listed in a chosen sheet of an xlsx "
                    "workbook (code + docs + security-relevant files). No other source "
                    "and no other sheet is scraped.")
    p.add_argument('--xlsx', help="Path to the input spreadsheet (env SCRAPER_XLSX)")
    p.add_argument('--dest', help="Output directory (env SCRAPER_DEST)")
    p.add_argument('--repo-workers', type=int, help="Concurrent repo downloads (env SCRAPER_REPO_WORKERS)")
    p.add_argument('--limit', type=int, help="Only process the first N repos (for a test run)")
    p.add_argument('--sheet', default=None,
                    help=f"Worksheet name to read repos from (case-insensitive). "
                         f"Default: {DEFAULT_SHEET}. Overridden by a language flag if given.")

    # One boolean flag per language, generated straight from LANGUAGE_SHEETS.
    # Add a language to that dict and its --flag shows up here automatically.
    lang_group = p.add_argument_group('language shortcuts')
    lang_exclusive = lang_group.add_mutually_exclusive_group()
    for lang_key in LANGUAGE_SHEETS:
        lang_exclusive.add_argument(
            f'--{lang_key}', action='store_true',
            help=f"Shortcut for --sheet {LANGUAGE_SHEETS[lang_key]}"
        )

    return p.parse_args(argv)


def resolve_sheet(args):
    """Figure out which worksheet to read from: a --<language> flag wins if
    set, otherwise --sheet, otherwise DEFAULT_SHEET."""
    for lang_key, sheet_name in LANGUAGE_SHEETS.items():
        if getattr(args, lang_key, False):
            return sheet_name
    if args.sheet:
        return args.sheet
    return DEFAULT_SHEET


def apply_args(args):
    global XLSX_PATH, DEST_ROOT, REPO_WORKERS
    if args.xlsx:
        XLSX_PATH = Path(args.xlsx).expanduser()
    if args.dest:
        DEST_ROOT = Path(args.dest).expanduser()
    if args.repo_workers:
        REPO_WORKERS = args.repo_workers


def main(argv=None):
    global existing_filenames
    args = parse_args(argv)
    apply_args(args)
    sheet_name = resolve_sheet(args)
    configure()

    log("=" * 70)
    log("GitHub repo scraper - xlsx-only (all other sources removed)")
    log("=" * 70)
    log(f"  XLSX={XLSX_PATH}")
    log(f"  SHEET={sheet_name}")
    log(f"  DEST={DEST_ROOT}")
    log(f"  GitHub token: {'yes' if GITHUB_TOKEN else 'NO (rate-limited to 60 req/hr - set GITHUB_TOKEN)'}")
    log(f"  Repo workers (concurrency): {REPO_WORKERS}")
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        needed = REPO_WORKERS * 4  # rough headroom: sockets + temp files + tar members per worker
        log(f"  Open-file limit (ulimit -n): soft={soft}, hard={hard}")
        if soft < needed:
            log(f"  WARNING: ulimit -n ({soft}) may be too low for {REPO_WORKERS} workers. "
                f"Run 'ulimit -n {min(needed, hard)}' in this shell before starting, "
                f"or you may see 'Too many open files' errors under load.")
    except Exception:
        pass  # resource module is POSIX-only; skip silently on other platforms

    if not XLSX_PATH.exists():
        log(f"ERROR: xlsx not found at {XLSX_PATH}")
        sys.exit(1)

    repos = read_repos_from_xlsx(XLSX_PATH, sheet_name=sheet_name)
    if not repos:
        log(f"ERROR: no repos found in sheet '{sheet_name}' - aborting")
        sys.exit(1)
    log(f"  Found {len(repos)} unique repo(s) in sheet '{sheet_name}', sorted by Stars (highest first)")

    if args.limit:
        repos = repos[:args.limit]
        log(f"  --limit applied: processing {len(repos)} repo(s)")

    log(f"Starting {REPO_WORKERS} worker PROCESSES (real OS-level parallelism, "
        f"one requests.Session per process, reused across that process's repos)...")

    start_time = time.time()
    with ProcessPoolExecutor(
        max_workers=REPO_WORKERS,
        initializer=_worker_init,
        initargs=(str(DEST_ROOT), str(XLSX_PATH), GITHUB_TOKEN, REPO_WORKERS, REQUEST_TIMEOUT, HEADERS),
    ) as executor:
        futures = {executor.submit(github_repo_scraper, owner, repo): (owner, repo)
                   for owner, repo in repos}
        for future in as_completed(futures):
            owner, repo = futures[future]
            try:
                future.result()
            except Exception as e:
                log(f"FAIL: {owner}/{repo} -> {e}")
                record_failure(f"{owner}/{repo}", str(e))

    elapsed = time.time() - start_time

    # Live in-memory counters (`stats`/`total_saved`) can't be safely shared
    # across separate OS processes without extra IPC overhead per file, so
    # the final summary is derived here from MANIFEST_FILE instead - every
    # successful save is durably recorded there regardless of which worker
    # process wrote it, so this is exactly as accurate, just computed once
    # at the end rather than tracked live.
    total_saved_final = 0
    stats_final = {}
    try:
        if MANIFEST_FILE and MANIFEST_FILE.exists():
            with open(MANIFEST_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    if rec.get('saved'):
                        total_saved_final += 1
                        cat = ext_folder(rec.get('filename', ''))
                        stats_final[cat] = stats_final.get(cat, 0) + 1
    except Exception as e:
        log(f"WARNING: could not compute final stats from manifest: {e}")

    log("\n" + "=" * 70)
    log("SCRAPING COMPLETE")
    log("=" * 70)
    log(f"Time: {elapsed/60:.1f} minutes")
    log(f"Total files saved: {total_saved_final}")
    log("\nFiles per extension:")
    for cat, count in sorted(stats_final.items(), key=lambda x: -x[1]):
        log(f"  {cat}: {count}")
    log(f"\nManifest: {MANIFEST_FILE}")
    log(f"Failures: {FAILURES_FILE}")
    log("=" * 70)


if __name__ == "__main__":
    main()
