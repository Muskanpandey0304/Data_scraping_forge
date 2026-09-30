#!/usr/bin/env python3
"""
collect_code_files_v3.py

Walks through one or more "batch" source folders (e.g. part1, part2, part3,
part3_new, part4, part5, latest-raw-data, nvd_cve, google, ...) - each of
which can contain any number of nested subfolders - finds every file
matching a set of allowed extensions/filenames, and copies them into one
MAIN destination folder (default: CODES).

IMPORTANT behaviour:
  - The batch folder's own name (part3, part4, nvd_cve, google, ...) is NOT
    included in the output path - only the subfolders INSIDE it are kept.
        part4/cert/xyz.json        -> CODES/cert/xyz.json
        nvd_cve/2021/jan/foo.json  -> CODES/2021/jan/foo.json
  - If you re-run the script over the SAME batch folder again (resuming an
    interrupted run, or re-scraping), a file that was already copied from
    that exact batch is just SKIPPED - not recopied.
  - If a DIFFERENT batch folder happens to produce a file at the same
    output path (e.g. part3/cert/xyz.json and part4/cert/xyz.json both map
    to CODES/cert/xyz.json), the second one is NEVER deleted or
    overwritten - it is copied alongside the first under a renamed path
    (e.g. CODES/cert/xyz__part4.json).
  - This is tracked with a small manifest file (CODES/.collect_manifest.json)
    that remembers which batch produced which output file, so the
    skip-vs-rename decision stays correct even across separate runs.

Usage:
    python collect_code_files_v3.py --sources part1 part2 part3 part3_new part4 part5 latest-raw-data --dest CODES

    # Or point it at a parent directory and it will auto-discover every
    # immediate subfolder as a batch:
    python collect_code_files_v3.py --parent /path/to/project --dest CODES

    # Dry run (see what would be copied/skipped/renamed without touching anything):
    python collect_code_files_v3.py --sources part1 part2 --dest CODES --dry-run

    # Old fully-flat behaviour (no subfolders at all, just filenames):
    python collect_code_files_v3.py --sources part1 part2 --dest CODES --flatten
"""

import argparse
import json
import shutil
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

MANIFEST_NAME = ".collect_manifest.json"
CATEGORIZED_SUBDIR = "CODES1"


def long_path(p: Path) -> str:
    """
    On Windows, prefix absolute paths with \\\\?\\ so the OS bypasses the
    classic 260-character MAX_PATH limit. This is a no-op on other platforms.
    """
    if sys.platform != "win32":
        return str(p)
    s = str(p.resolve())
    if s.startswith("\\\\?\\"):
        return s
    if s.startswith("\\\\"):  # UNC path -> \\?\UNC\server\share\...
        return "\\\\?\\UNC\\" + s[2:]
    return "\\\\?\\" + s

# ---------------------------------------------------------------------------
# Extensions / filenames to collect. ONLY files matching one of these are
# copied - everything else is ignored, no matter how deep it's nested.
# Anything not starting with '.' is matched against the exact filename
# (case-insensitive), e.g. "makefile".
# Multi-dot suffixes like ".bpf.c" are matched against the filename's tail.
# ---------------------------------------------------------------------------
SUPPORTED_EXTENSIONS = {
    '.c', '.h', '.cpp', '.hpp', '.cc', '.cxx',
    '.go', '.rs',
    '.py', '.sh', '.bash', '.zsh',
    'makefile', '.mk', '.cmake', '.ld',
    '.yaml', '.yml', '.json', '.toml', '.ini', '.conf', '.cfg',
    '.md', '.rst', '.tex',
    '.rules', '.yar', '.yara', '.sigma',
    '.bpf', '.c',
    '.xml', '.proto',
}

_EXACT_NAMES = {e.lower() for e in SUPPORTED_EXTENSIONS if not e.startswith('.')}
_SUFFIXES = sorted(
    (e.lower() for e in SUPPORTED_EXTENSIONS if e.startswith('.')),
    key=len,
    reverse=True,  # check longer/multi-dot suffixes first (e.g. '.bpf.c' before '.c')
)


def is_supported(path: Path) -> bool:
    name = path.name.lower()
    if name in _EXACT_NAMES:
        return True
    for suf in _SUFFIXES:
        if name.endswith(suf):
            return True
    return False


def get_category_name(path: Path) -> str:
    name = path.name.lower()
    if name in _EXACT_NAMES:
        return name
    for suf in _SUFFIXES:
        if name.endswith(suf):
            return suf.lstrip('.')
    ext = path.suffix.lower().lstrip('.')
    return ext if ext else "misc"


def is_tar_archive(p: Path) -> bool:
    name = p.name.lower()
    return p.is_file() and (
        name.endswith(".tar.gz") or name.endswith(".tgz")
        or name.endswith(".tar") or name.endswith(".tar.bz2") or name.endswith(".tar.xz")
    )


def is_zip_archive(p: Path) -> bool:
    name = p.name.lower()
    return p.is_file() and name.endswith(".zip")


def is_archive(p: Path) -> bool:
    return is_tar_archive(p) or is_zip_archive(p)


def extract_archive(archive: Path, extract_root: Path) -> Path:
    """
    Extract a .tar.gz/.tgz/.tar/... OR a .zip archive into
    extract_root/<archive-stem>/ (skipping extraction if that folder
    already exists and is non-empty), and return the path to the
    extracted folder so it can be treated as a normal batch source folder.
    """
    stem = archive.name
    for suf in (".tar.gz", ".tgz", ".tar.bz2", ".tar.xz", ".tar", ".zip"):
        if stem.lower().endswith(suf):
            stem = stem[: -len(suf)]
            break
    target_dir = extract_root / stem
    if target_dir.exists() and any(target_dir.iterdir()):
        print(f"[extract] Already extracted, reusing: {target_dir}")
        return target_dir

    target_dir.mkdir(parents=True, exist_ok=True)
    print(f"[extract] {archive} -> {target_dir}")

    if is_zip_archive(archive):
        with zipfile.ZipFile(archive) as zf:
            # Guard against path-traversal ("zip slip") entries in the archive.
            safe_members = []
            for member in zf.infolist():
                member_path = (target_dir / member.filename).resolve()
                if target_dir.resolve() in member_path.parents or member_path == target_dir.resolve():
                    safe_members.append(member)
                else:
                    print(f"[warn] Skipping unsafe archive entry: {member.filename}", file=sys.stderr)
            zf.extractall(path=target_dir, members=safe_members)
    else:
        with tarfile.open(archive) as tf:
            # Guard against path-traversal ("zip slip") entries in the archive.
            safe_members = []
            for member in tf.getmembers():
                member_path = (target_dir / member.name).resolve()
                if target_dir.resolve() in member_path.parents or member_path == target_dir.resolve():
                    safe_members.append(member)
                else:
                    print(f"[warn] Skipping unsafe archive entry: {member.name}", file=sys.stderr)
            tf.extractall(path=target_dir, members=safe_members)
    return target_dir


def find_anchor(root: Path, anchors) -> Path:
    """
    Search recursively inside `root` for a directory whose name matches
    ANY of the names in `anchors` (case-insensitive) and return it. Used
    to strip away a junk absolute path prefix baked into a tar/zip (e.g.
    archives made with `tar -czf x.tar.gz /home/user/full/path/...`
    instead of `-C dir .`).

    Different sources can have their real batch folder buried under
    different names (e.g. one archive's real content starts at
    ".../Revised1/...", another's at ".../scraped_data/...", another's
    at ".../Tools/...") - passing all of them lets each source match
    whichever one actually applies to it.

    If multiple matches exist (same or different anchor names), the
    shallowest (closest to root) is used and a warning is printed. If
    none is found, `root` itself is returned unchanged and a warning is
    printed.
    """
    anchor_names = {a.lower() for a in anchors}
    matches = sorted(
        (d for d in root.rglob('*') if d.is_dir() and d.name.lower() in anchor_names),
        key=lambda d: len(d.parts),
    )
    if not matches:
        print(f"[warn] none of --anchor {sorted(anchor_names)} found under {root}; using {root} as-is.",
              file=sys.stderr)
        return root
    if len(matches) > 1:
        print(f"[warn] --anchor matched {len(matches)} folders under {root}; "
              f"using the shallowest: {matches[0]}", file=sys.stderr)
    return matches[0]


def prepare_sources(raw_sources, extract_root: Path, anchors=None):
    """
    Turn a list of raw --sources entries into a list of real directory
    Paths, transparently extracting any .tar.gz/.tgz/.tar/.zip archives
    found along the way (each into its own folder under extract_root) so
    the rest of the script can treat everything uniformly as a batch
    folder. If `anchors` is given (a list of names), each resulting
    folder is further resolved down to the first subfolder matching any
    of those names (see find_anchor), which becomes the real batch root.
    """
    prepared = []
    for s in raw_sources:
        p = Path(s).resolve()
        if is_archive(p):
            batch_dir = extract_archive(p, extract_root)
        elif p.is_dir():
            batch_dir = p
        else:
            print(f"[skip] Not a directory or supported archive: {p}", file=sys.stderr)
            continue
        if anchors:
            batch_dir = find_anchor(batch_dir, anchors)
        prepared.append(batch_dir)
    return prepared


def natural_rel(source_root: Path, source_file: Path, flatten: bool) -> Path:
    """
    The 'natural' output path for a file, BEFORE collision resolution.
    - flatten=True: just the filename, no subfolders at all.
    - flatten=False (default): the path relative to the batch folder,
      i.e. the batch folder's own name (part3, part4, nvd_cve, ...) is
      dropped, but everything inside it is preserved.
    """
    if flatten:
        return Path(source_file.name)
    return source_file.relative_to(source_root)


def load_manifest(dest: Path) -> dict:
    path = dest / MANIFEST_NAME
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            print(f"[warn] Could not read manifest at {path}, starting fresh.", file=sys.stderr)
    return {}


def save_manifest(dest: Path, manifest: dict):
    path = dest / MANIFEST_NAME
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
    except OSError as e:
        print(f"[warn] Could not write manifest at {path}: {e}", file=sys.stderr)


def resolve_collision(dest: Path, rel: Path, batch_name: str) -> Path:
    """
    Find a free output path for `rel` by tagging the filename with the
    batch name that's colliding with an existing (different-batch) file.
    Never overwrites - keeps incrementing a counter until a free name is found.
    """
    stem = rel.stem
    suffix = rel.suffix
    parent = rel.parent
    counter = 0
    while True:
        if counter == 0:
            candidate_name = f"{stem}__{batch_name}{suffix}"
        else:
            candidate_name = f"{stem}__{batch_name}_{counter}{suffix}"
        candidate = parent / candidate_name
        if not (dest / candidate).exists():
            return candidate
        counter += 1


def print_progress(current: int, total: int, prefix: str = "Copying"):
    if total <= 0:
        return
    bar_len = 40
    filled = int(bar_len * current / total)
    bar = "#" * filled + "-" * (bar_len - filled)
    pct = current / total * 100
    sys.stdout.write(f"\r{prefix} |{bar}| {current}/{total} ({pct:5.1f}%)")
    sys.stdout.flush()
    if current >= total:
        sys.stdout.write("\n")
        sys.stdout.flush()


def collect(sources, dest: Path, dry_run: bool = False, flatten: bool = False):
    dest.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(dest)
    dest_resolved = dest.resolve()

    # --- Phase 1: scan every batch folder and build the list of matching files ---
    matched = []  # list of (source_path, batch_name, natural_rel_path)
    for src in sources:
        src = Path(src).resolve()
        if not src.is_dir():
            print(f"[skip] Not a directory: {src}", file=sys.stderr)
            continue

        print(f"[scan] {src}")
        batch_name = src.name
        for path in src.rglob('*'):
            if not is_supported(path):
                continue
            if not path.is_file():
                continue
            # Never re-copy files that already live inside the destination
            try:
                if dest_resolved in path.absolute().parents:
                    continue
            except Exception:
                pass
            matched.append((path, batch_name, natural_rel(src, path, flatten)))

    total_found = len(matched)
    mode = "flattened, no subfolders" if flatten else "subfolders preserved, batch-folder name dropped"
    print(f"\nFound {total_found} matching file(s) ({mode}). Starting copy...\n" if not dry_run
          else f"\nFound {total_found} matching file(s) ({mode}). (dry run)\n")

    # --- Phase 2: copy, using the manifest to decide skip vs. rename ---
    total_copied = 0
    already_present = 0
    renamed = 0
    failed = []

    for i, (path, batch_name, rel) in enumerate(matched, start=1):
        key = f"{batch_name}::{rel.as_posix()}"

        if key in manifest:
            # This exact (batch, relative path) combo was handled before.
            target = dest / manifest[key]
            if target.exists():
                already_present += 1
                if not dry_run:
                    print_progress(i, total_found)
                continue
            # Recorded before but missing on disk now (e.g. deleted) - recopy
            # to the same spot it was assigned before.
            action = "copy"
        else:
            candidate = rel
            if (dest / candidate).exists():
                # Same relative path already exists but from a DIFFERENT
                # batch (no manifest key matched) - never overwrite, rename.
                candidate = resolve_collision(dest, rel, batch_name)
                action = "rename"
            else:
                action = "copy"
            manifest[key] = candidate.as_posix()
            target = dest / candidate

        if dry_run:
            tag = " (renamed - collides with a different batch)" if action == "rename" else ""
            print(f"  would copy: {path}  ->  {target.relative_to(dest)}{tag}")
        else:
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(long_path(path), long_path(target))
                total_copied += 1
                if action == "rename":
                    renamed += 1
            except OSError as e:
                failed.append((path, e))
                print(f"\n  [FAILED] {path} -> {target}  ({e})", file=sys.stderr)

        if not dry_run:
            print_progress(i, total_found)
            if i % 10000 == 0:
                save_manifest(dest, manifest)

    if not dry_run:
        save_manifest(dest, manifest)

    print()
    print(f"Matched files found: {total_found}")
    if dry_run:
        print("Dry run - nothing was copied, manifest not updated.")
    else:
        print(f"Files copied to '{dest}': {total_copied}")
    if renamed:
        print(f"  of which {renamed} were renamed due to a cross-batch filename collision (originals untouched).")
    if already_present:
        print(f"Skipped {already_present} file(s) already scraped from the same batch before (not recopied).")
    if failed:
        print(f"\n{len(failed)} file(s) FAILED to copy (see errors above). Common causes on Windows:")
        print("  - path longer than 260 characters (this script now works around that")
        print("    automatically, but extremely long paths, or long-path support being")
        print("    disabled in Windows, can still fail)")
        print("  - illegal characters, or a trailing space/dot, in a file or folder name")
        print("  - a reserved device name (con, nul, aux, prn, com1, lpt1, ...) in the path")
        print("These files were skipped; everything else was still copied.")


def categorize_collect(sources, dest: Path, dry_run: bool = False):
    """
    For each batch source folder (e.g. part3, part3_new), look at its
    immediate ("main") subfolders. For each main subfolder, walk it
    recursively, keep ONLY files whose extension/filename is in
    SUPPORTED_EXTENSIONS, and copy them into:

        <dest>/CODES1/<main_subfolder>/<extension-category>/<relative-path>

    Files with unsupported extensions are simply left out of the output
    (never deleted from the original source). Re-running is safe: files
    already copied for a given (batch, main-subfolder, relative-path) are
    skipped, and a same-path collision from a different batch is renamed
    rather than overwritten - tracked via the same manifest file used by
    the normal collect() mode, under a separate 'cat::' key namespace.
    """
    out_root = dest / CATEGORIZED_SUBDIR
    out_root.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(dest)

    matched = []  # (source_path, batch_name, main_subfolder_name, rel_within_subfolder, category)
    for src in sources:
        src = Path(src)
        if not src.is_dir():
            print(f"[skip] Not a directory: {src}", file=sys.stderr)
            continue

        batch_name = src.name
        main_subfolders = [p for p in src.iterdir() if p.is_dir()]
        if not main_subfolders:
            print(f"[warn] No main subfolders found directly inside {src}; nothing to categorize.", file=sys.stderr)
            continue

        for sub in main_subfolders:
            print(f"[scan] {src.name}/{sub.name}")
            for path in sub.rglob('*'):
                if not path.is_file() or not is_supported(path):
                    continue
                rel = path.relative_to(sub)
                category = get_category_name(path)
                matched.append((path, batch_name, sub.name, rel, category))

    total_found = len(matched)
    print(f"\nFound {total_found} supported file(s) across main subfolders. "
          f"{'(dry run)' if dry_run else 'Categorizing into ' + str(out_root)}\n")

    total_copied = 0
    already_present = 0
    renamed = 0
    failed = []

    for i, (path, batch_name, sub_name, rel, category) in enumerate(matched, start=1):
        key = f"cat::{batch_name}::{sub_name}::{rel.as_posix()}"
        natural = Path(sub_name) / category / rel

        if key in manifest:
            target = out_root / manifest[key]
            if target.exists():
                already_present += 1
                if not dry_run:
                    print_progress(i, total_found, prefix="Categorizing")
                continue
            action = "copy"
            candidate = Path(manifest[key])
        else:
            candidate = natural
            if (out_root / candidate).exists():
                candidate = resolve_collision(out_root, natural, batch_name)
                action = "rename"
            else:
                action = "copy"
            manifest[key] = candidate.as_posix()

        target = out_root / candidate

        if dry_run:
            tag = " (renamed - collides with a different batch)" if action == "rename" else ""
            print(f"  would copy: {path}  ->  {target.relative_to(out_root)}{tag}")
        else:
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(long_path(path), long_path(target))
                total_copied += 1
                if action == "rename":
                    renamed += 1
            except OSError as e:
                failed.append((path, e))
                print(f"\n  [FAILED] {path} -> {target}  ({e})", file=sys.stderr)

        if not dry_run:
            print_progress(i, total_found, prefix="Categorizing")
            if i % 10000 == 0:
                save_manifest(dest, manifest)

    if not dry_run:
        save_manifest(dest, manifest)

    print()
    print(f"Matched (supported) files found: {total_found}")
    if dry_run:
        print("Dry run - nothing was copied, manifest not updated.")
    else:
        print(f"Files copied to '{out_root}': {total_copied}")
    if renamed:
        print(f"  of which {renamed} were renamed due to a cross-batch filename collision (originals untouched).")
    if already_present:
        print(f"Skipped {already_present} file(s) already categorized before (not recopied).")
    if failed:
        print(f"\n{len(failed)} file(s) FAILED to copy (see errors above).")


def main():
    parser = argparse.ArgumentParser(
        description="Collect source/config files from batch folders (any depth of subfolders) into one "
                    "main folder, dropping the batch folder's own name but keeping everything inside it."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--sources", nargs="+",
        help="List of batch folders to scan, e.g. --sources part1 part_2 part3 part3_new part4 part5 latest-raw-data"
    )
    group.add_argument(
        "--parent",
        help="Path to a parent directory; every immediate subfolder inside it will be treated as a batch "
             "(useful if you don't want to type out every folder name)."
    )
    parser.add_argument(
        "--dest", default="CODES",
        help="Main destination folder where matching files will be copied (default: CODES)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be copied/skipped/renamed without actually touching anything."
    )
    parser.add_argument(
        "--flatten", action="store_true",
        help="Fully flatten: dump every matching file directly into --dest with no subfolders at all. "
             "Skip-vs-rename logic still applies (based on filename only)."
    )
    parser.add_argument(
        "--categorize", action="store_true",
        help="Instead of the normal collect, look inside each batch's MAIN subfolders, keep only files "
             "with a supported extension, and copy them - organized by extension category - into "
             f"<dest>/{CATEGORIZED_SUBDIR}/<category>/<batch>/<main_subfolder>/... "
             "Unsupported files are left out of the output entirely (originals are never deleted)."
    )
    parser.add_argument(
        "--extract-dir",
        help="Where to extract any .tar.gz/.tgz/.tar/.zip sources before scanning them "
             "(default: <dest>_extracted, as a SIBLING of --dest, not inside it - "
             "files copied into --dest are intentionally ignored on re-scans, so "
             "extracting archives inside --dest would make them invisible)."
    )
    parser.add_argument(
        "--anchor", nargs="+",
        help="If archives/folders have junk parent directories baked in (e.g. tarred with an "
             "absolute path like /home/user/full/path/RealBatchName/...), list the name(s) of "
             "the real batch folder(s) here, e.g. --anchor Revised1 scraped_data Tools. For "
             "each source, the script searches for a folder matching ANY of these names "
             "(case-insensitive) and uses it as the batch root, dropping everything above it - "
             "so only RealBatchName/... and its real subfolders are kept in the output."
    )
    args = parser.parse_args()

    dest = Path(args.dest)
    if args.extract_dir:
        extract_root = Path(args.extract_dir).resolve()
    else:
        extract_root = dest.resolve().parent / (dest.resolve().name + "_extracted")
    extract_root.mkdir(parents=True, exist_ok=True)

    if args.parent:
        parent = Path(args.parent).resolve()
        raw_sources = [p for p in parent.iterdir() if p.is_dir() or is_archive(p)]
        raw_sources = [p for p in raw_sources if p.name != args.dest]
        if not raw_sources:
            print(f"No subfolders or archives found in {parent}", file=sys.stderr)
            sys.exit(1)
    else:
        raw_sources = args.sources

    sources = prepare_sources(raw_sources, extract_root, anchors=args.anchor)
    if not sources:
        print("No valid batch folders to process.", file=sys.stderr)
        sys.exit(1)

    if args.categorize:
        categorize_collect(sources, dest, dry_run=args.dry_run)
    else:
        collect(sources, dest, dry_run=args.dry_run, flatten=args.flatten)


if __name__ == "__main__":
    main()
