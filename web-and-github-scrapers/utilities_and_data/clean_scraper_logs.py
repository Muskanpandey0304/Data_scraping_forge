#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.9"
# dependencies = []
# ///
"""
clean_git_stars.py

Recursively walks a given root folder and deletes, wherever found:
  - any file named scraper.log
  - any file named repo_progress.jsonl
  - any file named failures.jsonl
  - any folder named repo_state (and everything inside it)

Usage (with uv):
    uv run clean_git_stars.py /path/to/GIT_STARS/M              # dry run
    uv run clean_git_stars.py /path/to/GIT_STARS/M --apply       # actually delete

If no root is given, defaults to the current directory.
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

TARGET_FILES = {"scraper.log", "repo_progress.jsonl", "failures.jsonl"}
TARGET_DIR = "repo_state"


def main():
    parser = argparse.ArgumentParser(
        description="Recursively delete scraper.log, repo_progress.jsonl, "
        "failures.jsonl, and repo_state/ folders anywhere under root."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=".",
        help="Root directory to search under (default: current directory)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete files/folders. Without this flag, runs as a dry run.",
    )
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        print(f"Error: '{root}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    print(f"Searching under: {root}")
    print(
        "Mode: APPLY (files will be permanently deleted)."
        if args.apply
        else "Mode: DRY RUN (nothing will be deleted). Re-run with --apply to actually delete."
    )
    print("-" * 67)

    files_removed = 0
    dirs_removed = 0

    # Walk top-down so we can prune repo_state dirs from further recursion
    for dirpath, dirnames, filenames in os.walk(root, topdown=True):
        dirpath = Path(dirpath)

        # Handle repo_state folders, and prevent descending into them
        keep_dirs = []
        for d in dirnames:
            if d == TARGET_DIR:
                target = dirpath / d
                if args.apply:
                    shutil.rmtree(target)
                    print(f"[deleted folder] {target}")
                else:
                    print(f"[would delete folder] {target}")
                dirs_removed += 1
                # don't recurse into it (deleted or dry-run skip)
            else:
                keep_dirs.append(d)
        dirnames[:] = keep_dirs

        # Handle target files
        for f in filenames:
            if f in TARGET_FILES:
                target = dirpath / f
                if args.apply:
                    target.unlink()
                    print(f"[deleted file] {target}")
                else:
                    print(f"[would delete file] {target}")
                files_removed += 1

    print("-" * 67)
    print(f"Files deleted (or that would be deleted): {files_removed}")
    print(f"repo_state folders deleted (or that would be deleted): {dirs_removed}")

    if not args.apply:
        print()
        print("This was a dry run. To actually delete these items, run:")
        print(f"  uv run {Path(__file__).name} \"{root}\" --apply")


if __name__ == "__main__":
    main()
