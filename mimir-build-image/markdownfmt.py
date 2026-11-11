#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

"""Format Markdown with Bun's native Wasm formatter and the existing ignore file."""

import argparse
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()

    # Prettier does not traverse hidden paths or node_modules by default.
    excludes = ["**/.*", "**/node_modules"]
    ignore_file = Path(".prettierignore")
    if ignore_file.is_file():
        for line in ignore_file.read_text().splitlines():
            pattern = line.strip()
            if not pattern or pattern.startswith("#"):
                continue
            negation = "!" if pattern.startswith("!") else ""
            pattern = pattern.removeprefix("!")
            # Gitignore patterns without a slash apply at every directory level.
            if pattern.startswith("/"):
                pattern = pattern[1:]
            elif "/" not in pattern.rstrip("/"):
                pattern = "**/" + pattern
            excludes.append(negation + pattern)

    result = subprocess.run([
        "dprint", "fmt" if args.write else "check",
        "--config-discovery=false",
        "--plugins", os.environ.get("DPRINT_BUN_PLUGIN", "/usr/local/lib/dprint/bun.wasm"),
        # Prettier uses .prettierignore, not .gitignore, to select Markdown files.
        "--no-gitignore", "--incremental=false", "--allow-no-files",
        "--excludes", *excludes,
        "--", *args.files,
    ])
    # Match Prettier's check-mode exit code for formatting differences.
    return 1 if result.returncode == 20 else result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
