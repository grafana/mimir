#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Reject staged writes the signed file API cannot represent safely."""
import subprocess
import sys

records = subprocess.check_output([
    "git", "diff", "--cached", "--raw", "--no-abbrev", "--no-renames", "-z", "--", *sys.argv[1:]
]).split(b"\0")
for header, path in zip(records[0::2], records[1::2]):
    old, new, _, _, status = header.split()
    if status == b"D":
        continue
    if new != b"100644" or old not in (b":000000", b":100644"):
        raise SystemExit("Manual publication required for unsupported file mode: " + repr(path))
