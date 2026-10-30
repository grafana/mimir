#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Update the build image tag while requiring an immutable reference."""
import pathlib
import re
import sys


def update(path, reference, expected_repository):
    match = re.fullmatch(r"([^@\s]+):([^:@\s]+)@(sha256:[0-9a-f]{64})", reference)
    if match is None:
        raise ValueError("expected repository:tag@sha256:digest")
    image, tag, digest = match.groups()
    source = path.read_text()
    if image != expected_repository:
        raise ValueError("reference must use the configured build repository")
    result, count = re.subn(r"^LATEST_BUILD_IMAGE_TAG \?= .+$", lambda _: "LATEST_BUILD_IMAGE_TAG ?= " + tag + "@" + digest, source, flags=re.M)
    if count != 1:
        raise ValueError("expected exactly one build image tag assignment")
    if result != source:
        path.write_text(result)


if __name__ == "__main__":
    update(pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3])
