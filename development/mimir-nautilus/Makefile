# SPDX-License-Identifier: AGPL-3.0-only
#
# Unlike development/mimir-ingest-storage this directory's
# docker-compose.yml is hand-written rather than rendered from a
# jsonnet template — the stack is small enough that the indirection
# adds more friction than it saves. The `check` target is a no-op
# stub kept so CI invocations match the other dev cells.

.DEFAULT_GOAL := check

.PHONY: check
check:
	@true
