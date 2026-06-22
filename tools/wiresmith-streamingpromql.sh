#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Regenerates the streamingpromql wiresmith proto cluster
# (pkg/streamingpromql/{types,planning,planning/core,optimize/plan/*}).
#
# The cluster's protos import each other by Go module path
# (github.com/grafana/mimir/pkg/streamingpromql/...). wiresmith resolves imports
# by their import-statement path under --proto_path, so we stage a temporary
# tree mirroring the module-path layout and emit the whole cluster in one run.
# mimir.proto (already wiresmith) and operators/functions/functions.proto (an
# enum-only gogo leaf) are staged import-only and pinned with -M to their real
# Go import paths.

set -eu -o pipefail

MODULE=github.com/grafana/mimir
P=${MODULE}/pkg
STAGE=.streamingpromql-stage
OUT=.streamingpromql-out

# Cluster protos that are emitted (relative to the repo root).
EMIT_PROTOS=(
	pkg/streamingpromql/types/types.proto
	pkg/streamingpromql/planning/plan.proto
	pkg/streamingpromql/planning/core/core.proto
	pkg/streamingpromql/optimize/plan/commonsubexpressionelimination/node.proto
	pkg/streamingpromql/optimize/plan/rangevectorsplitting/node.proto
	pkg/streamingpromql/optimize/plan/rangevectorsplitting/functions.proto
	pkg/streamingpromql/optimize/plan/multiaggregation/node.proto
	pkg/streamingpromql/optimize/plan/splitandcache/node.proto
	pkg/streamingpromql/optimize/plan/remoteexec/node.proto
)

# Imported-but-not-emitted protos, staged so their cross-file references resolve.
IMPORT_PROTOS=(
	pkg/mimirpb/mimir.proto
	pkg/streamingpromql/operators/functions/functions.proto
)

rm -rf "${STAGE}" "${OUT}"

stage_proto() {
	local rel="$1"
	mkdir -p "${STAGE}/${P}/$(dirname "${rel#pkg/}")"
	cp "${rel}" "${STAGE}/${P}/${rel#pkg/}"
}

for proto in "${EMIT_PROTOS[@]}" "${IMPORT_PROTOS[@]}"; do
	stage_proto "${proto}"
done

# Pin every staged proto's Go import path to its real package path. wiresmith
# resolves imports by import-statement path, and -M overrides the emitted import
# string so the cluster's cross-references (and the gogo importers) line up.
M_FLAGS=()
for proto in "${EMIT_PROTOS[@]}" "${IMPORT_PROTOS[@]}"; do
	M_FLAGS+=(-M "${P}/${proto#pkg/}=${P}/$(dirname "${proto#pkg/}")")
done

EMIT_ARGS=()
for proto in "${EMIT_PROTOS[@]}"; do
	EMIT_ARGS+=("./${STAGE}/${P}/${proto#pkg/}")
done

mkdir -p "${OUT}"
wiresmith --proto_path="./${STAGE}" --out="./${OUT}" --module="${MODULE}" \
	"${M_FLAGS[@]}" \
	"${EMIT_ARGS[@]}"

# Copy the generated set (X.pb.go + the _compare/_util siblings) back
# into the real package directories.
for proto in "${EMIT_PROTOS[@]}"; do
	dir=$(dirname "${proto}")
	base=$(basename "${proto}" .proto)
	for sfx in .pb.go _compare.pb.go _util.pb.go; do
		cp "${OUT}/${P}/${dir#pkg/}/${base}${sfx}" "${dir}/${base}${sfx}"
	done
done

rm -rf "${STAGE}" "${OUT}"
