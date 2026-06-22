#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Regenerates the cqa.2 wiresmith proto cluster:
#   - pkg/querier/querierpb/querier.proto
#   - pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache/cache.proto
#
# These protos form a dependency cluster:
#   cache.proto imports querier.proto (same cluster)
#   querier.proto imports mimir.proto, stats.proto, plan.proto, types.proto
#     (all previously migrated wiresmith protos)
#
# Note: pkg/frontend/v2/frontendv2pb/frontend.proto was part of this cluster
# before cqa.3. It has since been moved to wiresmith-cqa3.sh, which uses the
# local httpgrpcpb bridge instead of the dskit httpgrpc dependency.
#
# wiresmith resolves imports by their import-statement path under --proto_path,
# so we stage a temporary tree mirroring the module-path layout.

set -eu -o pipefail

MODULE=github.com/grafana/mimir
P=${MODULE}/pkg
STAGE=.cqa2-stage
OUT=.cqa2-out

# Cluster protos that are emitted (relative to the repo root).
EMIT_PROTOS=(
	pkg/querier/querierpb/querier.proto
	pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache/cache.proto
)

# Imported-but-not-emitted wiresmith protos (staged for cross-file reference resolution).
IMPORT_PROTOS=(
	pkg/mimirpb/mimir.proto
	pkg/querier/stats/stats.proto
	pkg/streamingpromql/planning/plan.proto
	pkg/streamingpromql/types/types.proto
)

rm -rf "${STAGE}" "${OUT}"

stage_proto() {
	local rel="$1"
	local dest="${STAGE}/${P}/${rel#pkg/}"
	mkdir -p "$(dirname "${dest}")"
	cp "${rel}" "${dest}"
}

for proto in "${EMIT_PROTOS[@]}"; do
	stage_proto "${proto}"
done

for proto in "${IMPORT_PROTOS[@]}"; do
	stage_proto "${proto}"
done

# Build -M flags for all staged protos (emitted + imported).
M_FLAGS=()
for proto in "${EMIT_PROTOS[@]}"; do
	M_FLAGS+=(-M "${P}/${proto#pkg/}=${P}/$(dirname "${proto#pkg/}")")
done
for proto in "${IMPORT_PROTOS[@]}"; do
	# stats.proto has a non-default go_package that includes the full path; use
	# the same approach as the rules.proto staging (package = import path of the
	# parent dir).
	M_FLAGS+=(-M "${P}/${proto#pkg/}=${P}/$(dirname "${proto#pkg/}")")
done

# Positional emit args (paths under STAGE).
EMIT_ARGS=()
for proto in "${EMIT_PROTOS[@]}"; do
	EMIT_ARGS+=("./${STAGE}/${P}/${proto#pkg/}")
done

mkdir -p "${OUT}"
wiresmith --proto_path="./${STAGE}" --out="./${OUT}" --module="${MODULE}" \
	"${M_FLAGS[@]}" \
	"${EMIT_ARGS[@]}"

# Copy the generated set (X.pb.go + the _compare/_util siblings, and the
# gRPC _grpc.pb.go from service-bearing protos) back into the real package
# directories.
for proto in "${EMIT_PROTOS[@]}"; do
	dir=$(dirname "${proto}")
	base=$(basename "${proto}" .proto)
	for sfx in .pb.go _compare.pb.go _util.pb.go; do
		src="${OUT}/${P}/${dir#pkg/}/${base}${sfx}"
		if [ -f "${src}" ]; then
			cp "${src}" "${dir}/${base}${sfx}"
		fi
	done
	# Copy the gRPC stub file if wiresmith emitted one (service-bearing protos).
	grpc_src="${OUT}/${P}/${dir#pkg/}/${base}_grpc.pb.go"
	if [ -f "${grpc_src}" ]; then
		cp "${grpc_src}" "${dir}/${base}_grpc.pb.go"
	fi
done

rm -rf "${STAGE}" "${OUT}"
