#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Regenerates the cqa.3 wiresmith proto cluster:
#   - pkg/util/httpgrpcpb/httpgrpcpb.proto       (local wiresmith copy of dskit httpgrpc types)
#   - pkg/scheduler/schedulerpb/scheduler.proto   (httpgrpcpb + Any; two gRPC services)
#   - pkg/ruler/ruler.proto                       (mimir.proto + rulespb; gRPC service)
#   - pkg/blockbuilder/schedulerpb/scheduler.proto (plain service)
#   - pkg/compactor/scheduler/compactorschedulerpb/compactorscheduler.proto (plain service)
#   - pkg/frontend/v2/frontendv2pb/frontend.proto  (httpgrpcpb bridge + querierpb; gRPC service)
#
# The dskit httpgrpc.proto is gogo-annotated and not directly migratable.
# scheduler.proto and frontendv2pb/frontend.proto reference a local wiresmith
# copy (pkg/util/httpgrpcpb/) with conversion helpers. Those protos import
# httpgrpcpb instead of dskit httpgrpc; the -M flag maps the staged path to
# the real httpgrpcpb Go package.
#
# wiresmith resolves imports by their import-statement path under --proto_path,
# so we stage a temporary tree mirroring the module-path layout.

set -eu -o pipefail

MODULE=github.com/grafana/mimir
DSKIT=github.com/grafana/dskit
P=${MODULE}/pkg
STAGE=.cqa3-stage
OUT=.cqa3-out

# Cluster protos that are emitted (relative to the repo root).
EMIT_PROTOS=(
	pkg/util/httpgrpcpb/httpgrpcpb.proto
	pkg/scheduler/schedulerpb/scheduler.proto
	pkg/ruler/ruler.proto
	pkg/blockbuilder/schedulerpb/scheduler.proto
	pkg/compactor/scheduler/compactorschedulerpb/compactorscheduler.proto
	pkg/frontend/v2/frontendv2pb/frontend.proto
)

# Imported-but-not-emitted wiresmith protos (staged for cross-file reference resolution).
IMPORT_PROTOS=(
	pkg/mimirpb/mimir.proto
	pkg/querier/stats/stats.proto
	pkg/ruler/rulespb/rules.proto
	pkg/querier/querierpb/querier.proto
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

for proto in "${IMPORT_PROTOS[@]}"; do
	stage_proto "${proto}"
done

for proto in "${EMIT_PROTOS[@]}"; do
	stage_proto "${proto}"
done

# Build -M flags for emitted protos.
M_FLAGS=()
# httpgrpcpb
M_FLAGS+=(-M "${P}/util/httpgrpcpb/httpgrpcpb.proto=${P}/util/httpgrpcpb")
# scheduler schedulerpb
M_FLAGS+=(-M "${P}/scheduler/schedulerpb/scheduler.proto=${P}/scheduler/schedulerpb")
# ruler
M_FLAGS+=(-M "${P}/ruler/ruler.proto=${P}/ruler")
# blockbuilder schedulerpb
M_FLAGS+=(-M "${P}/blockbuilder/schedulerpb/scheduler.proto=${P}/blockbuilder/schedulerpb")
# compactorschedulerpb
M_FLAGS+=(-M "${P}/compactor/scheduler/compactorschedulerpb/compactorscheduler.proto=${P}/compactor/scheduler/compactorschedulerpb")
# frontendv2pb
M_FLAGS+=(-M "${P}/frontend/v2/frontendv2pb/frontend.proto=${P}/frontend/v2/frontendv2pb")

# -M flags for imported-but-not-emitted wiresmith protos.
for proto in "${IMPORT_PROTOS[@]}"; do
	M_FLAGS+=(-M "${P}/${proto#pkg/}=${P}/$(dirname "${proto#pkg/}")")
done

# Positional emit args (paths under STAGE).
EMIT_ARGS=(
	"./${STAGE}/${P}/util/httpgrpcpb/httpgrpcpb.proto"
	"./${STAGE}/${P}/scheduler/schedulerpb/scheduler.proto"
	"./${STAGE}/${P}/ruler/ruler.proto"
	"./${STAGE}/${P}/blockbuilder/schedulerpb/scheduler.proto"
	"./${STAGE}/${P}/compactor/scheduler/compactorschedulerpb/compactorscheduler.proto"
	"./${STAGE}/${P}/frontend/v2/frontendv2pb/frontend.proto"
)

mkdir -p "${OUT}"
wiresmith --proto_path="./${STAGE}" --out="./${OUT}" --module="${MODULE}" \
	"${M_FLAGS[@]}" \
	"${EMIT_ARGS[@]}"

# Copy generated files back into the real package directories.
copy_back() {
	local src_dir="$1"
	local dst_dir="$2"
	for sfx in .pb.go _compare.pb.go _util.pb.go _grpc.pb.go; do
		for f in "${OUT}/${src_dir}"/*"${sfx}"; do
			# The glob may expand to the literal pattern when no files match;
			# test -f guards against that without triggering set -e.
			if [ -f "${f}" ]; then
				cp "${f}" "${dst_dir}/"
			fi
		done
	done
}

copy_back "${P}/util/httpgrpcpb" "pkg/util/httpgrpcpb"
copy_back "${P}/scheduler/schedulerpb" "pkg/scheduler/schedulerpb"
copy_back "${P}/ruler" "pkg/ruler"
copy_back "${P}/blockbuilder/schedulerpb" "pkg/blockbuilder/schedulerpb"
copy_back "${P}/compactor/scheduler/compactorschedulerpb" "pkg/compactor/scheduler/compactorschedulerpb"
copy_back "${P}/frontend/v2/frontendv2pb" "pkg/frontend/v2/frontendv2pb"

rm -rf "${STAGE}" "${OUT}"
