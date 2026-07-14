#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Regenerates one wiresmith proto cluster from Mimir's own gogo->wiresmith
# migration. Consolidates what were previously five near-identical scripts
# (tools/wiresmith-{streamingpromql,cqa2,cqa3,cqa4,cqa5}.sh, one per migration
# batch) into a single script parameterized by cluster name; see git history
# for the pre-consolidation scripts and their per-batch commit messages.
#
# Usage: ./tools/wiresmith-regen.sh <cluster>
#   where <cluster> is one of: streamingpromql cqa2 cqa3 cqa4 cqa5
#
# wiresmith resolves cross-file imports by their import-statement path under
# --proto_path. Mimir's protos live nested under pkg/** and import each other
# by full Go module path, so each cluster is staged into a temporary tree
# mirroring the module-path layout, emitted in one wiresmith invocation with
# -M pinning every staged proto to its real Go import path, and the outputs
# are copied back into the real package directories.

set -eu -o pipefail

MODULE=github.com/grafana/mimir
P=${MODULE}/pkg

# regen_cluster STAGE OUT
# Reads the EMIT_PROTOS and IMPORT_PROTOS arrays (repo-relative paths). EMIT
# protos are passed positionally (emitted + their transitive imports
# resolved); IMPORT protos are staged import-only so cross-file references
# resolve. Every staged proto is -M-mapped to <module>/pkg/<dir>. IMPORT_PROTOS
# may be empty (clusters with no cross-module imports); ALL_PROTOS is built
# with a length guard because expanding an empty array via "${arr[@]}" under
# `set -u` is an unbound-variable error on some bash versions.
regen_cluster() {
	local stage="$1" out="$2"
	rm -rf "${stage}" "${out}"

	local ALL_PROTOS=("${EMIT_PROTOS[@]}")
	if [ "${#IMPORT_PROTOS[@]}" -gt 0 ]; then
		ALL_PROTOS+=("${IMPORT_PROTOS[@]}")
	fi

	stage_proto() {
		local rel="$1"
		local dest="${stage}/${P}/${rel#pkg/}"
		mkdir -p "$(dirname "${dest}")"
		cp "${rel}" "${dest}"
	}

	local proto
	for proto in "${ALL_PROTOS[@]}"; do
		stage_proto "${proto}"
	done

	local M_FLAGS=()
	for proto in "${ALL_PROTOS[@]}"; do
		M_FLAGS+=(-M "${P}/${proto#pkg/}=${P}/$(dirname "${proto#pkg/}")")
	done

	local EMIT_ARGS=()
	for proto in "${EMIT_PROTOS[@]}"; do
		EMIT_ARGS+=("./${stage}/${P}/${proto#pkg/}")
	done

	mkdir -p "${out}"
	wiresmith --proto_path="./${stage}" --out="./${out}" --module="${MODULE}" \
		"${M_FLAGS[@]}" \
		"${EMIT_ARGS[@]}"

	local dir base sfx src grpc_src
	for proto in "${EMIT_PROTOS[@]}"; do
		dir=$(dirname "${proto}")
		base=$(basename "${proto}" .proto)
		for sfx in .pb.go _compare.pb.go _util.pb.go; do
			src="${out}/${P}/${dir#pkg/}/${base}${sfx}"
			if [ -f "${src}" ]; then
				cp "${src}" "${dir}/${base}${sfx}"
			fi
		done
		grpc_src="${out}/${P}/${dir#pkg/}/${base}_grpc.pb.go"
		if [ -f "${grpc_src}" ]; then
			cp "${grpc_src}" "${dir}/${base}_grpc.pb.go"
		fi
	done

	rm -rf "${stage}" "${out}"
}

cluster="${1:-}"
case "${cluster}" in

streamingpromql)
	# pkg/streamingpromql/{types,planning,planning/core,optimize/plan/*}. The
	# cluster's protos import each other by Go module path. mimir.proto and
	# operators/functions/functions.proto (an enum-only gogo leaf) are staged
	# import-only and pinned with -M to their real Go import paths.
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
	IMPORT_PROTOS=(
		pkg/mimirpb/mimir.proto
		pkg/streamingpromql/operators/functions/functions.proto
	)
	regen_cluster .wiresmith-regen-streamingpromql-stage .wiresmith-regen-streamingpromql-out
	;;

cqa2)
	# pkg/querier/querierpb/querier.proto + rangevectorsplitting/cache/cache.proto
	# (cqa.2): cache.proto imports querier.proto (same cluster); querier.proto
	# imports mimir.proto, stats.proto, plan.proto, types.proto (all previously
	# migrated wiresmith protos).
	EMIT_PROTOS=(
		pkg/querier/querierpb/querier.proto
		pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache/cache.proto
	)
	IMPORT_PROTOS=(
		pkg/mimirpb/mimir.proto
		pkg/querier/stats/stats.proto
		pkg/streamingpromql/planning/plan.proto
		pkg/streamingpromql/types/types.proto
	)
	regen_cluster .wiresmith-regen-cqa2-stage .wiresmith-regen-cqa2-out
	;;

cqa3)
	# pkg/util/httpgrpcpb (local wiresmith copy of dskit httpgrpc types) +
	# scheduler/schedulerpb + ruler + blockbuilder/schedulerpb +
	# compactor/scheduler/compactorschedulerpb + frontend/v2/frontendv2pb
	# (cqa.3): the dskit httpgrpc.proto is gogo-annotated and not directly
	# migratable, so scheduler.proto and frontendv2pb/frontend.proto import
	# the local httpgrpcpb bridge instead.
	EMIT_PROTOS=(
		pkg/util/httpgrpcpb/httpgrpcpb.proto
		pkg/scheduler/schedulerpb/scheduler.proto
		pkg/ruler/ruler.proto
		pkg/blockbuilder/schedulerpb/scheduler.proto
		pkg/compactor/scheduler/compactorschedulerpb/compactorscheduler.proto
		pkg/frontend/v2/frontendv2pb/frontend.proto
	)
	IMPORT_PROTOS=(
		pkg/mimirpb/mimir.proto
		pkg/querier/stats/stats.proto
		pkg/ruler/rulespb/rules.proto
		pkg/querier/querierpb/querier.proto
		pkg/streamingpromql/planning/plan.proto
		pkg/streamingpromql/types/types.proto
	)
	regen_cluster .wiresmith-regen-cqa3-stage .wiresmith-regen-cqa3-out
	;;

cqa4)
	# The final full switch of mimir's own protos (cqa.4) -- three independent
	# clusters, each emitted in its own invocation so intra-cluster imports
	# resolve without cross-cluster staging collisions.

	# splitandcache/cache.proto imports querierpb + streamingpromql/types
	# (both wiresmith); those transitively pull in mimir/stats/plan, staged
	# import-only.
	EMIT_PROTOS=(
		pkg/streamingpromql/optimize/plan/splitandcache/cache.proto
	)
	IMPORT_PROTOS=(
		pkg/querier/querierpb/querier.proto
		pkg/streamingpromql/types/types.proto
		pkg/mimirpb/mimir.proto
		pkg/querier/stats/stats.proto
		pkg/streamingpromql/planning/plan.proto
	)
	regen_cluster .wiresmith-regen-cqa4-splitandcache-stage .wiresmith-regen-cqa4-splitandcache-out

	# storepb/{types,rpc,cache} + hintspb/hints: types.proto is the leaf
	# (imports mimir); rpc.proto and hintspb/hints.proto import types;
	# cache.proto imports mimir. All four emit in one invocation so the
	# intra-cluster imports resolve. The deprecated Any "hints" fields carry
	# storepb.AnyAdapter (gogo types.Any) on the direct fields and native
	# anypb on the SeriesResponse oneof variant (customtype is rejected on
	# oneof).
	EMIT_PROTOS=(
		pkg/storegateway/storepb/types.proto
		pkg/storegateway/storepb/rpc.proto
		pkg/storegateway/storepb/cache.proto
		pkg/storegateway/hintspb/hints.proto
	)
	IMPORT_PROTOS=(
		pkg/mimirpb/mimir.proto
	)
	regen_cluster .wiresmith-regen-cqa4-storepb-stage .wiresmith-regen-cqa4-storepb-out

	# querymiddleware/model.proto imports mimir + stats (both wiresmith). It
	# is JSON-sensitive: every field carries an explicit jsontag matching the
	# gogoproto output, and Extent.response bridges to gogo types.Any via
	# querymiddleware.AnyAdapter.
	EMIT_PROTOS=(
		pkg/frontend/querymiddleware/model.proto
	)
	IMPORT_PROTOS=(
		pkg/mimirpb/mimir.proto
		pkg/querier/stats/stats.proto
	)
	regen_cluster .wiresmith-regen-cqa4-model-stage .wiresmith-regen-cqa4-model-out
	;;

cqa5)
	# Standalone protos (cqa.5): ingester/client + distributorpb import
	# mimir.proto by Go module path, so they're staged together; usagetracker
	# and indexheader have no cross-module imports beyond wiresmith/options.proto
	# (which wiresmith bundles), so they go through the same regen_cluster
	# mechanism below with an empty IMPORT_PROTOS.
	EMIT_PROTOS=(
		pkg/ingester/client/ingester.proto
		pkg/distributor/distributorpb/distributor.proto
	)
	IMPORT_PROTOS=(
		pkg/mimirpb/mimir.proto
	)
	regen_cluster .wiresmith-regen-cqa5-stage .wiresmith-regen-cqa5-out

	# distributor.proto is a pure-service proto with no message types of its
	# own. Wiresmith emits only distributor_grpc.pb.go + distributor_util.pb.go;
	# the distributor.pb.go that gogo used to emit contained only gRPC stubs
	# (now in distributor_grpc.pb.go) and must be replaced with a minimal stub
	# so that make's grouped &: rule (which lists distributor.pb.go as a
	# target) sees the file exist and the generic %.pb.go: %.proto gogo rule
	# doesn't fire.
	cat >"pkg/distributor/distributorpb/distributor.pb.go" <<'EOF'
// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributorpb/distributor.proto
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

// Code generated by wiresmith. DO NOT EDIT.
// source: github.com/grafana/mimir/pkg/distributor/distributorpb/distributor.proto

// distributor.proto defines only a gRPC service (no message types of its own).
// All generated code lives in distributor_grpc.pb.go and distributor_util.pb.go.

package distributorpb
EOF

	EMIT_PROTOS=(
		pkg/usagetracker/usagetrackerpb/usagetracker.proto
	)
	IMPORT_PROTOS=()
	regen_cluster .wiresmith-regen-cqa5-usagetracker-stage .wiresmith-regen-cqa5-usagetracker-out

	EMIT_PROTOS=(
		pkg/storage/indexheader/indexheaderpb/sparse.proto
	)
	IMPORT_PROTOS=()
	regen_cluster .wiresmith-regen-cqa5-indexheader-stage .wiresmith-regen-cqa5-indexheader-out
	;;

*)
	echo "usage: $0 {streamingpromql|cqa2|cqa3|cqa4|cqa5}" >&2
	exit 1
	;;

esac
