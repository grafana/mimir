#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Regenerates the cqa.4 wiresmith proto clusters -- the final gogo->wiresmith
# switch of mimir's own protos:
#   - pkg/streamingpromql/optimize/plan/splitandcache/cache.proto
#   - pkg/storegateway/storepb/{types,rpc,cache}.proto
#     + pkg/storegateway/hintspb/hints.proto
#   - pkg/frontend/querymiddleware/model.proto
#
# wiresmith resolves imports by their import-statement path under --proto_path,
# so each cluster is staged into a temporary tree mirroring the module-path
# layout, emitted in one invocation with -M pinning every staged proto to its
# real Go import path (path.Base = Go package name), and the outputs copied
# back into the real package directories.

set -eu -o pipefail

MODULE=github.com/grafana/mimir
P=${MODULE}/pkg

# regen_cluster STAGE OUT
# Reads the EMIT_PROTOS and IMPORT_PROTOS arrays (repo-relative paths). EMIT
# protos are passed positionally (emitted + their transitive imports resolved);
# IMPORT protos are staged import-only so cross-file references resolve. Every
# staged proto is -M-mapped to <module>/pkg/<dir>.
regen_cluster() {
	local stage="$1" out="$2"
	rm -rf "${stage}" "${out}"

	stage_proto() {
		local rel="$1"
		local dest="${stage}/${P}/${rel#pkg/}"
		mkdir -p "$(dirname "${dest}")"
		cp "${rel}" "${dest}"
	}

	local proto
	for proto in "${EMIT_PROTOS[@]}" "${IMPORT_PROTOS[@]}"; do
		stage_proto "${proto}"
	done

	local M_FLAGS=()
	for proto in "${EMIT_PROTOS[@]}" "${IMPORT_PROTOS[@]}"; do
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

# --- splitandcache cluster --------------------------------------------------
# cache.proto imports querierpb + streamingpromql/types (both wiresmith); those
# transitively pull in mimir/stats/plan, staged import-only.
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
regen_cluster .cqa4-splitandcache-stage .cqa4-splitandcache-out
