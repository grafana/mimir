#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# verify.sh — end-to-end smoke test for the nautilus / readcache dev
# stack. Exits 0 (printing "PASS") when:
#
#   1. /ready returns 200 on distributor, query-frontend, both
#      readcache pods, and the rebalancer.
#   2. Known samples pushed for two experimental Nautilus tenants can
#      be read back independently via the query-frontend.
#   3. Both Nautilus tenants acquire valid full-space assignments
#      spanning multiple Kafka partitions.
#   4. A negative test: pushing a sample for `default-tenant` is
#      readable via the production ingester path (proving the
#      runtime-config gate works in both directions).
#
# Bounded retry budget (~60s wall clock). Prints PASS or FAIL the
# parent agent can grep.

set -u -o pipefail

PASS_PREFIX="PASS:"
FAIL_PREFIX="FAIL:"

DISTRIBUTOR_URL="${DISTRIBUTOR_URL:-http://localhost:8000}"
QUERY_FRONTEND_URL="${QUERY_FRONTEND_URL:-http://localhost:8007}"
READCACHE_1_URL="${READCACHE_1_URL:-http://localhost:8014}"
READCACHE_2_URL="${READCACHE_2_URL:-http://localhost:8015}"
REBALANCER_URL="${REBALANCER_URL:-http://localhost:8019}"
INGESTER_URL="${INGESTER_URL:-http://localhost:8002}"

NAUTILUS_TENANT="${NAUTILUS_TENANT:-nautilus-tenant}"
NAUTILUS_TENANT_B="${NAUTILUS_TENANT_B:-nautilus-tenant-b}"
DEFAULT_TENANT="${DEFAULT_TENANT:-default-tenant}"

SCRIPT_DIR="$(cd "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERIFY_TOOL="$SCRIPT_DIR/.verify-tool"
ASSIGNMENT_CHECK_OUTPUT="$SCRIPT_DIR/.assignment-check-output"

docker_compose() {
    if [ -x "$(command -v docker-compose)" ]; then
        docker-compose "$@"
    else
        docker compose "$@"
    fi
}

# Build the helper once per verify.sh invocation so it picks up any
# Go changes the iteration loop has made. We use -mod=vendor so this
# stays hermetic against the developer's GOPATH cache.
(
    cd "$REPO_ROOT" &&
    go build -mod=vendor -o "$VERIFY_TOOL" ./development/mimir-nautilus/verify-tool
) || {
    echo "$FAIL_PREFIX could not build verify-tool"
    exit 1
}

retry_until() {
    local deadline=$(( $(date +%s) + ${RETRY_BUDGET:-60} ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if "$@"; then
            return 0
        fi
        sleep 1
    done
    return 1
}

probe_ready() {
    local url="$1"
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$url/ready" 2>/dev/null)" = "200" ]
}

require_ready() {
    local name="$1" url="$2"
    if retry_until probe_ready "$url"; then
        echo "$PASS_PREFIX $name is ready ($url)"
    else
        echo "$FAIL_PREFIX $name not ready within budget ($url)"
        exit 1
    fi
}

push_sample() {
    local tenant="$1" metric="$2" value="$3" ts_ms="$4"
    "$VERIFY_TOOL" push \
        -url "$DISTRIBUTOR_URL" \
        -tenant "$tenant" \
        -metric "$metric" \
        -value "$value" \
        -timestamp-ms "$ts_ms"
}

query_metric_expect() {
    local tenant="$1" metric="$2" expect="$3"
    "$VERIFY_TOOL" query \
        -url "$QUERY_FRONTEND_URL" \
        -tenant "$tenant" \
        -query "$metric" \
        -expect-value "$expect" \
        -expect-result-count 1
}

push_spike() {
    local tenant="$1" metric="$2" count="$3" ts_ms="$4"
    "$VERIFY_TOOL" spike \
        -url "$DISTRIBUTOR_URL" \
        -tenant "$tenant" \
        -metric "$metric" \
        -metrics 32 \
        -count "$count" \
        -timestamp-ms "$ts_ms" \
        -id-prefix "${tenant}-"
}

multitenant_assignment_ready() {
    local min_partitions="$1"
    docker_compose -f "$SCRIPT_DIR/docker-compose.yml" exec -T nautilus-rebalancer \
        cat /data/nautilus-rebalancer/assignment-log.json |
        "$VERIFY_TOOL" assignment-log \
            -file - \
            -tenants "$NAUTILUS_TENANT,$NAUTILUS_TENANT_B" \
            -min-partitions "$min_partitions" >"$ASSIGNMENT_CHECK_OUTPUT" 2>&1
}

# 1. Readiness.
require_ready "distributor"        "$DISTRIBUTOR_URL"
require_ready "query-frontend"     "$QUERY_FRONTEND_URL"
require_ready "ingester"           "$INGESTER_URL"
require_ready "readcache-1"        "$READCACHE_1_URL"
require_ready "readcache-2"        "$READCACHE_2_URL"
require_ready "nautilus-rebalancer" "$REBALANCER_URL"

NOW_MS=$(($(date +%s) * 1000))

# 2. Push the same metric identity with different values to two Nautilus
# tenants. Reading each value back proves both tenants traverse the readcache
# path without sharing series.
echo "pushing samples for two nautilus tenants..."
push_sample "$NAUTILUS_TENANT" "verify_metric" 42 "$NOW_MS" || {
    echo "$FAIL_PREFIX first nautilus tenant push failed"
    exit 1
}
push_sample "$NAUTILUS_TENANT_B" "verify_metric" 84 "$NOW_MS" || {
    echo "$FAIL_PREFIX second nautilus tenant push failed"
    exit 1
}

# Allow time for the Kafka roundtrip into the readcache partitionTSDB
# head and for the readcache slicer to pick up the new lease if it
# hasn't already.
echo "waiting for sample to land in readcache..."
if ! retry_until query_metric_expect "$NAUTILUS_TENANT" "verify_metric" "42"; then
    echo "$FAIL_PREFIX nautilus-tenant query did not return 42 within budget"
    exit 1
fi
echo "$PASS_PREFIX nautilus-tenant query returned the expected sample"

if ! retry_until query_metric_expect "$NAUTILUS_TENANT_B" "verify_metric" "84"; then
    echo "$FAIL_PREFIX nautilus-tenant-b query did not return 84 within budget"
    exit 1
fi
echo "$PASS_PREFIX nautilus-tenant-b query returned the expected sample"

# 3. Wait for the bootstrap round to configure tenant ranges on partition 0
# before generating load. Samples ingested before SetHashRanges cannot be
# attributed to a range and therefore cannot drive the slicer.
echo "waiting for both tenants to bootstrap on partition 0..."
if ! RETRY_BUDGET="${REBALANCE_RETRY_BUDGET:-180}" retry_until multitenant_assignment_ready 1; then
    echo "$FAIL_PREFIX tenants did not acquire valid bootstrap assignments"
    cat "$ASSIGNMENT_CHECK_OUTPUT" 2>/dev/null || true
    exit 1
fi
echo "$PASS_PREFIX both nautilus tenants acquired valid bootstrap assignments"

# Generate enough tenant-local load for the slicer to move ranges off the
# bootstrap partition. Then inspect the durable authoritative log and require
# each tenant to independently tile the full uint32 space across at least two
# partitions.
echo "pushing multi-tenant load to trigger rebalancing..."
push_spike "$NAUTILUS_TENANT" "verify_spike_a" 6000 "$NOW_MS" || {
    echo "$FAIL_PREFIX first nautilus tenant spike failed"
    exit 1
}
push_spike "$NAUTILUS_TENANT_B" "verify_spike_b" 4000 "$NOW_MS" || {
    echo "$FAIL_PREFIX second nautilus tenant spike failed"
    exit 1
}

echo "waiting for independent multi-partition tenant assignments..."
if ! RETRY_BUDGET="${REBALANCE_RETRY_BUDGET:-180}" retry_until multitenant_assignment_ready 2; then
    echo "$FAIL_PREFIX tenants did not acquire valid multi-partition assignments"
    cat "$ASSIGNMENT_CHECK_OUTPUT" 2>/dev/null || true
    exit 1
fi
cat "$ASSIGNMENT_CHECK_OUTPUT"
echo "$PASS_PREFIX both nautilus tenants have valid multi-partition assignments"

# Verify writes issued after rebalancing remain independently queryable.
AFTER_REBALANCE_MS=$(($(date +%s) * 1000))
push_sample "$NAUTILUS_TENANT" "verify_after_rebalance" 142 "$AFTER_REBALANCE_MS" || {
    echo "$FAIL_PREFIX first post-rebalance push failed"
    exit 1
}
push_sample "$NAUTILUS_TENANT_B" "verify_after_rebalance" 184 "$AFTER_REBALANCE_MS" || {
    echo "$FAIL_PREFIX second post-rebalance push failed"
    exit 1
}
if ! retry_until query_metric_expect "$NAUTILUS_TENANT" "verify_after_rebalance" "142"; then
    echo "$FAIL_PREFIX first post-rebalance query failed"
    exit 1
fi
if ! retry_until query_metric_expect "$NAUTILUS_TENANT_B" "verify_after_rebalance" "184"; then
    echo "$FAIL_PREFIX second post-rebalance query failed"
    exit 1
fi
echo "$PASS_PREFIX post-rebalance writes remained tenant-isolated"

# 4. Push to the default tenant; expect the value back too.
echo "pushing default sample..."
push_sample "$DEFAULT_TENANT" "verify_metric_default" 7 "$NOW_MS" || {
    echo "$FAIL_PREFIX default push failed"
    exit 1
}
if ! retry_until query_metric_expect "$DEFAULT_TENANT" "verify_metric_default" "7"; then
    echo "$FAIL_PREFIX default-tenant query did not return 7 within budget"
    exit 1
fi
echo "$PASS_PREFIX default-tenant query returned the expected sample"

rm -f "$ASSIGNMENT_CHECK_OUTPUT"
echo "$PASS_PREFIX all verify steps succeeded"
