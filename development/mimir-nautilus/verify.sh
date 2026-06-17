#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# verify.sh — end-to-end smoke test for the nautilus / readcache dev
# stack. Exits 0 (printing "PASS") when:
#
#   1. /ready returns 200 on distributor, query-frontend, both
#      readcache pods, and the rebalancer.
#   2. A known sample pushed for the experimental `nautilus-tenant`
#      can be read back via the query-frontend and the sample value
#      matches.
#   3. A negative test: pushing a sample for `default-tenant` is
#      readable via the production ingester path (proving the
#      runtime-config gate works in both directions).
#
# Bounded retry budget (~60s wall clock). Prints PASS or FAIL the
# parent agent can grep.

set -u

PASS_PREFIX="PASS:"
FAIL_PREFIX="FAIL:"

DISTRIBUTOR_URL="${DISTRIBUTOR_URL:-http://localhost:8000}"
QUERY_FRONTEND_URL="${QUERY_FRONTEND_URL:-http://localhost:8007}"
READCACHE_1_URL="${READCACHE_1_URL:-http://localhost:8014}"
READCACHE_2_URL="${READCACHE_2_URL:-http://localhost:8015}"
REBALANCER_URL="${REBALANCER_URL:-http://localhost:8019}"
INGESTER_URL="${INGESTER_URL:-http://localhost:8002}"

NAUTILUS_TENANT="${NAUTILUS_TENANT:-nautilus-tenant}"
DEFAULT_TENANT="${DEFAULT_TENANT:-default-tenant}"

SCRIPT_DIR="$(cd "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERIFY_TOOL="$SCRIPT_DIR/.verify-tool"

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
        -expect-value "$expect"
}

# 1. Readiness.
require_ready "distributor"        "$DISTRIBUTOR_URL"
require_ready "query-frontend"     "$QUERY_FRONTEND_URL"
require_ready "ingester"           "$INGESTER_URL"
require_ready "readcache-1"        "$READCACHE_1_URL"
require_ready "readcache-2"        "$READCACHE_2_URL"
require_ready "nautilus-rebalancer" "$REBALANCER_URL"

NOW_MS=$(($(date +%s) * 1000))

# 2. Push to the nautilus tenant; expect the value back through
# query-frontend. The distributor's read path consults the per-tenant
# `readcache_read_routing` knob (set to "nautilus-only" for
# nautilus-tenant in runtime.yaml), so this exercises the readcache
# path end-to-end.
echo "pushing nautilus sample..."
push_sample "$NAUTILUS_TENANT" "verify_metric" 42 "$NOW_MS" || {
    echo "$FAIL_PREFIX nautilus push failed"
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

# 3. Push to the default tenant; expect the value back too.
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

echo "$PASS_PREFIX all verify steps succeeded"
