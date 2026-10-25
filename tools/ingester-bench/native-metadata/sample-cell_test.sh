#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Tests sample-cell.sh against a kubectl that only pretends, with dummy tools: it creates a Job of its own, deletes
# only the Job it created, and keeps the report. Nothing is built, and no cluster is reached.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$tmp/fake" "$tmp/tools/amd64" "$tmp/tools/arm64" "$tmp/out"
for arch in amd64 arm64; do
  for tool in kafkafetch kafkatool ingester-bench; do
    echo "dummy $arch $tool" > "$tmp/tools/$arch/$tool"
  done
done
# The fake kubectl logs each call to FAKE_LOG, keeps each Job as a file in FAKE_JOBS, and fails the step FAKE_FAIL
# names: create or exec.
cat > "$tmp/fake/kubectl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
args=("$@")
while [[ ${1:-} == --context || ${1:-} == -n ]]; do shift 2; done
echo "$*" >> "$FAKE_LOG"
case "$1 ${2:-}" in
  "create -f")
    manifest="$(cat)"
    [[ ${FAKE_FAIL:-} == create ]] && { echo "error: create refused" >&2; exit 1; }
    name="$(sed -n 's/^  name: //p' <<< "$manifest" | head -1)"
    if [[ -e "$FAKE_JOBS/$name" ]]; then echo "AlreadyExists" >&2; exit 1; fi
    touch "$FAKE_JOBS/$name"
    ;;
  "get pod")
    name="${3#job-name=}"; name="${name#-l }"
    for a in "${args[@]}"; do [[ $a == job-name=* ]] && name="${a#job-name=}"; done
    if [[ " $* " == *" -o name "* ]]; then echo "pod/$name-pod"; else echo "$name-pod"; fi
    ;;
  "wait "* | "cp "*) ;;
  "exec "*)
    if [[ " $* " == *" uname -m"* ]]; then echo aarch64; exit 0; fi
    [[ ${FAKE_FAIL:-} == exec ]] && { echo "error: the sample failed" >&2; exit 1; }
    echo '{"records": 6000, "series_per_record": 30}'
    ;;
  "delete job") rm -f "$FAKE_JOBS/$3" ;;
  *) echo "unexpected kubectl call: $*" >&2; exit 1 ;;
esac
EOF
chmod +x "$tmp/fake/kubectl"

run() {
  PATH="$tmp/fake:$PATH" FAKE_LOG="$tmp/log" FAKE_JOBS="$tmp/jobs" SAMPLE_CELL_BIN="$tmp/tools" SAMPLE_CELL_OUT="$tmp/out" \
    KUBE_CONTEXT=test NAMESPACE=test KAFKA_ADDRESS=kafka:9092 KAFKA_CLIENT_ID=test KAFKA_SASL_USERNAME=u KAFKA_SASL_PASSWORD=p \
    "$@" "$here/sample-cell.sh" > "$tmp/stdout" 2> "$tmp/stderr"
}
fail() { echo "FAIL: $*"; cat "$tmp/log" 2>/dev/null; exit 1; }
reset() { rm -rf "$tmp/log" "$tmp/jobs" "$tmp/out"/*; mkdir -p "$tmp/jobs"; }
jobs_left() { ls "$tmp/jobs"; }

# A Job that couldn't be created isn't deleted: a Job of that name, if any, isn't this run's.
reset
if run env FAKE_FAIL=create; then fail "a failed create succeeded"; fi
grep -q "^create -f -" "$tmp/log" || fail "the Job wasn't created with create"
grep -q "^delete" "$tmp/log" && fail "a Job this run didn't create was deleted"
echo "ok: nothing deleted when the create fails"

# A Job that was created goes, also when the sample fails, and only it, without waiting for it to complete.
reset
if run env FAKE_FAIL=exec; then fail "a failed sample succeeded"; fi
grep -q "^wait --for=condition=complete" "$tmp/log" && fail "a failed sample waited for its Job to complete"
[[ "$(grep -c '^delete job' "$tmp/log")" == 1 ]] || fail "the created Job wasn't deleted once"
[[ -z "$(jobs_left)" ]] || fail "the created Job is left: $(jobs_left)"
echo "ok: the created Job is deleted when the sample fails"

# A run that works keeps its report, with the tools it ran, and deletes its Job.
reset
run || fail "the run failed: $(cat "$tmp/stderr")"
report="$(ls "$tmp/out"/*.txt)"
grep -q '"records": 6000' "$report" || fail "the report isn't kept"
grep -q "tools prebuilt" "$report" || fail "the report doesn't say what ran"
grep -q "arm64-kafkafetch" "$report" || fail "the report doesn't have the tools' hashes"
grep -q "apply" "$tmp/log" && fail "the Job was applied rather than created"
[[ -z "$(jobs_left)" ]] || fail "the Job is left"
# The Job is deleted only once it completed, with the report in its log.
name="$(sed -n 's/^delete job \([^ ]*\).*/\1/p' "$tmp/log")"
completed="$(grep -n "^wait --for=condition=complete job/$name " "$tmp/log" | cut -d: -f1)"
deleted="$(grep -n "^delete job $name " "$tmp/log" | cut -d: -f1)"
[[ -n $completed && $completed -lt $deleted ]] || fail "the Job was deleted before it completed"
# The probe's fetch is bounded too, by a limit of its own.
grep -q "PROBE_MAX_BYTES=256000000 " "$tmp/log" || fail "the probe's byte limit isn't passed to the Job"
# shellcheck disable=SC2016 # The Job's script, as it's passed, names the variables.
grep -q -- '-records 10 -max-bytes "$PROBE_MAX_BYTES"' "$tmp/log" || fail "the probe's fetch isn't bounded by its byte limit"
# shellcheck disable=SC2016 # Likewise.
grep -q -- '-max-bytes "$MAX_BYTES"' "$tmp/log" || fail "the sample's fetch isn't bounded by its byte limit"
echo "ok: the report is kept, the probe and the sample bounded, and the Job deleted once it completed"

# Two runs at once have Jobs of their own, and each deletes only its own.
reset
run & first=$!
run & second=$!
wait "$first" || fail "the first of two runs failed"
wait "$second" || fail "the second of two runs failed"
names="$(grep '^delete job' "$tmp/log" | awk '{print $3}' | sort -u | wc -l | tr -d ' ')"
[[ $names == 2 ]] || fail "two runs didn't have Jobs of their own"
[[ -z "$(jobs_left)" ]] || fail "a Job is left: $(jobs_left)"
echo "ok: runs at once have Jobs of their own"

# Without the credentials, nothing is done.
reset
if PATH="$tmp/fake:$PATH" FAKE_LOG="$tmp/log" SAMPLE_CELL_BIN="$tmp/tools" KUBE_CONTEXT=test NAMESPACE=test KAFKA_ADDRESS=kafka:9092 \
  KAFKA_CLIENT_ID=test "$here/sample-cell.sh" > /dev/null 2>&1; then
  fail "a run without credentials succeeded"
fi
[[ ! -e "$tmp/log" ]] || fail "kubectl was called without credentials"
echo "ok: nothing is done without the credentials"
