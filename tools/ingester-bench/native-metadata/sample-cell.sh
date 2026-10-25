#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Samples a cell's ingest records for the native metadata prototype, as the threshold profile's composition and help
# lengths were sampled: the last 2000 records of 3 partitions that take records, of which only their composition and the
# lengths of their metric families' helps leave the cluster, as counts and percentiles:
#
#   KUBE_CONTEXT=<context> NAMESPACE=<namespace> \
#     KAFKA_ADDRESS=<the ingesters' -ingest-storage.kafka.address> \
#     KAFKA_CLIENT_ID=<the ingesters' -ingest-storage.kafka.client-id> \
#     KAFKA_SASL_USERNAME=... KAFKA_SASL_PASSWORD=... \
#     tools/ingester-bench/native-metadata/sample-cell.sh
#
# The address and the client ID are the ingesters': the client ID's routing settings are what lets the fetches reach
# the partitions. The SASL credentials are the cell's, from wherever they're documented: the script reads none itself.
# Creating the Job and exec'ing into it in the namespace needs Timed Access.
#
# It builds kafkafetch, kafkatool and ingester-bench for both architectures from this checkout, which has to be clean,
# or takes them prebuilt from SAMPLE_CELL_BIN, a directory with amd64/ and arm64/ ones. It creates a Job of its own,
# of a pinned image: no service account token, non-root with a read-only root, no privileges, and the raw dumps in a
# size-limited emptyDir, gone with the Job. In it, it first samples 10 records of 3 partitions, to check that they can
# be fetched and decoded, then tries kafkatool's dump export on the first of them, then samples the 2000 records of the
# same partitions, and fails unless every partition's sample is complete. The report, with what it was made from, goes
# to a file in SAMPLE_CELL_OUT, by default a directory in the temporary directory, as well as the Job's log. The Job goes
# when the script ends, or by its deadline and TTL; the script deletes only the Job it created.
set -euo pipefail

: "${KUBE_CONTEXT:?the kubectl context}"
: "${NAMESPACE:?the namespace of the cell}"
: "${KAFKA_ADDRESS:?the Kafka address of the ingesters}"
: "${KAFKA_CLIENT_ID:?the Kafka client ID of the ingesters, with its routing settings}"
: "${KAFKA_SASL_USERNAME:?the Kafka SASL username of the cell}"
: "${KAFKA_SASL_PASSWORD:?the Kafka SASL password of the cell}"
# alpine:3.22, by its multi-architecture digest.
IMAGE=alpine@sha256:2e0b81d60597f449baa56dc25d355367869d2dca4cd57e866f1e2b52af3eb55f
RECORDS=2000
# Each partition's sample may take this many bytes, which 3 of fit the emptyDir with room for the tools; the probe's
# 10 records of each, as many as 10 records of the largest size, at most 16 MiB and a third more as JSON.
MAX_BYTES=3000000000
PROBE_MAX_BYTES=256000000
owner="$(printf '%s' "${USER:-anonymous}" | tr -c 'a-z0-9-' '-' | cut -c1-16)"
# A name of this run's own: the script creates the Job, never takes one over, and deletes only it.
NAME="native-metadata-sample-$owner-$(date -u +%m%d%H%M%S)-$((RANDOM % 1000))"

dir="$(cd "$(dirname "$0")/.." && pwd)"
root="$(git -C "$dir" rev-parse --show-toplevel)"
out_dir="${SAMPLE_CELL_OUT:-${TMPDIR:-/tmp}/native-metadata-samples}"
mkdir -p "$out_dir"
out="$out_dir/$NAME.txt"
bin="$(mktemp -d)"
created=false
k() { kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" "$@"; }
cleanup() {
  if [[ $created == true ]]; then
    k delete job "$NAME" --wait=false >/dev/null 2>&1 || true
  fi
  rm -rf "$bin"
}
trap cleanup EXIT

{
  echo "sample $NAME, $(date -u +%Y-%m-%dT%H:%M:%SZ), of $KUBE_CONTEXT/$NAMESPACE"
  echo "$RECORDS records of 3 partitions, at most $MAX_BYTES bytes each after a probe of at most $PROBE_MAX_BYTES, in $IMAGE"
} | tee "$out"
if [[ -n "${SAMPLE_CELL_BIN:-}" ]]; then
  for arch in amd64 arm64; do
    for tool in kafkafetch kafkatool ingester-bench; do
      cp "$SAMPLE_CELL_BIN/$arch/$tool" "$bin/$arch-$tool"
    done
  done
  echo "tools prebuilt, from $SAMPLE_CELL_BIN" | tee -a "$out"
else
  # What's built is a commit's, dependencies and vendored code included, with nothing overlaid.
  if [[ -n "$(git -C "$root" status --porcelain)" ]]; then
    echo "the checkout has changes: commit them, so that what runs is a commit's" >&2
    exit 1
  fi
  work="$(cd "$root" && go env GOWORK)"
  if [[ -n $work && $work != off ]]; then
    echo "a Go workspace is in use ($work): unset GOWORK, so that the module's own dependencies are built" >&2
    exit 1
  fi
  case "$(cd "$root" && go env GOFLAGS)" in
    *-overlay* | *-modfile* | *-mod=mod*)
      echo "GOFLAGS overlays or replaces the module's files: unset them" >&2
      exit 1
      ;;
  esac
  echo "tools built at $(git -C "$root" rev-parse HEAD), with $(cd "$root" && go version)" | tee -a "$out"
  for arch in amd64 arm64; do
    for tool in tools/ingester-bench/kafkafetch tools/kafkatool tools/ingester-bench; do
      (cd "$root" && GOOS=linux GOARCH=$arch CGO_ENABLED=0 go build -mod=vendor -o "$bin/$arch-$(basename "$tool")" "./$tool")
    done
  done
fi
(cd "$bin" && shasum -a 256 ./*) | tee -a "$out"

k create -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $NAME
  labels: {purpose: native-metadata-composition, owner: $owner}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 3600
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels: {purpose: native-metadata-composition, owner: $owner}
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 65534
        runAsGroup: 65534
        fsGroup: 65534
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: sample
          image: $IMAGE
          # The script copies the tools in and runs them; the Job ends once it says it's done, with the report in its
          # log, or by its deadline.
          command: [sh, -c, 'until [ -e /work/done ]; do sleep 5; done; cat /work/report.txt']
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: {drop: [ALL]}
          resources:
            requests: {cpu: "1", memory: 2Gi, ephemeral-storage: 1Gi}
            limits: {memory: 4Gi, ephemeral-storage: 13Gi}
          volumeMounts:
            - {name: work, mountPath: /work}
      volumes:
        - name: work
          emptyDir: {sizeLimit: 12Gi}
EOF
created=true
for _ in $(seq 60); do
  [[ -n "$(k get pod -l "job-name=$NAME" -o name)" ]] && break
  sleep 2
done
k wait --for=condition=Ready pod -l "job-name=$NAME" --timeout=180s
pod="$(k get pod -l "job-name=$NAME" -o jsonpath='{.items[0].metadata.name}')"

case "$(k exec "$pod" -- uname -m)" in
  x86_64) arch=amd64 ;;
  aarch64) arch=arm64 ;;
  *) echo "unknown architecture" >&2; exit 1 ;;
esac
for tool in kafkafetch kafkatool ingester-bench; do
  k cp "$bin/$arch-$tool" "$pod:/work/$tool"
done

# shellcheck disable=SC2016 # The Job's shell expands these, not this one.
k exec "$pod" -- env U="$KAFKA_SASL_USERNAME" P="$KAFKA_SASL_PASSWORD" ADDR="$KAFKA_ADDRESS" CID="$KAFKA_CLIENT_ID" \
  RECORDS="$RECORDS" MAX_BYTES="$MAX_BYTES" PROBE_MAX_BYTES="$PROBE_MAX_BYTES" sh -c '
  set -eo pipefail
  cd /work
  chmod +x kafkafetch kafkatool ingester-bench
  fetch() { ./kafkafetch -kafka-address "$ADDR" -kafka-client-id "$CID" "$@" 2>&1 | grep -v "^dumped"; }
  {
    echo "== 10 records of 3 partitions, to check they can be fetched and decoded"
    fetch -partitions active:3 -records 10 -max-bytes "$PROBE_MAX_BYTES" -file-prefix /work/probe- -timeout 3m | tee /work/probe.log
    ./ingester-bench -composition="$(ls /work/probe-*.json | paste -sd, -)" | grep "\"records\""
    partitions=$(sed -n "s/^partitions \[\(.*\)\]$/\1/p" /work/probe.log | tr " " ",")
    first=${partitions%%,*}
    start=$(sed -n "s/^partition $first: \([0-9]*\) to .*/\1/p" /work/probe.log)
    rm -f /work/probe-*.json

    echo "== kafkatool dump export of 10 records of partition $first, with the client ID"
    if timeout 60 ./kafkatool --kafka-address "$ADDR" --kafka-client-id "$CID" --kafka-sasl-username "$U" --kafka-sasl-password "$P" \
      dump export --file /work/kafkatool.json --topic ingest --partition "$first" --offset "$start" --export-max-records 10 > /work/kafkatool.log 2>&1; then
      echo "kafkatool: exported $(wc -l < /work/kafkatool.json) records; $(./ingester-bench -composition=/work/kafkatool.json 2>&1 | grep "\"records\"" || echo "they did not decode")"
    else
      echo "kafkatool: failed with status $?, its last line: $(tail -1 /work/kafkatool.log)"
    fi
    rm -f /work/kafkatool.json /work/kafkatool.log

    echo "== the sample: $RECORDS records of partitions $partitions"
    fetch -partitions "$partitions" -records "$RECORDS" -max-bytes "$MAX_BYTES" -file-prefix /work/dump- -timeout 15m | tee /work/sample.log
    if [ "$(sed -n "s/^partitions \[\(.*\)\]$/\1/p" /work/sample.log | tr " " ",")" != "$partitions" ]; then
      echo "the sample is not of the partitions the probe chose" >&2
      exit 1
    fi
    ./ingester-bench -composition="$(ls /work/dump-*.json | paste -sd, -)"
  } 2>&1 | tee /work/report.txt
  rm -f /work/dump-*.json
  touch /work/done' | tee -a "$out"
echo "report in $out"
# The Job's container prints the report to its log once it sees the sample done: it's deleted once it has, or after a
# minute, as the report is kept anyway.
k wait --for=condition=complete "job/$NAME" --timeout=60s >/dev/null || echo "the Job didn't complete within a minute, so its log may not have the report" >&2
