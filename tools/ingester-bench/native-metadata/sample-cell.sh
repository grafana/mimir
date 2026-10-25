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
# The address and the client ID are the ingesters': with WarpStream, the client ID's routing settings, such as
# ws_pt=proxy-consume, are what lets the fetches reach the partitions' agents. The SASL credentials are the cell's,
# from wherever they're documented: the script reads none itself. Creating the Job and exec'ing into it in the
# namespace needs Timed Access.
#
# It builds kafkafetch, kafkatool and ingester-bench from this checkout for both architectures, and runs a Job of a
# pinned image: no service account token, non-root with a read-only root, no privileges, and the raw dumps in a
# size-limited emptyDir, gone with the Job. In it, it first samples 10 records of 3 partitions, to check that they can be
# fetched and decoded, then tries kafkatool's dump export with the client ID, which reports whether it works with
# WarpStream, then samples the 2000 records, and fails unless every partition's sample is complete. The Job goes when
# the script ends, or by its deadline and TTL.
set -euo pipefail

: "${KUBE_CONTEXT:?the kubectl context}"
: "${NAMESPACE:?the namespace of the cell}"
: "${KAFKA_ADDRESS:?the Kafka address of the ingesters}"
: "${KAFKA_CLIENT_ID:?the Kafka client ID of the ingesters, with its routing settings}"
: "${KAFKA_SASL_USERNAME:?the Kafka SASL username of the cell}"
: "${KAFKA_SASL_PASSWORD:?the Kafka SASL password of the cell}"
# alpine:3.22, by its multi-architecture digest.
IMAGE=alpine@sha256:2e0b81d60597f449baa56dc25d355367869d2dca4cd57e866f1e2b52af3eb55f
NAME=native-metadata-sample-${USER:-anonymous}
OWNER=${USER:-anonymous}

dir="$(cd "$(dirname "$0")/.." && pwd)"
root="$(git -C "$dir" rev-parse --show-toplevel)"
bin="$(mktemp -d)"
k() { kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" "$@"; }
cleanup() {
  k delete job "$NAME" --wait=false >/dev/null 2>&1 || true
  rm -rf "$bin"
}
trap cleanup EXIT

if [[ -n "$(git -C "$root" status --porcelain -- tools/ingester-bench tools/kafkatool pkg)" ]]; then
  echo "the checkout has changes: commit them, so that what runs is a commit's" >&2
  exit 1
fi
echo "building at $(git -C "$root" rev-parse --short=10 HEAD)"
for arch in amd64 arm64; do
  for tool in tools/ingester-bench/kafkafetch tools/kafkatool tools/ingester-bench; do
    (cd "$root" && GOOS=linux GOARCH=$arch CGO_ENABLED=0 go build -o "$bin/$arch/$(basename "$tool")" "./$tool")
  done
done
(cd "$bin" && shasum -a 256 ./*/*)

k apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $NAME
  labels: {purpose: native-metadata-composition, owner: $OWNER}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 3600
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels: {purpose: native-metadata-composition, owner: $OWNER}
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
          # The script copies the tools in and runs them; the Job ends once it says it's done, or by its deadline.
          command: [sh, -c, 'until [ -e /work/done ]; do sleep 5; done']
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: {drop: [ALL]}
          resources:
            requests: {cpu: "1", memory: 2Gi}
            limits: {memory: 4Gi}
          volumeMounts:
            - {name: work, mountPath: /work}
      volumes:
        - name: work
          emptyDir: {sizeLimit: 12Gi}
EOF
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
  k cp "$bin/$arch/$tool" "$pod:/work/$tool"
done

k exec "$pod" -- env U="$KAFKA_SASL_USERNAME" P="$KAFKA_SASL_PASSWORD" ADDR="$KAFKA_ADDRESS" CID="$KAFKA_CLIENT_ID" sh -c '
  set -eo pipefail
  cd /work
  chmod +x kafkafetch kafkatool ingester-bench
  fetch() { ./kafkafetch -kafka-address "$ADDR" -kafka-client-id "$CID" "$@" 2>&1 | grep -v "^dumped"; }

  echo "== 10 records of 3 partitions, to check they can be fetched and decoded"
  fetch -partitions active:3 -records 10 -file-prefix /work/probe- -timeout 3m | tee /work/probe.log
  ./ingester-bench -composition="$(ls /work/probe-*.json | paste -sd, -)" | grep "\"records\""
  partition=$(sed -n "s/^partition \([0-9]*\): \([0-9]*\) to .*/\1/p" /work/probe.log | head -1)
  start=$(sed -n "s/^partition [0-9]*: \([0-9]*\) to .*/\1/p" /work/probe.log | head -1)
  rm -f /work/probe-*.json

  echo "== kafkatool dump export of 10 records of partition $partition, with the client ID"
  if timeout 60 ./kafkatool --kafka-address "$ADDR" --kafka-client-id "$CID" --kafka-sasl-username "$U" --kafka-sasl-password "$P" \
    dump export --file /work/kafkatool.json --topic ingest --partition "$partition" --offset "$start" --export-max-records 10 >/dev/null 2>&1; then
    echo "kafkatool: exported $(wc -l < /work/kafkatool.json) of 10 records"
  else
    echo "kafkatool: no export within 60s, with $(wc -l < /work/kafkatool.json 2>/dev/null || echo 0) records"
  fi
  rm -f /work/kafkatool.json

  echo "== the sample: 2000 records of 3 partitions"
  fetch -partitions active:3 -records 2000 -file-prefix /work/dump- -timeout 15m
  ./ingester-bench -composition="$(ls /work/dump-*.json | paste -sd, -)"
  rm -f /work/dump-*.json
  touch /work/done'
