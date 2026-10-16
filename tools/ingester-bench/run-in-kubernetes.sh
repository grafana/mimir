#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Builds the bench's image from the working tree, runs job.yaml in a cluster, waits for its report, adds it to a
# results directory and deletes the job:
#
#   IMAGE_REPO=registry.example.com/me/ingester-bench KUBE_CONTEXT=my-cluster NAMESPACE=my-namespace \
#     RESULTS=~/ingester-bench-results tools/ingester-bench/run-in-kubernetes.sh [extra bench flags...]
#
# PROFILE, a profile file, is put in a ConfigMap for the run; NODE_SELECTOR (key=value) places it. The cluster's nodes
# should have the kubelet's static CPU manager policy, so that the containers' CPUs are their own.
set -euo pipefail

: "${IMAGE_REPO:?the repository to push the image to}"
: "${KUBE_CONTEXT:?the kubectl context}"
: "${NAMESPACE:?the namespace}"
: "${RESULTS:?the results directory}"
NAME="${NAME:-ingester-bench}"

cd "$(git rev-parse --show-toplevel)"
dir=tools/ingester-bench
revision="$(git rev-parse --short=10 HEAD)$(git diff --quiet || echo -dirty)"
k() { kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" "$@"; }

echo "building $IMAGE_REPO:$revision"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$dir/mimir" -ldflags "-X github.com/grafana/mimir/pkg/util/version.Revision=$revision" ./cmd/mimir
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$dir/ingester-bench" ./tools/ingester-bench
metadata="$(mktemp)"
docker buildx build -o type=registry --metadata-file "$metadata" --platform linux/amd64 -t "$IMAGE_REPO:$revision" "$dir"
digest="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["containerimage.digest"])' "$metadata")"

if [[ -n "${PROFILE:-}" ]]; then
  # Server side, as a client side apply keeps the whole profile in an annotation, which is too small for it.
  k create configmap "$NAME-profile" --from-file="profile.json${PROFILE##*.json}=$PROFILE" --dry-run=client -o yaml | k apply --server-side --force-conflicts -f -
fi
# The job of job.yaml, with the image, the profile, a node selector and the extra flags.
python3 - "$dir/job.yaml" "$NAME" "$NAMESPACE" "$IMAGE_REPO:$revision@$digest" "${PROFILE:-}" "${NODE_SELECTOR:-}" "$@" > "$metadata.yaml" <<'PY'
import sys
path, name, namespace, image, profile, selector, extra = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7:]
s = open(path).read().replace('image: IMAGE', 'image: ' + image)
s = s.replace('  name: ingester-bench\n', f'  name: {name}\n  namespace: {namespace}\n', 1)
if profile:
    file = 'profile.json' + profile.split('.json', 1)[1] if '.json' in profile else 'profile.json'
    s = s.replace('            # - -profile=/profile/profile.json\n', f'            - -profile=/profile/{file}\n')
    s = s.replace('            # - { name: profile, mountPath: /profile }\n', '            - { name: profile, mountPath: /profile }\n')
    s = s.replace('        # - { name: profile, configMap: { name: ingester-bench-profile } }\n', f'        - {{ name: profile, configMap: {{ name: {name}-profile }} }}\n')
if selector:
    key, value = selector.split('=', 1)
    s = s.replace('      shareProcessNamespace: true\n', f'      shareProcessNamespace: true\n      nodeSelector: {{ {key}: {value} }}\n')
for flag in extra:
    s = s.replace('            - -hold=1h\n', f'            - -hold=1h\n            - {flag}\n', 1)
print(s)
PY
k delete job "$NAME" --ignore-not-found --wait=true
k apply -f "$metadata.yaml"
# By the job's uid, as the pods of the job it replaced may still be terminating.
uid="$(k get job "$NAME" -o jsonpath='{.metadata.uid}')"

echo "waiting for the run's report"
# By the report being served, which the driver does once the run is done: the driver's log rotates during a run, so the
# line that says it is may no longer be in it.
out="$(mktemp -d)"
forward=
trap 'kill $forward 2>/dev/null || true' EXIT
while true; do
  pod="$(k get pods -l controller-uid="$uid" -o name | head -1)"
  if [[ -n "$pod" ]] && k get "$pod" -o jsonpath='{.status.containerStatuses[*].state.terminated.reason}' | grep -q .; then
    k logs "$pod" -c driver --tail=50 || true
    echo "a container of the run ended early" >&2
    exit 1
  fi
  if [[ -n "$pod" ]] && [[ "$(k get "$pod" -o jsonpath='{.status.phase}')" == Running ]]; then
    if [[ -z "$forward" ]] || ! kill -0 "$forward" 2>/dev/null; then
      k port-forward "$pod" 28090:8090 >/dev/null 2>&1 &
      forward=$!
      sleep 3
    fi
    if curl -sf -o "$out/report.json" "http://localhost:28090/report.json"; then break; fi
  fi
  sleep 30
done
# The profiles and slow reads, which a run may not have.
for file in report.html slow.log cpu-a.pb.gz cpu-b.pb.gz heap-a.pb.gz heap-b.pb.gz allocs-a.pb.gz allocs-b.pb.gz; do
  curl -sf -o "$out/$file" "http://localhost:28090/$file" || true
done
go run ./tools/ingester-bench -results="$RESULTS" -add-result="$out/report.json"
k delete job "$NAME" --wait=false
