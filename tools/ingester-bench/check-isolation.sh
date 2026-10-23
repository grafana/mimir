#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Checks that the containers of a running bench pod have CPUs of their own, as a Guaranteed pod on a node with the
# static CPU manager should (run-in-kubernetes.sh with GUARANTEED=1, kind-up.sh):
#
#   KUBE_CONTEXT=kind-ingester-bench NAMESPACE=default tools/ingester-bench/check-isolation.sh [job name]
#
# Then, before the first measured run, check that a container's load doesn't slow the others: run an A/A pair with
#
#   kubectl exec <pod> -c ingester-a -- /shared/ingester-bench -burn=10
#
# from the start of the replay to the end of the window, and again without it. B's replay CPU per sample has to stay
# within the A/A runs' noise; otherwise the host couples them, and the bench needs bare metal.
set -euo pipefail

: "${KUBE_CONTEXT:?the kubectl context}"
: "${NAMESPACE:?the namespace}"
name="${1:-ingester-bench}"
k() { kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" "$@"; }

pod="$(k get pods -l job-name="$name" -o name | head -1)"
[[ -n "$pod" ]] || { echo "no pod of job $name" >&2; exit 1; }
echo "$pod: QoS $(k get "$pod" -o jsonpath='{.status.qosClass}')"
cpusets=()
for container in driver ingester-a ingester-b; do
  cpuset="$(k exec "$pod" -c "$container" -- /shared/ingester-bench -cpuset)"
  echo "$container: CPUs $cpuset"
  cpusets+=("$container=$cpuset")
done
python3 -I - "${cpusets[@]}" <<'PY'
import sys
def cpus(spec):
    out = set()
    for part in spec.strip().split(','):
        lo, _, hi = part.partition('-')
        out.update(range(int(lo), int(hi or lo) + 1))
    return out
sets = {name: cpus(spec) for name, spec in (arg.split('=', 1) for arg in sys.argv[1:])}
names = sorted(sets)
bad = [(a, b, sorted(sets[a] & sets[b])) for i, a in enumerate(names) for b in names[i + 1:] if sets[a] & sets[b]]
for a, b, shared in bad:
    print(f'{a} and {b} share CPUs {shared}')
sys.exit(1 if bad else 0)
PY
echo "the containers' CPUs are their own"
