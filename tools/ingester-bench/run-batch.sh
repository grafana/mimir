#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Runs a batch file's runs one after another with run-in-kubernetes.sh, each starting at the same phase of the 2h block
# cycle, so that the same compactions fall at the same points of every run:
#
#   IMAGE_REPO=localhost:5001/ingester-bench KUBE_CONTEXT=kind-ingester-bench NAMESPACE=default RESULTS=~/results \
#     tools/ingester-bench/run-batch.sh tools/ingester-bench/native-metadata/baselines.batch
#
# A batch file has a line per build and per run, and # comments:
#
#   build <build> <commit> [tags=<build tags>]
#   run <run> <build A> <engine A> <build B> <engine B> [burn=<CPUs>] [metadata=a|b|ab] [profile=<file>] [phase=<minutes>]
#     [bench flags...]
#
# Each build's Mimir goes in an image of its own, built once. Each run's ingesters are named <build>-<engine>, with
# -metadata when they keep the series' own metadata, which
# -analyze compares them by, and run with the run hygiene's flags: no compaction jitter and no head snapshots. With
# burn, ingester A's container also keeps that many CPUs busy, for the isolation check. With metadata, the ingesters
# named keep the series' own metric metadata; profile is the run's series and reads profile. check-isolation.sh's output
# for each run goes next to the batch's log in RESULTS. PHASE_MINUTES (default 5) is when, past an even UTC hour, each
# run starts, which a run waits for, and a run's phase its own. Run it on the host of kind-up.sh.
set -euo pipefail

: "${IMAGE_REPO:?the repository to push the images to}"
: "${KUBE_CONTEXT:?the kubectl context}"
: "${NAMESPACE:?the namespace}"
: "${RESULTS:?the results directory}"
batch="$(realpath "${1:?the batch file}")"
phase=$((${PHASE_MINUTES:-5} * 60))

cd "$(git rev-parse --show-toplevel)"
dir=tools/ingester-bench
mkdir -p "$RESULTS"
log="$RESULTS/batch-$(date -u +%Y%m%d-%H%M%S).log"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
say() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$log"; }

# image builds a commit's Mimir into an image of the bench's layout and prints it, by digest.
declare -A images=()
image() {
  local commit tag src ctx tags=${2:-}
  commit="$(git rev-parse --verify "$1^{commit}")"
  tag="mimir-${commit:0:10}${tags:+-${tags//,/-}}"
  src="$work/src-${commit:0:10}" ctx="$work/ctx-${commit:0:10}"
  git worktree add --quiet --detach "$src" "$commit"
  mkdir -p "$ctx"
  (cd "$src" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$ctx/mimir" ${tags:+-tags "$tags"} \
    -ldflags "-X github.com/grafana/mimir/pkg/util/version.Revision=${commit:0:10}" ./cmd/mimir)
  git worktree remove --force "$src"
  # The Dockerfile copies a bench too, which the job's ingesters don't run: they run the one the job installs.
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$ctx/ingester-bench" ./tools/ingester-bench
  cp "$dir/Dockerfile" "$ctx/"
  docker buildx build --quiet -o type=registry --metadata-file "$ctx/metadata.json" --platform linux/amd64 \
    -t "$IMAGE_REPO:$tag" "$ctx" >&2
  echo "$IMAGE_REPO:$tag@$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["containerimage.digest"])' "$ctx/metadata.json")"
}

# config writes a run's configuration of its two ingesters.
config() {
  local path=$1 build_a=$2 engine_a=$3 build_b=$4 engine_b=$5 metadata=$6 id build engine
  {
    echo "common:"
    echo "  args:"
    echo '    blocks-storage.tsdb.head-compaction-interval-jitter-enabled: "false"'
    echo '    blocks-storage.tsdb.seriesstore-compaction-jitter-enabled: "false"'
    echo '    blocks-storage.tsdb.seriesstore-snapshot-interval: "0"'
    for id in a b; do
      if [[ $id == a ]]; then build=$build_a engine=$engine_a; else build=$build_b engine=$engine_b; fi
      echo "$id:"
      if [[ $metadata == *$id* ]]; then
        echo "  name: $build-$engine-metadata"
      else
        echo "  name: $build-$engine"
      fi
      echo "  image: ${images[$build]}"
      echo "  args:"
      echo "    blocks-storage.tsdb.engine: $engine"
      if [[ $engine == seriesstore ]]; then
        echo '    blocks-storage.tsdb.memory-snapshot-on-shutdown: "false"'
      fi
      if [[ $metadata == *$id* ]]; then
        echo '    ingester.native-metric-metadata-enabled: "true"'
      fi
    done
  } > "$path"
}

# wait_for_phase sleeps until the next time that's $1 seconds past an even UTC hour.
wait_for_phase() {
  local now wait
  now="$(date -u +%s)"
  wait=$((($1 - now % 7200 + 7200) % 7200))
  say "waiting $((wait / 60))m for the phase"
  sleep "$wait"
}

# burner keeps CPUs of run's ingester A busy once its pod runs, until the run's job is gone.
burner() {
  local name=$1 cpus=$2 pod
  until pod="$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods -l job-name="$name" \
    --field-selector=status.phase=Running -o name 2>/dev/null | head -1)" && [[ -n "$pod" ]]; do
    sleep 10
  done
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" exec "$pod" -c ingester-a -- /shared/ingester-bench -burn="$cpus" >/dev/null 2>&1 || true
}

# isolation saves check-isolation.sh's output for the run once its pod runs.
isolation() {
  local name=$1
  until [[ -n "$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods -l job-name="$name" \
    --field-selector=status.phase=Running -o name 2>/dev/null)" ]]; do
    sleep 10
  done
  sleep 30
  KUBE_CONTEXT="$KUBE_CONTEXT" NAMESPACE="$NAMESPACE" "$dir/check-isolation.sh" "$name" > "$RESULTS/isolation-$name.txt" 2>&1 || true
}

say "batch $batch at $(git rev-parse --short=10 HEAD), phase ${PHASE_MINUTES:-5}m"
# The batch is read on its own descriptor, so that the commands in the loop can't read it from their stdin.
while read -r kind rest <&3; do
  case "$kind" in
    "" | "#"*) continue ;;
    build)
      read -r build commit options <<< "$rest"
      tags=''
      for option in $options; do
        case "$option" in
          tags=*) tags="${option#tags=}" ;;
          *) say "build $build: unknown option $option"; exit 1 ;;
        esac
      done
      say "building $build at $commit${tags:+ with tags $tags}"
      images[$build]="$(image "$commit" "$tags")"
      say "$build is ${images[$build]}"
      ;;
    run)
      read -r run build_a engine_a build_b engine_b options <<< "$rest"
      [[ "$run" =~ ^[a-z0-9-]+$ ]] || { say "run names are lowercase letters, digits and dashes: $run"; exit 1; }
      for build in "$build_a" "$build_b"; do
        [[ -n "${images[$build]:-}" ]] || { say "run $run: no build $build"; exit 1; }
      done
      burn='' metadata='' profile='' run_phase=$phase flags=()
      for option in $options; do
        case "$option" in
          burn=*) burn="${option#burn=}" ;;
          metadata=*) metadata="${option#metadata=}" ;;
          profile=*) profile="$(realpath "${option#profile=}")" ;;
          phase=*) run_phase=$((${option#phase=} * 60)) ;;
          *) flags+=("$option") ;;
        esac
      done
      config "$work/$run.yaml" "$build_a" "$engine_a" "$build_b" "$engine_b" "$metadata"
      name="bench-$run"
      wait_for_phase "$run_phase"
      say "run $run: A $build_a-$engine_a, B $build_b-$engine_b${metadata:+, metadata on $metadata}${profile:+, profile $profile}${burn:+, burning $burn CPUs of A}"
      pids=()
      isolation "$name" & pids+=($!)
      if [[ -n "$burn" ]]; then
        burner "$name" "$burn" & pids+=($!)
      fi
      if NAME="$name" CONFIG="$work/$run.yaml" PROFILE="$profile" GUARANTEED=1 "$dir/run-in-kubernetes.sh" "${flags[@]}" >> "$log" 2>&1; then
        say "run $run: done"
      else
        say "run $run: failed, see the log"
      fi
      kill "${pids[@]}" 2>/dev/null || true
      wait "${pids[@]}" 2>/dev/null || true
      ;;
    *)
      say "unknown line: $kind $rest"
      exit 1
      ;;
  esac
done 3< "$batch"
say "batch done"
