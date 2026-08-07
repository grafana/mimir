# CI unit test speedup: findings

Measured on 2026-08-06 against `main` @ `298b1f3206`, Go 1.26.5.
Local box: 18 cores / 48GB (darwin/arm64). CI-like measurements use `GOMAXPROCS=4`
(GitHub `ubuntu-latest` = 4 vCPU / 16GB), `-race`, `-tags=netgo,stringlabels`,
`GOFLAGS=-count=1` (to defeat Go's test result cache), Go build cache warm.
CI reference run: `31099284996` (push to main).

## Root cause

`run-unit-tests-group.sh` ran `go test` **once per package, serially**
(introduced by #14669 to get per-package retry; it was a single batched
invocation before that). A group used **0.4 of 4 cores** — 98s CPU over 242s
wall. Unit tests here are latency-bound, not CPU-bound, so serializing packages
wastes almost the whole runner.

## Applied

1. `.github/workflows/scripts/run-unit-tests-group.sh`: one batched `go test`
   per group (two invocations: race set + the MQE no-race set). Per-package
   retry kept by parsing `FAIL\t<pkg>` from the output and re-running only those.
2. `.github/workflows/test-build-deploy.yml`: unit test groups 10 → 5
   (20 → 10 jobs, since group time is now bounded by the slowest package).

### Results

| Case (GOMAXPROCS=4, cold test cache) | before | after |
|---|---|---|
| group 0 of 5 (48 pkgs) | 231s | **94s** (2.5x) |
| group 5 of 10 (24 pkgs) | 242s | 121s (2.0x) |
| group 1 of 5 (heaviest, has `pkg/ingester`) | ~580s (est. from sum) | **254s** |
| whole suite, 18 cores, one pkg at a time | 1884s | — |
| whole suite, 18 cores, batched | — | ~700s |
| CI unit stage critical path | 635s, 20 jobs, 7163 runner-s | ~300s, 10 jobs, ~2500 runner-s |

⚠️ Group count 10 → 5 renames the required status checks (`test (0, 10)` →
`test (0, 5)`); branch protection must be updated or merges will block.

## Tested and rejected

- **`-parallel 16`** (default is GOMAXPROCS=4): a further 25-30%
  (group 5/10: 121s → 90s; `pkg/distributor` alone: 119s → 87s), but a full-suite
  run at `-parallel 16` failed
  `TestIngester_compactBlocksToReduceInMemorySeries_Concurrency`. Not worth the flake.
- **Duration-balanced (LPT) group split**: round-robin (`NR % TOTAL == INDEX`) is
  1.86x imbalanced by group *sum* (matches CI: max group 635s vs mean 358s), and
  LPT would cut that to 1.19x. But **after batching, group wall time equals the
  slowest single package**, so LPT buys ~0: group 1 of 5 has a 485s sum and ran
  in 254s ≈ `pkg/ingester` (233s) alone. Not worth a timings file.
- **`-p 2` or `GOMEMLIMIT=3GiB`** as an OOM guard: peak RSS is driven by the
  individual heavy packages, not by `-p` (4 heaviest packages together:
  `-p 4` 19.5GB/73s, `-p 2` 17.1GB/133s, `-p 4 GOMEMLIMIT=3GiB` 17.8GB/76s —
  macOS race-shadow accounting inflates these). `-p` = GOMAXPROCS is what CI ran
  for years before #14669, so no guard added.

## Next levers, in order of value

### 1. Warm the Go cache with *test results*, not just compiled objects

`warmup-build-cache-unit-tests` compiles with `-run=^$`, so the restored
`GOCACHE` contains no test results. The test script does *not* pass `-count=1`,
so caching is already enabled and would take effect immediately.

Measured:

| Scenario (whole suite, GOMAXPROCS=4) | wall |
|---|---|
| all results cached, no source change | **14s** |
| after a real change to `pkg/ingester/ingester.go` | **250s** (only 11 of 159 pkgs re-run) |

Go's cache key is the *linked test binary's* content hash, so dead-code-eliminated
or comment-only changes don't even invalidate.

Blocker: **cache budget**. Current caches are unit 5.1G + image-and-lint 3.4G +
integration 1.9G = 10.4GB against GitHub's 10GB-per-repo limit — they are already
evicting each other. Fix the budget first (or a per-group cache would be worse:
10 groups x 2 tag variants).

Trade-off to accept: a package unchanged by the PR is not re-run, so PR runs stop
re-rolling the flake dice on untouched code (the `flaky-tests.yml` workflow still does).

### 2. Integration test split is 3.03x imbalanced

From CI logs of run `31099284996` (110 tests, 3110s of test time, 20 groups):

- round-robin by test name: max group 471s vs 156s ideal (**3.03x**)
- LPT by measured duration: max group **290s** (1.86x) — floor is
  `TestIngesterSharding` at 290s
- observed CI job max was 654s; fixed per-job overhead is only ~45s

Unlike unit tests, batching is not an option: no integration test calls
`t.Parallel()` and they each drive Docker scenarios. This one genuinely needs a
committed timings file (regenerate from CI logs with
`grep -E -- '--- (PASS|FAIL): Test' | sort` and greedy LPT), plus a default weight
for unseen tests. ~40 lines, real maintenance.

### 3. `pkg/ingester` is now the floor for the whole unit stage

233s at 4 cores (`pkg/storegateway` 175s, `pkg/frontend/querymiddleware` 136s,
`pkg/distributor` 116s are next). Nothing below ~300s per job is reachable
without splitting that package's slowest tests into a separate package
(the same trick #14745 used for MQE) or trimming them.

### 4. The `nopools` matrix doubles the whole unit stage

`extra_build_tags: ["", "nopools"]` re-runs all 242 packages, and the tag only
switches implementations in `pkg/mimirpb` (5 files). Half of all unit-test runner
minutes. Options: run `nopools` on main/nightly only, or restrict it to a package
subset. Policy call, not a mechanical one.

### 5. Smaller items

- `test` and `build-race-enabled-image` both `needs: warmup-go-build-cache-unit-tests`,
  which is a no-op job on PR branches but still costs a runner scheduling round-trip.
- 20 integration jobs x ~45s fixed setup ≈ 15 runner-minutes of pure overhead; with a
  balanced split, 12 groups would be as fast and cheaper.

## Local-only gotcha (not CI)

`pkg/frontend/v2` hangs indefinitely on darwin/arm64 (gRPC
`GoAway ... ENHANCE_YOUR_CALM / too_many_pings`), so plain `make test` never
finishes locally. Also, running the suite with `-p 18` locally fails
`pkg/distributor`, `pkg/alertmanager` and others with
`dial tcp 127.0.0.1:x: can't assign requested address` — macOS ephemeral port
exhaustion, not a Linux/CI problem at `-p 4`.
