# Prometheus query cost sampling

`prometheus-query-cost.patch` adds the measurement-only sampling capability used by
Mimir's distributed query cost API and opens native TSDB cost readers through the
existing block pinning and truncation checks. It applies to the Prometheus version
pinned in `go.mod`.

`make mod-check` reapplies this patch after `go mod vendor`. If running vendoring
manually, run `git apply tools/vendor-patches/prometheus-query-cost.patch` afterward.
The patch deliberately fails on incompatible dependency changes. Reconcile it when
upgrading Prometheus, and remove it and the Makefile application step when the
pinned dependency includes these changes.

Mimir tests cover the estimator hook through the HTTP API and native TSDB readers
through ingester sampling. This patch does not claim an upstream release contains
the capability.
