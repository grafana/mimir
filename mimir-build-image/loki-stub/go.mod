// This is a minimal stand-in for github.com/grafana/loki/v3, used only to build
// the tools installed in the build image (specifically mixtool, which pulls in
// github.com/grafana/dashboard-linter). Dashboard-linter imports the LogQL
// parser from loki/v3 in a single file (lint/rule_target_logql_auto.go) to lint
// the range vectors of Loki dashboard panels, yet depending on the real module
// pulls the entirety of Loki (and its huge transitive dependency tree) into the
// build image. This stub provides just enough of the loki/v3 logql/syntax API to
// keep that lint rule working without the bloat. See the replace directive in
// ../go.mod.
module github.com/grafana/loki/v3

go 1.21
