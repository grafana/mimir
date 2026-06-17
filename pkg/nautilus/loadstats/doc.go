// SPDX-License-Identifier: AGPL-3.0-only

// Package loadstats provides per-partition query-load and per-hash-range
// active-series tracking used by the nautilus rebalancer. The trackers
// live here (rather than inside pkg/ingester or pkg/readcache) so that
// the rebalancer and the component reporting load each import a single
// shared definition.
//
// Today only pkg/readcache uses these trackers; pkg/ingester is
// nautilus-free. See pkg/readcache/loadstats_integration.go for the
// wiring on the producer side and pkg/nautilus/rebalancer for the
// consumer side.
package loadstats
