// SPDX-License-Identifier: AGPL-3.0-only

// Package streaminglabelvalues provides string-value filters and filter-builder
// helpers used by the Mimir streaming label/value search RPCs. Filters
// implement the Prometheus storage.Filter interface; servers receive a
// proto-encoded filter spec, translate it to Params, and call BuildFilter.
//
// Field names and defaults mirror the user-facing contract from Prometheus
// PR #18573 (case_sensitive default true, fuzz_threshold int 0-100, fuzz_alg
// default subsequence) so the HTTP layer added in a later PR is a thin
// translation.
//
// Concurrency: the fuzzy filters wrap Prometheus matchers that lazily cache
// rune slices and are not safe for concurrent use. Build one filter per
// goroutine via BuildFilter when fanning out. Mimir deliberately does not
// adopt Prometheus's RWMutex-protected FuzzyFilter pattern; per-goroutine
// construction avoids per-Accept lock acquisitions on multi-million-value
// scans.
package streaminglabelvalues
