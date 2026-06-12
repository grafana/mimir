// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"sync"
	"time"
)

const (
	// otelTranslationWarningDedupTTL is how long a given per-tenant translation
	// warning is suppressed after having been logged.
	otelTranslationWarningDedupTTL = 15 * time.Minute

	// otelTranslationWarningMaxPerTTL caps how many translation warning lines a
	// single tenant can log per TTL window, bounding log volume even when a
	// tenant produces an unbounded stream of distinct warnings. The window is
	// fixed, anchored at the tenant's first logged line, not sliding: a burst
	// exhausting the budget silences further lines, including the
	// suppressed-summary line, until the window expires. The budget is the
	// total number of log lines per tenant per window, by design.
	otelTranslationWarningMaxPerTTL = 30

	// otelTranslationWarningDedupMaxEntries bounds the dedup map size across
	// all tenants. When full of unexpired entries, warnings are logged without
	// being remembered: bounded memory is favored over exact suppression.
	otelTranslationWarningDedupMaxEntries = 16384

	// otelTranslationWarningPurgeInterval is how often expired dedup entries
	// get purged. Purging scans the whole dedup map under the lock, so it runs
	// on a schedule rather than on map pressure, bounding its amortized cost
	// on the request path.
	otelTranslationWarningPurgeInterval = time.Minute

	// maxLoggedOTelTranslationWarningsPerRequest caps warning log lines per
	// request. Warnings suppressed by this cap are not marked as seen, so they
	// get logged on a later request instead.
	maxLoggedOTelTranslationWarningsPerRequest = 10

	// otelTranslationWarningsSuppressedKey is the deduper key for the summary
	// line about warnings suppressed by the per-request cap, so that the
	// summary itself is logged once per TTL per tenant. The raw \x00 byte
	// cannot appear in real warning messages, whose attribute names are
	// %q-quoted.
	otelTranslationWarningsSuppressedKey = "\x00suppressed-summary"
)

// otelTranslationWarningDeduper rate-limits OTLP translation warning log lines.
// Each distinct (tenant, message) pair is allowed once per TTL, and each tenant
// is allowed a bounded number of lines per fixed (non-sliding) TTL window.
// Translation warnings are typically chronic properties of a tenant's
// instrumentation, re-detected on every request; without suppression an
// affected tenant would log them at request rate.
type otelTranslationWarningDeduper struct {
	ttl time.Duration
	now func() time.Time

	mtx sync.Mutex
	// seen tracks when a (tenant, message) pair was last logged, keyed by
	// tenant + "\x00" + message.
	seen map[string]time.Time
	// tenantWindows tracks per-tenant log budgets, keyed by tenant.
	tenantWindows map[string]*tenantWarningWindow
	// lastPurge is when expired entries were last purged from both maps.
	lastPurge time.Time
}

type tenantWarningWindow struct {
	start  time.Time
	logged int
}

func newOTelTranslationWarningDeduper(ttl time.Duration, now func() time.Time) *otelTranslationWarningDeduper {
	return &otelTranslationWarningDeduper{
		ttl:           ttl,
		now:           now,
		seen:          map[string]time.Time{},
		tenantWindows: map[string]*tenantWarningWindow{},
		lastPurge:     now(),
	}
}

// allow reports whether the warning should be logged now for the given tenant,
// and if so, suppresses further occurrences for the TTL.
func (d *otelTranslationWarningDeduper) allow(tenantID, warning string) bool {
	key := tenantID + "\x00" + warning
	now := d.now()

	d.mtx.Lock()
	defer d.mtx.Unlock()

	if now.Sub(d.lastPurge) >= otelTranslationWarningPurgeInterval {
		d.purgeExpired(now)
		d.lastPurge = now
	}

	if last, ok := d.seen[key]; ok && now.Sub(last) < d.ttl {
		return false
	}

	window := d.tenantWindows[tenantID]
	if window == nil || now.Sub(window.start) >= d.ttl {
		window = &tenantWarningWindow{start: now}
		d.tenantWindows[tenantID] = window
	}
	if window.logged >= otelTranslationWarningMaxPerTTL {
		return false
	}
	window.logged++

	// When the map is full of unexpired entries between purges, the warning is
	// logged without being remembered; the per-tenant budget above still bounds
	// the log volume.
	if len(d.seen) < otelTranslationWarningDedupMaxEntries {
		d.seen[key] = now
	}
	return true
}

func (d *otelTranslationWarningDeduper) purgeExpired(now time.Time) {
	for key, last := range d.seen {
		if now.Sub(last) >= d.ttl {
			delete(d.seen, key)
		}
	}
	for tenantID, window := range d.tenantWindows {
		if now.Sub(window.start) >= d.ttl {
			delete(d.tenantWindows, tenantID)
		}
	}
}
