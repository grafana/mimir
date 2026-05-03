// SPDX-License-Identifier: Apache-2.0
// Provenance-includes-location: vendor-local-patch (Mimir)
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Mimir Authors.
//
// This file is a vendor-local patch carried by the Mimir
// arve/opt-store-gateway branch. The corresponding upstream change is
// expected to land in dskit before the next vendor refresh; if you see this
// file disappear after a `go mod vendor`, the upstream change is in.

package ring

import (
	"encoding/binary"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

// descHashCache is the process-global sidecar that maps each live *Desc to a
// pair of lazily-computed digests:
//
//   - structural — the union of every field RingCompare returns Different on:
//     Addr, Zone, Tokens, RegisteredTimestamp, ReadOnly,
//     ReadOnlyUpdatedTimestamp, plus the set of instance names.
//   - full — structural plus Timestamp and State (the
//     EqualButStatesAndTimestamps discriminators).
//
// Two Descs whose structural hashes agree are Equal modulo State/Timestamp
// (collision negligible at xxhash64 against this domain). Two Descs whose
// full hashes also agree are wholly Equal. Hash mismatch means the field
// walk must run — but in the steady state, gossip overwhelmingly produces
// hash matches, so the field walk is rarely reached.
//
// Entries are removed by (*Desc).invalidateHash on every mutation. The map
// can briefly retain entries for Descs that are about to be GC'd; we accept
// the small leak rather than introduce a finalizer.
var descHashCache sync.Map // map[*Desc]*descHashEntry

type descHashEntry struct {
	structural atomic.Uint64 // 0 = uncomputed; nonzero = valid
	full       atomic.Uint64
}

func descHashesFor(d *Desc) *descHashEntry {
	if v, ok := descHashCache.Load(d); ok {
		return v.(*descHashEntry)
	}
	e := &descHashEntry{}
	actual, _ := descHashCache.LoadOrStore(d, e)
	return actual.(*descHashEntry)
}

// invalidateHash drops the cached hashes for d. Callers (every mutator on
// *Desc) must invoke this after changing any field that RingCompare reads.
// Idempotent and safe to call on a Desc that has never been hashed.
func (d *Desc) invalidateHash() {
	descHashCache.Delete(d)
}

// computeHashes (re)populates the sidecar entry for d and returns the pair
// (structural, full). Idempotent: the first goroutine to write wins; later
// callers race to identical results. Safe under concurrent calls.
//
// Determinism: instance IDs are sorted before hashing so map iteration order
// can't change the digest.
func (d *Desc) computeHashes() (structural, full uint64) {
	e := descHashesFor(d)
	if s := e.structural.Load(); s != 0 {
		return s, e.full.Load()
	}
	if d == nil || len(d.Ingesters) == 0 {
		// An empty Desc has a stable pair of "non-zero sentinel" hashes.
		// 1 and 2 are arbitrary; the only requirement is that they're
		// non-zero (so the sentinel doesn't collide with "uncomputed") and
		// distinct from any plausible real hash.
		e.structural.Store(1)
		e.full.Store(2)
		return 1, 2
	}

	names := make([]string, 0, len(d.Ingesters))
	for n := range d.Ingesters {
		names = append(names, n)
	}
	sort.Strings(names)

	h1 := xxhash.New()
	h2 := xxhash.New()
	var buf [8]byte
	for _, n := range names {
		ing := d.Ingesters[n]
		// Structural fields go to both hashes.
		for _, h := range [...]*xxhash.Digest{h1, h2} {
			_, _ = h.WriteString(n)
			_, _ = h.WriteString("\x00")
			_, _ = h.WriteString(ing.Addr)
			_, _ = h.WriteString("\x00")
			_, _ = h.WriteString(ing.Zone)
			_, _ = h.WriteString("\x00")
			binary.BigEndian.PutUint64(buf[:], uint64(ing.RegisteredTimestamp))
			_, _ = h.Write(buf[:])
			if ing.ReadOnly {
				_, _ = h.WriteString("r")
			} else {
				_, _ = h.WriteString("w")
			}
			binary.BigEndian.PutUint64(buf[:], uint64(ing.ReadOnlyUpdatedTimestamp))
			_, _ = h.Write(buf[:])
			binary.BigEndian.PutUint64(buf[:], uint64(len(ing.Tokens)))
			_, _ = h.Write(buf[:])
			for _, t := range ing.Tokens {
				binary.BigEndian.PutUint32(buf[:4], t)
				_, _ = h.Write(buf[:4])
			}
		}
		// h2 also covers Timestamp and State — the discriminators between
		// Equal and EqualButStatesAndTimestamps.
		binary.BigEndian.PutUint64(buf[:], uint64(ing.Timestamp))
		_, _ = h2.Write(buf[:])
		binary.BigEndian.PutUint64(buf[:], uint64(ing.State))
		_, _ = h2.Write(buf[:])
	}
	// Force the result non-zero so 0 always means "uncomputed". xxhash can
	// legitimately return 0 (vanishingly rarely); the |1 nudge yields a
	// trivially distinguishable value for that pathological case.
	s := h1.Sum64() | 1
	f := h2.Sum64() | 1
	e.structural.Store(s)
	e.full.Store(f)
	return s, f
}
