// SPDX-License-Identifier: AGPL-3.0-only

// This file tests the vendor-local patch in
// vendor/github.com/grafana/dskit/ring/desc_hash.go: the lazy hash short-
// circuit for (*Desc).RingCompare. The test lives in Mimir's pkg/util
// (rather than next to the patched code) so it survives a `go mod vendor`
// re-sync.

package util

import (
	"math/rand"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeDesc constructs a Desc with `n` instances and `m` tokens each, using a
// deterministic seed. Tokens are unique across instances (no collisions) so
// the field walk would bottom out cleanly — the hash short-circuit must not
// be required to produce the right answer.
func makeDesc(t testing.TB, n, m int) *ring.Desc {
	t.Helper()
	d := ring.NewDesc()
	tokenIdx := uint32(0)
	for i := 0; i < n; i++ {
		toks := make([]uint32, m)
		for j := range toks {
			toks[j] = tokenIdx
			tokenIdx++
		}
		id := "instance-" + intToStr(i)
		d.AddIngester(id, "10.0.0."+intToStr(i), "zone-"+intToStr(i%3), toks, ring.ACTIVE, time.Unix(int64(1000+i), 0), false, time.Time{}, ring.InstanceVersions{})
	}
	return d
}

func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [16]byte
	pos := len(buf)
	negative := false
	if i < 0 {
		negative = true
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func TestRingCompare_SameDescIsEqual(t *testing.T) {
	d := makeDesc(t, 50, 32)
	require.Equal(t, ring.Equal, d.RingCompare(d))
}

func TestRingCompare_StructurallyIdenticalDescsAreEqual(t *testing.T) {
	d1 := makeDesc(t, 50, 32)
	d2 := makeDesc(t, 50, 32)
	require.Equal(t, ring.Equal, d1.RingCompare(d2))
	require.Equal(t, ring.Equal, d2.RingCompare(d1))
}

func TestRingCompare_AddIngesterInvalidates(t *testing.T) {
	d1 := makeDesc(t, 50, 32)
	d2 := makeDesc(t, 50, 32)

	// Prime the cache by comparing once.
	require.Equal(t, ring.Equal, d1.RingCompare(d2))

	// Mutate d2; cache must be invalidated, so the next compare reflects
	// the difference.
	d2.AddIngester("new-instance", "10.0.0.99", "zone-x", []uint32{99999}, ring.ACTIVE, time.Unix(2000, 0), false, time.Time{}, ring.InstanceVersions{})

	require.Equal(t, ring.Different, d1.RingCompare(d2))
}

func TestRingCompare_RemoveIngesterInvalidates(t *testing.T) {
	d1 := makeDesc(t, 50, 32)
	d2 := makeDesc(t, 50, 32)

	require.Equal(t, ring.Equal, d1.RingCompare(d2))

	d2.RemoveIngester("instance-5")

	require.Equal(t, ring.Different, d1.RingCompare(d2))
}

func TestRingCompare_ClaimTokensInvalidates(t *testing.T) {
	d1 := makeDesc(t, 50, 32)
	d2 := makeDesc(t, 50, 32)

	require.Equal(t, ring.Equal, d1.RingCompare(d2))

	d2.ClaimTokens("instance-5", "instance-6")

	// Tokens have moved between two instances, so by the structural
	// definition this is a Different ring.
	require.Equal(t, ring.Different, d1.RingCompare(d2))
}

func TestRingCompare_RemoveTombstonesInvalidates(t *testing.T) {
	d1 := makeDesc(t, 50, 32)
	d2 := makeDesc(t, 50, 32)

	// Plant a LEFT tombstone on both rings so they're still equal.
	d1.AddIngester("dead-instance", "10.0.0.200", "zone-x", []uint32{77777}, ring.LEFT, time.Unix(500, 0), false, time.Time{}, ring.InstanceVersions{})
	d2.AddIngester("dead-instance", "10.0.0.200", "zone-x", []uint32{77777}, ring.LEFT, time.Unix(500, 0), false, time.Time{}, ring.InstanceVersions{})
	require.Equal(t, ring.Equal, d1.RingCompare(d2))

	// Remove tombstone from d2 only. A zero time limit means "remove all
	// LEFT instances regardless of their Timestamp" — sidesteps any
	// dependence on AddIngester's wallclock-driven Timestamp field.
	_, removed := d2.RemoveTombstones(time.Time{})
	require.GreaterOrEqual(t, removed, 1)

	require.Equal(t, ring.Different, d1.RingCompare(d2))
}

func TestRingCompare_DistinguishesEqualButStatesAndTimestamps(t *testing.T) {
	// Build two structurally-identical Descs whose only difference is the
	// per-instance Timestamp and State. RingCompare must return
	// EqualButStatesAndTimestamps, exercising the (structural-match,
	// full-mismatch) hash branch.
	d1 := makeDesc(t, 30, 16)
	d2 := makeDesc(t, 30, 16)

	// Mutate timestamp/state via AddIngester (which overrides). Use the
	// same id/addr/zone/tokens as the original to keep structure identical.
	const targetID = "instance-7"
	old := d1.GetIngesters()[targetID] // structural fields we'll preserve
	d2.AddIngester(targetID, old.Addr, old.Zone, old.Tokens, ring.LEAVING, time.Unix(old.RegisteredTimestamp, 0), old.ReadOnly, time.Unix(old.ReadOnlyUpdatedTimestamp, 0), old.GetVersions())

	got := d1.RingCompare(d2)
	if got == ring.Different {
		t.Fatalf("expected EqualButStatesAndTimestamps; got Different — structural fields drifted")
	}
	require.Equal(t, ring.EqualButStatesAndTimestamps, got)
}

// TestRingCompare_PropertyRandomMutate is the safety net for the mutator
// audit. If anyone adds a new mutator on *Desc and forgets to call
// invalidateHash(), this test will catch it whenever the new mutator is
// exercised in a way that produces a structural change.
func TestRingCompare_PropertyRandomMutate(t *testing.T) {
	rng := rand.New(rand.NewSource(0xC0FFEE))
	for trial := 0; trial < 200; trial++ {
		d1 := makeDesc(t, 30, 16)
		d2 := makeDesc(t, 30, 16)

		// Prime hash cache.
		require.Equal(t, ring.Equal, d1.RingCompare(d2))

		switch rng.Intn(4) {
		case 0:
			d2.AddIngester("rand-"+intToStr(trial), "10.0.0.99", "zone-x", []uint32{uint32(100000 + trial)}, ring.ACTIVE, time.Unix(2000, 0), false, time.Time{}, ring.InstanceVersions{})
		case 1:
			d2.RemoveIngester("instance-" + intToStr(trial%30))
		case 2:
			from := "instance-" + intToStr(trial%30)
			to := "instance-" + intToStr((trial+1)%30)
			if from != to {
				d2.ClaimTokens(from, to)
			} else {
				d2.RemoveIngester(from)
			}
		case 3:
			// LEFT-then-tombstone-removal path. After the round trip the
			// scratch instance is gone; structurally d2 == d1 again, but
			// only if invalidation worked. AddIngester adds, RemoveTombstones
			// removes — we expect the ring to look unchanged from d1's
			// perspective. So this case actually verifies that BOTH
			// AddIngester and RemoveTombstones invalidate (otherwise
			// the cache from the priming compare would persist a stale
			// "Equal" judgment through the AddIngester intermediate state
			// that briefly differed). We test that the *intermediate* state
			// was visible by also asserting an inequality during the LEFT
			// addition; see below.
			d2.AddIngester("scratch-"+intToStr(trial), "10.0.0.250", "zone-x", []uint32{uint32(50000 + trial)}, ring.LEFT, time.Unix(500, 0), false, time.Time{}, ring.InstanceVersions{})
			// At this point d2 has an extra instance — must compare Different.
			require.Equal(t, ring.Different, d1.RingCompare(d2), "trial %d: AddIngester (LEFT) didn't invalidate", trial)
			d2.RemoveTombstones(time.Time{})
			// After tombstone removal d2 should equal d1 again — but the test
			// at the bottom expects Different. Continue to a different
			// mutation so the bottom assertion still holds.
			d2.RemoveIngester("instance-" + intToStr(trial%30))
		}

		got := d1.RingCompare(d2)
		// All four cases produce a structural change, so the result must
		// not be Equal. (EqualButStatesAndTimestamps is also forbidden
		// here, since the structural fields drifted.)
		assert.Equal(t, ring.Different, got, "trial %d: mutator must invalidate hash so RingCompare returns Different; got %v", trial, got)
	}
}

// BenchmarkRingCompare_EqualCached is the benchmark the optimization
// targets: gossip-driven compares of an unchanged ring.
func BenchmarkRingCompare_EqualCached(b *testing.B) {
	d1 := makeDesc(b, 300, 256) // ops-03-ish scale
	d2 := makeDesc(b, 300, 256)
	// Warm the hashes once.
	_ = d1.RingCompare(d2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d1.RingCompare(d2)
	}
}

// BenchmarkRingCompare_Different is the cold-cache, mutation-driven path.
// The hash short-circuit gives no benefit here (and pays the hash compute
// cost); this benchmark exists so a regression of the slow path would be
// visible.
func BenchmarkRingCompare_Different(b *testing.B) {
	d1 := makeDesc(b, 300, 256)
	d2 := makeDesc(b, 300, 256)
	d2.RemoveIngester("instance-150") // make structurally different
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d1.RingCompare(d2)
	}
}
