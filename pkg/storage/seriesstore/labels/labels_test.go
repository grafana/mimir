// SPDX-License-Identifier: AGPL-3.0-only

package labels

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestShortComparisonsSeeEveryByte(t *testing.T) {
	for n := 0; n < 40; n++ {
		a := make([]byte, n)
		for i := range a {
			a[i] = byte(i)
		}
		require.True(t, ShortEq(string(a), string(append([]byte(nil), a...))))
		require.False(t, ShortEq(string(a), string(append(append([]byte(nil), a...), 0))))
		for position := 0; position < n; position++ {
			b := append([]byte(nil), a...)
			b[position] ^= 0x10
			require.False(t, ShortEq(string(a), string(b)), "len %d, position %d", n, position)
		}
	}
	labels := FromStrings("__name__", "up", "job", "api")
	require.True(t, labels.EqPairs([][2]string{{"__name__", "up"}, {"job", "api"}}))
	require.False(t, labels.EqPairs([][2]string{{"__name__", "up"}}))
	require.False(t, labels.EqPairs([][2]string{{"__name__", "up"}, {"job", "api"}, {"x", ""}}))
	require.False(t, labels.EqPairs([][2]string{{"__name__", "up"}, {"job", "apI"}}))
}

func TestMissingNamesDoNotLockTheIndexOnEveryLookup(t *testing.T) {
	missing := "a_name_no_series_has"
	_, ok := Lookup(missing)
	require.False(t, ok)
	locks := indexLocks.Load()
	for i := 0; i < 100; i++ {
		_, ok := Lookup(missing)
		require.False(t, ok)
	}
	// Tests running in parallel add names, which may take the lock, but lookups never do.
	require.Less(t, indexLocks.Load()-locks, uint64(50), "missing names locked on every lookup")
	// A name another goroutine adds later is found.
	idc := make(chan uint32)
	go func() { idc <- Intern(missing) }()
	id := <-idc
	got, ok := Lookup(missing)
	require.True(t, ok)
	require.Equal(t, id, got)
}

func TestLabelsRoundTripAndCompareByNameThenValue(t *testing.T) {
	long := strings.Repeat("x", 300)
	a := FromStrings("__name__", "up", "job", "api", "long", long)
	require.Equal(t, [][2]string{{"__name__", "up"}, {"job", "api"}, {"long", long}}, a.Pairs())
	require.Equal(t, "api", a.Get("job"))
	require.Equal(t, "", a.Get("missing"))
	require.Equal(t, 3, a.Len())
	b := FromStrings("__name__", "up", "job", "b")
	require.Negative(t, Compare(a, b))
	require.Equal(t, a, FromSorted(a.Pairs()))
	// 19 labels cost their bytes plus a few per label, not 40 each.
	var many [][2]string
	for i := 0; i < 19; i++ {
		many = append(many, [2]string{fmt.Sprintf("label_%02d", i), fmt.Sprintf("value_%04d", i)})
	}
	require.Less(t, FromSorted(many).HeapSize(), 19*14)
}

func TestNameIdsAreStableAcrossGoroutines(t *testing.T) {
	names := make([]string, 500)
	for i := range names {
		names[i] = fmt.Sprintf("name_%d", i)
	}
	ids := make([][]uint32, 4)
	var wg sync.WaitGroup
	for g := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, name := range names {
				ids[g] = append(ids[g], Intern(name))
			}
		}()
	}
	wg.Wait()
	for g := 1; g < len(ids); g++ {
		require.Equal(t, ids[0], ids[g])
	}
	for i, name := range names {
		require.Equal(t, name, Name(ids[0][i]))
		id, ok := Lookup(name)
		require.True(t, ok)
		require.Equal(t, ids[0][i], id)
	}
}

// The hashes are Go's, so Go and Rust ingesters shard queries and key series alike.
func TestHashesMatchPrometheus(t *testing.T) {
	for _, pairs := range [][]string{
		{"__name__", "up"},
		{"__name__", "up", "job", "api", "long", strings.Repeat("v", 300)},
		{"a", "", "b", strings.Repeat("w", 70000)},
	} {
		prom := promlabels.FromStrings(pairs...)
		ours := FromStrings(pairs...)
		require.Equal(t, promlabels.StableHash(prom), StableHash(ours))
		require.Equal(t, StableHash(ours), StableHashPairs(ours.Pairs()))
		require.Equal(t, ours.Hash(), HashPairs(ours.Pairs()))
	}
}

func TestValueOfMatchesRangeForMultiByteIdsAndLengths(t *testing.T) {
	// Interning enough names pushes later ids past one varint byte.
	for i := 0; i < 300; i++ {
		Intern(fmt.Sprintf("value_of_name_%03d", i))
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 500 {
		var pairs [][2]string
		for i := 0; i < 300; i += 1 + rng.IntN(60) {
			pairs = append(pairs, [2]string{fmt.Sprintf("value_of_name_%03d", i), strings.Repeat("v", rng.IntN(300))})
		}
		l := FromSorted(pairs)
		l.Range(func(name, value string) {
			id, _ := Lookup(name)
			require.Equal(t, value, l.ValueOf(id), name)
		})
		require.Empty(t, l.ValueOf(Intern("value_of_absent")))
	}
}

func BenchmarkValueOf(b *testing.B) {
	l := FromStrings("__name__", "http_requests_total", "cluster", "prod-us-central-0", "instance", "10.0.0.1:9090", "job", "api", "pod", "api-7d9f8b6c5-x2k4j", "zone", "us-central1-a")
	id, _ := Lookup("zone")
	var sink string
	for b.Loop() {
		sink = l.ValueOf(id)
	}
	_ = sink
}
