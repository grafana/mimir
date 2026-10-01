// SPDX-License-Identifier: AGPL-3.0-only

package labels

import (
	"fmt"
	"math/rand/v2"
	"slices"
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

// randomSharingLabels returns labels that mostly share leading pairs, the case SharedPairs skips,
// with name ids and value lengths of one and more varint bytes, and labels that are other ones'
// prefixes.
func randomSharingLabels(rng *rand.Rand, count int) []Labels {
	names := []string{"__name__", "cluster", "job", "pod"}
	// Interned after many others, so their ids take two varint bytes.
	for i := range 200 {
		Intern(fmt.Sprintf("compare_filler_%d", i))
	}
	names = append(names, "zone_late", "a_late")
	slices.Sort(names)
	values := []string{"", "a", "ab", "abcdefgh", "abcdefghi", "abcdefgj", strings.Repeat("v", 130), strings.Repeat("v", 131), strings.Repeat("v", 129) + "w"}
	out := make([]Labels, 0, count)
	for range count {
		chosen := map[string]string{}
		for _, name := range names {
			if rng.IntN(3) > 0 {
				chosen[name] = values[rng.IntN(len(values))]
			}
		}
		var pairs []string
		for _, name := range names {
			if value, ok := chosen[name]; ok {
				pairs = append(pairs, name, value)
			}
		}
		out = append(out, FromStrings(pairs...))
	}
	return out
}

func TestCompareOrdersLikePrometheus(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	all := randomSharingLabels(rng, 400)
	shared := 0
	for _, a := range all {
		for _, b := range all {
			want := promlabels.Compare(promlabels.FromStrings(flatten(a.Pairs())...), promlabels.FromStrings(flatten(b.Pairs())...))
			require.Equal(t, sign(want), sign(Compare(a, b)), "%v vs %v", a, b)
			if start, _ := SharedPairs(a, b); start > 0 {
				shared++
			}
		}
	}
	require.Greater(t, shared, len(all)*len(all)/50, "enough comparisons skip shared pairs")
}

func flatten(pairs [][2]string) []string {
	var out []string
	for _, pair := range pairs {
		out = append(out, pair[0], pair[1])
	}
	return out
}

func sign(c int) int {
	switch {
	case c < 0:
		return -1
	case c > 0:
		return 1
	}
	return 0
}

func TestSharedPairsStopsBeforeTheFirstDifferentPair(t *testing.T) {
	long := strings.Repeat("x", 200)
	a := FromStrings("__name__", "metric", "job", long, "pod", "a")
	b := FromStrings("__name__", "metric", "job", long, "pod", "b")
	start, equal := SharedPairs(a, b)
	require.False(t, equal)
	require.Equal(t, len(FromStrings("__name__", "metric", "job", long)), start)
	// A difference inside a value's bytes, not at its pair's start.
	c := FromStrings("__name__", "metric", "job", long[:199]+"y")
	start, _ = SharedPairs(a, c)
	require.Equal(t, len(FromStrings("__name__", "metric")), start)
	// Labels that are a prefix of the other.
	prefix := FromStrings("__name__", "metric", "job", long)
	start, _ = SharedPairs(prefix, a)
	require.Equal(t, len(prefix), start)
	require.Negative(t, Compare(prefix, a))
	require.Positive(t, Compare(a, prefix))
	_, equal = SharedPairs(a, FromStrings("__name__", "metric", "job", long, "pod", "a"))
	require.True(t, equal)
}

// Labels keep their allocation as long as their series: it holds their encoding and no more.
func TestFromSortedAllocatesTheEncodedSize(t *testing.T) {
	pairs := [][2]string{{"__name__", "http_requests_total"}, {"cluster", "prod-us"}, {"job", "api"}, {"pod", "api-7-abcde"}}
	encoded := FromSorted(pairs)
	// Ids take one or two varint bytes, by when other tests interned their names.
	size := int64(len(encoded))
	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			encoded = FromSorted(pairs)
		}
	})
	require.Equal(t, int64(1), result.AllocsPerOp())
	// Rounded up to its size class, 16 bytes apart at these sizes, but no room for longer varints.
	require.Less(t, result.AllocedBytesPerOp(), size+16)
	require.Equal(t, [][2]string{{"__name__", "http_requests_total"}, {"cluster", "prod-us"}, {"job", "api"}, {"pod", "api-7-abcde"}}, encoded.Pairs())
}
