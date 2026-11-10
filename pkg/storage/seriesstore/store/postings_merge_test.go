// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func (b *seriesByName) refsOfFilteredReference(postings []uint32, total int, filters [][]uint32, groupID uint32, hasGroup bool) []seriesRef {
	if len(filters) == 0 && !hasGroup {
		return b.refsOf(postings, total)
	}
	refs := make([]seriesRef, 0, min(total, 1024))
	keep := func(id uint32) bool {
		if hasGroup && b.refs[id].group != groupID {
			return false
		}
		for _, filter := range filters {
			if !b.inPostings(id, filter) {
				return false
			}
		}
		return true
	}
	for _, posting := range postings {
		one, many := b.posting(posting)
		if many == nil {
			if keep(one) {
				refs = append(refs, b.refs[one])
			}
			continue
		}
		for _, id := range many {
			if keep(id) {
				refs = append(refs, b.refs[id])
			}
		}
	}
	return sortRefs(refs)
}

func TestFilteredPostingsMerge(t *testing.T) {
	r := rand.New(rand.NewPCG(71, 29))
	for trial := range 500 {
		b := seriesByName{refs: make([]seriesRef, 8192)}
		for i := range b.refs {
			b.refs[i] = seriesRef{group: uint32(i % 3), index: int32(len(b.refs) - i), hash: uint64(i)}
		}
		sizes := []int{0, 1, 31, 32, 33, 64, 256, 257, 4096}
		var handles []uint32
		for n := range 5 {
			size := sizes[(trial+n)%len(sizes)]
			if size == 1 {
				handles = append(handles, uint32(r.IntN(len(b.refs))))
				continue
			}
			var ids []uint32
			for i := range b.refs {
				if r.IntN(len(b.refs)) < size {
					ids = append(ids, uint32(i))
				}
			}
			handles = append(handles, sharedPosting|uint32(len(b.sharedPostings)))
			b.sharedPostings = append(b.sharedPostings, ids)
		}
		postings := handles[:1]
		filters := [][]uint32{{handles[1]}}
		switch trial % 4 {
		case 1:
			postings = handles[:2]
		case 2:
			filters = append(filters, []uint32{handles[2]})
		case 3:
			filters[0] = handles[1:3]
		}
		total := 0
		for _, p := range postings {
			total += b.postingLen(p)
		}
		for _, group := range []bool{false, true} {
			want := b.refsOfFilteredReference(postings, total, filters, 1, group)
			got := b.refsOfFiltered(postings, total, filters, 1, group)
			require.Equal(t, want, got, "trial %d", trial)
		}
	}
}

func BenchmarkFilteredPostings(b *testing.B) {
	for _, ratio := range []int{1, 8, 9, 64} {
		for _, size := range []int{16, 32, 4096} {
			index := seriesByName{refs: make([]seriesRef, size*ratio*2)}
			for i := range index.refs {
				index.refs[i] = seriesRef{index: int32(i), hash: uint64(i)}
			}
			a, c := make([]uint32, size), make([]uint32, size*ratio)
			for i := range a {
				a[i] = uint32(i * 2)
			}
			for i := range c {
				c[i] = uint32(i)
			}
			index.sharedPostings = [][]uint32{a, c}
			for _, merge := range []bool{false, true} {
				b.Run(fmt.Sprintf("size%d/ratio%d/merge%t", size, ratio, merge), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						var refs []seriesRef
						if merge {
							refs = index.refsOfFiltered([]uint32{sharedPosting}, size, [][]uint32{{sharedPosting | 1}}, 0, false)
						} else {
							refs = index.refsOfFilteredReference([]uint32{sharedPosting}, size, [][]uint32{{sharedPosting | 1}}, 0, false)
						}
						runtime.KeepAlive(refs)
					}
				})
			}
		}
	}
}

func postingsBenchEngine(t testing.TB) *Engine {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	app := e.Appender(t.Context())
	for n := range 65536 {
		l := promlabels.FromStrings("__name__", "metric", "pod", fmt.Sprint(n), "zone", fmt.Sprint(n%24), "rack", fmt.Sprint((n/24)%16))
		_, err := app.Append(0, l, 0, 0, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	return e
}

func postingsBenchQueries() []struct {
	name     string
	matchers []*promlabels.Matcher
} {
	eq := func(k, v string) *promlabels.Matcher { return promlabels.MustNewMatcher(promlabels.MatchEqual, k, v) }
	return []struct {
		name     string
		matchers []*promlabels.Matcher
	}{
		{"dict_dense", []*promlabels.Matcher{eq("__name__", "metric"), promlabels.MustNewMatcher(promlabels.MatchNotEqual, "zone", "3")}},
		{"dict_sparse", []*promlabels.Matcher{eq("__name__", "metric"), promlabels.MustNewMatcher(promlabels.MatchRegexp, "zone", "[0-3]")}},
		{"postings", []*promlabels.Matcher{eq("zone", "3"), eq("rack", "4")}},
		{"control", []*promlabels.Matcher{eq("pod", "123")}},
	}
}

func BenchmarkPostingsQueries(b *testing.B) {
	// Cache admission depends on elapsed time, so host load must not decide which path is measured.
	old := minGeneralBuild
	minGeneralBuild = time.Hour
	b.Cleanup(func() { minGeneralBuild = old })
	e := postingsBenchEngine(b)
	for _, qcase := range postingsBenchQueries() {
		for _, op := range []string{"values", "names", "stream"} {
			b.Run(qcase.name+"/"+op, func(b *testing.B) {
				run := func() {
					if op == "stream" {
						q, err := e.ChunkQuerier(0, 1)
						if err != nil {
							b.Fatal(err)
						}
						got, err := q.(*engineChunkQuerier).SelectStreamed(nil, qcase.matchers...)
						if err != nil {
							b.Fatal(err)
						}
						runtime.KeepAlive(got)
						if err := q.Close(); err != nil {
							b.Fatal(err)
						}
					} else {
						q, err := e.Querier(0, 1)
						if err != nil {
							b.Fatal(err)
						}
						if op == "names" {
							got, _, err := q.LabelNames(b.Context(), nil, qcase.matchers...)
							if err != nil {
								b.Fatal(err)
							}
							runtime.KeepAlive(got)
						} else {
							got, _, err := q.LabelValues(b.Context(), "pod", nil, qcase.matchers...)
							if err != nil {
								b.Fatal(err)
							}
							runtime.KeepAlive(got)
						}
						if err := q.Close(); err != nil {
							b.Fatal(err)
						}
					}
				}
				// Metadata dictionaries must be built before timing repeated requests.
				run()
				b.ReportAllocs()
				for b.Loop() {
					run()
				}
			})
		}
	}
}

func BenchmarkPostingsQueriesCached(b *testing.B) {
	old := minGeneralBuild
	minGeneralBuild = 0
	b.Cleanup(func() { minGeneralBuild = old })
	e := postingsBenchEngine(b)
	matchers := postingsBenchQueries()[2].matchers
	for range 10 {
		q, err := e.ChunkQuerier(0, 1)
		require.NoError(b, err)
		_, err = q.(*engineChunkQuerier).SelectStreamed(nil, matchers...)
		require.NoError(b, err)
		require.NoError(b, q.Close())
	}
	b.ReportAllocs()
	for b.Loop() {
		q, err := e.ChunkQuerier(0, 1)
		if err != nil {
			b.Fatal(err)
		}
		got, err := q.(*engineChunkQuerier).SelectStreamed(nil, matchers...)
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(got)
		if err = q.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFilteredPostingsOutsideGroup(b *testing.B) {
	index := seriesByName{refs: make([]seriesRef, 8192)}
	for i := range index.refs {
		index.refs[i] = seriesRef{group: 1, index: int32(i)}
	}
	a, c := make([]uint32, 4096), make([]uint32, 4096)
	for i := range a {
		a[i] = uint32(i * 2)
		c[i] = uint32(i)
	}
	index.sharedPostings = [][]uint32{a, c}
	for _, merge := range []bool{false, true} {
		b.Run(fmt.Sprint(merge), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var refs []seriesRef
				if merge {
					refs = index.refsOfFiltered([]uint32{sharedPosting}, len(a), [][]uint32{{sharedPosting | 1}}, 0, true)
				} else {
					refs = index.refsOfFilteredReference([]uint32{sharedPosting}, len(a), [][]uint32{{sharedPosting | 1}}, 0, true)
				}
				runtime.KeepAlive(refs)
			}
		})
	}
}
