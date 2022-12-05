// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/postings_codec_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/storepb/testutil/series.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"context"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
)

func TestDiffVarintCodec(t *testing.T) {
	chunksDir := t.TempDir()

	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = chunksDir
	headOpts.ChunkRange = 1000
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, h.Close())
		assert.NoError(t, os.RemoveAll(chunksDir))
	})

	appendTestSeries(1e4)(t, h.Appender(context.Background()))

	idx, err := h.Index()
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, idx.Close())
	})

	postingsMap := map[string]index.Postings{
		"all":      allPostings(t, idx),
		`n="1"`:    matchPostings(t, idx, labels.MustNewMatcher(labels.MatchEqual, "n", "1"+labelLongSuffix)),
		`j="foo"`:  matchPostings(t, idx, labels.MustNewMatcher(labels.MatchEqual, "j", "foo")),
		`j!="foo"`: matchPostings(t, idx, labels.MustNewMatcher(labels.MatchNotEqual, "j", "foo")),
		`i=~".*"`:  matchPostings(t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", ".*")),
		`i=~".+"`:  matchPostings(t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", ".+")),
		`i=~"1.+"`: matchPostings(t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", "1.+")),
		`i=~"^$"'`: matchPostings(t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", "^$")),
		`i!~""`:    matchPostings(t, idx, labels.MustNewMatcher(labels.MatchNotEqual, "i", "")),
		`n!="2"`:   matchPostings(t, idx, labels.MustNewMatcher(labels.MatchNotEqual, "n", "2"+labelLongSuffix)),
		`i!~"2.*"`: matchPostings(t, idx, labels.MustNewMatcher(labels.MatchNotRegexp, "i", "^2.*$")),
	}

	codecs := map[string]struct {
		codingFunction   func(index.Postings, int) ([]byte, error)
		decodingFunction func([]byte) (index.Postings, error)
	}{
		"raw":    {codingFunction: diffVarintEncodeNoHeader, decodingFunction: func(bytes []byte) (index.Postings, error) { return newDiffVarintPostings(bytes), nil }},
		"snappy": {codingFunction: diffVarintSnappyEncode, decodingFunction: diffVarintSnappyDecode},
	}

	for postingName, postings := range postingsMap {
		p, err := toUint64Postings(postings)
		assert.NoError(t, err)

		for cname, codec := range codecs {
			name := cname + "/" + postingName

			t.Run(name, func(t *testing.T) {
				t.Log("postings entries:", p.len())
				t.Log("original size (4*entries):", 4*p.len(), "bytes")
				p.reset() // We reuse postings between runs, so we need to reset iterator.

				data, err := codec.codingFunction(p, p.len())
				assert.NoError(t, err)

				t.Log("encoded size", len(data), "bytes")
				t.Logf("ratio: %0.3f", float64(len(data))/float64(4*p.len()))

				decodedPostings, err := codec.decodingFunction(data)
				assert.NoError(t, err)

				p.reset()
				comparePostings(t, p, decodedPostings)
			})
		}
	}
}

func comparePostings(t *testing.T, p1, p2 index.Postings) {
	for p1.Next() {
		if !p2.Next() {
			t.Fatal("p1 has more values")
			return
		}

		if p1.At() != p2.At() {
			t.Fatalf("values differ: %d, %d", p1.At(), p2.At())
			return
		}
	}

	if p2.Next() {
		t.Fatal("p2 has more values")
		return
	}

	assert.NoError(t, p1.Err())
	assert.NoError(t, p2.Err())
}

func matchPostings(t testing.TB, ix tsdb.IndexReader, m *labels.Matcher) index.Postings {
	vals, err := ix.LabelValues(m.Name)
	assert.NoError(t, err)

	matching := []string(nil)
	for _, v := range vals {
		if m.Matches(v) {
			matching = append(matching, v)
		}
	}

	p, err := ix.Postings(m.Name, matching...)
	assert.NoError(t, err)
	return p
}

func toUint64Postings(p index.Postings) (*uint64Postings, error) {
	var vals []storage.SeriesRef
	for p.Next() {
		vals = append(vals, p.At())
	}
	return &uint64Postings{vals: vals, ix: -1}, p.Err()
}

// Postings with no decoding step.
type uint64Postings struct {
	vals []storage.SeriesRef
	ix   int
}

func (p *uint64Postings) At() storage.SeriesRef {
	if p.ix < 0 || p.ix >= len(p.vals) {
		return 0
	}
	return p.vals[p.ix]
}

func (p *uint64Postings) Next() bool {
	if p.ix < len(p.vals)-1 {
		p.ix++
		return true
	}
	return false
}

func (p *uint64Postings) Seek(x storage.SeriesRef) bool {
	if p.At() >= x {
		return true
	}

	// We cannot do any search due to how values are stored,
	// so we simply advance until we find the right value.
	for p.Next() {
		if p.At() >= x {
			return true
		}
	}

	return false
}

func (p *uint64Postings) Err() error {
	return nil
}

func (p *uint64Postings) reset() {
	p.ix = -1
}

func (p *uint64Postings) len() int {
	return len(p.vals)
}

func BenchmarkEncodePostings(b *testing.B) {
	const max = 1000000
	r := rand.New(rand.NewSource(0))

	p := make([]storage.SeriesRef, max)

	for ix := 1; ix < len(p); ix++ {
		// Use normal distribution, with stddev=64 (i.e. most values are < 64).
		// This is very rough approximation of experiments with real blocks.v
		d := math.Abs(r.NormFloat64()*64) + 1

		p[ix] = p[ix-1] + storage.SeriesRef(d)
	}

	for _, count := range []int{10000, 100000, 1000000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ps := &uint64Postings{vals: p[:count]}

				_, err := diffVarintEncodeNoHeader(ps, ps.len())
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func allPostings(t testing.TB, ix tsdb.IndexReader) index.Postings {
	k, v := index.AllPostingsKey()
	p, err := ix.Postings(k, v)
	assert.NoError(t, err)
	return p
}
