// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
)

// fakeLabelNamesAndValuesStream replays a fixed list of messages, then either fails or ends.
type fakeLabelNamesAndValuesStream struct {
	msgs    []*ingester_client.LabelNamesAndValuesResponse
	idx     int
	tailErr error
}

func (f *fakeLabelNamesAndValuesStream) Recv() (*ingester_client.LabelNamesAndValuesResponse, error) {
	if f.idx < len(f.msgs) {
		msg := f.msgs[f.idx]
		f.idx++
		return msg, nil
	}
	if f.tailErr != nil {
		return nil, f.tailErr
	}
	return nil, io.EOF
}

// lnvMsg builds one wire message from alternating label name and value-list arguments.
func lnvMsg(pairs ...any) *ingester_client.LabelNamesAndValuesResponse {
	res := &ingester_client.LabelNamesAndValuesResponse{}
	for i := 0; i < len(pairs); i += 2 {
		res.Items = append(res.Items, &ingester_client.LabelValues{
			LabelName: pairs[i].(string),
			Values:    pairs[i+1].([]string),
		})
	}
	return res
}

// newTestCursors primes one cursor per stream. Cancel funcs are nil, which is the in-process test
// path: there is no gRPC stream or ring resource to release.
func newTestCursors(t *testing.T, streams ...*fakeLabelNamesAndValuesStream) []*labelNamesAndValuesCursor {
	t.Helper()
	cursors := make([]*labelNamesAndValuesCursor, 0, len(streams))
	for _, s := range streams {
		c := newLabelNamesAndValuesCursor(s, nil, nil)
		require.NoError(t, c.prime())
		cursors = append(cursors, c)
	}
	return cursors
}

func lnvStream(msgs ...*ingester_client.LabelNamesAndValuesResponse) *fakeLabelNamesAndValuesStream {
	return &fakeLabelNamesAndValuesStream{msgs: msgs}
}

func TestMergeLabelNamesCardinality(t *testing.T) {
	tests := map[string]struct {
		streams  []*fakeLabelNamesAndValuesStream
		limit    int
		expected LabelNamesCardinalityResult
	}{
		"no streams": {
			streams:  nil,
			limit:    10,
			expected: LabelNamesCardinalityResult{Items: []LabelNameCardinality{}},
		},
		"a stream that ends immediately": {
			streams:  []*fakeLabelNamesAndValuesStream{lnvStream()},
			limit:    10,
			expected: LabelNamesCardinalityResult{Items: []LabelNameCardinality{}},
		},
		"single stream": {
			streams: []*fakeLabelNamesAndValuesStream{
				lnvStream(lnvMsg("a", []string{"1", "2"}, "b", []string{"1"})),
			},
			limit: 10,
			expected: LabelNamesCardinalityResult{
				LabelNamesCount:       2,
				LabelValuesCountTotal: 3,
				Items: []LabelNameCardinality{
					{LabelName: "a", LabelValuesCount: 2},
					{LabelName: "b", LabelValuesCount: 1},
				},
			},
		},
		"deduplicates overlapping values across streams": {
			streams: []*fakeLabelNamesAndValuesStream{
				lnvStream(lnvMsg("a", []string{"1", "2"})),
				lnvStream(lnvMsg("a", []string{"2", "3"})),
				lnvStream(lnvMsg("a", []string{"1", "3"})),
			},
			limit: 10,
			expected: LabelNamesCardinalityResult{
				LabelNamesCount:       1,
				LabelValuesCountTotal: 3,
				Items:                 []LabelNameCardinality{{LabelName: "a", LabelValuesCount: 3}},
			},
		},
		"a label name split across several messages in one stream": {
			streams: []*fakeLabelNamesAndValuesStream{
				lnvStream(
					lnvMsg("a", []string{"1"}),
					lnvMsg("a", []string{"2"}),
					lnvMsg("a", []string{"3"}),
					lnvMsg("b", []string{"1"}),
				),
			},
			limit: 10,
			expected: LabelNamesCardinalityResult{
				LabelNamesCount:       2,
				LabelValuesCountTotal: 4,
				Items: []LabelNameCardinality{
					{LabelName: "a", LabelValuesCount: 3},
					{LabelName: "b", LabelValuesCount: 1},
				},
			},
		},
		"streams holding disjoint label names": {
			streams: []*fakeLabelNamesAndValuesStream{
				lnvStream(lnvMsg("a", []string{"1"}, "c", []string{"1"})),
				lnvStream(lnvMsg("b", []string{"1", "2"})),
			},
			limit: 10,
			expected: LabelNamesCardinalityResult{
				LabelNamesCount:       3,
				LabelValuesCountTotal: 4,
				Items: []LabelNameCardinality{
					{LabelName: "b", LabelValuesCount: 2},
					{LabelName: "a", LabelValuesCount: 1},
					{LabelName: "c", LabelValuesCount: 1},
				},
			},
		},
		"a label name carrying no values is still counted": {
			streams: []*fakeLabelNamesAndValuesStream{
				lnvStream(lnvMsg("a", []string{}, "b", []string{"1"})),
			},
			limit: 10,
			expected: LabelNamesCardinalityResult{
				LabelNamesCount:       2,
				LabelValuesCountTotal: 1,
				Items: []LabelNameCardinality{
					{LabelName: "b", LabelValuesCount: 1},
					{LabelName: "a", LabelValuesCount: 0},
				},
			},
		},
		"one stream ending before the others": {
			streams: []*fakeLabelNamesAndValuesStream{
				lnvStream(lnvMsg("a", []string{"1"})),
				lnvStream(lnvMsg("a", []string{"2"}, "b", []string{"1"}, "c", []string{"1"})),
			},
			limit: 10,
			expected: LabelNamesCardinalityResult{
				LabelNamesCount:       3,
				LabelValuesCountTotal: 4,
				Items: []LabelNameCardinality{
					{LabelName: "a", LabelValuesCount: 2},
					{LabelName: "b", LabelValuesCount: 1},
					{LabelName: "c", LabelValuesCount: 1},
				},
			},
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			cursors := newTestCursors(t, testData.streams...)
			res, err := mergeLabelNamesCardinality(cursors, testData.limit, math.MaxInt)
			require.NoError(t, err)
			assert.Equal(t, testData.expected, res)
		})
	}
}

func TestMergeLabelNamesCardinality_TopN(t *testing.T) {
	// Counts 3, 2, 2, 1: enough to exercise both ranking and the name-ascending tie-break.
	newStream := func() *fakeLabelNamesAndValuesStream {
		return lnvStream(lnvMsg(
			"a", []string{"1", "2", "3"},
			"b", []string{"1", "2"},
			"c", []string{"1", "2"},
			"d", []string{"1"},
		))
	}

	tests := map[string]struct {
		limit         int
		expectedItems []LabelNameCardinality
	}{
		"limit of 0 returns no items but still counts everything": {
			limit:         0,
			expectedItems: []LabelNameCardinality{},
		},
		"limit of 1 keeps the highest count": {
			limit:         1,
			expectedItems: []LabelNameCardinality{{LabelName: "a", LabelValuesCount: 3}},
		},
		"ties are broken by label name ascending": {
			limit: 3,
			expectedItems: []LabelNameCardinality{
				{LabelName: "a", LabelValuesCount: 3},
				{LabelName: "b", LabelValuesCount: 2},
				{LabelName: "c", LabelValuesCount: 2},
			},
		},
		"a limit above the label name count returns everything": {
			limit: 100,
			expectedItems: []LabelNameCardinality{
				{LabelName: "a", LabelValuesCount: 3},
				{LabelName: "b", LabelValuesCount: 2},
				{LabelName: "c", LabelValuesCount: 2},
				{LabelName: "d", LabelValuesCount: 1},
			},
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			res, err := mergeLabelNamesCardinality(newTestCursors(t, newStream()), testData.limit, math.MaxInt)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedItems, res.Items)
			// The totals are exact regardless of the limit.
			assert.Equal(t, 4, res.LabelNamesCount)
			assert.Equal(t, 8, res.LabelValuesCountTotal)
		})
	}
}

func TestMergeLabelNamesCardinality_OutOfOrderStream(t *testing.T) {
	tests := map[string]*fakeLabelNamesAndValuesStream{
		"a label name regressing within a message": lnvStream(lnvMsg("b", []string{"1"}, "a", []string{"1"})),
		"a label name regressing across messages": lnvStream(
			lnvMsg("b", []string{"1"}),
			lnvMsg("a", []string{"1"}),
		),
		"a label name reappearing after a greater one": lnvStream(
			lnvMsg("a", []string{"1"}),
			lnvMsg("b", []string{"1"}),
			lnvMsg("a", []string{"2"}),
		),
	}

	for name, s := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := mergeLabelNamesCardinality(newTestCursors(t, s), 10, math.MaxInt)
			require.ErrorIs(t, err, errLabelNamesStreamOutOfOrder)
		})
	}
}

func TestMergeLabelNamesCardinality_StreamError(t *testing.T) {
	streamErr := errors.New("ingester went away")
	s := &fakeLabelNamesAndValuesStream{
		msgs:    []*ingester_client.LabelNamesAndValuesResponse{lnvMsg("a", []string{"1"})},
		tailErr: streamErr,
	}

	_, err := mergeLabelNamesCardinality(newTestCursors(t, s), 10, math.MaxInt)
	require.ErrorIs(t, err, streamErr)
}

func TestMergeLabelNamesCardinality_SizeLimit(t *testing.T) {
	// The limit is scoped to a single label name, so a wide name trips it while the sum across
	// names does not: "b" alone is 1+3 = 4 bytes, whereas all names together are 8 bytes.
	newStreams := func() []*fakeLabelNamesAndValuesStream {
		return []*fakeLabelNamesAndValuesStream{
			lnvStream(lnvMsg("a", []string{"1", "2"}, "b", []string{"1", "2", "3"})),
		}
	}

	t.Run("fails when one label name exceeds the limit", func(t *testing.T) {
		_, err := mergeLabelNamesCardinality(newTestCursors(t, newStreams()...), 10, 3)
		require.ErrorIs(t, err, ErrResponseTooLarge)
		require.ErrorContains(t, err, "label name b")
	})

	t.Run("succeeds when every individual label name is within the limit", func(t *testing.T) {
		res, err := mergeLabelNamesCardinality(newTestCursors(t, newStreams()...), 10, 4)
		require.NoError(t, err)
		require.Equal(t, 5, res.LabelValuesCountTotal)
	})
}

func TestLabelNamesAndValuesCursor_CloseIsIdempotent(t *testing.T) {
	var streamCancelled, dscCancelled int
	c := newLabelNamesAndValuesCursor(
		lnvStream(lnvMsg("a", []string{"1"})),
		func(error) { streamCancelled++ },
		func(error) { dscCancelled++ },
	)
	require.NoError(t, c.prime())

	require.NoError(t, c.Close())
	require.NoError(t, c.Close())

	// Both must fire: skipping dscCancel leaks the ring resource.
	assert.Equal(t, 1, streamCancelled)
	assert.Equal(t, 1, dscCancelled)
}

// BenchmarkMergeLabelNamesCardinality guards the memory property the merge exists for: because each
// label name is counted and released before the next begins, allocations track the widest single
// label name rather than the total number of (name, value) pairs. Compare the cases below: doubling
// numNames doubles the work but leaves B/op broadly flat, whereas doubling numValues does not.
func BenchmarkMergeLabelNamesCardinality(b *testing.B) {
	const numStreams = 3

	cases := []struct{ numNames, numValues int }{
		{numNames: 20, numValues: 5000},
		{numNames: 40, numValues: 5000},
		{numNames: 80, numValues: 5000},
		{numNames: 20, numValues: 10000},
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("names=%d/values=%d", c.numNames, c.numValues), func(b *testing.B) {
			values := make([]string, c.numValues)
			for i := range values {
				values[i] = "value-" + strconv.Itoa(i)
			}
			// Built once and shared: the merge only reads the messages, and keeping setup out of the
			// loop is what makes the reported allocations those of the merge itself.
			msgs := make([]*ingester_client.LabelNamesAndValuesResponse, 0, c.numNames)
			for i := 0; i < c.numNames; i++ {
				msgs = append(msgs, lnvMsg(fmt.Sprintf("label_%03d", i), values))
			}

			b.ReportAllocs()
			for b.Loop() {
				cursors := make([]*labelNamesAndValuesCursor, 0, numStreams)
				for s := 0; s < numStreams; s++ {
					cur := newLabelNamesAndValuesCursor(&fakeLabelNamesAndValuesStream{msgs: msgs}, nil, nil)
					if err := cur.prime(); err != nil {
						b.Fatal(err)
					}
					cursors = append(cursors, cur)
				}

				res, err := mergeLabelNamesCardinality(cursors, 20, math.MaxInt)
				if err != nil {
					b.Fatal(err)
				}
				if res.LabelValuesCountTotal != c.numNames*c.numValues {
					b.Fatalf("unexpected total %d", res.LabelValuesCountTotal)
				}
			}
		})
	}
}
