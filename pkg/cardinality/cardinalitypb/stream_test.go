// SPDX-License-Identifier: AGPL-3.0-only

package cardinalitypb_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

func TestSendGroups_SplitsAGroupOfHashesOverTheMessageSize(t *testing.T) {
	hashes := make([]uint64, 5_000)
	for i := range hashes {
		// Large values, so each takes the 10 bytes of the widest varint.
		hashes[i] = 1<<63 + uint64(i)
	}
	big := &cardinalitypb.SeriesCountGroup{Value: "up", Hashes: hashes}
	small := &cardinalitypb.SeriesCountGroup{Value: "v", Hashes: []uint64{1, 2}}
	resp := &storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{big, small}, SeriesCounted: 5_002}
	const maxBytes = 4 << 10

	var sent []*storegatewaypb.SeriesCountsResponse
	require.NoError(t, cardinalitypb.SendGroups(func(m *storegatewaypb.SeriesCountsResponse) error {
		sent = append(sent, m)
		return nil
	}, resp, 100, maxBytes))

	require.Greater(t, len(sent), 10, "5,000 hashes of 10 bytes are over twelve messages of 4 KiB")
	parts := 0
	got := map[uint64]bool{}
	for _, m := range sent {
		// The size limit holds for every message, the per-message overhead aside.
		assert.LessOrEqual(t, m.Size(), maxBytes+64)
		for _, g := range m.Groups {
			if g.Value == "up" {
				parts++
				for _, h := range g.Hashes {
					assert.False(t, got[h], "hash %d sent twice", h)
					got[h] = true
				}
			}
		}
	}
	assert.Greater(t, parts, 1)
	assert.Len(t, got, len(hashes))
	assert.Equal(t, int64(5_002), sent[0].SeriesCounted, "the other fields go on the first message")
	assert.Len(t, resp.Groups[0].Hashes, len(hashes), "the response is not modified")

	// ReceiveGroups keeps the parts as groups of the same value, for the caller to union.
	i := 0
	out, err := cardinalitypb.ReceiveGroups(func() (*storegatewaypb.SeriesCountsResponse, error) {
		if i == len(sent) {
			return nil, io.EOF
		}
		i++
		return sent[i-1], nil
	}, nil)
	require.NoError(t, err)
	total := 0
	for _, g := range out.Groups {
		if g.Value == "up" {
			total += len(g.Hashes)
		}
	}
	assert.Equal(t, len(hashes), total)
}

func TestSendGroups_NeverSplitsCounts(t *testing.T) {
	resp := &storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{7}}}}
	var sent []*storegatewaypb.SeriesCountsResponse
	require.NoError(t, cardinalitypb.SendGroups(func(m *storegatewaypb.SeriesCountsResponse) error {
		sent = append(sent, m)
		return nil
	}, resp, 100, 1))
	require.Len(t, sent, 1)
	assert.Equal(t, []int64{7}, sent[0].Groups[0].Counts)
}
