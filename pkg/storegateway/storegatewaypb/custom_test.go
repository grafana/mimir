// SPDX-License-Identifier: AGPL-3.0-only

package storegatewaypb

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
)

type seriesCountsStreamMock struct {
	grpc.ServerStream
	sent []*SeriesCountsResponse
}

func (m *seriesCountsStreamMock) Send(r *SeriesCountsResponse) error {
	m.sent = append(m.sent, r)
	return nil
}

func (m *seriesCountsStreamMock) Context() context.Context { return context.Background() }

type seriesCountsRecvMock struct {
	grpc.ClientStream
	msgs []*SeriesCountsResponse
}

func (m *seriesCountsRecvMock) Recv() (*SeriesCountsResponse, error) {
	if len(m.msgs) == 0 {
		return nil, ioEOF()
	}
	r := m.msgs[0]
	m.msgs = m.msgs[1:]
	return r, nil
}

func TestSendSeriesCountsResponse(t *testing.T) {
	t.Run("an empty answer is one message with every other field", func(t *testing.T) {
		s := &seriesCountsStreamMock{}
		require.NoError(t, SendSeriesCountsResponse(s, &SeriesCountsResponse{BlockIds: []string{"a"}, SeriesCounted: 3, BlocksFromOffsetsTable: 1}))
		require.Len(t, s.sent, 1)
		assert.Equal(t, []string{"a"}, s.sent[0].BlockIds)
		assert.Equal(t, int64(1), s.sent[0].BlocksFromOffsetsTable)
	})

	t.Run("groups are sent in order, in batches of at most cardinalitypb.MessageGroups, the other fields on the first", func(t *testing.T) {
		resp := &SeriesCountsResponse{BlockIds: []string{"a"}, SeriesCounted: 7, WindowMs: 60000}
		for i := range 2*cardinalitypb.MessageGroups + 1 {
			resp.Groups = append(resp.Groups, &cardinalitypb.SeriesCountGroup{Value: fmt.Sprintf("v%06d", i), Counts: []int64{1}})
		}
		s := &seriesCountsStreamMock{}
		require.NoError(t, SendSeriesCountsResponse(s, resp))
		require.Len(t, s.sent, 3)
		assert.Equal(t, int64(7), s.sent[0].SeriesCounted)
		assert.Equal(t, int64(60000), s.sent[0].WindowMs)
		assert.Empty(t, s.sent[1].BlockIds)
		var values []string
		for _, msg := range s.sent {
			assert.LessOrEqual(t, len(msg.Groups), cardinalitypb.MessageGroups)
			for _, g := range msg.Groups {
				values = append(values, g.Value)
			}
		}
		require.Len(t, values, len(resp.Groups))
		assert.IsIncreasing(t, values)

		got, err := cardinalitypb.ReceiveGroups((&seriesCountsRecvMock{msgs: s.sent}).Recv, nil)
		require.NoError(t, err)
		assert.Equal(t, resp.Groups, got.Groups)
		assert.Equal(t, int64(7), got.SeriesCounted)
	})

	t.Run("groups of hashes are split by size", func(t *testing.T) {
		resp := &SeriesCountsResponse{}
		hashes := make([]uint64, 60_000) // about 600 KB a group: 10 bytes a hash
		for i := range hashes {
			hashes[i] = ^uint64(i)
		}
		for i := range 3 {
			resp.Groups = append(resp.Groups, &cardinalitypb.SeriesCountGroup{Value: fmt.Sprint(i), Hashes: hashes})
		}
		s := &seriesCountsStreamMock{}
		require.NoError(t, SendSeriesCountsResponse(s, resp))
		assert.Equal(t, 3, len(s.sent), "two groups would pass cardinalitypb.MessageBytes")
	})
}

func ioEOF() error { return io.EOF }
