// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"testing"

	"github.com/grafana/dskit/grpcutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/cost"
)

func TestBucketStoreSampleQueryCost(t *testing.T) {
	store := prepareSearchTestStore(t)
	res, err := store.SampleQueryCost(t.Context(), cost.NewRequest(0, math.MaxInt64, false, []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "a", ".+")}))
	require.NoError(t, err)
	require.NoError(t, res.Validate())
	require.False(t, res.Unsupported)
	require.Positive(t, res.Points)
	require.Positive(t, res.IntervalMilliseconds)
	require.NotEmpty(t, res.AvailableBlocks)
	require.LessOrEqual(t, res.SeriesExamined, uint64(cost.MaxSeries))
	require.LessOrEqual(t, res.ChunksExamined, uint64(cost.MaxChunks))
	require.LessOrEqual(t, res.ChunkBytes, uint64(cost.MaxChunkBytes))

	res, err = store.SampleQueryCost(t.Context(), &cost.SamplingRequest{Start: 0, End: math.MaxInt64, BlockIds: []string{"unassigned"}})
	require.NoError(t, err)
	require.Zero(t, res.Points)
	require.Empty(t, res.AvailableBlocks)
	_, err = store.SampleQueryCost(t.Context(), &cost.SamplingRequest{Start: 1, End: 0})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = store.SampleQueryCost(ctx, &cost.SamplingRequest{End: math.MaxInt64})
	grpcStatus, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok)
	require.Equal(t, codes.Canceled, grpcStatus.Code())
}

type samplingRangeBucket struct {
	objstore.BucketReader
	data   []byte
	reads  [][2]int64
	closed int
}

func (b *samplingRangeBucket) GetRange(_ context.Context, _ string, offset, length int64) (io.ReadCloser, error) {
	b.reads = append(b.reads, [2]int64{offset, length})
	return &samplingRangeReader{Reader: bytes.NewReader(b.data[offset:min(int64(len(b.data)), offset+length)]), close: func() { b.closed++ }}, nil
}

type samplingRangeReader struct {
	io.Reader
	close func()
}

func (r *samplingRangeReader) Close() error { r.close(); return nil }

func TestReadCostSampleChunkBoundsIO(t *testing.T) {
	chunk := chunkenc.NewXORChunk()
	app, err := chunk.Appender()
	require.NoError(t, err)
	app.Append(0, 0, 1)
	app.Append(0, 15000, 2)
	payload := append([]byte{byte(chunk.Encoding())}, chunk.Bytes()...)
	data := binary.AppendUvarint(nil, uint64(len(chunk.Bytes())))
	data = append(data, payload...)
	data = binary.BigEndian.AppendUint32(data, crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
	for _, tc := range []struct {
		name    string
		budget  uint64
		corrupt bool
		reads   int
	}{
		{"too large", uint64(len(chunk.Bytes()) - 1), false, 1},
		{"exact budget", uint64(len(chunk.Bytes())), false, 2},
		{"corrupt", uint64(len(chunk.Bytes())), true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bkt := &samplingRangeBucket{data: bytes.Clone(data)}
			if tc.corrupt {
				bkt.data[len(bkt.data)-1] ^= 1
			}
			b := &bucketBlock{bkt: bkt, chunkObjs: []string{"chunks/000001"}}
			got, err := readCostSampleChunk(t.Context(), b, seriesChunkRef{}, tc.budget, newSafeQueryStats())
			require.Len(t, bkt.reads, tc.reads)
			require.Equal(t, tc.reads, bkt.closed)
			require.Equal(t, [2]int64{0, binary.MaxVarintLen32}, bkt.reads[0])
			if tc.corrupt {
				require.ErrorContains(t, err, "crc32 mismatch")
				return
			}
			require.NoError(t, err)
			if tc.reads == 1 {
				require.Nil(t, got)
			} else {
				require.Equal(t, chunk.Bytes(), got.Bytes())
			}
		})
	}
}
