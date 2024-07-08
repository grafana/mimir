package chunkreplyformatter

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func TestChunkFormatter_IngesterChunk(t *testing.T) {
	formatter := NewChunkReplyFormatter()
	formatter.StartSeries("series1")
	formatter.FormatIngesterChunkInfo("source1", []client.Chunk{
		{
			StartTimestampMs: 1000,
			EndTimestampMs:   2000,
			Data:             []byte("data1"),
		},
		{
			StartTimestampMs: 2000,
			EndTimestampMs:   4000,
			Data:             []byte("data2"),
		},
	})
	formatter.EndSeries()
	formatter.StartSeries("series2")
	formatter.FormatIngesterChunkInfo("source1", []client.Chunk{
		{
			StartTimestampMs: 1000,
			EndTimestampMs:   2000,
			Data:             []byte("data1"),
		},
	})
	formatter.FormatIngesterChunkInfo("source2", []client.Chunk{
		{
			StartTimestampMs: 2000,
			EndTimestampMs:   4000,
			Data:             []byte("data2"),
		},
	})
	formatter.EndSeries()
	expected := `{"series1":{"source1":["1:1:5:57ca2ca6","0:2:5:cec37d1c"]},"series2":{"source1":["1:1:5:57ca2ca6"],"source2":["2:2:5:cec37d1c"]}}`
	require.Equal(t, expected, formatter.GetChunkInfo())
	require.True(t, json.Valid([]byte(expected)))
}

func TestChunkFormatter_StoreGatewayChunk(t *testing.T) {
	formatter := NewChunkReplyFormatter()
	formatter.StartSeries("series1")
	formatter.FormatStoreGatewayChunkInfo("source1", []storepb.AggrChunk{
		{
			MinTime: 1000,
			MaxTime: 2000,
			Raw:     storepb.Chunk{Data: []byte("data1")},
		},
		{
			MinTime: 2000,
			MaxTime: 4000,
			Raw:     storepb.Chunk{Data: []byte("data2")},
		},
	})
	formatter.EndSeries()
	formatter.StartSeries("series2")
	formatter.FormatStoreGatewayChunkInfo("source1", []storepb.AggrChunk{
		{
			MinTime: 1000,
			MaxTime: 2000,
			Raw:     storepb.Chunk{Data: []byte("data1")},
		},
	})
	formatter.FormatStoreGatewayChunkInfo("source2", []storepb.AggrChunk{
		{
			MinTime: 2000,
			MaxTime: 4000,
			Raw:     storepb.Chunk{Data: []byte("data2")},
		},
	})
	formatter.EndSeries()

	expected := `{"series1":{"source1":["1:1:5:57ca2ca6","0:2:5:cec37d1c"]},"series2":{"source1":["1:1:5:57ca2ca6"],"source2":["2:2:5:cec37d1c"]}}`
	require.Equal(t, expected, formatter.GetChunkInfo())
	require.True(t, json.Valid([]byte(expected)))
}

// Test nil chunks array.
func TestChunkFormatter_NilChunks(t *testing.T) {
	formatter := NewChunkReplyFormatter()
	formatter.StartSeries("series1")
	formatter.FormatIngesterChunkInfo("source1", nil)
	formatter.EndSeries()
	expected := `{"series1":{"source1":[]}}`
	require.Equal(t, expected, formatter.GetChunkInfo())
	require.True(t, json.Valid([]byte(expected)))
}

// Find at least 2 split points and check that the output is valid JSON.
func TestChunkFormatter_MaxSize(t *testing.T) {
	formatter := NewChunkReplyFormatter()
	splits := 0
	for i := int64(0); i < maxSize; i++ {
		formatter.StartSeries(fmt.Sprintf("series%d", i))
		formatter.FormatIngesterChunkInfo("source1", []client.Chunk{
			{
				StartTimestampMs: 2000 * i,
				EndTimestampMs:   2000*i + 1000,
				Data:             []byte("data1"),
			},
		})
		if formatter.EndSeries() {
			splits++
			part := formatter.GetChunkInfo()
			require.True(t, json.Valid([]byte(part)))
		}
		if splits == 2 {
			break
		}
	}
	require.Equal(t, 2, splits)
}
