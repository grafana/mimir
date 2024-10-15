// SPDX-License-Identifier: AGPL-3.0-only

package chunkinfologger

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

var (
	series1Labels = labels.FromStrings("__name__", "test_series", "series_id", "series1")
	series2Labels = labels.FromStrings("__name__", "test_series", "series_id", "series2")
)

func TestChunkFormatter_IngesterChunk(t *testing.T) {
	logger := &testLogger{}
	formatter := NewChunkInfoLogger("test", "123", "456", logger, []string{"series_id"})
	formatter.StartSeries(series1Labels)
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
	formatter.EndSeries(false)
	formatter.StartSeries(series2Labels)
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
	formatter.EndSeries(true)
	expected := `{"series1":{"source1":["1:1:5:57ca2ca6","2:2:5:cec37d1c"]},"series2":{"source1":["1:1:5:57ca2ca6"],"source2":["2:2:5:cec37d1c"]}}`
	require.Len(t, logger.logs, 1)
	require.Contains(t, logger.logs[0], "msg")
	require.Equal(t, "test", logger.logs[0]["msg"])
	require.Contains(t, logger.logs[0], "traceId")
	require.Equal(t, "123", logger.logs[0]["traceId"])
	require.Contains(t, logger.logs[0], "spanId")
	require.Equal(t, "456", logger.logs[0]["spanId"])
	require.Contains(t, logger.logs[0], "info")
	require.JSONEq(t, expected, logger.logs[0]["info"])
}

func TestChunkFormatter_StoreGatewayChunk(t *testing.T) {
	logger := &testLogger{}
	formatter := NewChunkInfoLogger("test", "123", "456", logger, []string{"series_id"})
	formatter.StartSeries(series1Labels)
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
	formatter.EndSeries(false)
	formatter.StartSeries(series2Labels)
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
	formatter.EndSeries(true)

	expected := `{"series1":{"source1":["1:1:5:57ca2ca6","2:2:5:cec37d1c"]},"series2":{"source1":["1:1:5:57ca2ca6"],"source2":["2:2:5:cec37d1c"]}}`
	require.Contains(t, logger.logs[0], "info")
	require.JSONEq(t, expected, logger.logs[0]["info"])
}

// Test nil chunks array.
func TestChunkFormatter_NilChunks(t *testing.T) {
	logger := &testLogger{}
	formatter := NewChunkInfoLogger("test", "123", "456", logger, []string{"series_id"})
	formatter.StartSeries(series1Labels)
	formatter.FormatIngesterChunkInfo("source1", nil)
	formatter.EndSeries(true)
	expected := `{"series1":{"source1":[]}}`
	require.Contains(t, logger.logs[0], "info")
	require.JSONEq(t, expected, logger.logs[0]["info"])
}

// Test multiple labels.
func TestChunkFormatter_MultipleLabels(t *testing.T) {
	logger := &testLogger{}
	formatter := NewChunkInfoLogger("test", "123", "456", logger, []string{"__name__", "series_id"})
	formatter.StartSeries(series1Labels)
	formatter.FormatIngesterChunkInfo("source1", []client.Chunk{
		{
			StartTimestampMs: 1000,
			EndTimestampMs:   2000,
			Data:             []byte("data1"),
		},
	})
	formatter.EndSeries(true)
	expected := `{"test_series,series1":{"source1":["1:1:5:57ca2ca6"]}}`
	require.Contains(t, logger.logs[0], "info")
	require.JSONEq(t, expected, logger.logs[0]["info"])
}

// Find at least 2 split points and check that the output is valid JSON.
func TestChunkFormatter_MaxSize(t *testing.T) {
	logger := &testLogger{}
	formatter := NewChunkInfoLogger("test", "123", "456", logger, []string{"series_id"})
	for i := int64(0); i < maxSize; i++ {
		lbls := labels.FromStrings("series_id", fmt.Sprintf("series%d", i))
		formatter.StartSeries(lbls)
		formatter.FormatIngesterChunkInfo("source1", []client.Chunk{
			{
				StartTimestampMs: 2000 * i,
				EndTimestampMs:   2000*i + 1000,
				Data:             []byte("data1"),
			},
		})
		formatter.EndSeries(false)
		if len(logger.logs) > 1 {
			break
		}
	}
	require.Len(t, logger.logs, 2)
	for i := 0; i < 2; i++ {
		require.Contains(t, logger.logs[i], "info")
		require.True(t, json.Valid([]byte(logger.logs[i]["info"])))
	}
}

func TestChunkFormatter_LogSelect(t *testing.T) {
	logger := &testLogger{}
	formatter := NewChunkInfoLogger("test", "123", "456", logger, []string{"series_id"})
	formatter.LogSelect("test", 1000, 2000)
	require.Len(t, logger.logs, 1)
	require.Contains(t, logger.logs[0], "msg")
	require.Equal(t, "test", logger.logs[0]["msg"])
	require.Contains(t, logger.logs[0], "traceId")
	require.Equal(t, "123", logger.logs[0]["traceId"])
	require.Contains(t, logger.logs[0], "spanId")
	require.Equal(t, "456", logger.logs[0]["spanId"])
	require.Contains(t, logger.logs[0], "minT")
	require.Equal(t, "1000", logger.logs[0]["minT"])
	require.Contains(t, logger.logs[0], "maxT")
	require.Equal(t, "2000", logger.logs[0]["maxT"])
}

type testLogger struct {
	logs []map[string]string
}

func (l *testLogger) Log(keyvals ...interface{}) error {
	log := make(map[string]string)
	for i := 0; i < len(keyvals); i += 2 {
		key := keyvals[i].(string)
		if _, ok := log[key]; ok {
			panic("duplicate key")
		}
		log[key] = keyvals[i+1].(string)
	}
	l.logs = append(l.logs, log)
	return nil
}
