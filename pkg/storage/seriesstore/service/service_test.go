// SPDX-License-Identifier: AGPL-3.0-only

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/record"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/store"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func testStore(t *testing.T, series int) *store.Store {
	s := store.Default()
	for index := range series {
		decoded := record.DecodedSeries{Labels: [][2]string{{"__name__", "metric"}, {"job", fmt.Sprintf("job-%d", index%3)}, {"n", fmt.Sprint(index)}}}
		for sample := range int64(300) {
			decoded.Samples = append(decoded.Samples, mimirpb.Sample{TimestampMs: sample * 15_000, Value: float64(index) + float64(sample)})
		}
		require.NoError(t, s.Ingest("tenant", record.DecodedRequest{Series: []record.DecodedSeries{decoded}}))
	}
	return s
}

func serveTest(t *testing.T, s *store.Store) client.IngesterClient {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(ServerCodec())
	client.RegisterIngesterServer(server, New(s))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	return client.NewIngesterClient(connection)
}

func tenantContext() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "x-scope-orgid", "tenant")
}

// Every response the service encodes itself decodes to a message gogo marshals to the same bytes.
func TestQueryStreamResponsesEncodeLikeGogo(t *testing.T) {
	s := testStore(t, 1500)
	views, err := s.SelectChunks("tenant", 0, 1<<62, nil)
	require.NoError(t, err)
	require.Len(t, views, 1500)
	var responses [][]byte
	for start := 0; start < len(views); start += seriesBatchSize {
		end := min(start+seriesBatchSize, len(views))
		responses = append(responses, seriesResponse(views[start:end], end == len(views)))
	}
	require.NoError(t, sendChunks(views, 7, func(encoded []byte) error {
		responses = append(responses, encoded)
		return nil
	}))
	series, chunkItems := 0, 0
	for index, encoded := range responses {
		var decoded client.QueryStreamResponse
		require.NoError(t, decoded.Unmarshal(encoded))
		remarshalled, err := decoded.Marshal()
		require.NoError(t, err)
		require.Equal(t, remarshalled, encoded, "response %d", index)
		series += len(decoded.StreamingSeries)
		chunkItems += len(decoded.StreamingSeriesChunks)
		require.LessOrEqual(t, len(decoded.StreamingSeriesChunks), 7)
	}
	require.Equal(t, 1500, series)
	require.Equal(t, 1500, chunkItems)
}

func TestQueryStreamSendsEncodedResponsesThroughTheCodec(t *testing.T) {
	s := testStore(t, 20)
	ingester := serveTest(t, s)
	stream, err := ingester.QueryStream(tenantContext(), &client.QueryRequest{
		StartTimestampMs: 0, EndTimestampMs: 1 << 62,
		Matchers:                 []*client.LabelMatcher{{Type: client.EQUAL, Name: "job", Value: "job-1"}},
		StreamingChunksBatchSize: 2,
	})
	require.NoError(t, err)
	views, err := s.SelectChunks("tenant", 0, 1<<62, []store.LabelMatcher{{Type: 0, Name: "job", Value: "job-1"}})
	require.NoError(t, err)
	var received []client.QueryStreamResponse
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		received = append(received, *response)
	}
	// One labels response, then the chunks two series at a time.
	require.Len(t, received, 1+(len(views)+1)/2)
	require.True(t, received[0].IsEndOfSeriesStream)
	require.Len(t, received[0].StreamingSeries, len(views))
	for index, view := range views {
		var expected client.QueryStreamSeries
		require.NoError(t, expected.Unmarshal(view.EncodedLabels))
		require.Equal(t, expected.Labels, received[0].StreamingSeries[index].Labels)
		chunks := received[1+index/2].StreamingSeriesChunks[index%2]
		require.Equal(t, uint64(index), chunks.SeriesIndex)
		require.Len(t, chunks.Chunks, len(view.Chunks))
		for chunkIndex, chunk := range view.Chunks {
			marshalled, err := chunks.Chunks[chunkIndex].Marshal()
			require.NoError(t, err)
			require.Equal(t, chunk.Wire, marshalled)
		}
	}
}

func TestEmptyQueriesEndTheSeriesStream(t *testing.T) {
	ingester := serveTest(t, testStore(t, 1))
	stream, err := ingester.QueryStream(tenantContext(), &client.QueryRequest{
		StartTimestampMs: 0, EndTimestampMs: 1 << 62,
		Matchers: []*client.LabelMatcher{{Type: client.EQUAL, Name: "job", Value: "missing"}},
	})
	require.NoError(t, err)
	response, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, response.IsEndOfSeriesStream)
	require.Empty(t, response.StreamingSeries)
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func TestReadsNeedATenantAndValidLimits(t *testing.T) {
	ingester := serveTest(t, testStore(t, 3))
	_, err := ingester.LabelNames(context.Background(), &client.LabelNamesRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	_, err = ingester.LabelNames(tenantContext(), &client.LabelNamesRequest{Limit: -1})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	names, err := ingester.LabelNames(tenantContext(), &client.LabelNamesRequest{EndTimestampMs: 1 << 62, Limit: 2})
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "job"}, names.LabelNames)
	_, err = ingester.UserStats(tenantContext(), &client.UserStatsRequest{CountMethod: 7})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = ingester.Push(tenantContext(), &mimirpb.WriteRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	metricsResponse, err := ingester.MetricsForLabelMatchers(tenantContext(), &client.MetricsForLabelMatchersRequest{
		EndTimestampMs: 1 << 62,
		// Overlapping sets return each series once, sorted by labels.
		MatchersSet: []*client.LabelMatchers{
			{Matchers: []*client.LabelMatcher{{Type: client.EQUAL, Name: "__name__", Value: "metric"}}},
			{Matchers: []*client.LabelMatcher{{Type: client.EQUAL, Name: "job", Value: "job-1"}}},
		},
	})
	require.NoError(t, err)
	require.Len(t, metricsResponse.Metric, 3)
	require.Equal(t, "job-0", metricsResponse.Metric[0].Labels[1].Value)
}

func TestSearchScoresAndOrdersLikeTheRustIngester(t *testing.T) {
	values := []string{"api_requests", "http_api", "apiserver", "unrelated"}
	var batches []*client.SearchResultBatch
	collect := func(batch *client.SearchResultBatch) error {
		batches = append(batches, batch)
		return nil
	}
	require.NoError(t, search(values, &client.SearchFilter{Terms: []string{"api"}}, client.ORDER_BY_SCORE_DESC, 0, collect))
	require.Len(t, batches, 1)
	var ordered []string
	for _, result := range batches[0].Results {
		ordered = append(ordered, result.Value)
	}
	// Prefixes score 1, then the subsequence found later in the value.
	require.Equal(t, []string{"api_requests", "apiserver", "http_api"}, ordered)
	require.Equal(t, 1.0, batches[0].Results[0].Score)
	// A run of 3 starting 5 characters into 8: (3² - 5/8) / 3², scaled below a prefix match.
	require.InDelta(t, 0.999*(9-5.0/8)/9, batches[0].Results[2].Score, 1e-9)
	batches = nil
	require.NoError(t, search(values, &client.SearchFilter{Terms: []string{"APISRV"}, CaseInsensitive: true, FuzzAlg: 1, FuzzThreshold: 80}, client.ORDER_BY_VALUE_ASC, 0, collect))
	require.Len(t, batches, 1)
	require.Equal(t, "apiserver", batches[0].Results[0].Value)
	require.InDelta(t, jaroWinkler("apisrv", "apiserver"), batches[0].Results[0].Score, 1e-12)
	for _, invalid := range []struct {
		filter   *client.SearchFilter
		ordering client.SearchOrdering
		limit    int64
	}{
		{nil, 0, -1},
		{nil, 3, 0},
		{&client.SearchFilter{FuzzThreshold: 101}, 0, 0},
		{&client.SearchFilter{Terms: []string{""}}, 0, 0},
		{&client.SearchFilter{FuzzAlg: 2}, 0, 0},
	} {
		require.Equal(t, codes.InvalidArgument, status.Code(search(values, invalid.filter, invalid.ordering, invalid.limit, collect)))
	}
}

func TestContainsScoreCountsBytesLikeRust(t *testing.T) {
	score, ok := containsScore("ab", "xxab")
	require.True(t, ok)
	require.InDelta(t, 1-0.9*2.0/2.0, score, 1e-12)
	_, ok = containsScore("zz", "xxab")
	require.False(t, ok)
	require.Equal(t, 0.0, jaroWinkler("", "a"))
	require.Equal(t, 1.0, jaroWinkler("same", "same"))
}
