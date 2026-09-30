// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func TestStoreGatewayComparator_Series(t *testing.T) {
	up := []mimirpb.LabelAdapter{{Name: "__name__", Value: "up"}}
	down := []mimirpb.LabelAdapter{{Name: "__name__", Value: "down"}}
	chunk := func(data string) storepb.AggrChunk {
		return storepb.AggrChunk{MinTime: 1, MaxTime: 2, Raw: storepb.Chunk{Type: storepb.Chunk_XOR, Data: []byte(data)}}
	}
	seriesBatch := func(end bool, series ...[]mimirpb.LabelAdapter) *storepb.SeriesResponse {
		batch := &storepb.StreamingSeriesBatch{IsEndOfSeriesStream: end}
		for _, s := range series {
			batch.Series = append(batch.Series, &storepb.StreamingSeries{Labels: s})
		}
		return storepb.NewStreamingSeriesResponse(batch)
	}
	chunksBatch := func(seriesIndex uint64, chunks ...storepb.AggrChunk) *storepb.SeriesResponse {
		return storepb.NewStreamingChunksResponse(&storepb.StreamingChunksBatch{
			Series: []*storepb.StreamingChunks{{SeriesIndex: seriesIndex, Chunks: chunks}},
		})
	}

	// The expected responses have both series in one batch.
	primary := []*storepb.SeriesResponse{
		seriesBatch(true, up, down),
		storepb.NewStatsResponse(10),
		chunksBatch(0, chunk("a")),
		chunksBatch(1, chunk("b")),
	}

	tests := map[string]struct {
		secondary      []*storepb.SeriesResponse
		expectedResult ComparisonResult
		expectedErr    string
	}{
		"same data in different batches, different stats": {
			secondary: []*storepb.SeriesResponse{
				seriesBatch(false, up),
				seriesBatch(true, down),
				storepb.NewStatsResponse(20),
				chunksBatch(0, chunk("a")),
				chunksBatch(1, chunk("b")),
			},
			expectedResult: ComparisonMatch,
		},
		"different labels": {
			secondary: []*storepb.SeriesResponse{
				seriesBatch(true, up, up),
				chunksBatch(0, chunk("a")),
				chunksBatch(1, chunk("b")),
			},
			expectedResult: ComparisonMismatch,
			expectedErr:    `series 1: the primary backend returned {__name__="down"} but the secondary backend returned {__name__="up"}`,
		},
		"missing series": {
			secondary: []*storepb.SeriesResponse{
				seriesBatch(true, up),
				chunksBatch(0, chunk("a")),
			},
			expectedResult: ComparisonMismatch,
			expectedErr:    "the primary backend returned 2 series but the secondary backend returned 1",
		},
		"different chunk bytes": {
			secondary: []*storepb.SeriesResponse{
				seriesBatch(true, up, down),
				chunksBatch(0, chunk("a")),
				chunksBatch(1, chunk("c")),
			},
			expectedResult: ComparisonMismatch,
			expectedErr:    "series 1: the primary backend returned 1 chunks but the secondary backend returned 1 different chunks",
		},
		"different warnings": {
			secondary: []*storepb.SeriesResponse{
				seriesBatch(true, up, down),
				{Result: &storepb.SeriesResponse_Warning{Warning: "partial response"}},
				chunksBatch(0, chunk("a")),
				chunksBatch(1, chunk("b")),
			},
			expectedResult: ComparisonMismatch,
			expectedErr:    `the primary backend returned warnings [] but the secondary backend returned ["partial response"]`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := storeGatewayComparator{}.Compare(seriesMethod, decodedMessages(t, seriesMethod, primary), decodedMessages(t, seriesMethod, tc.secondary))
			require.Equal(t, tc.expectedResult, result)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
		})
	}
}

func TestStoreGatewayComparator_LabelNames(t *testing.T) {
	primary := decodedMessages(t, labelNamesMethod, []*storepb.LabelNamesResponse{{Names: []string{"__name__", "job"}}})

	result, err := storeGatewayComparator{}.Compare(labelNamesMethod, primary, decodedMessages(t, labelNamesMethod, []*storepb.LabelNamesResponse{{Names: []string{"__name__", "job"}}}))
	require.NoError(t, err)
	require.Equal(t, ComparisonMatch, result)

	result, err = storeGatewayComparator{}.Compare(labelNamesMethod, primary, decodedMessages(t, labelNamesMethod, []*storepb.LabelNamesResponse{{Names: []string{"__name__", "instance"}}}))
	require.Equal(t, ComparisonMismatch, result)
	require.EqualError(t, err, `value 1: the primary backend returned "job" but the secondary backend returned "instance"`)
}

func TestCompareCall_StatusRules(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "not available")

	tests := map[string]struct {
		primary, secondary backendCall
		expectedResult     ComparisonResult
		expectedErr        string
	}{
		"primary aborted": {
			primary:        backendCall{Aborted: true},
			expectedResult: ComparisonSkipped,
			expectedErr:    "the primary call is incomplete because the client side failed",
		},
		"secondary aborted": {
			secondary:      backendCall{Aborted: true},
			expectedResult: ComparisonSkipped,
			expectedErr:    "the secondary call was aborted because the secondary backend was too slow to receive the requests",
		},
		"different status codes": {
			primary:        backendCall{Err: unavailable},
			expectedResult: ComparisonMismatch,
			expectedErr:    "the primary backend returned status Unavailable but the secondary backend returned status OK",
		},
		"same failure": {
			primary:        backendCall{Err: unavailable},
			secondary:      backendCall{Err: unavailable},
			expectedResult: ComparisonMatch,
		},
		"decode error": {
			secondary:      backendCall{DecodeErr: status.Error(codes.Internal, "bad message")},
			expectedResult: ComparisonSkipped,
			expectedErr:    "failed to decode the messages of the call: rpc error: code = Internal desc = bad message",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			call := teeCall{FullMethod: seriesMethod, Primary: tc.primary, Secondary: &tc.secondary}
			result, err := compareCall(storeGatewayComparator{}, call)
			require.Equal(t, tc.expectedResult, result)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
		})
	}
}

func TestCallFinisher_Metrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	finish := newCallFinisher(storeGatewayComparator{}, newTeeMetrics(reg), log.NewNopLogger())

	responses := decodedMessages(t, labelNamesMethod, []*storepb.LabelNamesResponse{{Names: []string{"job"}}})
	finish(teeCall{
		FullMethod: labelNamesMethod,
		Primary:    backendCall{Backend: "primary", Responses: responses},
		Secondary:  &backendCall{Backend: "secondary", Responses: responses},
	})
	finish(teeCall{
		FullMethod: labelNamesMethod,
		Primary:    backendCall{Backend: "primary", Responses: responses},
		Secondary:  &backendCall{Backend: "secondary", Err: status.Error(codes.Unavailable, "not available")},
	})
	// A call without a secondary is not compared.
	finish(teeCall{FullMethod: labelNamesMethod, Primary: backendCall{Backend: "primary", Responses: responses}})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_grpctee_responses_compared_total Total number of calls for which grpc-tee compared the primary and secondary responses, by result.
		# TYPE cortex_grpctee_responses_compared_total counter
		cortex_grpctee_responses_compared_total{method="/gatewaypb.StoreGateway/LabelNames",result="match"} 1
		cortex_grpctee_responses_compared_total{method="/gatewaypb.StoreGateway/LabelNames",result="mismatch"} 1
	`), "cortex_grpctee_responses_compared_total"))

	require.Equal(t, 3, testutil.CollectAndCount(reg, "cortex_grpctee_backend_request_duration_seconds"))
}

// marshaler is a protobuf message that can be marshaled.
type marshaler interface {
	Marshal() ([]byte, error)
}

// decodedMessages returns the messages as proxied messages, the same way as the tee handler records them.
func decodedMessages[T marshaler](t *testing.T, method string, msgs []T) []proxiedMessage {
	codec := newStoreGatewayCodec()
	var out []proxiedMessage
	for _, msg := range msgs {
		data, err := msg.Marshal()
		require.NoError(t, err)
		decoded, err := codec.DecodeResponse(method, data)
		require.NoError(t, err)
		out = append(out, proxiedMessage{Payload: data, Decoded: decoded})
	}
	return out
}
