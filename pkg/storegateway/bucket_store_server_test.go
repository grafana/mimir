// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/bucket_store_inmemory_server.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// bucketStoreSeriesServer is a gRPC server and client implementation used to
// call Series() API endpoint going through the gRPC networking stack.
type storeTestServer struct {
	server         *grpc.Server
	serverListener net.Listener

	// requestSeries is the function to call the Series() API endpoint
	// via gRPC. The actual implementation depends whether we're calling
	// the StoreGateway or BucketStore API endpoint.
	requestSeries func(ctx context.Context, conn *grpc.ClientConn, req *storepb.SeriesRequest) (storegatewaypb.StoreGateway_SeriesClient, error)
}

func newStoreGatewayTestServer(t testing.TB, store storegatewaypb.StoreGatewayServer) *storeTestServer {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = listener.Close()
	})

	s := &storeTestServer{
		server:         grpc.NewServer(),
		serverListener: listener,
		requestSeries: func(ctx context.Context, conn *grpc.ClientConn, req *storepb.SeriesRequest) (storegatewaypb.StoreGateway_SeriesClient, error) {
			client := storegatewaypb.NewCustomStoreGatewayClient(conn)
			return client.Series(ctx, req)
		},
	}

	storegatewaypb.RegisterStoreGatewayServer(s.server, store)

	go func() {
		_ = s.server.Serve(listener)
	}()

	// Stop the gRPC server once the test has done.
	t.Cleanup(s.server.GracefulStop)

	return s
}

// Series calls the store server's Series() endpoint via gRPC and returns the responses collected
// via the gRPC stream.
func (s *storeTestServer) Series(ctx context.Context, req *storepb.SeriesRequest) (seriesSet []*storepb.Series, warnings annotations.Annotations, hints hintspb.SeriesResponseHints, estimatedChunks uint64, err error) {
	var (
		conn               *grpc.ClientConn
		stream             storegatewaypb.StoreGateway_SeriesClient
		res                *storepb.SeriesResponse
		streamingSeriesSet []*storepb.StreamingSeries
	)

	// Create a gRPC connection to the server.
	conn, err = s.dialConn()
	if err != nil {
		return
	}

	// Ensure we close the connection once done.
	defer func() {
		// Return the connection Close() error only if there no other previous error.
		if closeErr := conn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	stream, err = s.requestSeries(ctx, conn, req)
	if err != nil {
		return
	}

	for {
		res, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			// It's expected to get an EOF at the end of the stream.
			err = nil
			break
		}
		if err != nil {
			return
		}

		if res.GetWarning() != "" {
			warnings.Add(errors.New(res.GetWarning()))
		}

		if rawHints := res.GetHints(); rawHints != nil {
			// We expect only 1 hints entry so we just keep 1.
			if err = types.UnmarshalAny(rawHints, &hints); err != nil {
				err = errors.Wrap(err, "failed to unmarshal series hints")
				return
			}
		}

		if recvSeries := res.GetSeries(); recvSeries != nil {
			if !req.SkipChunks && req.StreamingChunksBatchSize > 0 {
				err = errors.New("got a normal series when streaming was enabled")
				return
			}
			var recvSeriesData []byte

			// We use a pool for the chunks and may use other pools in the future.
			// Given we need to retain the reference after the pooled slices are recycled,
			// we need to do a copy here. We prefer to stay on the safest side at this stage
			// so we do a marshal+unmarshal to copy the whole series.
			recvSeriesData, err = recvSeries.Marshal()
			if err != nil {
				err = errors.Wrap(err, "marshal received series")
				return
			}

			copiedSeries := &storepb.Series{}
			if err = copiedSeries.Unmarshal(recvSeriesData); err != nil {
				err = errors.Wrap(err, "unmarshal received series")
				return
			}

			seriesSet = append(seriesSet, copiedSeries)
		}

		if recvSeries := res.GetStreamingSeries(); recvSeries != nil {
			if req.StreamingChunksBatchSize == 0 || req.SkipChunks {
				err = errors.New("got a streaming series when streaming was disabled")
				return
			}

			var recvSeriesData []byte

			// We prefer to stay on the safest side at this stage
			// so we do a marshal+unmarshal to copy the whole series.
			recvSeriesData, err = recvSeries.Marshal()
			if err != nil {
				err = errors.Wrap(err, "marshal received series")
				return
			}

			copiedSeries := &storepb.StreamingSeriesBatch{}
			if err = copiedSeries.Unmarshal(recvSeriesData); err != nil {
				err = errors.Wrap(err, "unmarshal received series")
				return
			}

			streamingSeriesSet = append(streamingSeriesSet, copiedSeries.Series...)

			if recvSeries.IsEndOfSeriesStream {
				break
			}
		}
	}

	if req.StreamingChunksBatchSize > 0 && !req.SkipChunks {
		res, err = stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// This is expected if there are no matching series: no estimate is sent and the stream is closed.
				err = nil
			}

			return
		}

		estimate := res.GetStreamingChunksEstimate()
		if estimate == nil {
			err = fmt.Errorf("expected to get streaming chunks estimate message before all chunks messages, but got %T", res.Result)
			return
		}

		estimatedChunks = estimate.EstimatedChunkCount

		// Get the streaming chunks.
		idx := -1
		for idx < len(streamingSeriesSet)-1 {
			// We don't expect EOF errors here.
			res, err = stream.Recv()
			if err != nil {
				return
			}

			chksBatch := res.GetStreamingChunks()
			if chksBatch == nil {
				err = errors.Errorf("received unexpected response type %T, expected streaming chunks batch", res.Result)
				return
			}

			for _, chks := range chksBatch.Series {
				idx++
				if chksBatch == nil {
					err = errors.Errorf("expected streaming chunks, got something else")
					return
				}
				if chks.SeriesIndex != uint64(idx) {
					err = errors.Errorf("mismatch in series ref when getting streaming chunks, exp %d, got %d", idx, chks.SeriesIndex)
					return
				}

				// We prefer to stay on the safest side at this stage
				// so we do a marshal+unmarshal to copy the whole chunks.
				var data []byte
				data, err = chks.Marshal()
				if err != nil {
					err = errors.Wrap(err, "marshal received series")
					return
				}

				copiedChunks := &storepb.StreamingChunks{}
				if err = copiedChunks.Unmarshal(data); err != nil {
					err = errors.Wrap(err, "unmarshal received series")
					return
				}

				seriesSet = append(seriesSet, &storepb.Series{
					Labels: streamingSeriesSet[idx].Labels,
					Chunks: copiedChunks.Chunks,
				})
			}
		}

		res, err = stream.Recv()
		for err == nil {
			if res.GetHints() == nil && res.GetStats() == nil {
				err = errors.Errorf("got unexpected response type %T", res.Result)
				break
			}
			res, err = stream.Recv()
		}
		if errors.Is(err, io.EOF) {
			err = nil
		}
	}

	return
}

func (s *storeTestServer) dialConn() (*grpc.ClientConn, error) {
	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	return grpc.Dial(s.serverListener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*1024)),
	)
}

// Close releases all resources.
func (s *storeTestServer) Close() {
	s.server.GracefulStop()
	_ = s.serverListener.Close()
}
