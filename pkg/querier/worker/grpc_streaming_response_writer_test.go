// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
)

// ── grpcStreamingResponseWriter unit tests ───────────────────────────────────

func TestGrpcStreamingResponseWriter_FirstFlushOpensStreamAndSendsMetadata(t *testing.T) {
	pool := &mockFrontendClientPool{}
	w := newTestStreamingWriter(t, pool)

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"results":[{"name":"foo"}]}`)
	w.Flush()

	require.True(t, w.StreamOpened(), "stream should be marked open after first Flush")
	require.Equal(t, 1, pool.retrievedClientCount, "should get a gRPC client on first Flush")
	require.Len(t, pool.sentMessages, 2, "should send metadata + one body chunk")

	meta, ok := pool.sentMessages[0].Data.(*frontendv2pb.QueryResultStreamRequest_Metadata)
	require.True(t, ok, "first message must be metadata")
	assert.Equal(t, int32(http.StatusOK), meta.Metadata.Code)

	body, ok := pool.sentMessages[1].Data.(*frontendv2pb.QueryResultStreamRequest_Body)
	require.True(t, ok, "second message must be body chunk")
	assert.Contains(t, string(body.Body.Chunk), "foo")
}

func TestGrpcStreamingResponseWriter_EachFlushSendsOneBodyChunk(t *testing.T) {
	pool := &mockFrontendClientPool{}
	w := newTestStreamingWriter(t, pool)

	w.WriteHeader(http.StatusOK)
	for i := 0; i < 3; i++ {
		fmt.Fprintf(w, `{"results":[{"name":"batch%d"}]}`, i)
		w.Flush()
	}
	w.CloseStream()

	// 1 metadata + 3 body chunks (one per Flush).
	require.Len(t, pool.sentMessages, 4)
	for i := 1; i <= 3; i++ {
		_, ok := pool.sentMessages[i].Data.(*frontendv2pb.QueryResultStreamRequest_Body)
		require.Truef(t, ok, "message %d should be body chunk", i)
	}
	require.True(t, pool.streamClosed)
}

func TestGrpcStreamingResponseWriter_FlushWithEmptyBufferSendsNoBody(t *testing.T) {
	pool := &mockFrontendClientPool{}
	w := newTestStreamingWriter(t, pool)

	w.WriteHeader(http.StatusOK)
	// First Flush with nothing written: only metadata should be sent.
	w.Flush()

	require.Len(t, pool.sentMessages, 1, "only metadata when buffer is empty")
	_, ok := pool.sentMessages[0].Data.(*frontendv2pb.QueryResultStreamRequest_Metadata)
	require.True(t, ok)
}

func TestGrpcStreamingResponseWriter_CloseStreamSendsRemainingBuffer(t *testing.T) {
	pool := &mockFrontendClientPool{}
	w := newTestStreamingWriter(t, pool)

	w.WriteHeader(http.StatusOK)
	w.Flush() // opens stream + metadata

	// Write data but DON'T flush — CloseStream should flush it.
	fmt.Fprint(w, `{"status":"success","has_more":false}`)
	w.CloseStream()

	require.Len(t, pool.sentMessages, 2) // metadata + final chunk
	body, ok := pool.sentMessages[1].Data.(*frontendv2pb.QueryResultStreamRequest_Body)
	require.True(t, ok)
	assert.Contains(t, string(body.Body.Chunk), "success")
	assert.True(t, pool.streamClosed)
}

func TestGrpcStreamingResponseWriter_ChunkExceedingMaxMessageSizeIsAnError(t *testing.T) {
	const maxMsgSize = 100 // very small limit to exercise the check in tests

	pool := &mockFrontendClientPool{}
	w := newGrpcStreamingResponseWriter(
		context.Background(), context.Background(),
		42, "frontend:9095", pool, log.NewNopLogger(), nil, maxMsgSize,
	)

	w.WriteHeader(http.StatusOK)
	// Write 250 bytes — well above maxMsgSize — then flush.
	payload := make([]byte, 250)
	for i := range payload {
		payload[i] = 'x'
	}
	_, _ = w.Write(payload)
	w.Flush()

	// The stream was opened (metadata sent) but the body chunk was rejected.
	require.True(t, w.StreamOpened(), "stream should have been opened before the size check")
	require.True(t, w.failed, "writer should be marked failed after oversized chunk")

	// Only the metadata message should have been sent; no body chunk.
	require.Len(t, pool.sentMessages, 1)
	_, ok := pool.sentMessages[0].Data.(*frontendv2pb.QueryResultStreamRequest_Metadata)
	require.True(t, ok, "only the metadata message should have been sent")
}

func TestGrpcStreamingResponseWriter_StreamNotOpenedWhenHandlerNeverFlushes(t *testing.T) {
	pool := &mockFrontendClientPool{}
	w := newTestStreamingWriter(t, pool)

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusUnauthorized)
	fmt.Fprint(w, "unauthorized")

	require.False(t, w.StreamOpened())
	require.Equal(t, 0, pool.retrievedClientCount)

	resp := w.AsHTTPResponse()
	assert.Equal(t, int32(http.StatusUnauthorized), resp.Code)
	assert.Equal(t, []byte("unauthorized"), resp.Body)
}

func TestGrpcStreamingResponseWriter_AsHTTPResponseStripsStreamingHeader(t *testing.T) {
	pool := &mockFrontendClientPool{}
	w := newTestStreamingWriter(t, pool)

	w.Header().Set(ResponseStreamingEnabledHeader, "true")
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "body")

	resp := w.AsHTTPResponse()
	for _, h := range resp.Headers {
		assert.NotEqual(t, ResponseStreamingEnabledHeader, h.Key,
			"internal streaming header must be stripped from fallback response")
	}
}

func TestGrpcStreamingResponseWriter_RetriesToOpenStreamOnTransientFailure(t *testing.T) {
	pool := &mockFrontendClientPool{nextGetClientForCallsShouldFail: 2}
	w := newTestStreamingWriter(t, pool)

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "data")
	w.Flush()

	require.True(t, w.StreamOpened())
	require.Equal(t, 3, pool.retrievedClientCount, "should retry twice then succeed")
}

func TestGrpcStreamingResponseWriter_MarksFailedAfterExhaustingRetries(t *testing.T) {
	pool := &mockFrontendClientPool{nextGetClientForCallsShouldFail: maxNotifyFrontendRetries + 10}
	w := newTestStreamingWriter(t, pool)

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "data")
	w.Flush() // exhausts retries → failed

	require.False(t, w.StreamOpened())
	require.True(t, w.failed)

	// Subsequent Flush calls must be no-ops.
	fmt.Fprint(w, "more data")
	w.Flush()
	require.Empty(t, pool.sentMessages)
}

func TestGrpcStreamingResponseWriter_BodyChunkNotSentAfterRequestCancelled(t *testing.T) {
	pool := &mockFrontendClientPool{}
	reqCtx, cancel := context.WithCancel(context.Background())

	w := newGrpcStreamingResponseWriter(
		context.Background(), reqCtx, 42, "frontend:9095", pool, log.NewNopLogger(), nil, 0,
	)

	w.WriteHeader(http.StatusOK)
	w.Flush() // opens stream + metadata

	cancel()

	fmt.Fprint(w, "data after cancel")
	w.Flush() // should detect cancellation and mark failed

	require.True(t, w.failed)
}

func TestGrpcStreamingResponseWriter_QueryIDSetOnAllMessages(t *testing.T) {
	const queryID = uint64(9876)

	pool := &mockFrontendClientPool{}
	w := newGrpcStreamingResponseWriter(
		context.Background(), context.Background(),
		queryID, "frontend:9095", pool, log.NewNopLogger(), &querier_stats.SafeStats{}, 0,
	)

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "batch1")
	w.Flush()
	fmt.Fprint(w, "batch2")
	w.CloseStream()

	for i, msg := range pool.sentMessages {
		assert.Equalf(t, queryID, msg.QueryID, "message %d must carry queryID", i)
	}
}

// ── isStreamingSearchQueryPath ────────────────────────────────────────────────────────

func TestIsSearchQueryPath(t *testing.T) {
	trueList := []string{
		"/api/v1/search/metric_names",
		"/prometheus/api/v1/search/metric_names",
		"/prefix/api/v1/search/metric_names",
		"/api/v1/search/label_names",
		"/prometheus/api/v1/search/label_names",
		"/api/v1/search/label_values",
		"/prometheus/api/v1/search/label_values",
	}
	for _, p := range trueList {
		assert.Truef(t, isStreamingSearchQueryPath(p), "expected %q to be a search query path", p)
	}

	falseList := []string{
		"/api/v1/labels",
		"/api/v1/label/foo/values",
		"/api/v1/query",
		"/api/v1/search",
		"/api/v1/search/",
		"/api/v1/search/metric_names_extra",
	}
	for _, p := range falseList {
		assert.Falsef(t, isStreamingSearchQueryPath(p), "expected %q NOT to be a search query path", p)
	}
}

// ── HTTPRequestHandler ───────────────────────────────────────────────────────

func TestHTTPRequestHandler_HandleDelegatesToWrappedServer(t *testing.T) {
	wantResp := &httpgrpc.HTTPResponse{Code: http.StatusOK, Body: []byte("hello")}
	mockServer := &httpRequestHandlerMock{}
	req := &httpgrpc.HTTPRequest{Url: "/api/v1/labels"}
	mockServer.On("Handle", context.Background(), req).Return(wantResp, nil)

	h := NewHTTPRequestHandler(mockServer, nil)
	got, err := h.Handle(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, wantResp, got)
	mockServer.AssertExpectations(t)
}

func TestHTTPRequestHandler_ServeHTTPWithWriterCallsUnderlyingHandler(t *testing.T) {
	var capturedTenantID string
	underlying := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTenantID = r.Header.Get("X-Org-Id")
		w.Header().Set("X-Test", "streaming")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "search result")
	})

	h := NewHTTPRequestHandler(&httpRequestHandlerMock{}, underlying)

	pool := &mockFrontendClientPool{}
	w := newTestStreamingWriter(t, pool)

	req := &httpgrpc.HTTPRequest{
		Url:     "/api/v1/search/metric_names",
		Headers: []*httpgrpc.Header{{Key: "X-Org-Id", Values: []string{"tenant1"}}},
	}
	require.NoError(t, h.ServeHTTPWithWriter(context.Background(), req, w))

	assert.Equal(t, "tenant1", capturedTenantID)
	assert.Equal(t, "streaming", w.Header().Get("X-Test"))
}

// ── runStreamingHttpRequest integration tests ────────────────────────────────

// TestRunStreamingHttpRequest_BatchesArriveBeforeHandlerReturns is the key correctness
// test: it verifies that when the search handler calls Flush() after a batch, the batch
// is delivered to the query-frontend BEFORE the handler finishes running.
//
// Without the streaming change, the entire response would be buffered by httptest.Recorder
// and sent only after the handler returned. With the streaming change, the batch is
// delivered on each Flush() call.
func TestRunStreamingHttpRequest_BatchesArriveBeforeHandlerReturns(t *testing.T) {
	const queryID = uint64(55)

	firstBatchReceived := make(chan struct{})
	handlerMayProceed := make(chan struct{})

	sp, _, _, _, frontend := prepareSchedulerProcessor(t, true)
	sp.maxMessageSize = math.MaxInt
	frontend.responseStreamStarted = firstBatchReceived

	batchHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set(ResponseStreamingEnabledHeader, "true")
		w.WriteHeader(http.StatusOK)

		fmt.Fprint(w, `{"results":[{"name":"metric_a"}]}`+"\n")
		w.(http.Flusher).Flush()

		// Block until the test confirms the first batch arrived at the frontend.
		// This proves the batch was streamed, not buffered until handler return.
		<-handlerMayProceed

		fmt.Fprint(w, `{"results":[{"name":"metric_b"}]}`+"\n")
		w.(http.Flusher).Flush()
		fmt.Fprint(w, `{"status":"success","has_more":false}`+"\n")
		w.(http.Flusher).Flush()
	})

	httpHandler := NewHTTPRequestHandler(&httpRequestHandlerMock{}, batchHandler)
	sp.httpHandler = httpHandler

	// Inject a tenant ID: the gRPC client interceptors require one in the context.
	ctx := user.InjectOrgID(context.Background(), "test-org")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sp.runStreamingHttpRequest(
			ctx, log.NewNopLogger(),
			queryID, frontend.addr, &querier_stats.SafeStats{},
			&httpgrpc.HTTPRequest{Url: "/api/v1/search/metric_names"},
			httpHandler,
		)
	}()

	// Wait for the first batch to land at the frontend.
	select {
	case <-firstBatchReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first batch to arrive at frontend")
	}

	// The handler is still blocked (handlerMayProceed not closed yet).
	// Verify at least one body chunk has been received.
	assert.GreaterOrEqual(t, int(frontend.queryResultStreamBodyCalls.Load()), 1,
		"first batch must arrive at the frontend before the handler is unblocked")

	// Unblock the handler and wait for it to finish.
	close(handlerMayProceed)
	wg.Wait()

	// After completion, all content should be present.
	body := string(frontend.responses[queryID].body)
	assert.Contains(t, body, "metric_a")
	assert.Contains(t, body, "metric_b")
	assert.Contains(t, body, "success")
}

// TestRunStreamingHttpRequest_FallsBackToBufferedWhenHandlerNeverFlushes checks
// that an early error response (e.g. 401 from auth middleware) is delivered via
// the normal non-streaming QueryResult RPC.
func TestRunStreamingHttpRequest_FallsBackToBufferedWhenHandlerNeverFlushes(t *testing.T) {
	const queryID = uint64(99)

	sp, _, _, _, frontend := prepareSchedulerProcessor(t, true)
	sp.maxMessageSize = math.MaxInt

	errorHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		// No Flush() call.
	})

	httpHandler := NewHTTPRequestHandler(&httpRequestHandlerMock{}, errorHandler)
	sp.httpHandler = httpHandler

	// Inject a tenant ID: the gRPC client interceptors require one in the context.
	ctx := user.InjectOrgID(context.Background(), "test-org")

	sp.runStreamingHttpRequest(
		ctx, log.NewNopLogger(),
		queryID, frontend.addr, &querier_stats.SafeStats{},
		&httpgrpc.HTTPRequest{Url: "/api/v1/search/label_names"},
		httpHandler,
	)

	result, ok := frontend.responses[queryID]
	require.True(t, ok, "frontend should have received the response")
	assert.Equal(t, int32(http.StatusUnauthorized), result.metadata.Code)
	assert.Contains(t, string(frontend.responses[queryID].body), "unauthorized")

	// Streaming path must NOT have been used.
	assert.Equal(t, int64(0), frontend.queryResultStreamMetadataCalls.Load(),
		"streaming path must not be used when handler never flushes")
	assert.Equal(t, int64(1), frontend.queryResultCalls.Load(),
		"buffered QueryResult RPC should have been used instead")
}

// TestRunStreamingHttpRequest_OversizedChunkAbortsStream verifies that when a handler
// flushes a chunk larger than maxMessageSize, the streaming path rejects it and closes
// the gRPC stream rather than falling back to the buffered QueryResult RPC.
//
// This mirrors the behaviour of runHttpRequest, which returns a 413 for oversized
// buffered responses, applied consistently to the streaming path.
func TestRunStreamingHttpRequest_OversizedChunkAbortsStream(t *testing.T) {
	const (
		queryID    = uint64(66)
		maxMsgSize = 100
	)

	sp, _, _, _, frontend := prepareSchedulerProcessor(t, true)
	sp.maxMessageSize = maxMsgSize

	oversizedPayload := make([]byte, maxMsgSize+1)
	for i := range oversizedPayload {
		oversizedPayload[i] = 'x'
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)

		// First flush: small, well within the limit — this opens the stream.
		fmt.Fprint(w, `{"results":[{"name":"ok"}]}`)
		w.(http.Flusher).Flush()

		// Second flush: exceeds maxMessageSize — should be rejected.
		_, _ = w.Write(oversizedPayload)
		w.(http.Flusher).Flush()
	})

	httpHandler := NewHTTPRequestHandler(&httpRequestHandlerMock{}, handler)
	sp.httpHandler = httpHandler

	ctx := user.InjectOrgID(context.Background(), "test-org")
	sp.runStreamingHttpRequest(
		ctx, log.NewNopLogger(),
		queryID, frontend.addr, &querier_stats.SafeStats{},
		&httpgrpc.HTTPRequest{Url: "/api/v1/search/metric_names"},
		httpHandler,
	)

	// The streaming path must have been used (metadata was sent).
	assert.Equal(t, int64(1), frontend.queryResultStreamMetadataCalls.Load(),
		"streaming path must have been used")

	// Only the first (small) body chunk should have been delivered.
	assert.Equal(t, int64(1), frontend.queryResultStreamBodyCalls.Load(),
		"only the first valid chunk should have been sent")

	// The buffered fallback must NOT have been invoked.
	assert.Equal(t, int64(0), frontend.queryResultCalls.Load(),
		"buffered QueryResult RPC must not be used when the stream was already opened")
}

// TestRunHttpRequest_NonSearchPathUsesBufferedPath verifies that non-search paths
// continue to use the existing buffered httpgrpc handler.
func TestRunHttpRequest_NonSearchPathUsesBufferedPath(t *testing.T) {
	const queryID = uint64(77)

	sp, _, _, _, frontend := prepareSchedulerProcessor(t, true)
	sp.maxMessageSize = math.MaxInt

	// Replace httpHandler with one that returns a canned buffered response
	// via Handle() and would panic if ServeHTTPWithWriter is called.
	sp.httpHandler = &panicOnStreamingHandler{resp: &httpgrpc.HTTPResponse{
		Code: http.StatusOK,
		Body: []byte(`{"status":"success"}`),
	}}

	// Inject a tenant ID: the gRPC client interceptors require one in the context.
	ctx := user.InjectOrgID(context.Background(), "test-org")

	sp.runHttpRequest(
		ctx, log.NewNopLogger(),
		queryID, frontend.addr, &querier_stats.SafeStats{},
		&httpgrpc.HTTPRequest{Url: "/api/v1/query_range"},
	)

	result, ok := frontend.responses[queryID]
	require.True(t, ok)
	assert.Contains(t, string(result.body), "success")

	// Must not have used the streaming path.
	assert.Equal(t, int64(0), frontend.queryResultStreamMetadataCalls.Load())
}

// TestRunHttpRequest_SearchPathUsesStreamingWhenAvailable verifies that a search
// path with an HTTPRequestHandler uses the streaming path.
func TestRunHttpRequest_SearchPathUsesStreamingWhenAvailable(t *testing.T) {
	const queryID = uint64(88)

	sp, _, _, _, frontend := prepareSchedulerProcessor(t, true)
	sp.maxMessageSize = math.MaxInt

	streamingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(ResponseStreamingEnabledHeader, "true")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"results":[{"name":"cpu_usage"}]}`+"\n")
		w.(http.Flusher).Flush()
		fmt.Fprint(w, `{"status":"success","has_more":false}`+"\n")
		w.(http.Flusher).Flush()
	})

	httpHandler := NewHTTPRequestHandler(&httpRequestHandlerMock{}, streamingHandler)
	sp.httpHandler = httpHandler

	// Inject a tenant ID: the gRPC client interceptors require one in the context.
	ctx := user.InjectOrgID(context.Background(), "test-org")

	sp.runHttpRequest(
		ctx, log.NewNopLogger(),
		queryID, frontend.addr, &querier_stats.SafeStats{},
		&httpgrpc.HTTPRequest{Url: "/api/v1/search/metric_names"},
	)

	result, ok := frontend.responses[queryID]
	require.True(t, ok)
	assert.Contains(t, string(result.body), "cpu_usage")

	// Must have used the streaming path.
	assert.GreaterOrEqual(t, int(frontend.queryResultStreamMetadataCalls.Load()), 1,
		"streaming path must be used for search paths with HTTPRequestHandler")
}

// ── helpers ──────────────────────────────────────────────────────────────────

func newTestStreamingWriter(t *testing.T, pool *mockFrontendClientPool) *grpcStreamingResponseWriter {
	t.Helper()
	return newGrpcStreamingResponseWriter(
		context.Background(), context.Background(),
		42, "frontend:9095", pool, log.NewNopLogger(), nil, 0,
	)
}

// panicOnStreamingHandler implements RequestHandler (buffered) but panics if
// ServeHTTPWithWriter (streaming) is called. Used to assert non-search paths
// never reach the streaming path.
type panicOnStreamingHandler struct {
	resp *httpgrpc.HTTPResponse
}

func (h *panicOnStreamingHandler) Handle(_ context.Context, _ *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	return h.resp, nil
}
