// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/validation"
)

type remoteReadRoundTripperV2 struct {
	next http.RoundTripper

	middleware MetricsQueryMiddleware
	limits     LimitedParallelismLimits
}

func NewRemoteReadRoundTripperV2(next http.RoundTripper, limits LimitedParallelismLimits, middlewares ...MetricsQueryMiddleware) http.RoundTripper {
	return &remoteReadRoundTripperV2{
		next:       next,
		middleware: MergeMetricsQueryMiddlewares(middlewares...),
		limits:     limits,
	}
}

func errorFromUpstreamResponse(resp *http.Response) error {
	body, err := readResponseBody(resp)
	if err != nil {
		return err
	}

	return httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code:    int32(resp.StatusCode),
		Body:    body,
		Headers: httpgrpc.FromHeader(resp.Header),
	})
}

type jobResult struct {
	resp Response
	err  error
}

func collectOrCleanupResponses(
	ctx context.Context,
	queryCount int,
	maxParallelism int,
	runJob func(ctx context.Context, queryIdx int) (Response, error),
) ([]Response, error) {

	succeeded := false
	responses := make([]Response, queryCount)
	ctx, cancel := context.WithCancelCause(ctx)

	defer func() {
		if succeeded {
			return
		}

		cancel(nil)
		for _, resp := range responses {
			if resp != nil {
				resp.Close()
			}
		}
	}()

	wrappedF := func(jobCtx context.Context, queryIdx int) error {
		resultChan := make(chan jobResult)
		go func() {
			// We purposefully don't use `jobCtx` context, because it's cancelled
			// when all queries returned, but we may not have read the body when
			// processing a "streamed" response.
			queryCtx := ctx
			queryResp, err := runJob(queryCtx, queryIdx)

			resultChan <- jobResult{
				resp: queryResp,
				err:  err,
			}
		}()

		select {
		case <-jobCtx.Done():
			go func() {
				result := <-resultChan
				if result.resp != nil {
					result.resp.Close()
				}
			}()
			return jobCtx.Err()
		case result := <-resultChan:
			responses[queryIdx] = result.resp
			return result.err
		}
	}

	if err := concurrency.ForEachJob(ctx, queryCount, maxParallelism, wrappedF); err != nil {
		cancel(err)
		return nil, err
	}

	succeeded = true
	return responses, nil
}

func (r *remoteReadRoundTripperV2) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body == nil {
		return r.next.RoundTrip(req)
	}

	rtCtx := req.Context()
	defer req.Body.Close()

	tenantIDs, err := tenant.TenantIDs(req.Context())
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Limit the number of parallel sub-requests according to the MaxQueryParallelism tenant setting.
	maxParallelism := validation.SmallestPositiveIntPerTenant(tenantIDs, r.limits.MaxQueryParallelism)
	if maxParallelism <= 0 {
		maxParallelism = 1
	}

	remoteReadReq, err := unmarshalRemoteReadRequest(req.Context(), req.Body, int(req.ContentLength))
	if err != nil {
		return nil, err
	}

	// Because all the requests run concurrently and hitting potentially different servers,
	// we ensure that they will all return the same response type.
	respType, err := remote.NegotiateResponseType(remoteReadReq.AcceptedResponseTypes)
	if err != nil {
		return nil, err
	}

	acceptedResponseTypes := []prompb.ReadRequest_ResponseType{respType}
	queries := remoteReadReq.GetQueries()

	handler := r.middleware.Wrap(HandlerFunc(func(ctx context.Context, metricsReq MetricsQueryRequest) (Response, error) {
		updatedQueryReq, ok := metricsReq.(*remoteReadQueryRequest)
		if !ok {
			// This should never happen.
			return nil, errors.New("unexpected logic bug: remote read roundtripper received an unexpected data type")
		}

		newReadReq := &prompb.ReadRequest{
			Queries:               []*prompb.Query{updatedQueryReq.query},
			AcceptedResponseTypes: acceptedResponseTypes,
		}

		encodedData, err := marshalRemoteReadRequest(newReadReq)
		if err != nil {
			return nil, err
		}

		ctx, cancel := context.WithCancelCause(ctx)
		streamingResponse := false
		defer func() {
			if !streamingResponse {
				cancel(nil)
			}
		}()

		newReq := req.Clone(ctx)
		newReq.Body = io.NopCloser(bytes.NewBuffer(encodedData))
		newReq.ContentLength = int64(len(encodedData))
		newReq.Header.Set("Content-Encoding", "snappy")

		resp, err := r.next.RoundTrip(newReq)
		if err != nil {
			return nil, err
		}

		defer func() {
			if !streamingResponse {
				resp.Body.Close()
			}
		}()

		if resp.StatusCode/100 != 2 {
			return nil, errorFromUpstreamResponse(resp)
		}

		contentType := resp.Header.Get("Content-Type")
		switch {
		case strings.HasPrefix(contentType, "application/x-protobuf"):
			return r.handleSampledResponse(resp)
		case strings.HasPrefix(contentType, "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"):
			streamingResponse = true
			return r.handleStreamedResponse(resp, cancel)
		default:
			return nil, apierror.Newf(apierror.TypeInternal, "unsupported content-type %s", contentType)
		}
	}))

	responses, err := collectOrCleanupResponses(rtCtx, len(queries), maxParallelism, func(queryCtx context.Context, queryIdx int) (Response, error) {
		query := queries[queryIdx]

		rrReq, err := remoteReadToMetricsQueryRequest(req.URL.Path, query)
		if err != nil {
			return nil, apierror.AddDetails(err, fmt.Sprintf("remote read error (%s_%d)", matchersLogKey, queryIdx))
		}

		queryResp, err := handler.Do(queryCtx, rrReq)
		if err != nil {
			return nil, apierror.AddDetails(err, fmt.Sprintf("remote read error (%s_%d: %s)", matchersLogKey, queryIdx, rrReq.GetQuery()))
		}

		return queryResp, nil
	})

	if err != nil {
		return nil, err
	}

	closeResp := true
	defer func() {
		if !closeResp {
			return
		}

		for _, resp := range responses {
			if resp != nil {
				resp.Close()
			}
		}
	}()

	switch acceptedResponseTypes[0] {
	case prompb.ReadRequest_SAMPLES:
		buffer, err := mergeSampleResponses(responses)
		if err != nil {
			return nil, err
		}

		httpResp := &http.Response{
			StatusCode:    200,
			Status:        http.StatusText(http.StatusOK),
			Body:          io.NopCloser(buffer),
			ContentLength: int64(buffer.Len()),
			Header: http.Header{
				"Content-Type":     []string{"application/x-protobuf"},
				"Content-Encoding": []string{"snappy"},
			},
		}

		return httpResp, nil
	case prompb.ReadRequest_STREAMED_XOR_CHUNKS:
		closeResp = false
		reader, err := mergeStreamedResponses(responses)
		if err != nil {
			return nil, err
		}

		resp := &http.Response{
			StatusCode:    200,
			Status:        http.StatusText(http.StatusOK),
			Body:          reader,
			ContentLength: -1,
			Header: http.Header{
				"Content-Type": []string{"application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"},
			},
		}
		return resp, nil
	default:
		return nil, apierror.Newf(apierror.TypeInternal, "unknown response type %v", acceptedResponseTypes[0])
	}
}

func (r *remoteReadRoundTripperV2) handleSampledResponse(resp *http.Response) (Response, error) {
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	data, err = snappy.Decode(nil, data)
	if err != nil {
		return nil, err
	}

	var readResp prompb.ReadResponse
	if err = proto.Unmarshal(data, &readResp); err != nil {
		return nil, err
	}

	if len(readResp.Results) == 0 {
		return &remoteReadSampledResponse{Result: &prompb.QueryResult{}}, nil
	} else if len(readResp.Results) != 1 {
		return nil, fmt.Errorf("expected 1 result, got %d", len(readResp.Results))
	}

	return &remoteReadSampledResponse{Result: readResp.Results[0]}, nil
}

func (r *remoteReadRoundTripperV2) handleStreamedResponse(resp *http.Response, cancel context.CancelCauseFunc) (Response, error) {
	reader := &streamedChunkReader{
		reader: remote.NewChunkedReader(resp.Body, config.DefaultChunkedReadLimit, nil),
		cancel: cancel,
		closer: resp.Body,
	}
	retResp := &remoteReadStreamedResponse{
		reader: reader,
	}

	return retResp, nil
}

func mergeSampleResponses(responses []Response) (*bytes.Buffer, error) {
	rrResp := prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(responses)),
	}

	for idx, resp := range responses {
		switch value := resp.(type) {
		case *remoteReadSampledResponse:
			rrResp.Results[idx] = value.Result
		case nil:
			rrResp.Results[idx] = &prompb.QueryResult{}
		case *PrometheusResponse:
			// This is mostly there to handle the current state of the limits middleware
			// that return an "empty prometheus response" when outside the configured
			// limits. We still need to put a "query result" in the results, but we
			// can put whatever we want.
			rrResp.Results[idx] = &prompb.QueryResult{}
		default:
			return nil, fmt.Errorf("unsupported response type %T", resp)
		}
	}

	data, err := rrResp.Marshal()
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(snappy.Encode(nil, data)), nil
}

type mergedStreamedBody struct {
	*io.PipeReader
	reader *MergeChunkedReader
}

func (b *mergedStreamedBody) Close() error {
	return errors.Join(
		b.PipeReader.Close(),
		b.reader.Close(),
	)
}

func mergeStreamedResponses(responses []Response) (io.ReadCloser, error) {
	readers := make([]*streamedChunkReader, len(responses))
	for idx, resp := range responses {
		switch value := resp.(type) {
		case *remoteReadStreamedResponse:
			readers[idx] = value.reader
		case nil:
			readers[idx] = newEmptyStreamedChunkReader()
		case *PrometheusResponse:
			value.Close()
			readers[idx] = newEmptyStreamedChunkReader()
		default:
			return nil, fmt.Errorf("unsupported response type %T", resp)
		}
	}

	pr, pw := io.Pipe()
	chunkReader := NewMergeChunkedReader(readers...)
	go func() {
		defer chunkReader.Close()

		chunkWriter := remote.NewChunkedWriter(pw, nil)
		for {
			var chunk prompb.ChunkedReadResponse
			queryIdx, err := chunkReader.NextProto(&chunk)
			if err != nil {
				_ = pw.CloseWithError(err)
				break
			}
			chunk.QueryIndex = int64(queryIdx)
			data, err := proto.Marshal(&chunk)
			if err != nil {
				_ = pw.CloseWithError(err)
				break
			}
			if _, err = chunkWriter.Write(data); err != nil {
				_ = pw.CloseWithError(err)
				break
			}
		}
	}()

	return &mergedStreamedBody{pr, chunkReader}, nil
}

type remoteReadStreamedResponse struct {
	reader *streamedChunkReader
}

func (r *remoteReadStreamedResponse) GetHeaders() []*PrometheusHeader {
	return nil
}

func (r *remoteReadStreamedResponse) GetPrometheusResponse() (*PrometheusResponse, bool) {
	return nil, false
}

func (r *remoteReadStreamedResponse) Close() {
	_ = r.reader.Close()
}

func (r *remoteReadStreamedResponse) Reset() {
	panic("not implemented")
}

func (r *remoteReadStreamedResponse) String() string {
	panic("not implemented")
}

func (r *remoteReadStreamedResponse) ProtoMessage() {
	panic("not implemented")
}

type remoteReadSampledResponse struct {
	Result *prompb.QueryResult
}

func (r *remoteReadSampledResponse) GetHeaders() []*PrometheusHeader {
	return nil
}

func (r *remoteReadSampledResponse) GetPrometheusResponse() (*PrometheusResponse, bool) {
	return nil, false
}

func (r *remoteReadSampledResponse) Close() {
}

func (r *remoteReadSampledResponse) Reset() {
	panic("not implemented")
}

func (r *remoteReadSampledResponse) String() string {
	panic("not implemented")
}

func (r *remoteReadSampledResponse) ProtoMessage() {
	panic("not implemented")
}
