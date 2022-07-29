// SPDX-License-Identifier: AGPL-3.0-only

package push

import (
	"context"
	"net/http"
	"sync"

	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type Request struct {
	cleanups []func()

	httpReq                      *http.Request
	maxMessageSize               int
	allowSkipLabelNameValidation bool
	parser                       ParserFunc
	parseRequestOnce             sync.Once
	parsedRequest                *mimirpb.WriteRequest
	parseError                   error
}

func (r *Request) parseRequest(ctx context.Context) {
	bufHolder := bufferPool.Get().(*bufHolder)
	var req mimirpb.PreallocWriteRequest
	buf, err := r.parser(ctx, r.httpReq, r.maxMessageSize, bufHolder.buf, &req)
	buf = buf[:cap(buf)]
	if err != nil {
		bufferPool.Put(bufHolder)
		r.parseError = httpgrpc.Errorf(http.StatusBadRequest, err.Error())
		return
	}
	// If decoding allocated a bigger buffer, put that one back in the pool.
	if len(buf) > len(bufHolder.buf) {
		bufHolder.buf = buf
	}

	r.AddCleanup(func() {
		mimirpb.ReuseSlice(req.Timeseries)
		bufferPool.Put(bufHolder)
	})

	if r.allowSkipLabelNameValidation {
		req.SkipLabelNameValidation = req.SkipLabelNameValidation && r.httpReq.Header.Get(SkipLabelNameValidationHeader) == "true"
	} else {
		req.SkipLabelNameValidation = false
	}

	if req.Source == 0 {
		req.Source = mimirpb.API
	}

	r.parsedRequest = &req.WriteRequest
}

// WriteRequest parses the body of the push request and caches it.
// Subsequent calls to WriteRequest return the same results.
func (r *Request) WriteRequest(ctx context.Context) (*mimirpb.WriteRequest, error) {
	r.parseRequestOnce.Do(func() {
		r.parseRequest(ctx)
	})
	return r.parsedRequest, r.parseError
}

// AddCleanup adds a function that will be called once CleanUp is called.
func (r *Request) AddCleanup(f func()) {
	r.cleanups = append(r.cleanups, f)
}

// CleanUp calls all added cleanups.
func (r *Request) CleanUp() {
	for _, f := range r.cleanups {
		f()
	}
}

func NewParsedRequest(r *mimirpb.WriteRequest) *Request {
	req := &Request{parsedRequest: r}
	req.parseRequestOnce.Do(func() {}) // no need to parse anything, we have the request
	return req
}
