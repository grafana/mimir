// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"fmt"
	"time"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// supplierFunc should return either a non-nil body or a non-nil error. The returned cleanup function can be nil.
// uncompressedBodySize is the size of the original request body before any conversion (e.g., RW2 to RW1, OTLP to Prometheus).
// It may be 0 if unknown.
type supplierFunc func() (req *mimirpb.WriteRequest, cleanup func(), uncompressedBodySize int, err error)

// Request represents a push request. It allows lazy body reading from the underlying http request
// and adding cleanup functions that should be called after the request has been handled.
type Request struct {
	// have a backing array to avoid extra allocations
	cleanupsArr [10]func()
	cleanups    []func()

	getRequest supplierFunc

	request *mimirpb.WriteRequest
	err     error

	// group is the value of the configured `group` label for the customer metrics.
	group string

	// artificialDelay is the artificial delay for the request.
	// Negative values are treated as "not set".
	artificialDelay time.Duration

	contentLength int64

	// uncompressedBodySize is the uncompressed request body size (wire bytes before any conversion).
	// It may be 0 if unknown.
	uncompressedBodySize int
}

func newRequest(p supplierFunc) *Request {
	r := &Request{
		getRequest:      p,
		artificialDelay: -1,
	}
	r.cleanups = r.cleanupsArr[:0]
	return r
}

func NewParsedRequest(r *mimirpb.WriteRequest, uncompressedBodySize int) *Request {
	return newRequest(func() (*mimirpb.WriteRequest, func(), int, error) {
		return r, nil, uncompressedBodySize, nil
	})
}

// initWriteRequest initializes the write request by calling the supplier function.
// It is safe to call multiple times; the supplier is only invoked once.
func (r *Request) initWriteRequest() {
	if r.request == nil && r.err == nil {
		var cleanup func()
		r.request, cleanup, r.uncompressedBodySize, r.err = r.getRequest()
		if r.request == nil && r.err == nil {
			r.err = fmt.Errorf("push.Request supplierFunc returned a nil body and a nil error, either should be non-nil")
		}
		r.AddCleanup(cleanup)
	}
}

// WriteRequest returns request from supplier function. Function is only called once,
// and subsequent calls to WriteRequest return the same value.
func (r *Request) WriteRequest() (*mimirpb.WriteRequest, error) {
	r.initWriteRequest()
	return r.request, r.err
}

// UncompressedBodySize returns the uncompressed request body size (wire bytes before any conversion).
// Returns 0 if unknown.
func (r *Request) UncompressedBodySize() int {
	r.initWriteRequest()
	return r.uncompressedBodySize
}

// AddCleanup adds a function that will be called once CleanUp is called. If f is nil, it will not be invoked.
func (r *Request) AddCleanup(f func()) {
	if f == nil {
		return
	}
	r.cleanups = append(r.cleanups, f)
}

// CleanUp calls all added cleanups in reverse order - the last added is the first invoked. CleanUp removes
// each called cleanup function from the list of cleanups. So subsequent calls to CleanUp will not invoke the same cleanup functions.
func (r *Request) CleanUp() {
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		r.cleanups[i]()
	}
	r.cleanups = r.cleanups[:0]
}
