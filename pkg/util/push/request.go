// SPDX-License-Identifier: AGPL-3.0-only

package push

import (
	"fmt"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// supplierFunc should return either a non-nil body or a non-nil error. The returned cleanup function can be nil.
type supplierFunc func() (req *mimirpb.WriteRequest, cleanup func(), err error)

// Request represents a push request. It allows lazy body reading from the underlying http request
// and adding cleanup functions that should be called after the request has been handled.
type Request struct {
	// have a backing array to avoid extra allocations
	cleanupsArr [10]func()
	cleanups    []func()

	getRequest supplierFunc

	request *mimirpb.WriteRequest
	err     error
}

func newRequest(p supplierFunc) *Request {
	r := &Request{
		getRequest: p,
	}
	r.cleanups = r.cleanupsArr[:0]
	return r
}

func NewParsedRequest(r *mimirpb.WriteRequest) *Request {
	return newRequest(func() (*mimirpb.WriteRequest, func(), error) {
		return r, nil, nil
	})
}

// WriteRequest returns request from supplier function. Function is only called once,
// and subsequent calls to WriteRequest return the same value.
func (r *Request) WriteRequest() (*mimirpb.WriteRequest, error) {
	if r.request == nil && r.err == nil {
		var cleanup func()
		r.request, cleanup, r.err = r.getRequest()
		if r.request == nil && r.err == nil {
			r.err = fmt.Errorf("push.Request supplierFunc returned a nil body and a nil error, either should be non-nil")
		}
		r.AddCleanup(cleanup)
	}
	return r.request, r.err
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
