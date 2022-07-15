// SPDX-License-Identifier: AGPL-3.0-only

package push

import (
	"fmt"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// supplierFunc should return either a non-nil body or a non-nil error. The cleanup function can be nil.
type supplierFunc func() (*mimirpb.WriteRequest, func(), error)

// Request represents a push request. It allows lazy body reading from the underlying http request
// and adding cleanups that should be done after the request is completed.
type Request struct {
	cleanups []func()

	getRequest supplierFunc

	request *mimirpb.WriteRequest
	err     error
}

func newRequest(p supplierFunc) *Request {
	return &Request{
		cleanups:   make([]func(), 0, 10),
		getRequest: p,
	}
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

// CleanUp calls all added cleanups.
func (r *Request) CleanUp() {
	for _, f := range r.cleanups {
		f()
	}
}
