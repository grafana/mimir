// SPDX-License-Identifier: AGPL-3.0-only

package usagestats

import (
	"net/http"
)

// RequestsMiddleware tracks the number of requests.
type RequestsMiddleware struct {
	counter *Counter
}

// NewRequestsMiddleware makes a new RequestsMiddleware.
func NewRequestsMiddleware(name string) *RequestsMiddleware {
	return &RequestsMiddleware{
		counter: GetCounter(name),
	}
}

// Wrap implements middleware.Interface.
func (m *RequestsMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.counter.Inc(1)
		next.ServeHTTP(w, r)
	})
}
