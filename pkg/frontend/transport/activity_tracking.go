// SPDX-License-Identifier: AGPL-3.0-only

package transport

import (
	"net/http"
	"net/url"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/activitytracker"
)

type activityTrackingMiddleware struct {
	tracker *activitytracker.ActivityTracker
	log     log.Logger
	next    http.Handler
}

// NewActivityTrackingMiddleware wraps next with a middleware that inserts and deletes an activity tracker entry
// for each request. It must be placed outside the gzip middleware so that the tracker entry outlives gzip's
// deferred Close and covers the full response flush.
func NewActivityTrackingMiddleware(at *activitytracker.ActivityTracker, logger log.Logger, next http.Handler) http.Handler {
	return &activityTrackingMiddleware{tracker: at, log: logger, next: next}
}

func (m *activityTrackingMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var params url.Values
	var err error
	if r.Header.Get("Content-Type") == "application/x-protobuf" && querymiddleware.IsRemoteReadQuery(r.URL.Path) {
		params, err = querymiddleware.ParseRemoteReadRequestValuesWithoutConsumingBody(r)
	} else {
		params, err = util.ParseRequestFormWithoutConsumingBody(r)
	}

	if err != nil {
		// This is not expected to happen but if there was an error here and the request body can not be restored then the request is no longer valid.
		// Rather than fail it here we pass the request to the next handler so it can return its own specific bad request error.
		level.Error(m.log).Log("msg", "failed to parse request params for activity tracking", "err", err)
		m.next.ServeHTTP(w, r)
		return
	}

	ix := m.tracker.Insert(func() string {
		return httpRequestActivity(r, r.Header.Get("User-Agent"), params)
	})
	defer m.tracker.Delete(ix)
	if ix < 0 {
		// Logging for completeness in case there is an issue with the activity tracker
		level.Error(m.log).Log("msg", "failed to insert request for activity tracking")
	}

	m.next.ServeHTTP(w, r)
}
