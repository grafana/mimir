// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"

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

	// We attempt to read the query params or form bodies to get values which we can use to identify these requests.
	// To do this safely and not impact the next handlers we must do this in a way as to not disturb the body for subsequent handlers.
	// We er on the side of caution to make sure we do not risk consuming a body we can not safely restore into the request.

	if r.Header.Get("Content-Type") == "application/x-protobuf" && querymiddleware.IsRemoteReadQuery(r.URL.Path) {
		params, err = querymiddleware.ParseRemoteReadRequestValuesWithoutConsumingBody(r)

	} else if r.Header.Get("Content-Type") == "application/x-www-form-urlencoded" && r.ContentLength >= 0 {
		// Check the ContentLength as -1 could mean a chunked encoding transfer which we do not want to await to read
		params, err = util.ParseRequestFormWithoutConsumingBody(r)
	} else {
		params = r.URL.Query()
	}

	if err != nil {
		// This can occur for routes with a max body size set and the activity tracker has attempted to read the body which exceeds this limit.
		if util.IsRequestBodyTooLarge(err) {
			http.Error(w, "http: request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		// This is not expected to happen but if there was an error here and the request body can not be restored then the request is no longer valid.
		level.Error(m.log).Log("msg", "failed to parse request params for activity tracking", "path", r.URL.Path, "contentType", r.Header.Get("Content-Type"), "err", err)
		http.Error(w, "failed to parse request params for activity tracking", http.StatusInternalServerError)
		return
	}

	ix := m.tracker.Insert(func() string {
		return toActivityTrackerString(r, r.Header.Get("User-Agent"), params)
	})
	defer m.tracker.Delete(ix)
	if ix < 0 {
		// Logging for completeness in case there is an issue with the activity tracker
		level.Error(m.log).Log("msg", "failed to insert request for activity tracking")
	}

	m.next.ServeHTTP(w, r)
}

func toActivityTrackerString(request *http.Request, userAgent string, requestParams url.Values) string {
	tenantID := "(unknown)"

	// Usually we'd get the tenantId from the context, however this handler runs prior to the auth handler (which populates the context)
	// so we pull this value directly from the HTTP header.
	if orgID, _, err := user.ExtractOrgIDFromHTTPRequest(request); err == nil {
		tenantID = orgID
	}

	params := requestParams.Encode()
	if params == "" {
		params = "(no params)"
	}

	// This doesn't have to be pretty, just useful for debugging, so prioritize efficiency.
	return fmt.Sprintf("user:%s UA:%s req:%s %s %s", tenantID, userAgent, request.Method, request.URL.Path, params)
}
