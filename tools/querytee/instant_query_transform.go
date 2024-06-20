// SPDX-License-Identifier: AGPL-3.0-only

package querytee

import (
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/spanlogger"
)

var timeNow = time.Now // Interception point to allow tests to set the current time for AddMissingTimeParam.

// AddMissingTimeParam adds a 'time' parameter to any query request that does not have one.
//
// Instant queries without a 'time' parameter use the current time on the executing backend.
// However, this can vary between backends for the same request, which can cause comparison failures.
//
// So, to make comparisons more reliable, we add the 'time' parameter in the proxy to ensure all
// backends use the same value.
func AddMissingTimeParam(r *http.Request, body []byte, logger *spanlogger.SpanLogger) (*http.Request, []byte, error) {
	parsedBody, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, nil, err
	}

	if parsedBody.Has("time") {
		return r, body, nil
	}

	queryParams := r.URL.Query()

	if queryParams.Has("time") {
		return r, body, nil
	}

	// No 'time' parameter in either the body or URL. Add it.
	level.Debug(logger).Log("msg", "instant query had no explicit time parameter, adding it based on the current time")

	if len(body) > 0 {
		// Request has a body, add the 'time' parameter there.
		parsedBody.Set("time", timeNow().Format(time.RFC3339))
		body = []byte(parsedBody.Encode())

		// Outgoing requests should only rely on the request body, but we update Form here for consistency.
		r.Form = parsedBody

		// Update the content length to reflect the new body.
		r.ContentLength = int64(len(body))
		r.Header.Set("Content-Length", strconv.Itoa(len(body)))

		return r, body, nil
	}

	// Otherwise, add it to the URL.
	queryParams.Set("time", timeNow().Format(time.RFC3339))
	r.URL.RawQuery = queryParams.Encode()
	return r, body, nil
}
