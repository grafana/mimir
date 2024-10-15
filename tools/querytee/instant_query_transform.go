// SPDX-License-Identifier: AGPL-3.0-only

package querytee

import (
	"net/http"
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
	// ParseForm should have already been called, but we call it again to be sure.
	// ParseForm is idempotent.
	if err := r.ParseForm(); err != nil {
		return nil, nil, err
	}

	if r.Form.Has("time") {
		return r, body, nil
	}

	// No 'time' parameter in either the body or URL. Add it.
	t := timeNow().Format(time.RFC3339)
	level.Debug(logger).Log("msg", "instant query had no explicit time parameter, adding it based on the current time", "time", t)

	// Form should contain URL parameters + parameters from the body, and isn't updated automatically when we set PostForm or URL below,
	// so update it here to ensure everything remains consistent.
	r.Form.Set("time", t)

	if len(body) > 0 {
		// Request has a body, add the 'time' parameter there.
		r.PostForm.Set("time", t)
		body = []byte(r.PostForm.Encode())

		// Update the content length to reflect the new body.
		r.ContentLength = int64(len(body))
		r.Header.Set("Content-Length", strconv.Itoa(len(body)))

		return r, body, nil
	}

	// Otherwise, add it to the URL.
	queryParams := r.URL.Query()
	queryParams.Set("time", t)
	r.URL.RawQuery = queryParams.Encode()
	return r, body, nil
}
