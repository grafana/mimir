// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/errors.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"fmt"
	"net/http"

	"github.com/prometheus/prometheus/model/labels"
)

type validationError struct {
	err    error // underlying error
	code   int
	labels labels.Labels
}

func makeLimitError(err error) error {
	return &validationError{
		err:  err,
		code: http.StatusBadRequest,
	}
}

func makeMetricLimitError(labels labels.Labels, err error) error {
	return &validationError{
		err:    err,
		code:   http.StatusBadRequest,
		labels: labels,
	}
}

func (e *validationError) Error() string {
	if e.labels.IsEmpty() {
		return e.err.Error()
	}
	return fmt.Sprintf("%s This is for series %s", e.err.Error(), e.labels.String())
}

// wrapWithUser prepends the user to the error. It does not retain a reference to err.
func wrapWithUser(err error, userID string) error {
	return fmt.Errorf("user=%s: %s", userID, err)
}
