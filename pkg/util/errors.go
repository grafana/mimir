// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"fmt"

	"github.com/grafana/dskit/services"
)

type NotRunningError struct {
	Component string
	State     services.State
}

func (e NotRunningError) Error() string {
	return fmt.Sprintf("%v not running: %v", e.Component, e.State)
}

func (e NotRunningError) Is(target error) bool {
	if _, ok := target.(NotRunningError); ok {
		return ok
	}

	return false
}
