// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/errors.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"errors"
)

// ErrStopProcess is the error returned by a service as a hint to stop the server entirely.
var ErrStopProcess = errors.New("stop process")
