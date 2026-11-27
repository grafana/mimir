// SPDX-License-Identifier: AGPL-3.0-only

//go:build !(linux || darwin)

package ruler

import "time"

// processCPUTime returns 0 on this system.
// The benchmarks then report no CPU time.
func processCPUTime() time.Duration {
	return 0
}
