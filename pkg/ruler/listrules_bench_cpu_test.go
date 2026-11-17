// SPDX-License-Identifier: AGPL-3.0-only

//go:build linux || darwin

package ruler

import (
	"syscall"
	"time"
)

// processCPUTime returns the user and system CPU time of the process.
func processCPUTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
