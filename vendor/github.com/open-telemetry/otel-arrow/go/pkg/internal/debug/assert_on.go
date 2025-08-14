// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build assert

package debug

import "fmt"

// Assert will panic and print any message when assertions are enabled.
// Use --tags=assert in the Go build.
func Assert(cond bool, msg any) {
	if !cond {
		panic(fmt.Sprint(msg))
	}
}

// AssertionsOn indicates when an Assert() will be real.
func AssertionsOn() bool { return true }
