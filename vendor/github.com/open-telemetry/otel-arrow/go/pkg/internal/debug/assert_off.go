// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !assert

package debug

// Assert will not panic or print any message when assertions are disabled.
// Use --tags=assert in the Go build.
func Assert(cond bool, msg any) {}

// AssertionsOn indicates when an Assert() will be real.
func AssertionsOn() bool { return false }
