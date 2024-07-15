// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"testing"
	"time"

	"github.com/go-kit/log"
)

type testingLogger struct {
	t testing.TB
}

func NewTestingLogger(t testing.TB) log.Logger {
	return &testingLogger{
		t: t,
	}
}

func (l *testingLogger) Log(keyvals ...interface{}) error {
	// Prepend log with timestamp.
	keyvals = append([]interface{}{time.Now().String()}, keyvals...)
	l.t.Log(keyvals...)
	return nil
}
