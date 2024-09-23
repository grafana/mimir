// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
)

type testingLogger struct {
	t   testing.TB
	mtx sync.Mutex
}

func NewTestingLogger(t testing.TB) log.Logger {
	return &testingLogger{
		t: t,
	}
}

func (l *testingLogger) Log(keyvals ...interface{}) error {
	// Prepend log with timestamp.
	keyvals = append([]interface{}{time.Now().String()}, keyvals...)

	l.mtx.Lock()
	l.t.Log(keyvals...)
	l.mtx.Unlock()

	return nil
}
