// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
)

type TestingLogger struct {
	t   testing.TB
	mtx *sync.Mutex
}

func NewTestingLogger(t testing.TB) *TestingLogger {
	return &TestingLogger{
		t:   t,
		mtx: &sync.Mutex{},
	}
}

// WithT returns a new logger that logs to t. Writes between the new logger and the original logger are synchronized.
func (l *TestingLogger) WithT(t testing.TB) log.Logger {
	return &TestingLogger{
		t:   t,
		mtx: l.mtx,
	}
}

func (l *TestingLogger) Log(keyvals ...interface{}) error {
	// Prepend log with timestamp.
	keyvals = append([]interface{}{time.Now().String()}, keyvals...)

	l.mtx.Lock()
	l.t.Log(keyvals...)
	l.mtx.Unlock()

	return nil
}
