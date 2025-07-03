// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-logfmt/logfmt"
	"github.com/stretchr/testify/require"
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
	var line strings.Builder
	require.NoError(l.t, logfmt.NewEncoder(&line).EncodeKeyvals(keyvals...))

	l.mtx.Lock()
	l.t.Log(time.Now().Format(time.RFC3339Nano), line.String())
	l.mtx.Unlock()

	return nil
}
