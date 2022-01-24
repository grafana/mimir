// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"testing"

	"github.com/go-kit/log"
)

type testingLogger struct {
	t *testing.T
}

func NewTestingLogger(t *testing.T) log.Logger {
	return &testingLogger{
		t: t,
	}
}

func (l *testingLogger) Log(keyvals ...interface{}) error {
	l.t.Log(keyvals)
	return nil
}
