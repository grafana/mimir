// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRateLimitedLogger(t *testing.T) {
	const (
		limitInterval       = time.Second
		testDuration        = time.Second * 10
		attemptedCalls      = 1000
		expectedActualCalls = 10
	)

	testStart := time.Now()
	fakeNow := testStart

	mockLogger := &mockLogger{}
	mockLogger.On("Log").Times(expectedActualCalls).Return(nil)

	rl := NewRateLimitedLogger(limitInterval, mockLogger, func() time.Time { return fakeNow })

	timeStep := testDuration / attemptedCalls
	testEnd := testStart.Add(testDuration).Add(time.Nanosecond)
	for ; fakeNow.Before(testEnd); fakeNow = fakeNow.Add(timeStep) {
		assert.NoError(t, rl.Log())
	}
	mockLogger.AssertExpectations(t)
}

func TestRateLimitedLogger_Burst(t *testing.T) {
	const (
		limitInterval       = time.Second
		expectedActualCalls = 2
	)

	testStart := time.Now()
	fakeNow := testStart

	mockLogger := &mockLogger{}
	mockLogger.On("Log").Times(expectedActualCalls).Return(nil)

	rl := NewRateLimitedLogger(limitInterval, mockLogger, func() time.Time { return fakeNow })

	fakeNow = testStart.Add(time.Nanosecond)
	assert.NoError(t, rl.Log())

	// Only one of these should go through because we don't have a leaky bucket
	fakeNow = testStart.Add(limitInterval * 3)
	assert.NoError(t, rl.Log())
	assert.NoError(t, rl.Log())
	assert.NoError(t, rl.Log())

	mockLogger.AssertExpectations(t)
}

type mockLogger struct {
	mock.Mock
}

func (m *mockLogger) Log(keyvals ...interface{}) error {
	return m.MethodCalled("Log", keyvals...).Error(0)
}
