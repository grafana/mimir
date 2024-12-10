// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"context"
	"fmt"
	"testing"

	"github.com/grafana/dskit/middleware"
	"github.com/stretchr/testify/require"
)

const (
	errorSampleRate   = 5
	errorWithIDFormat = "this is an occurrence of error with id %d"
)

func TestSampler_Sample(t *testing.T) {
	sampler := NewSampler(errorSampleRate)
	require.True(t, sampler.Sample())
	for i := 1; i < errorSampleRate; i++ {
		require.False(t, sampler.Sample())
	}
	require.True(t, sampler.Sample())
}

func TestSampledError_Error(t *testing.T) {
	sampler := NewSampler(errorSampleRate)
	err := fmt.Errorf(errorWithIDFormat, 1)
	sampledErr := SampledError{err: err, sampler: sampler}

	require.EqualError(t, sampledErr, err.Error())
}

func TestSampledError_ShouldLog(t *testing.T) {
	sampler := NewSampler(errorSampleRate)
	err := fmt.Errorf(errorWithIDFormat, 1)
	sampledErr := SampledError{err: err, sampler: sampler}
	ctx := context.Background()

	shouldLog, reason := sampledErr.ShouldLog(ctx)
	require.True(t, shouldLog)
	require.Equal(t, fmt.Sprintf("sampled 1/%d", errorSampleRate), reason)

	for i := 1; i < errorSampleRate; i++ {
		shouldLog, reason = sampledErr.ShouldLog(ctx)
		require.False(t, shouldLog)
		require.Equal(t, fmt.Sprintf("sampled 1/%d", errorSampleRate), reason)
	}

	shouldLog, reason = sampledErr.ShouldLog(ctx)
	require.True(t, shouldLog)
	require.Equal(t, fmt.Sprintf("sampled 1/%d", errorSampleRate), reason)
}

func TestSampledError_ShouldImplementOptionalLoggingInterface(t *testing.T) {
	sampler := NewSampler(errorSampleRate)
	err := fmt.Errorf(errorWithIDFormat, 1)
	sampledErr := SampledError{err: err, sampler: sampler}

	var optionalLoggingErr middleware.OptionalLogging
	require.ErrorAs(t, sampledErr, &optionalLoggingErr)
}

func TestNilSampler(t *testing.T) {
	var s *Sampler
	err := fmt.Errorf("error")

	sampledErr := s.WrapError(err)
	require.NotNil(t, sampledErr)
	require.Equal(t, err, sampledErr)
}
