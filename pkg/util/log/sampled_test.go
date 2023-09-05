// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"context"
	"fmt"
	"testing"
	"time"

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
	sampledErr := sampledError{err: err, sampler: sampler}

	require.EqualError(t, sampledErr, fmt.Sprintf("%s (sampled 1/%d)", err.Error(), errorSampleRate))
}

func TestSampledError_ShouldLog(t *testing.T) {
	sampler := NewSampler(errorSampleRate)
	err := fmt.Errorf(errorWithIDFormat, 1)
	sampledErr := sampledError{err: err, sampler: sampler}
	ctx := context.Background()

	require.True(t, sampledErr.ShouldLog(ctx, time.Duration(0)))
	for i := 1; i < errorSampleRate; i++ {
		require.False(t, sampledErr.ShouldLog(ctx, time.Duration(0)))
	}
	require.True(t, sampledErr.ShouldLog(ctx, time.Duration(0)))
}
