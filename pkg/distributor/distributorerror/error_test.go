// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	errMsg = "this is an error"
)

func TestDoNotLogDistributorPushError(t *testing.T) {
	originalErr := errors.New(errMsg)
	originalErr = log.DoNotLogError{
		Err: originalErr,
	}

	err := NewDistributorPushError(originalErr)
	assert.Error(t, err)

	assert.True(t, errors.Is(err, originalErr))

	var doNotLogErr log.DoNotLogError
	assert.ErrorAs(t, err, &doNotLogErr)
	assert.False(t, doNotLogErr.ShouldLog(context.Background(), 0))
}

func TestNewReplicasNotMatchError(t *testing.T) {
	err := NewReplicasNotMatchError("a", "b")
	assert.Error(t, err)
}

func TestNewTooManyClustersError(t *testing.T) {
	err := NewTooManyClustersError(1)
	assert.Error(t, err)
}

func TestNewValidationDistributorPushError(t *testing.T) {
	originalErr := validation.ValidationError(errors.New(errMsg))
	err := NewValidationDistributorPushError(originalErr)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, originalErr))
}

func TestNewIngestionRateDistributorPushError(t *testing.T) {
	originalErr := validation.NewIngestionRateLimitedError(10, 10)
	err := NewIngestionRateDistributorPushError(10, 10)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, originalErr))
}

func TestNewRequestRateDistributorPushError(t *testing.T) {
	originalErr := validation.NewRequestRateLimitedError(10, 10)
	err := NewRequestRateDistributorPushError(10, 10, false)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, originalErr))
}
