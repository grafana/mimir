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
	var distributorError Error
	assert.False(t, errors.As(originalErr, &distributorError))

	originalErr = log.DoNotLogError{
		Err: originalErr,
	}

	err := NewDistributorPushError(originalErr)
	assert.Error(t, err)

	assert.True(t, errors.Is(err, originalErr))
	assert.ErrorAs(t, err, &distributorError)

	var doNotLogErr log.DoNotLogError
	assert.ErrorAs(t, err, &doNotLogErr)
	assert.False(t, doNotLogErr.ShouldLog(context.Background(), 0))
}

func TestNewReplicasNotMatchDistributorPushError(t *testing.T) {
	originalErr := errors.New(errMsg)
	var distributorError Error
	assert.False(t, errors.As(originalErr, &distributorError))

	err := NewReplicasNotMatchDistributorPushError(originalErr)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, originalErr))
	assert.ErrorAs(t, err, &distributorError)
}

func TestNewTooManyClustersDistributorPushError(t *testing.T) {
	originalErr := errors.New(errMsg)
	var distributorError Error
	assert.False(t, errors.As(originalErr, &distributorError))

	err := NewTooManyClustersDistributorPushError(originalErr)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, originalErr))
	assert.ErrorAs(t, err, &distributorError)
}

func TestNewValidationDistributorPushError(t *testing.T) {
	originalErr := validation.ValidationError(errors.New(errMsg))
	var distributorError Error
	assert.False(t, errors.As(originalErr, &distributorError))

	err := NewValidationDistributorPushError(originalErr)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, originalErr))
	assert.ErrorAs(t, err, &distributorError)
}

func TestNewIngestionRateDistributorPushError(t *testing.T) {
	originalErr := validation.NewIngestionRateLimitedError(10, 10)
	var distributorError Error
	assert.False(t, errors.As(originalErr, &distributorError))

	err := NewIngestionRateDistributorPushError(10, 10)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, originalErr))
	assert.ErrorAs(t, err, &distributorError)
}

func TestNewRequestRateDistributorPushError(t *testing.T) {
	originalErr := validation.NewRequestRateLimitedError(10, 10)
	var distributorError Error
	assert.False(t, errors.As(originalErr, &distributorError))

	err := NewRequestRateDistributorPushError(10, 10, false)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, originalErr))
	assert.ErrorAs(t, err, &distributorError)
}
