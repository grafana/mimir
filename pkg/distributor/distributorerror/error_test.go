// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	errMsg = "this is an error"
)

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
	err := NewValidationError(originalErr)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, originalErr))
}

func TestNewIngestionRateDistributorPushError(t *testing.T) {
	originalErr := validation.NewIngestionRateLimitedError(10, 10)
	err := NewIngestionRateError(10, 10)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, originalErr))
}

func TestNewRequestRateDistributorPushError(t *testing.T) {
	originalErr := validation.NewRequestRateLimitedError(10, 10)
	err := NewRequestRateError(10, 10, false)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, originalErr))
}
