// SPDX-License-Identifier: AGPL-3.0-only

package distributorerror

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestNewReplicasNotMatchError(t *testing.T) {
	replica := "a"
	elected := "b"
	err := NewReplicasNotMatchError(replica, elected)
	assert.Error(t, err)
	expectedMsg := fmt.Sprintf("replicas did not mach, rejecting sample: replica=%s, elected=%s", replica, elected)
	assert.EqualError(t, err, expectedMsg)
}

func TestNewTooManyClustersError(t *testing.T) {
	limit := 1
	err := NewTooManyClustersError(limit)
	assert.Error(t, err)
	expectedMsg := globalerror.TooManyHAClusters.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the write request has been rejected because the maximum number of high-availability (HA) clusters has been reached for this tenant (limit: %d)", limit),
		validation.HATrackerMaxClustersFlag,
	)
	assert.EqualError(t, err, expectedMsg)
}

func TestNewValidationError(t *testing.T) {
	validationMsg := "this is a validation error"
	validationErr := errors.New(validationMsg)
	err := NewValidationError(validationErr)
	assert.Error(t, err)
	assert.EqualError(t, err, validationMsg)
}

func TestNewIngestionRateError(t *testing.T) {
	limit := 10.0
	burst := 10
	err := NewIngestionRateLimitedError(limit, burst)
	assert.Error(t, err)
	expectedMsg := validation.FormatIngestionRateLimitedMessage(limit, burst)
	assert.EqualError(t, err, expectedMsg)
}

func TestNewRequestRateError(t *testing.T) {
	limit := 10.0
	burst := 10
	err := NewRequestRateLimitedError(limit, burst)
	assert.Error(t, err)
	expectedMsg := validation.FormatRequestRateLimitedMessage(limit, burst)
	assert.EqualError(t, err, expectedMsg)
}
