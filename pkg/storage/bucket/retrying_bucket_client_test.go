package bucket

import (
	"context"
	"testing"

	"github.com/grafana/dskit/backoff"
	"github.com/stretchr/testify/assert"
)

func TestRetryingBucketClient(t *testing.T) {
	mockBucket := &ClientMock{}
	mockBucket.MockGet("/meta.json", "{}", nil)
	timeoutBucket := NewMockBucketClientWithTimeouts(mockBucket, 2)
	c := NewRetryingBucketClient(timeoutBucket)

	ctx := context.Background()
	r, err := c.WithRetries(backoff.Config{MaxRetries: 3}).Get(ctx, "/meta.json")
	assert.NotNil(t, r)
	assert.NoError(t, err)

	ct, success := timeoutBucket.GetStats("/meta.json")
	assert.Equal(t, 3, ct)
	assert.True(t, success)

	assert.Len(t, mockBucket.Calls, 1)
}

func TestRetryingBucketClient_RetriesExceeded(t *testing.T) {
	mockBucket := &ClientMock{}
	mockBucket.MockGet("/meta.json", "{}", nil)
	timeoutBucket := NewMockBucketClientWithTimeouts(mockBucket, 10)
	c := NewRetryingBucketClient(timeoutBucket)

	ctx := context.Background()
	_, err := c.WithRetries(backoff.Config{MaxRetries: 7}).Get(ctx, "/meta.json")
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorContains(t, err, "failed with retries")

	ct, success := timeoutBucket.GetStats("/meta.json")
	assert.Equal(t, 7, ct)
	assert.False(t, success)

	assert.Len(t, mockBucket.Calls, 0)
}

func TestRetryingBucketClient_NoRetry(t *testing.T) {
	mockBucket := &ClientMock{}
	mockBucket.MockGet("/meta.json", "{}", nil)
	timeoutBucket := NewMockBucketClientWithTimeouts(mockBucket, 2)
	c := NewRetryingBucketClient(timeoutBucket)

	ctx := context.Background()
	_, err := c.WithRequestTimeout(0).Get(ctx, "/meta.json")
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	ct, success := timeoutBucket.GetStats("/meta.json")
	assert.Equal(t, 1, ct)
	assert.False(t, success)

	assert.Len(t, mockBucket.Calls, 0)
}
