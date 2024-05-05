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

	s := timeoutBucket.OpString("get", "/meta.json")
	ct, ok := timeoutBucket.Calls[s]
	assert.True(t, ok)
	assert.Equal(t, 3, ct)
	_, success := timeoutBucket.Success[s]
	assert.True(t, success)

	assert.Len(t, mockBucket.Calls, 1)
}

func TestRetryingBucketClient_NoRetry(t *testing.T) {
	mockBucket := &ClientMock{}
	mockBucket.MockGet("/meta.json", "{}", nil)
	timeoutBucket := NewMockBucketClientWithTimeouts(mockBucket, 2)
	c := NewRetryingBucketClient(timeoutBucket)

	ctx := context.Background()
	_, err := c.WithRequestDurationLimit(0).Get(ctx, "/meta.json")
	assert.Error(t, err)
	assert.ErrorIs(t, context.DeadlineExceeded, err)

	s := timeoutBucket.OpString("get", "/meta.json")
	ct, ok := timeoutBucket.Calls[s]
	assert.True(t, ok)
	assert.Equal(t, 1, ct)
	_, success := timeoutBucket.Success[s]
	assert.False(t, success)

	assert.Len(t, mockBucket.Calls, 0)
}
