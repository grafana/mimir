package bucket

import "testing"

func TestRetryingBucketClient(t *testing.T) {
	mockBucket := &ClientMock{}
	_ = NewRetryingBucketClient(mockBucket)
}
