// SPDX-License-Identifier: AGPL-3.0-only

package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/thanos-io/objstore"
)

// RetryingBucketClient is a bucket wrapper that knows how to add retries on top of
// (most of) the Bucket operations.  The Thanos Bucket providers each have some
// level of retries, but they can be inconsistent. And they don't handle things
// like the request context timing out due to a hung TCP connection.
type RetryingBucketClient struct {
	objstore.Bucket

	requestDurationLimit time.Duration
	retryPolicy          backoff.Config
}

var _ objstore.Bucket = (*RetryingBucketClient)(nil)

func NewRetryingBucketClient(wrappedBucket objstore.Bucket) *RetryingBucketClient {
	return &RetryingBucketClient{
		Bucket:               wrappedBucket,
		requestDurationLimit: 30 * time.Second,
		retryPolicy: backoff.Config{
			MaxRetries: 3,
			MinBackoff: 10 * time.Millisecond,
			MaxBackoff: 500 * time.Millisecond,
		},
	}
}

func shouldRetry(err error) bool {
	// We want to retry DeadlineExceeded, network connectivity things, and other
	// things like 5xx errors from blob storage.
	var tempErr interface{ Temporary() bool }

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return true
	case errors.As(err, &tempErr):
		return tempErr.Temporary()
	}

	return false
}

// WithRequestDurationLimit allows a caller to specify a limit on how long a
// single object storage request can take. This allows RetryingBucketClient to
// perform a number of retries on context.DeadlineExceeded errors within the
// overall deadline defined on the context passed to the individual
// objstore.Bucket methods.
// Example:
//
//	b := NewRetryingBucketClient(bucket)
//	ctx, cancel := context.WithTimeout(5*time.Minute)
//	err1 := b.WithRequestDurationLimit(10*time.Second).Get(ctx, "stuff/tinymanifest")
//	err2 := b.WithRequestDurationLimit(2*time.Minute).Upload(ctx, "stuff/bigmanifest", ...)
//	...
func (r *RetryingBucketClient) WithRequestDurationLimit(lim time.Duration) *RetryingBucketClient {
	clone := *r
	clone.requestDurationLimit = lim
	return &clone
}

func (r *RetryingBucketClient) WithRetries(bc backoff.Config) *RetryingBucketClient {
	clone := *r
	clone.retryPolicy = bc
	return &clone
}

func (r *RetryingBucketClient) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if r.requestDurationLimit <= 0 {
		return r.Bucket.Get(ctx, name)
	}

	var lastErr error
	b := backoff.New(ctx, r.retryPolicy)

	for b.Ongoing() {
		// This goes for any of these that return a Reader stream: we don't
		// call rctx's cancel function because it'll cancel the stream. We
		// let the parent context's cancel function do that work.
		rctx, _ := context.WithTimeout(ctx, r.requestDurationLimit)
		r, err := r.Bucket.Get(rctx, name)
		if err == nil || !shouldRetry(err) {
			return r, err
		}
		lastErr = err
		b.Wait()
	}

	return nil, fmt.Errorf("get failed with retries: %w (%w)", lastErr, b.Err())
}

func (r *RetryingBucketClient) GetRange(ctx context.Context, name string, off int64, length int64) (io.ReadCloser, error) {
	if r.requestDurationLimit <= 0 {
		return r.Bucket.GetRange(ctx, name, off, length)
	}

	var lastErr error
	b := backoff.New(ctx, r.retryPolicy)

	for b.Ongoing() {
		rctx, _ := context.WithTimeout(ctx, r.requestDurationLimit)
		r, err := r.Bucket.GetRange(rctx, name, off, length)
		if err == nil || !shouldRetry(err) {
			return r, err
		}
		lastErr = err
		b.Wait()
	}

	return nil, fmt.Errorf("get range failed with retries: %w (%w)", lastErr, b.Err())
}

func (r *RetryingBucketClient) Upload(ctx context.Context, name string, reader io.Reader) error {
	if r.requestDurationLimit <= 0 {
		return r.Bucket.Upload(ctx, name, reader)
	}

	var lastErr error
	b := backoff.New(ctx, r.retryPolicy)

	for b.Ongoing() {
		err := func() error {
			rctx, cancel := context.WithTimeout(ctx, r.requestDurationLimit)
			defer cancel()
			return r.Bucket.Upload(rctx, name, reader)
		}()
		if err == nil || !shouldRetry(err) {
			return err
		}
		lastErr = err
		b.Wait()
	}

	return fmt.Errorf("upload failed with retries: %w (%w)", lastErr, b.Err())
}

///////////////////////////////////////////////////
////// test support code that I will move.
///////////////////////////////////////////////////

// MockBucketWithTimeouts enables mocking of initial timeouts per {operation,
// object} pair that would require retries to eventually succeed.
type MockBucketWithTimeouts struct {
	objstore.Bucket

	InitialTimeouts int

	mu      sync.Mutex
	Calls   map[objStoreCall]int
	Success map[objStoreCall]struct{}
}

type objStoreCall struct {
	op     string
	object string
}

func NewMockBucketWithTimeouts(b objstore.Bucket, timeouts int) *MockBucketWithTimeouts {
	return &MockBucketWithTimeouts{
		Bucket:          b,
		InitialTimeouts: timeouts,
		Calls:           make(map[objStoreCall]int),
		Success:         make(map[objStoreCall]struct{}),
	}
}

func (m *MockBucketWithTimeouts) err(op, obj string) error {
	c := objStoreCall{op, obj}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.Calls[c]++
	if m.Calls[c] <= m.InitialTimeouts {
		return context.DeadlineExceeded
	}
	m.Success[c] = struct{}{}
	return nil
}

func (m *MockBucketWithTimeouts) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := m.err("get", name); err != nil {
		return nil, err
	}
	return m.Bucket.Get(ctx, name)
}

func (m *MockBucketWithTimeouts) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if err := m.err("get_range", name); err != nil {
		return nil, err
	}
	return m.Bucket.GetRange(ctx, name, off, length)
}
