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
		rctx, _ := context.WithTimeout(ctx, r.requestDurationLimit) //nolint:lostcancel
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
		rctx, _ := context.WithTimeout(ctx, r.requestDurationLimit) //nolint:lostcancel
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

func (r *RetryingBucketClient) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if r.requestDurationLimit <= 0 {
		return r.Bucket.Iter(ctx, dir, f, options...)
	}

	var lastErr error
	b := backoff.New(ctx, r.retryPolicy)

	for b.Ongoing() {
		err := func() error {
			rctx, cancel := context.WithTimeout(ctx, r.requestDurationLimit)
			defer cancel()
			return r.Bucket.Iter(rctx, dir, f, options...)
		}()
		if err == nil || !shouldRetry(err) {
			return err
		}
		lastErr = err
		b.Wait()
	}

	return fmt.Errorf("iter failed with retries: %w (%w)", lastErr, b.Err())
}

func (r *RetryingBucketClient) Exists(ctx context.Context, name string) (bool, error) {
	if r.requestDurationLimit <= 0 {
		return r.Bucket.Exists(ctx, name)
	}

	var lastErr error
	b := backoff.New(ctx, r.retryPolicy)

	for b.Ongoing() {
		exists, err := func() (bool, error) {
			rctx, cancel := context.WithTimeout(ctx, r.requestDurationLimit)
			defer cancel()
			return r.Bucket.Exists(rctx, name)
		}()
		if err == nil || !shouldRetry(err) {
			return exists, err
		}
		lastErr = err
		b.Wait()
	}

	return false, fmt.Errorf("exists failed with retries: %w (%w)", lastErr, b.Err())
}

func (r *RetryingBucketClient) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if r.requestDurationLimit <= 0 {
		return r.Bucket.Attributes(ctx, name)
	}

	var lastErr error
	b := backoff.New(ctx, r.retryPolicy)

	for b.Ongoing() {
		attr, err := func() (objstore.ObjectAttributes, error) {
			rctx, cancel := context.WithTimeout(ctx, r.requestDurationLimit)
			defer cancel()
			return r.Bucket.Attributes(rctx, name)
		}()
		if err == nil || !shouldRetry(err) {
			return attr, err
		}
		lastErr = err
		b.Wait()
	}

	return objstore.ObjectAttributes{}, fmt.Errorf("attributes failed with retries: %w (%w)", lastErr, b.Err())
}

// MockBucketClientWithTimeouts enables mocking of initial timeouts per
// {operation, object} pair that would require retries to eventually succeed.
// TODO(seizethedave): move this to testing.go with the other things in this
// folder that are only for testing.
type MockBucketClientWithTimeouts struct {
	objstore.Bucket

	initialTimeouts int

	mu       sync.Mutex
	callInfo map[string]*MockBucketStats
}

type MockBucketStats struct {
	Calls   int
	Success bool
}

func NewMockBucketClientWithTimeouts(b objstore.Bucket, timeouts int) *MockBucketClientWithTimeouts {
	return &MockBucketClientWithTimeouts{
		Bucket:          b,
		initialTimeouts: timeouts,
		callInfo:        make(map[string]*MockBucketStats),
	}
}

func opString(op, obj string) string {
	return fmt.Sprintf("%s/%s", op, obj)
}

func (m *MockBucketClientWithTimeouts) err(op, obj string) error {
	c := opString(op, obj)
	m.mu.Lock()
	defer m.mu.Unlock()

	ci, ok := m.callInfo[c]
	if !ok {
		ci = &MockBucketStats{0, false}
		m.callInfo[c] = ci
	}
	ci.Calls++
	if ci.Calls <= m.initialTimeouts {
		return context.DeadlineExceeded
	}
	ci.Success = true
	return nil
}

func (m *MockBucketClientWithTimeouts) AllStats() []*MockBucketStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := make([]*MockBucketStats, 0, len(m.callInfo))
	for _, stat := range m.callInfo {
		s = append(s, stat)
	}
	return s
}

func (m *MockBucketClientWithTimeouts) GetStats(name string) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.callInfo[opString("get", name)]; ok {
		return c.Calls, c.Success
	}
	return 0, false
}

func (m *MockBucketClientWithTimeouts) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := m.err("get", name); err != nil {
		return nil, err
	}
	return m.Bucket.Get(ctx, name)
}

func (m *MockBucketClientWithTimeouts) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if err := m.err("get_range", name); err != nil {
		return nil, err
	}
	return m.Bucket.GetRange(ctx, name, off, length)
}
