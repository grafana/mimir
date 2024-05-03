// SPDX-License-Identifier: AGPL-3.0-only

package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/thanos-io/objstore"
)

// retryingBucket is a bucket wrapper that knows how to add retries on top of
// (most of) the Bucket operations.  The Thanos Bucket providers each have some
// level of retries, but they can be inconsistent. And they don't handle things
// like the request context timing out due to a hung TCP connection.
type retryingBucket struct {
	objstore.Bucket

	requestDurationLimit time.Duration
	retryPolicy          backoff.Config
}

var _ objstore.Bucket = (*retryingBucket)(nil)

func NewRetryingBucket(wrappedBucket objstore.Bucket) *retryingBucket {

	return &retryingBucket{
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
// single object storage request can take. This allows retryingBucket to
// perform a number of retries on context.DeadlineExceeded errors within the
// overall deadline defined on the context passed to the individual
// objstore.Bucket methods.
// Example:
//
//	b := NewRetryingBucket(bucket).WithRequestDurationLimit(1*time.Minute)
//	ctx, cancel := context.WithTimeout(5*time.Minute)
//	err1 := b.WithRequestDurationLimit(10*time.Second).Get(ctx, "stuff/tinymanifest")
//	err2 := b.WithRequestDurationLimit(2*time.Minute).Upload(ctx, "stuff/bigmanifest", ...)
//	...
func (r *retryingBucket) WithRequestDurationLimit(lim time.Duration) *retryingBucket {
	return &retryingBucket{
		Bucket:               r.Bucket,
		requestDurationLimit: lim,
		retryPolicy:          r.retryPolicy,
	}
}

func (r *retryingBucket) WithRetries(bc backoff.Config) *retryingBucket {
	return &retryingBucket{
		Bucket:               r.Bucket,
		requestDurationLimit: r.requestDurationLimit,
		retryPolicy:          bc,
	}
}

func (r *retryingBucket) requestContextWithBackoff(ctx context.Context) (context.Context, context.CancelFunc, *backoff.Backoff) {
	rctx := ctx
	cancelFunc := func() {}
	if r.requestDurationLimit > 0 {
		rctx, cancelFunc = context.WithTimeout(rctx, r.requestDurationLimit)
	}
	return rctx, cancelFunc, backoff.New(ctx, r.retryPolicy)
}

func (r *retryingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	rctx, cancel, b := r.requestContextWithBackoff(ctx)
	defer cancel()
	var lastErr error

	for b.Ongoing() {
		if r, err := r.Bucket.Get(rctx, name); err == nil {
			return r, err
		} else if !shouldRetry(err) {
			return nil, err
		} else {
			lastErr = err
		}
		b.Wait()
	}

	return nil, fmt.Errorf("get failed with retries: %w (%w)", lastErr, b.Err())
}

func (r *retryingBucket) GetRange(ctx context.Context, name string, off int64, length int64) (io.ReadCloser, error) {
	rctx, cancel, b := r.requestContextWithBackoff(ctx)
	defer cancel()
	var lastErr error

	for b.Ongoing() {
		if r, err := r.Bucket.GetRange(rctx, name, off, length); err == nil {
			return r, err
		} else if !shouldRetry(err) {
			return nil, err
		} else {
			lastErr = err
		}
		b.Wait()
	}

	return nil, fmt.Errorf("get range failed with retries: %w (%w)", lastErr, b.Err())
}

func (r *retryingBucket) Upload(ctx context.Context, name string, reader io.Reader) error {
	rctx, cancel, b := r.requestContextWithBackoff(ctx)
	defer cancel()
	var lastErr error

	for b.Ongoing() {
		if err := r.Bucket.Upload(rctx, name, reader); err == nil {
			return err
		} else if !shouldRetry(err) {
			return err
		} else {
			lastErr = err
		}
		b.Wait()
	}

	return fmt.Errorf("upload failed with retries: %w (%w)", lastErr, b.Err())
}
