// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/bucket/bucket_util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package bucket

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/thanos-io/objstore"
)

// DeletePrefix removes all objects with given prefix, recursively.
// It returns number of deleted objects.
// If deletion of any object fails, it returns error and stops.
func DeletePrefix(ctx context.Context, bkt objstore.Bucket, prefix string, logger log.Logger) (int, error) {
	result := 0
	err := bkt.Iter(ctx, prefix, func(name string) error {
		if strings.HasSuffix(name, objstore.DirDelim) {
			deleted, err := DeletePrefix(ctx, bkt, name, logger)
			result += deleted
			return err
		}

		if err := bkt.Delete(ctx, name); err != nil {
			return err
		}
		result++
		level.Debug(logger).Log("msg", "deleted file", "file", name)
		return nil
	})

	return result, err
}

// WithTimeoutsRetried invokes the given function as many times as it takes according to
// the backoff config. Each invocation of f will be given perCallTimeout to
// complete. This is specifically designed to retry timeouts due to flaky
// connectivity with the objstore backend.
func WithTimeoutsRetried(ctx context.Context, perCallTimeout time.Duration, bc backoff.Config, logger log.Logger, f func(context.Context) error) error {
	if perCallTimeout <= 0 {
		return f(ctx)
	}

	var err error
	b := backoff.New(ctx, bc)

	for b.Ongoing() {
		rctx, cancel := context.WithTimeout(ctx, perCallTimeout)
		err = f(rctx)
		cancel()
		if err == nil || !shouldRetry(err) {
			return err
		}
		level.Info(logger).Log("msg", "single call failed with error", "err", err)
		b.Wait()
	}

	level.Warn(logger).Log("msg", "retries exhausted")
	return fmt.Errorf("failed with retries: %w (last err: %w)", b.Err(), err)
}

func shouldRetry(err error) bool {
	var tempErr interface{ Temporary() bool }

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return true
	case errors.As(err, &tempErr):
		return tempErr.Temporary()
	}

	return false
}
