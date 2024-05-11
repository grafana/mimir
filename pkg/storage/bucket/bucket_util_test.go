// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/bucket/bucket_util_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package bucket

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/backoff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestDeletePrefix(t *testing.T) {
	mem := objstore.NewInMemBucket()

	require.NoError(t, mem.Upload(context.Background(), "obj", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/1", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/2", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/sub1/3", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/sub2/4", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "outside/obj", strings.NewReader("hello")))

	del, err := DeletePrefix(context.Background(), mem, "prefix", log.NewNopLogger())
	require.NoError(t, err)
	assert.Equal(t, 4, del)
	assert.Equal(t, 2, len(mem.Objects()))
}

type tmpErr struct{}

func (e *tmpErr) Error() string   { return "invalid widget" }
func (e *tmpErr) Temporary() bool { return true }

func TestWithTimeoutsRetried(t *testing.T) {
	rc := backoff.Config{
		MinBackoff: 0,
		MaxBackoff: 0,
		MaxRetries: 3,
	}
	l := log.NewNopLogger()
	t.Run("eventually succeeds on deadline exceeded", func(t *testing.T) {
		calls := 0
		err := WithTimeoutsRetried(context.Background(), 10*time.Hour, rc, l, func(ctx context.Context) error {
			calls++
			if calls <= 2 {
				return context.DeadlineExceeded
			}
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, calls)
	})
	t.Run("eventually succeeds on temp err", func(t *testing.T) {
		calls := 0
		err := WithTimeoutsRetried(context.Background(), 64000*time.Hour, rc, l, func(ctx context.Context) error {
			calls++
			if calls <= 2 {
				return fmt.Errorf("problem: %w", &tmpErr{})
			}
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, calls)
	})
	t.Run("exhausts retries", func(t *testing.T) {
		calls := 0
		err := WithTimeoutsRetried(context.Background(), 10*time.Hour, rc, l, func(ctx context.Context) error {
			calls++
			if calls <= 900 {
				return context.DeadlineExceeded
			}
			return nil
		})
		assert.Error(t, err)
		assert.ErrorContains(t, err, "failed with retries:")
		assert.Equal(t, 3, calls)
	})
	t.Run("no retries attempted", func(t *testing.T) {
		calls := 0
		err := WithTimeoutsRetried(context.Background(), 0, rc, l, func(ctx context.Context) error {
			calls++
			if calls <= 9 {
				return context.DeadlineExceeded
			}
			return nil
		})
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, 1, calls)
	})
	t.Run("no retries needed", func(t *testing.T) {
		calls := 0
		err := WithTimeoutsRetried(context.Background(), 94000*time.Hour, rc, l, func(ctx context.Context) error {
			calls++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, calls)
	})
	t.Run("doesn't retry things that aren't timeouts", func(t *testing.T) {
		calls := 0
		err := WithTimeoutsRetried(context.Background(), 0, rc, l, func(ctx context.Context) error {
			calls++
			return io.ErrUnexpectedEOF
		})
		assert.Error(t, err)
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
		assert.Equal(t, 1, calls)
	})
}
