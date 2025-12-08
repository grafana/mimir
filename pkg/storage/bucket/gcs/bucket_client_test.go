// SPDX-License-Identifier: AGPL-3.0-only

package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/googleapi"
)

// mockBucket is a simple mock implementation of objstore.Bucket for testing.
type mockBucket struct {
	mu                      sync.Mutex
	uploadCount             int
	getCount                int
	getRangeCount           int
	existsCount             int
	attributesCount         int
	iterCount               int
	iterWithAttributesCount int
	deleteCount             int
	uploadFunc              func(ctx context.Context, name string, r io.Reader) error
	getFunc                 func(ctx context.Context, name string) (io.ReadCloser, error)
	getRangeFunc            func(ctx context.Context, name string, off, length int64) (io.ReadCloser, error)
}

func (m *mockBucket) Upload(ctx context.Context, name string, r io.Reader, _ ...objstore.ObjectUploadOption) error {
	m.mu.Lock()
	m.uploadCount++
	m.mu.Unlock()
	if m.uploadFunc != nil {
		return m.uploadFunc(ctx, name, r)
	}
	// Read the data to simulate actual upload.
	_, err := io.ReadAll(r)
	return err
}

func (m *mockBucket) Close() error                   { return nil }
func (m *mockBucket) Name() string                   { return "mock" }
func (m *mockBucket) Provider() objstore.ObjProvider { return "MOCK" }
func (m *mockBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	m.iterCount++
	return nil
}
func (m *mockBucket) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	m.iterWithAttributesCount++
	return nil
}
func (m *mockBucket) SupportedIterOptions() []objstore.IterOptionType { return nil }
func (m *mockBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	m.getCount++
	if m.getFunc != nil {
		return m.getFunc(ctx, name)
	}
	return io.NopCloser(strings.NewReader("test data")), nil
}
func (m *mockBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	m.getRangeCount++
	if m.getRangeFunc != nil {
		return m.getRangeFunc(ctx, name, off, length)
	}
	return io.NopCloser(strings.NewReader("test data")), nil
}
func (m *mockBucket) Exists(ctx context.Context, name string) (bool, error) {
	m.existsCount++
	return true, nil
}
func (m *mockBucket) IsObjNotFoundErr(err error) bool  { return false }
func (m *mockBucket) IsAccessDeniedErr(err error) bool { return false }
func (m *mockBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	m.attributesCount++
	return objstore.ObjectAttributes{Size: 100}, nil
}
func (m *mockBucket) Delete(ctx context.Context, name string) error {
	m.deleteCount++
	return nil
}

func TestRateLimitedBucket(t *testing.T) {
	t.Run("Upload", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:        mock,
				uploadLimiter: newRateLimiter("test", 10, 10, 20*time.Minute, uploadRateLimiter, reg),
			}

			ctx := context.Background()
			data := bytes.NewBufferString("test data")

			// First upload should succeed
			err := bucket.Upload(ctx, "test-object", data, nil)
			require.NoError(t, err)
			assert.Equal(t, 1, mock.uploadCount)
		})

		t.Run("waits when rate limit exceeded", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:        mock,
				uploadLimiter: newRateLimiter("test", 5, 5, 20*time.Minute, uploadRateLimiter, reg),
			}

			ctx := context.Background()

			// Consume all initial burst tokens (burst = 2 * maxQPS = 10).
			for range 10 {
				data := bytes.NewBufferString("test data")
				err := bucket.Upload(ctx, "test-object", data, nil)
				require.NoError(t, err)
			}

			// Next upload should be rate limited.
			start := time.Now()
			data := bytes.NewBufferString("test data")
			err := bucket.Upload(ctx, "test-object", data, nil)
			require.NoError(t, err)
			elapsed := time.Since(start)

			// Should have waited for rate limiter (at 5 QPS, ~200ms per token).
			assert.Greater(t, elapsed, 100*time.Millisecond)
		})

		t.Run("returns error when context cancelled", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:        mock,
				uploadLimiter: newRateLimiter("test", 1, 1, 20*time.Minute, uploadRateLimiter, reg),
			}

			// Consume all burst tokens (burst = 2 * maxQPS = 2).
			for range 2 {
				err := bucket.Upload(context.Background(), "test", bytes.NewBufferString("data"), nil)
				require.NoError(t, err)
			}

			// Create cancelled context
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// Upload should fail with context error
			data := bytes.NewBufferString("test data")
			err := bucket.Upload(ctx, "test-object", data, nil)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "context canceled")
		})

		t.Run("is safe for concurrent use", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:        mock,
				uploadLimiter: newRateLimiter("test", 20, 20, 20*time.Minute, uploadRateLimiter, reg),
			}

			ctx := context.Background()
			numUploads := 50
			done := make(chan error, numUploads)

			start := time.Now()

			// Launch concurrent uploads
			for i := 0; i < numUploads; i++ {
				go func(i int) {
					data := bytes.NewBufferString("test data")
					err := bucket.Upload(ctx, "test-object", data, nil)
					done <- err
				}(i)
			}

			// Wait for all uploads
			for i := 0; i < numUploads; i++ {
				err := <-done
				require.NoError(t, err)
			}

			elapsed := time.Since(start)

			// All uploads should complete
			assert.Equal(t, numUploads, mock.uploadCount)

			// Should take some time due to rate limiting (50 uploads at 20 QPS with burst = ~2.5 seconds)
			assert.Less(t, elapsed, 5*time.Second)
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 10, 10, 20*time.Minute, readRateLimiter, reg),
			}

			ctx := context.Background()

			// First get should succeed
			reader, err := bucket.Get(ctx, "test-object")
			require.NoError(t, err)
			require.NotNil(t, reader)
			_ = reader.Close()
			assert.Equal(t, 1, mock.getCount)
		})

		t.Run("waits when rate limit exceeded", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 5, 5, 20*time.Minute, readRateLimiter, reg),
			}

			ctx := context.Background()

			// Consume all initial burst tokens (burst = 2 * maxQPS = 10).
			for range 10 {
				reader, err := bucket.Get(ctx, "test-object")
				require.NoError(t, err)
				require.NoError(t, reader.Close())
			}

			// Next get should be rate limited.
			start := time.Now()
			reader, err := bucket.Get(ctx, "test-object")
			require.NoError(t, err)
			_ = reader.Close()
			elapsed := time.Since(start)

			// Should have waited for rate limiter (at 5 QPS, ~200ms per token).
			assert.Greater(t, elapsed, 100*time.Millisecond)
		})

		t.Run("returns error when context cancelled", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 1, 1, 20*time.Minute, readRateLimiter, reg),
			}

			// Consume all burst tokens (burst = 2 * maxQPS = 2).
			for range 2 {
				reader, err := bucket.Get(context.Background(), "test")
				require.NoError(t, err)
				if reader != nil {
					require.NoError(t, reader.Close())
				}
			}

			// Create cancelled context
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// Get should fail with context error
			_, err := bucket.Get(ctx, "test-object")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "context canceled")
		})
	})

	t.Run("GetRange", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 10, 10, 20*time.Minute, readRateLimiter, reg),
			}

			ctx := context.Background()

			// First get range should succeed
			reader, err := bucket.GetRange(ctx, "test-object", 0, 100)
			require.NoError(t, err)
			require.NotNil(t, reader)
			_ = reader.Close()
			assert.Equal(t, 1, mock.getRangeCount)
		})

		t.Run("waits when rate limit exceeded", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 5, 5, 20*time.Minute, readRateLimiter, reg),
			}

			ctx := context.Background()

			// Consume all initial burst tokens (burst = 2 * maxQPS = 10).
			for range 10 {
				reader, err := bucket.GetRange(ctx, "test-object", 0, 100)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
			}

			// Next get range should be rate limited.
			start := time.Now()
			reader, err := bucket.GetRange(ctx, "test-object", 0, 100)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			elapsed := time.Since(start)

			// Should have waited for rate limiter (at 5 QPS, ~200ms per token).
			assert.Greater(t, elapsed, 100*time.Millisecond)
		})
	})

	t.Run("Exists", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 10, 10, 20*time.Minute, readRateLimiter, reg),
			}

			ctx := context.Background()

			exists, err := bucket.Exists(ctx, "test-object")
			require.NoError(t, err)
			assert.True(t, exists)
			assert.Equal(t, 1, mock.existsCount)
		})

		t.Run("returns error when context cancelled", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 1, 1, 20*time.Minute, readRateLimiter, reg),
			}

			// Consume all burst tokens (burst = 2 * maxQPS = 2).
			for range 2 {
				_, err := bucket.Exists(context.Background(), "test")
				require.NoError(t, err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, err := bucket.Exists(ctx, "test-object")
			assert.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("Attributes", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 10, 10, 20*time.Minute, readRateLimiter, reg),
			}

			ctx := context.Background()

			attrs, err := bucket.Attributes(ctx, "test-object")
			require.NoError(t, err)
			assert.Equal(t, int64(100), attrs.Size)
			assert.Equal(t, 1, mock.attributesCount)
		})

		t.Run("returns error when context cancelled", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}

			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 1, 1, 20*time.Minute, readRateLimiter, reg),
			}

			// Consume all burst tokens (burst = 2 * maxQPS = 2).
			for range 2 {
				_, err := bucket.Attributes(context.Background(), "test")
				require.NoError(t, err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, err := bucket.Attributes(ctx, "test-object")
			assert.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("Iter", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}
			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 10, 10, 20*time.Minute, readRateLimiter, reg),
			}
			ctx := context.Background()

			err := bucket.Iter(ctx, "dir/", func(name string) error { return nil })
			require.NoError(t, err)
			assert.Equal(t, 1, mock.iterCount)
		})

		t.Run("returns error when context cancelled", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}
			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 1, 1, 20*time.Minute, readRateLimiter, reg),
			}

			// Consume all burst tokens (burst = 2 * maxQPS = 2).
			for range 2 {
				err := bucket.Iter(context.Background(), "dir/", func(name string) error { return nil })
				require.NoError(t, err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := bucket.Iter(ctx, "dir/", func(name string) error { return nil })
			assert.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("IterWithAttributes", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}
			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 10, 10, 20*time.Minute, readRateLimiter, reg),
			}
			ctx := context.Background()

			err := bucket.IterWithAttributes(ctx, "dir/", func(attrs objstore.IterObjectAttributes) error { return nil })
			require.NoError(t, err)
			assert.Equal(t, 1, mock.iterWithAttributesCount)
		})

		t.Run("returns error when context cancelled", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}
			bucket := &rateLimitedBucket{
				Bucket:      mock,
				readLimiter: newRateLimiter("test", 1, 1, 20*time.Minute, readRateLimiter, reg),
			}

			// Consume all burst tokens (burst = 2 * maxQPS = 2).
			for range 2 {
				err := bucket.IterWithAttributes(context.Background(), "dir/", func(attrs objstore.IterObjectAttributes) error { return nil })
				require.NoError(t, err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := bucket.IterWithAttributes(ctx, "dir/", func(attrs objstore.IterObjectAttributes) error { return nil })
			assert.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}
			bucket := &rateLimitedBucket{
				Bucket:        mock,
				uploadLimiter: newRateLimiter("test", 10, 10, 20*time.Minute, uploadRateLimiter, reg),
			}
			ctx := context.Background()

			err := bucket.Delete(ctx, "test-object")
			require.NoError(t, err)
			assert.Equal(t, 1, mock.deleteCount)
		})

		t.Run("returns error when context cancelled", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mock := &mockBucket{}
			bucket := &rateLimitedBucket{
				Bucket:        mock,
				uploadLimiter: newRateLimiter("test", 1, 1, 20*time.Minute, uploadRateLimiter, reg),
			}

			// Consume all burst tokens (burst = 2 * maxQPS = 2).
			for range 2 {
				err := bucket.Delete(context.Background(), "test")
				require.NoError(t, err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := bucket.Delete(ctx, "test-object")
			assert.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("all operations succeed with both limiters enabled", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		mock := &mockBucket{}

		bucket := &rateLimitedBucket{
			Bucket:        mock,
			uploadLimiter: newRateLimiter("test", 10, 10, 20*time.Minute, uploadRateLimiter, reg),
			readLimiter:   newRateLimiter("test", 20, 20, 20*time.Minute, readRateLimiter, reg),
		}

		ctx := context.Background()

		// Upload should use upload limiter
		err := bucket.Upload(ctx, "test-object", bytes.NewBufferString("data"), nil)
		require.NoError(t, err)
		assert.Equal(t, 1, mock.uploadCount)

		// Get should use read limiter
		reader, err := bucket.Get(ctx, "test-object")
		require.NoError(t, err)
		_ = reader.Close()
		assert.Equal(t, 1, mock.getCount)

		// GetRange should use read limiter
		reader, err = bucket.GetRange(ctx, "test-object", 0, 100)
		require.NoError(t, err)
		_ = reader.Close()
		assert.Equal(t, 1, mock.getRangeCount)

		exists, err := bucket.Exists(ctx, "test-object")
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, 1, mock.existsCount)

		attrs, err := bucket.Attributes(ctx, "test-object")
		require.NoError(t, err)
		assert.Equal(t, int64(100), attrs.Size)
		assert.Equal(t, 1, mock.attributesCount)
	})

	t.Run("reads bypass rate limiting when only upload limiter enabled", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		mock := &mockBucket{}

		bucket := &rateLimitedBucket{
			Bucket:        mock,
			uploadLimiter: newRateLimiter("test", 10, 10, 20*time.Minute, uploadRateLimiter, reg),
			// readLimiter is nil
		}

		ctx := context.Background()

		// Get should work without rate limiting
		reader, err := bucket.Get(ctx, "test-object")
		require.NoError(t, err)
		_ = reader.Close()
		assert.Equal(t, 1, mock.getCount)
	})

	t.Run("uploads bypass rate limiting when only read limiter enabled", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		mock := &mockBucket{}

		bucket := &rateLimitedBucket{
			Bucket: mock,
			// uploadLimiter is nil
			readLimiter: newRateLimiter("test", 10, 10, 20*time.Minute, readRateLimiter, reg),
		}

		ctx := context.Background()

		// Upload should work without rate limiting
		err := bucket.Upload(ctx, "test-object", bytes.NewBufferString("data"), nil)
		require.NoError(t, err)
		assert.Equal(t, 1, mock.uploadCount)
	})
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Config
		expectedErr error
	}{
		{
			name: "valid config with upload rate limiting enabled",
			cfg: Config{
				UploadRateLimitEnabled: true,
				UploadInitialQPS:       500,
				UploadMaxQPS:           1000,
				UploadRampPeriod:       20 * time.Minute,
			},
		},
		{
			name: "valid config with read rate limiting enabled",
			cfg: Config{
				ReadRateLimitEnabled: true,
				ReadInitialQPS:       500,
				ReadMaxQPS:           1000,
				ReadRampPeriod:       20 * time.Minute,
			},
		},
		{
			name: "valid config with both types of rate limiting enabled",
			cfg: Config{
				UploadRateLimitEnabled: true,
				UploadInitialQPS:       500,
				UploadMaxQPS:           1000,
				UploadRampPeriod:       20 * time.Minute,
				ReadRateLimitEnabled:   true,
				ReadInitialQPS:         1000,
				ReadMaxQPS:             2000,
				ReadRampPeriod:         20 * time.Minute,
			},
		},
		{
			name: "valid config with rate limiting disabled",
			cfg: Config{
				UploadRateLimitEnabled: false,
				UploadInitialQPS:       0, // Invalid value is OK when disabled.
				UploadMaxQPS:           0, // Invalid value is OK when disabled.
				UploadRampPeriod:       0, // Invalid value is OK when disabled.
				ReadRateLimitEnabled:   false,
				ReadInitialQPS:         0,
				ReadMaxQPS:             0,
				ReadRampPeriod:         0,
			},
		},
		{
			name: "invalid config: upload rate limiting enabled with zero initial QPS",
			cfg: Config{
				UploadRateLimitEnabled: true,
				UploadInitialQPS:       0,
				UploadMaxQPS:           1000,
				UploadRampPeriod:       20 * time.Minute,
			},
			expectedErr: errInvalidUploadInitialQPS,
		},
		{
			name: "invalid config: upload rate limiting enabled with negative initial QPS",
			cfg: Config{
				UploadRateLimitEnabled: true,
				UploadInitialQPS:       -1,
				UploadMaxQPS:           1000,
				UploadRampPeriod:       20 * time.Minute,
			},
			expectedErr: errInvalidUploadInitialQPS,
		},
		{
			name: "invalid config: upload rate limiting enabled with zero max QPS",
			cfg: Config{
				UploadRateLimitEnabled: true,
				UploadInitialQPS:       500,
				UploadMaxQPS:           0,
				UploadRampPeriod:       20 * time.Minute,
			},
			expectedErr: errInvalidUploadMaxQPS,
		},
		{
			name: "invalid config: upload rate limiting enabled with negative max QPS",
			cfg: Config{
				UploadRateLimitEnabled: true,
				UploadInitialQPS:       500,
				UploadMaxQPS:           -1,
				UploadRampPeriod:       20 * time.Minute,
			},
			expectedErr: errInvalidUploadMaxQPS,
		},
		{
			name: "invalid config: upload rate limiting enabled with zero ramp period",
			cfg: Config{
				UploadRateLimitEnabled: true,
				UploadInitialQPS:       500,
				UploadMaxQPS:           1000,
				UploadRampPeriod:       0,
			},
			expectedErr: errInvalidUploadRampPeriod,
		},
		{
			name: "invalid config: upload rate limiting enabled with negative ramp period",
			cfg: Config{
				UploadRateLimitEnabled: true,
				UploadInitialQPS:       500,
				UploadMaxQPS:           1000,
				UploadRampPeriod:       -1 * time.Minute,
			},
			expectedErr: errInvalidUploadRampPeriod,
		},
		{
			name: "invalid config: read rate limiting enabled with zero initial QPS",
			cfg: Config{
				ReadRateLimitEnabled: true,
				ReadInitialQPS:       0,
				ReadMaxQPS:           1000,
				ReadRampPeriod:       20 * time.Minute,
			},
			expectedErr: errInvalidReadInitialQPS,
		},
		{
			name: "invalid config: read rate limiting enabled with negative initial QPS",
			cfg: Config{
				ReadRateLimitEnabled: true,
				ReadInitialQPS:       -1,
				ReadMaxQPS:           1000,
				ReadRampPeriod:       20 * time.Minute,
			},
			expectedErr: errInvalidReadInitialQPS,
		},
		{
			name: "invalid config: read rate limiting enabled with zero max QPS",
			cfg: Config{
				ReadRateLimitEnabled: true,
				ReadInitialQPS:       500,
				ReadMaxQPS:           0,
				ReadRampPeriod:       20 * time.Minute,
			},
			expectedErr: errInvalidReadMaxQPS,
		},
		{
			name: "invalid config: read rate limiting enabled with negative max QPS",
			cfg: Config{
				ReadRateLimitEnabled: true,
				ReadInitialQPS:       500,
				ReadMaxQPS:           -1,
				ReadRampPeriod:       20 * time.Minute,
			},
			expectedErr: errInvalidReadMaxQPS,
		},
		{
			name: "invalid config: read rate limiting enabled with zero ramp period",
			cfg: Config{
				ReadRateLimitEnabled: true,
				ReadInitialQPS:       500,
				ReadMaxQPS:           1000,
				ReadRampPeriod:       0,
			},
			expectedErr: errInvalidReadRampPeriod,
		},
		{
			name: "invalid config: read rate limiting enabled with negative ramp period",
			cfg: Config{
				ReadRateLimitEnabled: true,
				ReadInitialQPS:       500,
				ReadMaxQPS:           1000,
				ReadRampPeriod:       -1 * time.Minute,
			},
			expectedErr: errInvalidReadRampPeriod,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.expectedErr != nil {
				assert.ErrorIs(t, err, tc.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIsRateLimitError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "generic error",
			err:      errors.New("some error"),
			expected: false,
		},
		{
			name:     "googleapi 429 error",
			err:      &googleapi.Error{Code: 429},
			expected: true,
		},
		{
			name:     "googleapi 500 error",
			err:      &googleapi.Error{Code: 500},
			expected: false,
		},
		{
			name:     "googleapi 404 error",
			err:      &googleapi.Error{Code: 404},
			expected: false,
		},
		{
			name:     "wrapped googleapi 429 error",
			err:      fmt.Errorf("operation failed: %w", &googleapi.Error{Code: 429}),
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isRateLimitError(tc.err)
			assert.Equal(t, tc.expected, result)
		})
	}
}
