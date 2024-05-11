package bucket

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/thanos-io/objstore"
)

// MockBucketClientWithTimeouts enables mocking of initial timeouts per
// {operation, object} pair that would require retries to eventually succeed.
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

func (m *MockBucketClientWithTimeouts) GetStats(object string) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.callInfo[opString("get", object)]; ok {
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

func (m *MockBucketClientWithTimeouts) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if err := m.err("attributes", name); err != nil {
		return objstore.ObjectAttributes{}, err
	}
	return m.Bucket.Attributes(ctx, name)
}

func (m *MockBucketClientWithTimeouts) Upload(ctx context.Context, name string, r io.Reader) error {
	if err := m.err("upload", name); err != nil {
		return err
	}
	return m.Bucket.Upload(ctx, name, r)
}
