// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"errors"
	"io"
	"math"
	"time"

	"github.com/thanos-io/objstore"
)

var errLowThroughput = errors.New("throughput is lower than expected minimum")

type bucketRangeReader interface {
	// GetRange returns a new range reader for the given object name and range.
	GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error)
	// Attributes returns information about the specified object.
	Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error)
}

type minThroughputBucketRangeReader struct {
	bucketRangeReader
	// Minimum expected throughput in bytes/s.
	throughput int
}

func bucketRangeReaderWithEnforcedThroughput(bkt bucketRangeReader, throughput int) bucketRangeReader {
	return &minThroughputBucketRangeReader{
		bucketRangeReader: bkt,
		throughput:        throughput,
	}
}

func (m *minThroughputBucketRangeReader) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, m.expectedTime(length), errLowThroughput)
	defer cancel()
	return m.bucketRangeReader.GetRange(ctx, name, off, length)
}

func (m *minThroughputBucketRangeReader) expectedTime(n int64) time.Duration {
	if m.throughput == 0 {
		return time.Duration(math.MaxInt)
	}
	return time.Duration(n) * time.Second / time.Duration(m.throughput)
}
