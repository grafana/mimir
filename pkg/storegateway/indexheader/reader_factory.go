// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/mimir/pkg/storegateway/threadpool"
)

// DefaultReaderFactory creates new BinaryReader instances directly in the calling goroutine.
var DefaultReaderFactory = ReaderFactoryFunc(NewBinaryReader)

// ReaderFactory creates new BinaryReader instances.
type ReaderFactory interface {
	NewBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, cfg BinaryReaderConfig) (*BinaryReader, error)
}

// ReaderFactoryFunc is a ReaderFactory implementation for a function.
type ReaderFactoryFunc func(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, cfg BinaryReaderConfig) (*BinaryReader, error)

func (f ReaderFactoryFunc) NewBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, cfg BinaryReaderConfig) (*BinaryReader, error) {
	return f(ctx, logger, bkt, dir, id, postingOffsetsInMemSampling, cfg)
}

// threadedReaderFactory creates new BinaryReader instances using a pool of dedicated OS threads.
//
// This exists so that we can optionally create new instances in an isolated way, avoiding page faults when
// accessing memory mapped files. This is beneficial since the go runtime is not aware when accesses to memory
// mapped files block an entire OS thread and all other goroutines running on it.
type threadedReaderFactory struct {
	pool *threadpool.ThreadPool
}

// NewThreadedReaderFactory creates a new ReaderFactory that uses a threadpool to ensure creation
// of BinaryReader instances does not block the calling OS thread.
func NewThreadedReaderFactory(pool *threadpool.ThreadPool) ReaderFactory {
	return &threadedReaderFactory{pool: pool}
}

func (f *threadedReaderFactory) NewBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, cfg BinaryReaderConfig) (*BinaryReader, error) {
	res, err := f.pool.Execute(func() (interface{}, error) {
		return NewBinaryReader(ctx, logger, bkt, dir, id, postingOffsetsInMemSampling, cfg)
	})

	if err != nil {
		return nil, err
	}

	return res.(*BinaryReader), nil
}
