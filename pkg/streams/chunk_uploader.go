package streams

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/oklog/ulid"
	"github.com/thanos-io/objstore"
)

// ChunkUploader is responsible to upload chunks to the object storage.
type ChunkUploader struct {
	client objstore.InstrumentedBucket
}

func NewChunkUploader(client objstore.InstrumentedBucket) *ChunkUploader {
	return &ChunkUploader{
		client: client,
	}
}

func (u *ChunkUploader) UploadSync(chunk *ChunkBuffer) (CommitRef, error) {
	now := ulid.Now()

	// TODO define an extensible format for the key (e.g. include distributor ID, bloom filter with partitions)
	chunkID := ulid.MustNew(now, rand.Reader)
	key := fmt.Sprintf("%d/%s", now/1000, chunkID)

	marshaller, err := chunk.Marshal()
	if err != nil {
		return CommitRef{}, err
	}

	// TODO context, timeout, retry
	err = u.client.Upload(context.Background(), key, marshaller)
	if err != nil {
		return CommitRef{}, err
	}

	return CommitRef{StorageKey: key}, nil
}

func (u *ChunkUploader) UploadAsync(chunk *ChunkBuffer, done func(CommitRef, error)) {
	// TODO this is as bad as it looks, cause goroutines are unbounded. Fix it.
	go func() {
		done(u.UploadSync(chunk))
	}()
}
