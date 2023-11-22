package streams

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Writer is responsible to buffer and write incoming data (i.e. timeseries and metadata)
// to the object storage.
type Writer struct {
	services.Service

	currChunkMx sync.Mutex
	currChunk   *ChunkBuffer

	uploader *ChunkUploader
}

func NewWriter(bufferPeriod time.Duration, client objstore.InstrumentedBucket) *Writer {
	w := &Writer{
		currChunk: NewChunkBuffer(),
		uploader:  NewChunkUploader(client),
	}

	// TODO stopping function to wait until all chunks have been uploaded
	w.Service = services.NewTimerService(bufferPeriod, nil, w.onBufferPeriod, nil)

	return w
}

// WriteSync the input data to the object storage. The function blocks until the data has been successfully committed
// to the partition, or an error occurs.
func (w *Writer) WriteSync(ctx context.Context, partitionID uint32, userID string, timeseries []mimirpb.PreallocTimeseries, metadata []*mimirpb.MetricMetadata, source mimirpb.WriteRequest_SourceEnum) (CommitRef, error) {
	var lastErr error

	for try := 0; try < 3; try++ {
		// Get the current chunk.
		w.currChunkMx.Lock()
		chunk := w.currChunk
		w.currChunkMx.Unlock()

		// Try to append to the current chunk.
		lastErr = chunk.Append(ctx, partitionID, userID, timeseries, metadata, source)
		if errors.Is(lastErr, ErrChunkClosed) {
			// The chunk has been closed in the meanwhile. We need to append to the new one.
			continue
		} else if lastErr != nil {
			return CommitRef{}, lastErr
		}

		// Wait until the chunk is committed to the object storage.
		return chunk.WaitCommit(ctx)
	}

	return CommitRef{}, lastErr
}

func (w *Writer) onBufferPeriod(ctx context.Context) error {
	// Cut a new chunk.
	newChunk := NewChunkBuffer()

	w.currChunkMx.Lock()
	oldChunk := w.currChunk
	w.currChunk = newChunk
	w.currChunkMx.Unlock()

	// Close the old chunk to ensure no more data will be appended.
	oldChunk.Close()

	// Add the chunk to the upload queue.
	w.uploader.UploadAsync(oldChunk, func(ref CommitRef, err error) {
		oldChunk.NotifyCommit(ref, err)
	})

	return nil
}

// CommitRef contains a reference to the commit containing the written data.
type CommitRef struct {
	// StorageKey holds the key (prefix + name) of the object containing the written data.
	StorageKey string
}
