package streams

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

func TestWriter_WriteSync_ShouldWaitUntilDataIsCommittedToObjectStorage(t *testing.T) {
	var (
		ctx          = context.Background()
		bufferPeriod = time.Second
	)

	timeseries := []mimirpb.PreallocTimeseries{
		{
			TimeSeries: &mimirpb.TimeSeries{
				Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: "series_1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 1, Value: 2}},
			},
		},
	}

	client, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)

	// Run the writer.
	w := NewWriter(bufferPeriod, objstore.WrapWithMetrics(client, nil, "test"))
	require.NoError(t, services.StartAndAwaitRunning(ctx, w))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, w))
	})

	ref, err := w.WriteSync(ctx, 123, "user-1", timeseries, nil, mimirpb.API)
	require.NoError(t, err)

	// Assert on the committed chunk.
	reader, err := client.Get(ctx, ref.StorageKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reader.Close())
	})

	data, err := io.ReadAll(reader)
	require.NoError(t, err)

	toc, err := NewChunkBufferTOCFromBytes(data)
	require.NoError(t, err)

	assert.Equal(t, uint32(1), toc.partitionsLength)
	assert.Equal(t, uint32(123), toc.partitions[0].partitionID)
}

func TestWriter_WriteSync_ShouldReturnErrorOnFailureUploadingToObjectStorage(t *testing.T) {
	var (
		ctx     = context.Background()
		errMock = errors.New("failed upload")
	)

	client := &bucket.ClientMock{}
	client.On("Upload", mock.Anything, mock.Anything, mock.Anything).Return(errMock)

	// Run the writer.
	w := NewWriter(100*time.Millisecond, objstore.WrapWithMetrics(client, nil, "test"))
	require.NoError(t, services.StartAndAwaitRunning(ctx, w))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, w))
	})

	ref, err := w.WriteSync(ctx, 123, "user-1", nil, nil, mimirpb.API)
	require.Error(t, err)
	require.Zero(t, ref)
	assert.Equal(t, errMock, err)
}
