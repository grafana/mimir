// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

func TestPartitionReader(t *testing.T) {
	const (
		partitionID = 1
	)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	bucketDir := t.TempDir()
	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: bucketDir})
	require.NoError(t, err)

	metadataDB := newMetadataDatabaseMemory()
	storage := NewSegmentStorage(bucket, NewMetadataStore(metadataDB, log.NewNopLogger()))

	content := "special content"
	consumer := newTestConsumer(2)
	startReader(ctx, t, metadataDB, partitionID, consumer, withFilesystemBucket(bucketDir))

	produceSegment(ctx, t, storage, partitionID, content)
	produceSegment(ctx, t, storage, partitionID, content)

	records, err := consumer.waitRequests(2, 5*time.Second, 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{content, content}, decodeContent(records))
}

func TestReader_ConsumerError(t *testing.T) {
	const (
		partitionID = 1
	)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	invocations := atomic.NewInt64(0)
	returnErrors := atomic.NewBool(true)
	trackingConsumer := newTestConsumer(2)
	consumer := consumerFunc(func(ctx context.Context, segment *Segment) error {
		invocations.Inc()
		if !returnErrors.Load() {
			return trackingConsumer.consume(ctx, segment)
		}
		// There may be more writeRequests, but we only care that the one we failed to consume in the first place is still there.
		wr, err := segment.Data.Pieces[0].WriteRequest()
		require.NoError(t, err)
		assert.Equal(t, "1", wr.Metadata[0].Help)
		return errors.New("consumer error")
	})

	bucketDir := t.TempDir()
	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: bucketDir})
	require.NoError(t, err)

	metadataDB := newMetadataDatabaseMemory()
	storage := NewSegmentStorage(bucket, NewMetadataStore(metadataDB, log.NewNopLogger()))

	startReader(ctx, t, metadataDB, partitionID, consumer, withFilesystemBucket(bucketDir))

	produceSegment(ctx, t, storage, partitionID, "1")
	produceSegment(ctx, t, storage, partitionID, "2")

	// There are more than one invocation because the reader will retry.
	assert.Eventually(t, func() bool { return invocations.Load() > 1 }, 5*time.Second, 100*time.Millisecond)

	returnErrors.Store(false)

	records, err := trackingConsumer.waitRequests(2, time.Second, 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, decodeContent(records))
}

func TestPartitionReader_WaitReadConsistency(t *testing.T) {
	const (
		partitionID = 0
	)

	var (
		ctx = context.Background()
	)

	setup := func(t *testing.T, consumer recordConsumer) (*PartitionReader, *SegmentStorage, *prometheus.Registry) {
		reg := prometheus.NewPedanticRegistry()

		bucketDir := t.TempDir()
		bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: bucketDir})
		require.NoError(t, err)

		metadataDB := newMetadataDatabaseMemory()
		storage := NewSegmentStorage(bucket, NewMetadataStore(metadataDB, log.NewNopLogger()))

		// Configure the reader to poll the "last produced offset" frequently.
		reader := startReader(ctx, t, metadataDB, partitionID, consumer,
			withFilesystemBucket(bucketDir),
			withLastProducedOffsetPollInterval(100*time.Millisecond),
			withRegistry(reg))

		return reader, storage, reg
	}

	t.Run("should return after all produced writeRequests have been consumed", func(t *testing.T) {
		t.Parallel()

		consumedRecords := atomic.NewInt64(0)

		// We define a custom consume function which introduces a delay once the 2nd record
		// has been consumed but before the function returns. From the PartitionReader perspective,
		// the 2nd record consumption will be delayed.
		consumer := consumerFunc(func(ctx context.Context, segment *Segment) error {
			for _, piece := range segment.Data.Pieces {
				// Introduce a delay before returning from the consume function once
				// the 2nd piece has been consumed.
				if consumedRecords.Load()+1 == 2 {
					time.Sleep(time.Second)
				}

				consumedRecords.Inc()
				wr, err := piece.WriteRequest()
				require.NoError(t, err)

				content := wr.Metadata[0].Help
				assert.Equal(t, fmt.Sprintf("piece-%d", consumedRecords.Load()), content)
				t.Logf("consumed piece: %s", content)
			}

			return nil
		})

		reader, storage, reg := setup(t, consumer)

		// Produce some writeRequests.
		produceSegment(ctx, t, storage, partitionID, "piece-1")
		produceSegment(ctx, t, storage, partitionID, "piece-2")
		t.Log("produced 2 writeRequests")

		// WaitReadConsistency() should return after all writeRequests produced up until this
		// point have been consumed.
		t.Log("started waiting for read consistency")

		err := reader.WaitReadConsistency(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2), consumedRecords.Load())
		t.Log("finished waiting for read consistency")

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested.
			# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
			cortex_ingest_storage_strong_consistency_requests_total 1

			# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
			# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
			cortex_ingest_storage_strong_consistency_failures_total 0
		`), "cortex_ingest_storage_strong_consistency_requests_total", "cortex_ingest_storage_strong_consistency_failures_total"))
	})

	t.Run("should block until the context deadline exceed if produced writeRequests are not consumed", func(t *testing.T) {
		t.Parallel()

		// Create a consumer with no buffer capacity.
		consumer := newTestConsumer(0)

		reader, storage, reg := setup(t, consumer)

		// Produce some writeRequests.
		produceSegment(ctx, t, storage, partitionID, "record-1")
		t.Log("produced 1 record")

		err := reader.WaitReadConsistency(createTestContextWithTimeout(t, time.Second))
		require.ErrorIs(t, err, context.DeadlineExceeded)

		// Consume the writeRequests.
		records, err := consumer.waitRequests(1, time.Second, 0)
		assert.NoError(t, err)
		assert.Equal(t, []string{"record-1"}, decodeContent(records))

		// Now the WaitReadConsistency() should return soon.
		err = reader.WaitReadConsistency(createTestContextWithTimeout(t, time.Second))
		require.NoError(t, err)

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested.
			# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
			cortex_ingest_storage_strong_consistency_requests_total 2

			# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
			# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
			cortex_ingest_storage_strong_consistency_failures_total 1
		`), "cortex_ingest_storage_strong_consistency_requests_total", "cortex_ingest_storage_strong_consistency_failures_total"))
	})

	t.Run("should return if no writeRequests have been produced yet", func(t *testing.T) {
		t.Parallel()

		reader, _, reg := setup(t, newTestConsumer(0))

		err := reader.WaitReadConsistency(createTestContextWithTimeout(t, time.Second))
		require.NoError(t, err)

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested.
			# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
			cortex_ingest_storage_strong_consistency_requests_total 1

			# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
			# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
			cortex_ingest_storage_strong_consistency_failures_total 0
		`), "cortex_ingest_storage_strong_consistency_requests_total", "cortex_ingest_storage_strong_consistency_failures_total"))
	})

	t.Run("should return an error if the PartitionReader is not running", func(t *testing.T) {
		t.Parallel()

		reader, _, reg := setup(t, newTestConsumer(0))

		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		err := reader.WaitReadConsistency(createTestContextWithTimeout(t, time.Second))
		require.ErrorContains(t, err, "partition reader service is not running")

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested.
			# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
			cortex_ingest_storage_strong_consistency_requests_total 1

			# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
			# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
			cortex_ingest_storage_strong_consistency_failures_total 1
		`), "cortex_ingest_storage_strong_consistency_requests_total", "cortex_ingest_storage_strong_consistency_failures_total"))
	})
}

func produceSegment(ctx context.Context, t *testing.T, segmentStorage *SegmentStorage, partitionID int32, content string) {
	_, err := segmentStorage.CommitSegment(ctx, partitionID, encodeSegment(content), time.Now())
	require.NoError(t, err)
}

type readerTestCfg struct {
	config         Config
	partitionID    int32
	consumer       recordConsumer
	registry       prometheus.Registerer
	logger         log.Logger
	commitInterval time.Duration
}

type readerTestCfgOtp func(cfg *readerTestCfg)

func withFilesystemBucket(dir string) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.config.Bucket.Backend = bucket.Filesystem
		cfg.config.Bucket.Filesystem.Directory = dir
	}
}

func withCommitInterval(i time.Duration) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.commitInterval = i
	}
}

func withLastProducedOffsetPollInterval(i time.Duration) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.config.LastProducedOffsetPollInterval = i
	}
}

func withRegistry(reg prometheus.Registerer) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.registry = reg
	}
}

func defaultReaderTestConfig(partitionID int32, consumer recordConsumer) *readerTestCfg {
	return &readerTestCfg{
		registry:       prometheus.NewPedanticRegistry(),
		logger:         log.NewNopLogger(),
		config:         createTestIngestConfig(),
		partitionID:    partitionID,
		consumer:       consumer,
		commitInterval: 10 * time.Second,
	}
}

func createTestIngestConfig() Config {
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	return cfg
}

func startReader(ctx context.Context, t *testing.T, metadataDB MetadataStoreDatabase, partitionID int32, consumer recordConsumer, opts ...readerTestCfgOtp) *PartitionReader {
	cfg := defaultReaderTestConfig(partitionID, consumer)
	for _, o := range opts {
		o(cfg)
	}
	reader, err := newPartitionReader(cfg.config, metadataDB, cfg.partitionID, "test-group", cfg.consumer, cfg.logger, cfg.registry)
	require.NoError(t, err)
	reader.commitInterval = cfg.commitInterval

	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
	})

	return reader
}

func TestPartitionReader_Commit(t *testing.T) {
	const (
		partitionID = 1
	)

	t.Run("resume at committed", func(t *testing.T) {
		t.Parallel()

		const commitInterval = 100 * time.Millisecond
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(errors.New("test done")) })

		// Reuse bucket and metadataDB across shutdowns
		bucketDir := t.TempDir()
		bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: bucketDir})
		require.NoError(t, err)

		metadataDB := newMetadataDatabaseMemory()
		storage := NewSegmentStorage(bucket, NewMetadataStore(metadataDB, log.NewNopLogger()))
		consumer := newTestConsumer(3)

		// Start the reader.
		reader := startReader(ctx, t, metadataDB, partitionID, consumer,
			withFilesystemBucket(bucketDir),
			withCommitInterval(commitInterval))

		produceSegment(ctx, t, storage, partitionID, "1")
		produceSegment(ctx, t, storage, partitionID, "2")
		produceSegment(ctx, t, storage, partitionID, "3")

		_, err = consumer.waitRequests(3, time.Second, commitInterval*2) // wait for a few commits to make sure empty commits don't cause issues
		require.NoError(t, err)

		// Stop the reader.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		recordsSentAfterShutdown := "4"
		produceSegment(ctx, t, storage, partitionID, recordsSentAfterShutdown)

		// Restart the reader.
		startReader(ctx, t, metadataDB, partitionID, consumer,
			withFilesystemBucket(bucketDir),
			withCommitInterval(commitInterval))

		records, err := consumer.waitRequests(1, time.Second, 0)
		assert.NoError(t, err)
		content := decodeContent(records)
		assert.Equal(t, []string{recordsSentAfterShutdown}, content)
	})

	t.Run("commit at shutdown", func(t *testing.T) {
		t.Parallel()

		// A very long commit interval effectively means no regular commits.
		const commitInterval = time.Second * 15
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(errors.New("test done")) })

		// Reuse bucket and metadataDB across shutdowns
		bucketDir := t.TempDir()
		bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: bucketDir})
		require.NoError(t, err)

		metadataDB := newMetadataDatabaseMemory()
		storage := NewSegmentStorage(bucket, NewMetadataStore(metadataDB, log.NewNopLogger()))
		consumer := newTestConsumer(4)

		// Start the reader.
		reader := startReader(ctx, t, metadataDB, partitionID, consumer,
			withFilesystemBucket(bucketDir),
			withCommitInterval(commitInterval))

		produceSegment(ctx, t, storage, partitionID, "1")
		produceSegment(ctx, t, storage, partitionID, "2")
		produceSegment(ctx, t, storage, partitionID, "3")

		_, err = consumer.waitRequests(3, time.Second, 0)
		require.NoError(t, err)

		// Stop the reader.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		produceSegment(ctx, t, storage, partitionID, "4")

		// Restart the reader.
		startReader(ctx, t, metadataDB, partitionID, consumer,
			withFilesystemBucket(bucketDir),
			withCommitInterval(commitInterval))

		// There should be only one record - the one produced after the shutdown.
		// The offset of record "3" should have been committed at shutdown and the reader should have resumed from there.
		_, err = consumer.waitRequests(1, time.Second, time.Second)
		assert.NoError(t, err)
	})

	t.Run("commit at shutdown doesn't persist if we haven't consumed any writeRequests since startup", func(t *testing.T) {
		t.Parallel()
		// A very long commit interval effectively means no regular commits.
		const commitInterval = time.Second * 15
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(errors.New("test done")) })

		// Reuse bucket and metadataDB across shutdowns
		bucketDir := t.TempDir()
		bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: bucketDir})
		require.NoError(t, err)

		metadataDB := newMetadataDatabaseMemory()
		storage := NewSegmentStorage(bucket, NewMetadataStore(metadataDB, log.NewNopLogger()))
		consumer := newTestConsumer(4)

		// Start the reader.
		reader := startReader(ctx, t, metadataDB, partitionID, consumer,
			withFilesystemBucket(bucketDir),
			withCommitInterval(commitInterval))

		produceSegment(ctx, t, storage, partitionID, "1")
		produceSegment(ctx, t, storage, partitionID, "2")
		produceSegment(ctx, t, storage, partitionID, "3")

		_, err = consumer.waitRequests(3, time.Second, 0)
		require.NoError(t, err)

		// Stop the reader.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		// Restart the reader.
		reader = startReader(ctx, t, metadataDB, partitionID, consumer,
			withFilesystemBucket(bucketDir),
			withCommitInterval(commitInterval))

		// No new writeRequests since the last commit.
		_, err = consumer.waitRequests(0, time.Second, 0)
		assert.NoError(t, err)

		// Shut down without having consumed any writeRequests.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		_ = startReader(ctx, t, metadataDB, partitionID, consumer,
			withFilesystemBucket(bucketDir),
			withCommitInterval(commitInterval))

		// No new writeRequests since the last commit (2 shutdowns ago).
		_, err = consumer.waitRequests(0, time.Second, 0)
		assert.NoError(t, err)
	})
}

type testConsumer struct {
	writeRequests chan *mimirpb.WriteRequest
}

func newTestConsumer(capacity int) testConsumer {
	return testConsumer{
		writeRequests: make(chan *mimirpb.WriteRequest, capacity),
	}
}

func (t testConsumer) consume(ctx context.Context, segment *Segment) error {
	for _, piece := range segment.Data.Pieces {
		wr, err := piece.WriteRequest()
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case t.writeRequests <- wr:
			// Nothing to do.
		}
	}
	return nil
}

// waitRequests expects to receive numRecords writeRequests within waitTimeout.
// waitRequests waits for an additional drainPeriod after receiving numRecords writeRequests to ensure that no more writeRequests are received.
// waitRequests returns an error if a different number of writeRequests is received.
func (t testConsumer) waitRequests(numRequests int, waitTimeout, drainPeriod time.Duration) ([]*mimirpb.WriteRequest, error) {
	var writeRequests []*mimirpb.WriteRequest
	timeout := time.After(waitTimeout)
	for {
		select {
		case wr := <-t.writeRequests:
			writeRequests = append(writeRequests, wr)
			if len(writeRequests) != numRequests {
				continue
			}
			if drainPeriod == 0 {
				return writeRequests, nil
			}
			timeout = time.After(drainPeriod)
		case <-timeout:
			if len(writeRequests) != numRequests {
				return nil, fmt.Errorf("waiting for write requests: received %d, expected %d", len(writeRequests), numRequests)
			}
			return writeRequests, nil
		}
	}
}

type consumerFunc func(ctx context.Context, segment *Segment) error

func (c consumerFunc) consume(ctx context.Context, segment *Segment) error {
	return c(ctx, segment)
}

func encodeSegment(content string) *ingestpb.Segment {
	return &ingestpb.Segment{
		Pieces: []*ingestpb.Piece{
			{
				Data: marshal(&mimirpb.WriteRequest{
					Metadata: []*mimirpb.MetricMetadata{
						{
							Help: content,
						},
					},
				}),
			},
		},
	}
}

func decodeContent(writeRequests []*mimirpb.WriteRequest) []string {
	var result []string
	for _, wr := range writeRequests {
		result = append(result, wr.Metadata[0].Help)
	}
	return result
}
