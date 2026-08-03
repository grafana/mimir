// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

const compartmentsTestTopicFormat = "comp-<read-compartment-id>"

func TestDistributor_Push_ShouldSupportCompartments(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	const numCompartments = 3
	d, kafkaCluster, compartmentTopics := prepareCompartmentsTestDistributor(t, numCompartments)

	res, err := d.Push(ctx, &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, "metric_a"}, makeSamples(now.UnixMilli(), 1), nil, nil),
			makeTimeseries([]string{model.MetricNameLabel, "metric_b"}, makeSamples(now.UnixMilli(), 2), nil, nil),
			makeTimeseries([]string{model.MetricNameLabel, "metric_c"}, makeSamples(now.UnixMilli(), 3), nil, nil),
			makeTimeseries([]string{model.MetricNameLabel, "metric_d"}, makeSamples(now.UnixMilli(), 4), nil, nil),
		},
		Metadata: []*mimirpb.MetricMetadata{
			{MetricFamilyName: "metric_a", Type: mimirpb.COUNTER},
			{MetricFamilyName: "metric_b", Type: mimirpb.GAUGE},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, emptyResponse, res)

	router := compartmentsTestRouter(numCompartments)

	// Collect records per topic and assert each compartment's records land only on its ring's single
	// active partition.
	metricsByTopic := map[string][]string{}
	for compartmentID, topic := range compartmentTopics {
		records := readAllRecordsFromKafkaTopics(t, kafkaCluster.ListenAddrs(), []string{topic}, numCompartments, time.Second)
		for _, record := range records {
			assert.Equal(t, int32(compartmentID), record.Partition, "topic %s records must land on partition %d (its ring's only active partition)", topic, compartmentID)
		}
		metricsByTopic[topic] = metricNamesFromRecords(t, records)
	}

	// All 4 metrics are present exactly once across all compartment topics.
	var allMetrics []string
	for _, metrics := range metricsByTopic {
		allMetrics = append(allMetrics, metrics...)
	}
	assert.ElementsMatch(t, []string{"metric_a", "metric_b", "metric_c", "metric_d"}, allMetrics)

	// Each metric (and its metadata) landed in the topic the router assigns it to.
	for _, metricName := range []string{"metric_a", "metric_b", "metric_c", "metric_d"} {
		expectedTopic := router.TopicForMetric("user", metricName)
		assert.Contains(t, metricsByTopic[expectedTopic], metricName, "metric %s should be in topic %s", metricName, expectedTopic)
	}

	// No records land on the default topic when compartments are enabled.
	assert.Empty(t, readAllRecordsFromKafkaTopics(t, kafkaCluster.ListenAddrs(), []string{kafkaTopic}, numCompartments, time.Second))
}

func TestDistributor_Push_Compartments_ShouldNotWriteToEmptyCompartments(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	const numCompartments = 3
	d, kafkaCluster, _ := prepareCompartmentsTestDistributor(t, numCompartments)
	router := compartmentsTestRouter(numCompartments)

	// Track which partitions receive a Produce request. Each compartment's only active partition equals
	// its compartment ID, so the partition uniquely identifies the compartment (the produce request
	// itself carries the topic by ID, not name).
	var mu sync.Mutex
	producedPartitions := map[int32]struct{}{}
	kafkaCluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCluster.KeepControl()
		mu.Lock()
		for _, topic := range req.(*kmsg.ProduceRequest).Topics {
			for _, partition := range topic.Partitions {
				producedPartitions[partition.Partition] = struct{}{}
			}
		}
		mu.Unlock()
		return nil, nil, false
	})

	// A single series maps to exactly one read compartment; the others are empty and must not be written.
	_, err := d.Push(ctx, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
		makeTimeseries([]string{model.MetricNameLabel, "metric_a"}, makeSamples(time.Now().UnixMilli(), 1), nil, nil),
	}})
	require.NoError(t, err)

	expectedCompartment := router.CompartmentForMetric("user", "metric_a")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, map[int32]struct{}{int32(expectedCompartment): {}}, producedPartitions,
		"only the non-empty compartment should receive a Produce request")
}

func TestDistributor_Push_Compartments_SoftErrorInOneCompartmentDoesNotCancelOthers(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	const numCompartments = 3
	d, kafkaCluster, compartmentTopics := prepareCompartmentsTestDistributor(t, numCompartments)
	router := compartmentsTestRouter(numCompartments)

	// metric_a and metric_c shard to two different read compartments (asserted below as a precondition),
	// so one compartment can fail while the other succeeds.
	const failMetric, okMetric = "metric_a", "metric_c"
	failCompartment := router.CompartmentForMetric("user", failMetric)
	okCompartment := router.CompartmentForMetric("user", okMetric)
	require.NotEqual(t, failCompartment, okCompartment, "test precondition: the two metrics must shard to different compartments")

	// Fail the failing compartment's Produce with a soft (bad-data) error: MessageTooLarge maps to
	// ErrWriteRequestDataItemTooLarge, a client error. A soft error must not cancel the other
	// compartments.
	//
	// In this test, each compartment's only active partition equals its compartment ID, and the produce
	// request carries the topic by ID (not name), so we match on the partition.
	kafkaCluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCluster.KeepControl()
		for _, topic := range req.(*kmsg.ProduceRequest).Topics {
			for _, partition := range topic.Partitions {
				if partition.Partition == int32(failCompartment) {
					return testkafka.CreateProduceResponseError(req.GetVersion(), topic.Topic, partition.Partition, kerr.MessageTooLarge), nil, true
				}
			}
		}
		return nil, nil, false
	})

	// Inject a cleanup function so we can assert it's called exactly once.
	cleanupCount := atomic.NewInt64(0)
	origPush := d.PushWithMiddlewares
	d.PushWithMiddlewares = func(ctx context.Context, req *Request) error {
		req.AddCleanup(func() { cleanupCount.Inc() })
		return origPush(ctx, req)
	}

	_, err := d.Push(ctx, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
		makeTimeseries([]string{model.MetricNameLabel, failMetric}, makeSamples(now.UnixMilli(), 1), nil, nil),
		makeTimeseries([]string{model.MetricNameLabel, okMetric}, makeSamples(now.UnixMilli(), 2), nil, nil),
	}})

	// The soft error makes the whole request fail with a client (4xx / bad-data) error.
	requirePushErrorWithCause(t, err, codes.InvalidArgument, mimirpb.ERROR_CAUSE_BAD_DATA)

	// Cleanup ran exactly once even though one compartment failed.
	assert.Equal(t, int64(1), cleanupCount.Load())

	// The soft error did not cancel the other compartment: its record landed.
	okRecords := readAllRecordsFromKafkaTopics(t, kafkaCluster.ListenAddrs(), []string{compartmentTopics[okCompartment]}, numCompartments, time.Second)
	assert.Contains(t, metricNamesFromRecords(t, okRecords), okMetric)

	// The failing compartment stored nothing.
	assert.Empty(t, readAllRecordsFromKafkaTopics(t, kafkaCluster.ListenAddrs(), []string{compartmentTopics[failCompartment]}, numCompartments, time.Second))
}

func TestDistributor_Push_Compartments_HardErrorCancelsOtherCompartments(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	const numCompartments = 3
	d, kafkaCluster, _ := prepareCompartmentsTestDistributor(t, numCompartments)
	router := compartmentsTestRouter(numCompartments)

	// metric_a and metric_c shard to two different read compartments (asserted below as a precondition).
	const failMetric, slowMetric = "metric_a", "metric_c"
	failCompartment := router.CompartmentForMetric("user", failMetric)
	slowCompartment := router.CompartmentForMetric("user", slowMetric)
	require.NotEqual(t, failCompartment, slowCompartment, "test precondition: the two metrics must shard to different compartments")

	// The failing compartment errors immediately with a hard error, while the slow compartment's Produce
	// blocks until the test unblocks it (simulating a degraded Kafka). We then assert Push returns with the
	// hard error before the slow compartment is unblocked, proving its in-flight write was cancelled.
	// Each compartment's only active partition equals its compartment ID and is pinned to its own broker,
	// so blocking the slow compartment's broker does not hold up the failing compartment's.
	unblockSlow := make(chan struct{})
	kafkaCluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCluster.KeepControl()
		for _, topic := range req.(*kmsg.ProduceRequest).Topics {
			for _, partition := range topic.Partitions {
				switch partition.Partition {
				case int32(failCompartment):
					return testkafka.CreateProduceResponseError(req.GetVersion(), topic.Topic, partition.Partition, kerr.TopicAuthorizationFailed), nil, true
				case int32(slowCompartment):
					// SleepControl yields to other connections while this handler sleeps, so the failing
					// compartment (on a different broker) can still fail fast. kfake otherwise runs control
					// functions serially, which would deadlock a plain block here.
					kafkaCluster.SleepControl(func() { <-unblockSlow })
				}
			}
		}
		return nil, nil, false
	})

	pushErr := make(chan error, 1)
	go func() {
		_, err := d.Push(ctx, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, failMetric}, makeSamples(now.UnixMilli(), 1), nil, nil),
			makeTimeseries([]string{model.MetricNameLabel, slowMetric}, makeSamples(now.UnixMilli(), 2), nil, nil),
		}})
		pushErr <- err
	}()

	// Push must return before we unblock the slow compartment: the hard error cancels its in-flight write.
	// The remote timeout is set well above this window (see prepareCompartmentsTestDistributor), so a Push
	// that returns quickly can only be the result of fail-fast cancellation, not the timeout firing.
	select {
	case err := <-pushErr:
		requirePushErrorWithCause(t, err, codes.Internal, mimirpb.ERROR_CAUSE_UNKNOWN)
	case <-time.After(5 * time.Second):
		close(unblockSlow)
		t.Fatal("Push did not fail fast: it blocked waiting on the slow compartment")
	}
	close(unblockSlow)
}

// requirePushErrorWithCause asserts that err is a gRPC status error with the given code and cause detail.
func requirePushErrorWithCause(t *testing.T, err error, expectedCode codes.Code, expectedCause mimirpb.ErrorCause) {
	t.Helper()

	require.Error(t, err)
	stat, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok, "expected a gRPC status error, got %T: %v", err, err)
	require.Equal(t, expectedCode, stat.Code())

	details := stat.Details()
	require.Len(t, details, 1)
	errDetails, ok := details[0].(*mimirpb.ErrorDetails)
	require.True(t, ok)
	require.Equal(t, expectedCause, errDetails.Cause)
}

// metricNamesFromRecords deserializes the given Kafka records and returns the metric names of all
// timeseries they contain.
func metricNamesFromRecords(t testing.TB, records []*kgo.Record) []string {
	t.Helper()

	var names []string
	for _, record := range records {
		parsed := mimirpb.PreallocWriteRequest{}
		require.NoError(t, ingest.DeserializeRecordContent(record.Value, &parsed, ingest.ParseRecordVersion(record)))
		for _, ts := range parsed.Timeseries {
			name, err := extract.UnsafeMetricNameFromLabelAdapters(ts.Labels)
			require.NoError(t, err)
			names = append(names, string(name))
		}
	}
	return names
}

func readAllRecordsFromKafkaTopics(t testing.TB, kafkaAddresses []string, topics []string, numPartitions int32, timeout time.Duration) []*kgo.Record {
	topicOffsets := make(map[string]map[int32]kgo.Offset, len(topics))
	for _, topic := range topics {
		offsets := make(map[int32]kgo.Offset, numPartitions)
		for partitionID := int32(0); partitionID < numPartitions; partitionID++ {
			offsets[partitionID] = kgo.NewOffset().AtStart()
		}
		topicOffsets[topic] = offsets
	}

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddresses...),
		kgo.RetryBackoffFn(func(int) time.Duration { return 10 * time.Millisecond }),
		kgo.FetchMaxWait(timeout/4),
		kgo.ConsumePartitions(topicOffsets))
	require.NoError(t, err)
	t.Cleanup(kafkaClient.Close)

	var records []*kgo.Record
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		fetches := kafkaClient.PollFetches(ctx)
		cancel()

		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatal(err)
		}

		fetches.EachRecord(func(record *kgo.Record) {
			records = append(records, record)
		})
	}

	return records
}

// prepareCompartmentsTestDistributor sets up a distributor with compartments enabled and a Kafka
// cluster seeded with the default topic plus one topic per read compartment. Each compartment ring has
// a single active partition equal to its compartment ID, so the topic a record lands on identifies the
// compartment and the partition confirms its own ring was used. It returns the distributor, the Kafka
// cluster, and the per-compartment topic names.
func prepareCompartmentsTestDistributor(t *testing.T, numCompartments int) (*Distributor, *kfake.Cluster, []string) {
	t.Helper()

	router := compartmentsTestRouter(numCompartments)
	compartmentTopics := make([]string, numCompartments)
	for i := range compartmentTopics {
		compartmentTopics[i] = router.TopicForCompartment(i)
	}

	// Run one broker per read compartment and pin each compartment's active partition (== its ID) to its
	// own broker, so each compartment's Produce request is sent independently rather than batched with
	// the others. This lets tests fail one compartment's write without disturbing the others.
	kafkaCluster, _ := testkafka.CreateCluster(t, int32(numCompartments), kafkaTopic,
		func() []kfake.Opt { return []kfake.Opt{kfake.SeedTopics(int32(numCompartments), compartmentTopics...)} },
		testkafka.WithNumBrokers(numCompartments),
	)
	for c := 0; c < numCompartments; c++ {
		require.NoError(t, kafkaCluster.MoveTopicPartition(compartmentTopics[c], int32(c), int32(c)))
	}

	activePartitions := make(map[int][]int32, numCompartments)
	for c := 0; c < numCompartments; c++ {
		activePartitions[c] = []int32{int32(c)}
	}

	distributors, _, _, _ := prepare(t, prepConfig{
		numDistributors:             1,
		ingestStorageEnabled:        true,
		ingestStoragePartitions:     int32(numCompartments),
		ingestStorageKafka:          kafkaCluster,
		numCompartments:             numCompartments,
		compartmentActivePartitions: activePartitions,
		limits:                      prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.Compartments.Enabled = true
			cfg.Compartments.Read.NumCompartments = numCompartments
			cfg.IngestStorageConfig.KafkaConfig.Topic = compartmentsTestTopicFormat
			// Keep the remote timeout well above the fail-fast assertion window so that a Push returning
			// quickly reflects cancellation rather than the timeout firing.
			cfg.RemoteTimeout = 30 * time.Second
		},
	})
	require.Len(t, distributors, 1)

	return distributors[0], kafkaCluster, compartmentTopics
}

func compartmentsTestRouter(numCompartments int) *compartments.TopicRouter {
	return compartments.NewTopicRouter(numCompartments, compartmentsTestTopicFormat)
}
