// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/regexp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/plugin/kprom"
)

var (
	// Regular expression used to parse the ingester numeric ID.
	ingesterIDRegexp = regexp.MustCompile("-(zone-.-)?([0-9]+)$")

	// The Prometheus summary objectives used when tracking latency.
	latencySummaryObjectives = map[float64]float64{
		0.5:   0.05,
		0.90:  0.01,
		0.99:  0.001,
		0.995: 0.001,
		0.999: 0.001,
		1:     0.001,
	}
)

// IngesterPartition returns the partition ID to use to write to a specific ingester partition.
// The input ingester ID is expected to end either with "zone-X-Y" or only "-Y" where "X" is a letter in the range [a,d]
// and "Y" is a positive integer number. This means that this function supports up to 4 zones starting
// with letter "a" or no zones at all.
// Deprecated: We don't want per-zone partitions.
func IngesterPartition(ingesterID string) (int32, error) {
	match := ingesterIDRegexp.FindStringSubmatch(ingesterID)
	if len(match) == 0 {
		return 0, fmt.Errorf("name doesn't match regular expression %s %q", ingesterID, ingesterIDRegexp.String())
	}

	// Convert the zone ID to a number starting from 0.
	var zoneID int32
	if wholeZoneStr := match[1]; len(wholeZoneStr) > 1 {
		if !strings.HasPrefix(wholeZoneStr, "zone-") {
			return 0, fmt.Errorf("invalid zone ID %s in %s", wholeZoneStr, ingesterID)
		}

		zoneID = rune(wholeZoneStr[len(wholeZoneStr)-2]) - 'a'
		if zoneID < 0 || zoneID > 4 {
			return 0, fmt.Errorf("zone ID is not between a and d %s", ingesterID)
		}
	}

	// Parse the ingester sequence number.
	ingesterSeq, err := strconv.Atoi(match[2])
	if err != nil {
		return 0, fmt.Errorf("no ingester sequence in name %s", ingesterID)
	}

	partitionID := int32(ingesterSeq<<2) | (zoneID & 0b11)
	return partitionID, nil
}

// IngesterID returns ID from ingester. (TODO: replace IngesterPartition with this)
func IngesterID(ingesterID string) (int32, error) {
	match := ingesterIDRegexp.FindStringSubmatch(ingesterID)
	if len(match) == 0 {
		return 0, fmt.Errorf("name doesn't match regular expression %s %q", ingesterID, ingesterIDRegexp.String())
	}

	// Parse the ingester sequence number.
	ingesterSeq, err := strconv.Atoi(match[2])
	if err != nil {
		return 0, fmt.Errorf("no ingester sequence in name %s", ingesterID)
	}

	return int32(ingesterSeq), nil
}

func commonKafkaClientOptions(cfg KafkaConfig, metrics *kprom.Metrics, logger log.Logger) []kgo.Opt {
	return []kgo.Opt{
		kgo.ClientID(cfg.ClientID),
		kgo.SeedBrokers(cfg.Address),
		kgo.AllowAutoTopicCreation(),
		kgo.DialTimeout(cfg.DialTimeout),

		// A cluster metadata update is a request sent to a broker and getting back the map of partitions and
		// the leader broker for each partition. The cluster metadata can be updated (a) periodically or
		// (b) when some events occur (e.g. backoff due to errors).
		//
		// MetadataMinAge() sets the minimum time between two cluster metadata updates due to events.
		// MetadataMaxAge() sets how frequently the periodic update should occur.
		//
		// It's important to note that the periodic update is also used to discover new brokers (e.g. during a
		// rolling update or after a scale up). For this reason, it's important to run the update frequently.
		//
		// The other two side effects of frequently updating the cluster metadata:
		// 1. The "metadata" request may be expensive to run on the Kafka backend.
		// 2. If the backend returns each time a different authoritative owner for a partition, then each time
		//    the cluster metadata is updated the Kafka client will create a new connection for each partition,
		//    leading to a high connections churn rate.
		//
		// We currently set min and max age to the same value to have constant load on the Kafka backend: regardless
		// there are errors or not, the metadata requests frequency doesn't change.
		kgo.MetadataMinAge(10 * time.Second),
		kgo.MetadataMaxAge(10 * time.Second),

		kgo.WithHooks(metrics),
		kgo.WithLogger(newKafkaLogger(logger)),

		kgo.RetryTimeoutFn(func(key int16) time.Duration {
			switch key {
			case ((*kmsg.ListOffsetsRequest)(nil)).Key():
				return cfg.LastProducedOffsetRetryTimeout
			}

			// 30s is the default timeout in the Kafka client.
			return 30 * time.Second
		}),
	}
}

// resultPromise is a simple utility to have multiple goroutines waiting for a result from another one.
type resultPromise[T any] struct {
	// done is a channel used to wait the result. Once the channel is closed
	// it's safe to read resultValue and resultErr without any lock.
	done chan struct{}

	resultValue T
	resultErr   error
}

func newResultPromise[T any]() *resultPromise[T] {
	return &resultPromise[T]{
		done: make(chan struct{}),
	}
}

// notify the result to waiting goroutines. This function must be called exactly once.
func (w *resultPromise[T]) notify(value T, err error) {
	w.resultValue = value
	w.resultErr = err
	close(w.done)
}

func (w *resultPromise[T]) wait(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		var zero T
		return zero, context.Cause(ctx)
	case <-w.done:
		return w.resultValue, w.resultErr
	}
}
