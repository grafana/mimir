// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/regexp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/plugin/kotel"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	// Regular expression used to parse the ingester numeric ID.
	ingesterIDRegexp = regexp.MustCompile("-([0-9]+)$")
)

// IngesterPartitionID returns the partition ID owner the the given ingester.
func IngesterPartitionID(ingesterID string) (int32, error) {
	match := ingesterIDRegexp.FindStringSubmatch(ingesterID)
	if len(match) == 0 {
		return 0, fmt.Errorf("ingester ID %s doesn't match regular expression %q", ingesterID, ingesterIDRegexp.String())
	}

	// Parse the ingester sequence number.
	ingesterSeq, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("no ingester sequence number in ingester ID %s", ingesterID)
	}

	return int32(ingesterSeq), nil
}

type onlySampledTraces struct {
	propagation.TextMapPropagator
}

func (o onlySampledTraces) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsSampled() {
		return
	}
	o.TextMapPropagator.Inject(ctx, carrier)
}

func commonKafkaClientOptions(cfg KafkaConfig, metrics *kprom.Metrics, logger log.Logger) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.ClientID(cfg.ClientID),
		kgo.SeedBrokers(cfg.Address),
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

		kgo.WithLogger(NewKafkaLogger(logger)),

		kgo.RetryTimeoutFn(func(key int16) time.Duration {
			switch key {
			case ((*kmsg.ListOffsetsRequest)(nil)).Key():
				return cfg.LastProducedOffsetRetryTimeout
			}

			// 30s is the default timeout in the Kafka client.
			return 30 * time.Second
		}),
	}

	if cfg.AutoCreateTopicEnabled {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	// SASL plain auth.
	if cfg.SASLUsername != "" && cfg.SASLPassword.String() != "" {
		opts = append(opts, kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{
				User: cfg.SASLUsername,
				Pass: cfg.SASLPassword.String(),
			}, nil
		})))
	}

	opts = append(opts, kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(recordsTracer())).Hooks()...))

	if metrics != nil {
		opts = append(opts, kgo.WithHooks(metrics))
	}

	return opts
}

func recordsTracer() *kotel.Tracer {
	return kotel.NewTracer(kotel.TracerPropagator(propagation.NewCompositeTextMapPropagator(onlySampledTraces{propagation.TraceContext{}})))
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

// createTopic tries to set num.partitions config option on brokers.
// This is best-effort, if creating the topic fails, error is logged, but not returned.
func createTopic(cfg KafkaConfig, logger log.Logger) error {
	logger = log.With(logger, "task", "autocreate_topic")

	cl, err := kgo.NewClient(commonKafkaClientOptions(cfg, nil, logger)...)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}

	adm := kadm.NewClient(cl)
	defer adm.Close()
	ctx := context.Background()

	// As of kafka 2.4 we can pass -1 and the broker will use its default configuration.
	const defaultReplication = -1
	resp, err := adm.CreateTopic(ctx, int32(cfg.AutoCreateTopicDefaultPartitions), defaultReplication, nil, cfg.Topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		if errors.Is(err, kerr.TopicAlreadyExists) {
			level.Info(logger).Log(
				"msg", "topic already exists",
				"topic", resp.Topic,
				"num_partitions", resp.NumPartitions,
				"replication_factor", resp.ReplicationFactor,
			)
			return nil
		}
		return fmt.Errorf("failed to create topic %s: %w", cfg.Topic, err)
	}

	level.Info(logger).Log(
		"msg", "successfully created topic",
		"topic", resp.Topic,
		"num_partitions", resp.NumPartitions,
		"replication_factor", resp.ReplicationFactor,
	)
	return nil
}
