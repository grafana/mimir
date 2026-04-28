// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/regexp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	awssasl "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kotel"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	// Regular expression used to parse the ingester numeric ID.
	ingesterIDRegexp = regexp.MustCompile("-([0-9]+)$")
)

var tracer = otel.Tracer("pkg/storage/ingest")

// IngesterPartitionID returns the partition ID owner the the given ingester.
func IngesterPartitionID(ingesterID string) (int32, error) {
	match := ingesterIDRegexp.FindStringSubmatch(ingesterID)
	if len(match) == 0 {
		return 0, fmt.Errorf("ingester ID %s doesn't match regular expression %q", ingesterID, ingesterIDRegexp.String())
	}

	// Parse the ingester sequence number.
	ingesterSeq, err := strconv.ParseInt(match[1], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("no ingester sequence number in ingester ID %s", ingesterID)
	}

	return int32(ingesterSeq), nil
}

// Compile-time checks to ensure sampledOnlyTracer implements the same hook interfaces as kotel.Tracer.
var (
	_ kgo.HookProduceRecordBuffered   = new(sampledOnlyTracer)
	_ kgo.HookProduceRecordUnbuffered = new(sampledOnlyTracer)
	_ kgo.HookFetchRecordBuffered     = new(sampledOnlyTracer)
	_ kgo.HookFetchRecordUnbuffered   = new(sampledOnlyTracer)
)

// sampledOnlyTracer wraps a kotel.Tracer and skips span creation and header
// injection on the produce path for unsampled traces. Fetch hooks always
// delegate to the parent tracer because the consume side needs to extract
// trace context from record headers regardless of local sampling. Without
// this wrapper, kotel creates spans for every produced Kafka record regardless
// of sampling, which is expensive at high volume.
type sampledOnlyTracer struct {
	parent *kotel.Tracer
}

func newSampledOnlyTracer() *sampledOnlyTracer {
	return &sampledOnlyTracer{parent: recordsTracer()}
}

func (t *sampledOnlyTracer) OnProduceRecordBuffered(r *kgo.Record) {
	if !trace.SpanContextFromContext(r.Context).IsSampled() {
		return
	}
	t.parent.OnProduceRecordBuffered(r)
}

// OnProduceRecordUnbuffered is safe to skip when OnProduceRecordBuffered was also skipped:
// the record's context still has the original unsampled span, so the parent's
// OnProduceRecordUnbuffered would only call End() on a no-op span.
func (t *sampledOnlyTracer) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	if !trace.SpanContextFromContext(r.Context).IsSampled() {
		return
	}
	t.parent.OnProduceRecordUnbuffered(r, err)
}

func (t *sampledOnlyTracer) OnFetchRecordBuffered(r *kgo.Record) {
	t.parent.OnFetchRecordBuffered(r)
}

func (t *sampledOnlyTracer) OnFetchRecordUnbuffered(r *kgo.Record, polled bool) {
	t.parent.OnFetchRecordUnbuffered(r, polled)
}

// sampledOnlyPropagator is a propagation wrapper that only injects trace context
// into Kafka record headers when the trace is sampled. This avoids adding
// headers to every record when the trace won't be collected.
type sampledOnlyPropagator struct {
	propagation.TextMapPropagator
}

func (o sampledOnlyPropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	if !trace.SpanContextFromContext(ctx).IsSampled() {
		return
	}
	o.TextMapPropagator.Inject(ctx, carrier)
}

func commonKafkaClientOptions(cfg KafkaConfig, metrics *kprom.Metrics, logger log.Logger) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.ClientID(cfg.ClientID),
		kgo.SeedBrokers(cfg.Address...),
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

	if cfg.ClientRack != "" {
		opts = append(opts, kgo.Rack(cfg.ClientRack))
	}

	opts = append(opts, kafkaAuthOptions(cfg.SASL)...)

	if cfg.TLSEnabled {
		tlsConfig, err := cfg.TLS.GetTLSConfig()
		if err != nil {
			panic("must call Validate before trying to construct Kafka options")
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	opts = append(opts, kgo.WithHooks(newSampledOnlyTracer()))

	if metrics != nil {
		opts = append(opts, kgo.WithHooks(metrics))
	}

	return opts
}

func kafkaAuthOptions(cfg KafkaAuthConfig) []kgo.Opt {
	if (cfg.Mechanism == "" || cfg.Mechanism == SASLMechanismPlain) && cfg.Username == "" {
		return nil
	}

	var m sasl.Mechanism
	switch cfg.Mechanism {
	case SASLMechanismScramSHA256:
		m = scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password.String(),
		}.AsSha256Mechanism()
	case SASLMechanismScramSHA512:
		m = scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password.String(),
		}.AsSha512Mechanism()
	case SASLMechanismPlain:
		m = plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password.String(),
		}.AsMechanism()
	case SASLMechanismOauthbearer:
		m = cfg.Oauthbearer.mechanism()
	case SASLMechanismMSKIAM:
		m = cfg.MSKIAM.mechanism()
	default:
		panic(fmt.Errorf("unknown SASL mechanism: %v", cfg.Mechanism))
	}

	return []kgo.Opt{kgo.SASL(m)}
}

// saslSecretConfig configures a static secret. It may be empty.
type saslSecretConfig interface {
	// Validate returns errNoSecret when no static secret are set.
	// It may return other validation errors.
	Validate() error
	// mechanism constructs a sasl.Mechanism from the static secret, if it exists.
	mechanism() (sasl.Mechanism, bool)
}

func (cfg KafkaAuthOauthbearerConfig) mechanism() sasl.Mechanism {
	return saslMechanism((kafkaSASLConfig[KafkaOauthbearerStaticConfig])(cfg), oauth.Oauth)
}

func (s KafkaOauthbearerStaticConfig) mechanism() (sasl.Mechanism, bool) {
	if err := s.Validate(); err != nil {
		return nil, false
	}
	return oauth.Auth{
		Token:      s.Token.String(),
		Zid:        s.Zid,
		Extensions: s.Extensions.Read(),
	}.AsMechanism(), true
}

func (cfg KafkaAuthMSKIAMConfig) mechanism() sasl.Mechanism {
	return saslMechanism((kafkaSASLConfig[KafkaMSKIAMStaticConfig])(cfg), awssasl.ManagedStreamingIAM)
}

func (s KafkaMSKIAMStaticConfig) mechanism() (sasl.Mechanism, bool) {
	if err := s.Validate(); err != nil {
		return nil, false
	}
	return awssasl.Auth{
		AccessKey:    s.AccessKey.String(),
		SecretKey:    s.SecretKey.String(),
		SessionToken: s.SessionToken.String(),
		UserAgent:    s.UserAgent,
	}.AsManagedStreamingIAMMechanism(), true
}

// saslMechanism returns the sasl.Mechanism to be passed to the Kafka client.
func saslMechanism[T saslSecretConfig, A any](cfg kafkaSASLConfig[T], fromCallback func(func(context.Context) (A, error)) sasl.Mechanism) sasl.Mechanism {
	if m, ok := cfg.Secret.mechanism(); ok {
		return m
	}
	if cfg.FilePath != "" {
		return fromCallback(func(ctx context.Context) (A, error) {
			f, err := os.ReadFile(cfg.FilePath)
			if err != nil {
				var zero A
				return zero, err
			}
			var a A
			err = json.Unmarshal(f, &a)
			return a, err
		})
	}
	if cfg.HTTPSocketPath != "" {
		return fromCallback(func(ctx context.Context) (A, error) {
			return requestJSONFromSocket[A](ctx, cfg.HTTPSocketPath, cfg.HTTPSocketTimeout)
		})
	}
	panic("invalid kafkaSecretConfig; Validate must have been called first")
}

func requestJSONFromSocket[T any](ctx context.Context, socketPath string, timeout time.Duration) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}

	client := &http.Client{
		Transport: transport,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://credentials/", nil)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("creating request for HTTP socket %s: %w", socketPath, err)
	}

	resp, err := client.Do(req)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("requesting credentials from HTTP socket %s: %w", socketPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var zero T
		return zero, fmt.Errorf("requesting credentials from HTTP socket %s: unexpected status %s", socketPath, resp.Status)
	}

	var a T
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		var zero T
		return zero, fmt.Errorf("parsing credentials from HTTP socket %s: %w", socketPath, err)
	}
	return a, nil
}

func recordsTracer() *kotel.Tracer {
	return kotel.NewTracer(kotel.TracerPropagator(propagation.NewCompositeTextMapPropagator(sampledOnlyPropagator{propagation.TraceContext{}})))
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

// CreateTopic creates the topic in the Kafka cluster. If creating the topic fails, then an error is returned.
// If the topic already exists, then the function logs a message and returns nil.
func CreateTopic(cfg KafkaConfig, logger log.Logger) error {
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
