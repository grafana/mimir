package aggregator

import (
	"context"
	"math"
	"time"
	"unsafe"

	"github.com/go-kit/log"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/mimir/pkg/mimirpb"
	util_kafka "github.com/grafana/mimir/pkg/util/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/errors"
	"github.com/segmentio/kafka-go"
)

type Aggregator struct {
	services.Service
	logger       log.Logger
	cfg          Config
	shutdownCh   chan struct{}
	kafkaReaders []*kafka.Reader

	aggs userAggregations
}

func NewAggregator(cfg Config, logger log.Logger, reg prometheus.Registerer) (*Aggregator, error) {
	agg := &Aggregator{
		logger:     logger,
		shutdownCh: make(chan struct{}),
		cfg:        cfg,
	}

	agg.Service = services.NewBasicService(agg.starting, agg.running, agg.stopping)

	return agg, nil
}

func (a *Aggregator) starting(ctx context.Context) error {
	for _, partition := range a.cfg.kafkaPartitions {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   a.cfg.kafkaBrokers,
			Topic:     a.cfg.KafkaTopic,
			Partition: partition,
			MinBytes:  1e2,
			MaxBytes:  10e6,
		})

		a.kafkaReaders = append(a.kafkaReaders, reader)

		go newPartitionConsumer(a.cfg, reader, a.shutdownCh, a.logger)
	}

	return nil
}

func (a *Aggregator) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-a.shutdownCh:
		return nil
	}
}

func (a *Aggregator) stopping(_ error) error {
	close(a.shutdownCh)

	multiErr := errors.NewMulti()
	for _, reader := range a.kafkaReaders {
		err := reader.Close()
		if err != nil {
			multiErr.Add(err)
		}
	}

	return multiErr.Err()
}

type partitionConsumer struct {
	cfg             Config
	aggs            userAggregations
	shutdownCh      chan struct{}
	logger          log.Logger
	handleAggregate func(user string, aggSample mimirpb.Sample)
}

func newPartitionConsumer(cfg Config, reader *kafka.Reader, shutdownCh chan struct{}, logger log.Logger) {
	partitionConsumer{
		cfg:        cfg,
		aggs:       newUserAggregations(cfg.AggregationInterval, cfg.AggregationDelay),
		shutdownCh: shutdownCh,
		logger:     logger,
	}.consume(reader)
}

func (c partitionConsumer) isShuttingDown() bool {
	select {
	case <-c.shutdownCh:
		return true
	default:
		return false
	}
}

func (c partitionConsumer) consume(reader *kafka.Reader) {
	for !c.isShuttingDown() {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			level.Error(c.logger).Log("msg", "failed to read kafka message", "err", err)
			break
		}

		c.handleMessage(m)
	}
}

func (c partitionConsumer) handleMessage(message kafka.Message) {
	user, aggregatedLabels, err := util_kafka.DecomposeKafkaKey(message.Key)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to decompose kafka key, dropping message", "err", err)
		return
	}

	var value mimirpb.PreallocTimeseries
	err = value.Unmarshal(message.Value)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to unmarshal kafka message, dropping message", "err", err)
		return
	}

	c.handleTimeseries(user, aggregatedLabels, value)
}

func (c partitionConsumer) handleTimeseries(user, aggregatedLabels string, value mimirpb.PreallocTimeseries) {
	if len(value.Samples) == 0 {
		// No value, ignoring.
		return
	}

	rawLabels := labelsToString(value.Labels)
	for _, sample := range value.Samples {
		aggSample := c.aggs.ingest(user, aggregatedLabels, rawLabels, sample)
		if math.IsNaN(aggSample.Value) {
			c.handleAggregate(user, aggSample)
		}
	}
}

// labelsToString converts a slice of labels to a string.
// it assumes that every labelset has a metric name
func labelsToString(labels []mimirpb.LabelAdapter) string {
	res := make([]byte, 0, predictSize(labels))
	res = append(res, metricName(labels)...)
	if len(labels) == 1 {
		return yoloString(res)
	}

	res = append(res, '{')
	firstLabel := true
	for _, label := range labels {
		if label.Name == model.MetricNameLabel {
			continue
		}
		if !firstLabel {
			res = append(res, ',')
			firstLabel = false
		}
		res = append(res, label.Name...)
		res = append(res, '=')
		res = append(res, label.Value...)
	}
	res = append(res, '}')

	return yoloString(res)
}

func yoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

func metricName(labels []mimirpb.LabelAdapter) []byte {
	for _, label := range labels {
		if label.Name == model.MetricNameLabel {
			return []byte(label.Value)
		}
	}

	return []byte{}
}

func predictSize(labels []mimirpb.LabelAdapter) int {
	var size int

	for _, label := range labels {
		if label.Name != model.MetricNameLabel {
			size += len(label.Value)
			if len(labels) > 1 {
				size += 2 // for the {} chars
			}
		} else {
			size += len(label.Name)
			size += 2 // for the "" chars
			size += len(label.Value)

			if len(labels) > 2 {
				size += 1 // for the , after the label/value
			}
		}
	}

	return size
}

// userAggregations contains the aggregation state for all users.
// Note that it and all its sub-structs are not thread-safe, to gain parallelism we partition the Kafka topic.
type userAggregations struct {
	interval time.Duration
	delay    time.Duration
	byUser   map[string]aggregations
}

func newUserAggregations(interval, delay time.Duration) userAggregations {
	return userAggregations{
		interval: interval,
		delay:    delay,
		byUser:   map[string]aggregations{},
	}
}

// ingest ingests a sample for aggregation.
// It might return an aggregated sample if this ingestion has resulted in one, otherwise it will return a sample with a NaN value.
func (u *userAggregations) ingest(user, aggregatedLabels, rawLabels string, sample mimirpb.Sample) mimirpb.Sample {
	aggs, ok := u.byUser[user]
	if !ok {
		aggs = newAggregations()
		u.byUser[user] = aggs
	}

	aggSample := aggs.ingest(u.interval.Milliseconds(), u.delay.Milliseconds(), sample, aggregatedLabels, rawLabels)
	u.byUser[user] = aggs

	return aggSample
}

// aggregations represents the aggregation state for a single user.
type aggregations struct {
	// aggregations is keyed by the labelset of the aggregated series that we're generating.
	aggregations map[string]aggregation
}

func newAggregations() aggregations {
	return aggregations{
		aggregations: map[string]aggregation{},
	}
}

func (a *aggregations) ingest(interval, delay int64, sample mimirpb.Sample, aggregatedLabels, rawLabels string) mimirpb.Sample {
	agg, ok := a.aggregations[aggregatedLabels]
	if !ok {
		agg = newAggregation()
		a.aggregations[aggregatedLabels] = agg
	}

	aggSample := agg.ingest(interval, delay, sample, rawLabels)
	a.aggregations[aggregatedLabels] = agg

	return aggSample
}

// aggregation is the state of one aggregated series (not an aggregated metrics).
type aggregation struct {
	// rawSeries is keyed by the labelset of the raw series that we're aggregating.
	rawSeries map[string]timeBuckets

	// lastTimestamp is the last timestamp for which an aggregation has been generated.
	lastTimestamp int64
	// lastValue is the value of the last aggregation which has been generated.
	lastValue float64
}

func newAggregation() aggregation {
	return aggregation{
		rawSeries: map[string]timeBuckets{},
		lastValue: math.NaN(),
	}
}

func (a *aggregation) ingest(interval, delay int64, sample mimirpb.Sample, rawLabels string) (aggSample mimirpb.Sample) {
	aggSample.Value = math.NaN()

	aggregationTs := getAggregationTs(sample.TimestampMs, interval)
	if aggregationTs <= a.lastTimestamp {
		// Aggregated sample which would be generated from the sample with the given timestamp has already been generated.
		// Dropping the sample.
		return
	}

	bucketCount := bucketCount(interval, delay)
	targetBucket := int(sample.TimestampMs/interval) % bucketCount
	rawSeries, ok := a.rawSeries[rawLabels]
	if !ok {
		rawSeries = newTimeBuckets(interval, delay)
		a.rawSeries[rawLabels] = rawSeries
	}
	rawSeries.ingest(targetBucket, sample)

	aggSample.TimestampMs = getAggregationTs(sample.TimestampMs-delay-interval, interval)
	if aggSample.TimestampMs > a.lastTimestamp {
		aggSample.Value = a.aggregate(aggSample.TimestampMs, interval, bucketCount)
	}

	return
}

func (a *aggregation) aggregate(aggregationTs, intervalMs int64, bucketCount int) float64 {
	bucket := int(aggregationTs/intervalMs) % bucketCount
	increasesSum := math.NaN()
	for _, rawSeries := range a.rawSeries {
		increase := rawSeries.increaseToBucket(bucket)
		if !math.IsNaN(increase) {
			if math.IsNaN(increasesSum) {
				increasesSum = 0
			}

			increasesSum += increase
		}
	}

	a.lastTimestamp = aggregationTs

	if !math.IsNaN(increasesSum) && math.IsNaN(a.lastValue) {
		a.lastValue = increasesSum
	} else {
		a.lastValue += increasesSum
	}

	return a.lastValue
}

// timeBuckets is a fixed size ring buffer of time buckets for which we're generating aggregations.
// Its size is determined by the aggregation interval and aggregation delay, it is calculated like:
// ceil( ( <aggregation delay> + <aggregation interval> ) / <aggregation interval> )
type timeBuckets struct {
	buckets []timeBucket
}

func newTimeBuckets(interval, delay int64) timeBuckets {
	tb := timeBuckets{
		buckets: make([]timeBucket, bucketCount(interval, delay)),
	}
	for i := range tb.buckets {
		tb.buckets[i].sample.Value = math.NaN()
	}
	return tb
}

func bucketCount(interval, delay int64) int {
	totalRetention := delay + 2*interval
	totalRetention += interval - (delay % interval) // round up to next multiple of interval
	return int(totalRetention / interval)
}

// getAggregationTs takes the timestamp of a sample and the configured interval,
// it returns the timestamp to assign to the aggregated sample with the given timestamp.
func getAggregationTs(timestamp, interval int64) int64 {
	// the timestamp of the aggregated sample is the last millisecond of the interval bucket.
	return timestamp/interval*interval + interval - 1
}

func (t timeBuckets) ingest(targetBucket int, sample mimirpb.Sample) {
	if t.buckets[targetBucket].sample.TimestampMs >= sample.TimestampMs {
		// Sample older or same age as current sample in this bucket, ignoring.
		return
	}

	t.buckets[targetBucket].sample = sample
}

func (t timeBuckets) increaseToBucket(bucket int) float64 {
	curValue := t.buckets[bucket].sample.Value
	prevValue := t.buckets[decWrapped(bucket, len(t.buckets))].sample.Value

	if math.IsNaN(curValue) {
		return math.NaN()
	}

	if math.IsNaN(prevValue) {
		return curValue
	}

	if prevValue > curValue {
		// Counter reset, use absolute value as increase.
		return curValue
	}

	// Difference is the increase.
	return curValue - prevValue
}

// decWrapped decreases the integer <i> by one, if <i> is 0 it returns <size> - 1 (wrapping around).
func decWrapped(i, size int) int {
	if i == 0 {
		return size - 1
	}

	return i - 1
}

// time bucket represents one interval of time for which we're generating an aggregation,
// the time bucket begins and ends at multiples of the aggregation interval,
// its time range is inclusive - exclusive.
// Its timestamp and value properties are always set to the values of the most recent sample of this bucket.
type timeBucket struct {
	sample mimirpb.Sample
}
