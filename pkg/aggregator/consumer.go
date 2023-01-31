package aggregator

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/segmentio/kafka-go"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_kafka "github.com/grafana/mimir/pkg/util/kafka"
)

type AggregateHandler func(user, aggregatedLabels string, aggSample mimirpb.Sample)

type KafkaConsumer struct {
	services.Service
	logger             log.Logger
	cfg                Config
	shutdownCh         chan struct{}
	reg                prometheus.Registerer
	kafkaReaders       []*kafka.Reader
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
	aggregateHandler   AggregateHandler
}

type consumerMetrics struct {
	highWatermark  *prometheus.GaugeVec
	offsetConsumed *prometheus.GaugeVec
}

func newConsumerMetrics(reg prometheus.Registerer) consumerMetrics {
	return consumerMetrics{
		highWatermark: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "aggregator_kafka_consumer_high_watermark",
			Help:      "The high watermark of a kafka partition consumed by the aggregator",
		}, []string{"partition"}),
		offsetConsumed: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "aggregator_kafka_consumer_offset_consumed",
			Help:      "The last offset of a kafka partition consumed by the aggregator",
		}, []string{"partition"}),
	}
}

type consumerMetricsForPartition struct {
	consumerMetrics
	partition string
}

func newConsumerMetricsForPartition(m consumerMetrics, partition string) consumerMetricsForPartition {
	return consumerMetricsForPartition{
		consumerMetrics: m,
		partition:       partition,
	}
}

func (m consumerMetricsForPartition) SetHighWatermark(offset int64) {
	m.highWatermark.WithLabelValues(m.partition).Set(float64(offset))
}

func (m consumerMetricsForPartition) SetOffsetConsumed(offset int64) {
	m.offsetConsumed.WithLabelValues(m.partition).Set(float64(offset))
}

func NewKafkaConsumer(cfg Config, push Push, logger log.Logger, reg prometheus.Registerer) (*KafkaConsumer, error) {
	var err error
	batcher := NewBatcher(cfg, push, reg, logger)
	subservices := []services.Service(nil)
	subservices = append(subservices, batcher)

	kc := &KafkaConsumer{
		logger:     logger,
		cfg:        cfg,
		shutdownCh: make(chan struct{}),
		reg:        reg,
	}

	kc.aggregateHandler = batcher.AddSample
	kc.subservices, err = services.NewManager(subservices...)
	if err != nil {
		return nil, err
	}

	kc.subservicesWatcher = services.NewFailureWatcher()
	kc.subservicesWatcher.WatchManager(kc.subservices)

	kc.Service = services.NewBasicService(kc.starting, kc.running, kc.stopping)
	return kc, nil
}

func (kc *KafkaConsumer) starting(ctx context.Context) error {
	if err := services.StartManagerAndAwaitHealthy(ctx, kc.subservices); err != nil {
		return errors.Wrap(err, "unable to start kafka consumer subservices")
	}

	level.Info(kc.logger).Log("msg", "starting readers for partitions", "partitions", fmt.Sprintf("%v", kc.cfg.kafkaPartitions))

	aggMetrics := newAggregationMetrics(kc.reg)
	consMetrics := newConsumerMetrics(kc.reg)
	for _, partition := range kc.cfg.kafkaPartitions {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   kc.cfg.kafkaBrokers,
			Topic:     kc.cfg.KafkaTopic,
			Partition: partition,
			MinBytes:  1e2,
			MaxBytes:  10e6,
		})

		kc.kafkaReaders = append(kc.kafkaReaders, reader)
		consMetricsPart := newConsumerMetricsForPartition(consMetrics, strconv.Itoa(partition))
		go newPartitionConsumer(kc.cfg, reader, kc.aggregateHandler, consMetricsPart, aggMetrics, kc.shutdownCh, kc.logger)
	}

	return nil
}

func (kc *KafkaConsumer) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-kc.shutdownCh:
		return nil
	case err := <-kc.subservicesWatcher.Chan():
		return errors.Wrap(err, "kafka consumer subservice failed")
	}
}

func (kc *KafkaConsumer) stopping(_ error) error {
	close(kc.shutdownCh)

	multiErr := tsdb_errors.NewMulti()
	for _, reader := range kc.kafkaReaders {
		err := reader.Close()
		if err != nil {
			multiErr.Add(err)
		}
	}

	err := services.StopManagerAndAwaitStopped(context.Background(), kc.subservices)
	if err != nil {
		multiErr.Add(err)
	}

	return multiErr.Err()
}

type partitionConsumer struct {
	cfg              Config
	consMetrics      consumerMetricsForPartition
	aggs             userAggregations
	shutdownCh       chan struct{}
	logger           log.Logger
	aggregateHandler AggregateHandler
}

func newPartitionConsumer(cfg Config, reader *kafka.Reader, handler AggregateHandler, consMetrics consumerMetricsForPartition, aggMetrics aggregationMetrics, shutdownCh chan struct{}, logger log.Logger) {
	partitionConsumer{
		cfg:              cfg,
		consMetrics:      consMetrics,
		aggs:             newUserAggregations(cfg.AggregationInterval, cfg.AggregationDelay, aggMetrics),
		shutdownCh:       shutdownCh,
		logger:           logger,
		aggregateHandler: handler,
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

	c.consMetrics.SetHighWatermark(message.HighWaterMark)
	c.consMetrics.SetOffsetConsumed(message.Offset)

	var value mimirpb.PreallocTimeseries
	err = value.Unmarshal(message.Value)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to unmarshal kafka message, dropping message", "err", err)
		return
	}

	c.handleTimeseries(user, aggregatedLabels, value)

	mimirpb.ReuseTimeseries(value.TimeSeries)
}

func (c partitionConsumer) handleTimeseries(user, aggregatedLabels string, value mimirpb.PreallocTimeseries) {
	if len(value.Samples) == 0 || len(value.Labels) == 0 {
		level.Error(c.logger).Log("msg", "value without samples or without labels, dropping")
		return
	}

	rawLabels := labelsToString(value.Labels)
	for _, sample := range value.Samples {
		aggSample := c.aggs.ingest(user, aggregatedLabels, rawLabels, sample)
		if !math.IsNaN(aggSample.Value) {
			// If sample value isn't NaN then it is an aggregation result, handling it.
			c.aggregateHandler(user, aggregatedLabels, aggSample)
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
