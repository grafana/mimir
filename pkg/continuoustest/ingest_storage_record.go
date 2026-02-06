// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

type IngestStorageRecordTestConfig struct {
	Enabled                  bool               `yaml:"-"`
	Kafka                    ingest.KafkaConfig `yaml:"-"`
	ConsumerGroup            string             `yaml:"-"`
	MaxJumpLimitPerPartition int                `yaml:"-"`
	MaxRecordsPerRun         int                `yaml:"-"`
	RecordsProcessedPercent  int                `yaml:"-"`
}

func (cfg *IngestStorageRecordTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tests.ingest-storage-record.enabled", false, "Whether the test for ingest-storage record correctness is enabled.")
	f.StringVar(&cfg.ConsumerGroup, "tests.ingest-storage-record.consumer-group", "ingest-storage-record", "The Kafka consumer group used for getting/setting commmitted offsets.")
	f.IntVar(&cfg.MaxJumpLimitPerPartition, "tests.ingest-storage-record.max-jump-size", 500000000, "If a partition increases by this many offsets in a run, we skip processing it, to protect against downloading unexpectedly huge batches.")
	f.IntVar(&cfg.MaxRecordsPerRun, "tests.ingest-storage-record.max-records-per-run", 200000, "Limit on the number of total records to be processed in a run, to keep memory bounded in large cells. This limit is approximate, we might go over by a partial fetch.")
	f.IntVar(&cfg.RecordsProcessedPercent, "tests.ingest-storage-record.records-processed-percent", 5, "The approximate percent of records to actually fetch and compare.")
}

type IngestStorageRecordTestMetrics struct {
	batchesProcessedTotal    prometheus.Counter
	batchesFailedTotal       prometheus.Counter
	recordsProcessedTotal    *prometheus.CounterVec
	metadataProcessedTotal   *prometheus.CounterVec
	timeseriesProcessedTotal *prometheus.CounterVec
	samplesProcessedTotal    *prometheus.CounterVec
	exemplarsProcessedTotal  *prometheus.CounterVec
	histogramsProcessedTotal *prometheus.CounterVec
	avgCompressionRatio      prometheus.Gauge
	throughputBytes          prometheus.Counter
	recordsWithGrowth        prometheus.Counter
}

func NewIngestStorageRecordTestMetrics(reg prometheus.Registerer) *IngestStorageRecordTestMetrics {
	return &IngestStorageRecordTestMetrics{
		batchesProcessedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_batches_total",
			Help: "Number of batches analyzed by the tool.",
		}),
		batchesFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_batches_failed_total",
			Help: "Number of batches that failed the test.",
		}),
		recordsProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_records_processed_total",
			Help: "Number of records analyzed by the tool per tenant.",
		}, []string{"user"}),
		metadataProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_metadata_processed_total",
			Help: "Number of metadata analyzed by the tool per tenant.",
		}, []string{"user"}),
		timeseriesProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_timeseries_processed_total",
			Help: "Number of timeseries analyzed by the tool per tenant.",
		}, []string{"user"}),
		samplesProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_samples_processed_total",
			Help: "Number of samples analyzed by the tool per tenant.",
		}, []string{"user"}),
		exemplarsProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_exemplars_processed_total",
			Help: "Number of exemplars analyzed by the tool per tenant.",
		}, []string{"user"}),
		histogramsProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_histograms_processed_total",
			Help: "Number of histograms analyzed by the tool per tenant.",
		}, []string{"user"}),
		avgCompressionRatio: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "mimir_continuous_test_ingest_storage_avg_compression_ratio",
			Help: "Gauge of the average compression ratio by batch of records.",
		}),
		throughputBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_v2_throughput_bytes_total",
			Help: "The total number of bytes of v2 records generated.",
		}),
		recordsWithGrowth: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_records_processed_with_v2_growth_total",
			Help: "The number of records processed that were larger in the V2 format than they were in V1.",
		}),
	}
}

type IngestStorageRecordTest struct {
	name        string
	cfg         IngestStorageRecordTestConfig
	client      *kgo.Client
	adminClient *kadm.Client
	metrics     *IngestStorageRecordTestMetrics
	logger      log.Logger
	reg         prometheus.Registerer
}

func NewIngestStorageRecordTest(cfg IngestStorageRecordTestConfig, logger log.Logger, reg prometheus.Registerer) *IngestStorageRecordTest {
	const name = "ingest-storage-record"

	return &IngestStorageRecordTest{
		name:    name,
		cfg:     cfg,
		metrics: NewIngestStorageRecordTestMetrics(reg),
		logger:  logger,
		reg:     reg,
	}
}

// Name implements Test.
func (t *IngestStorageRecordTest) Name() string {
	return t.name
}

// Init implements Test.
func (t *IngestStorageRecordTest) Init(ctx context.Context, now time.Time) error {
	if !t.cfg.Enabled {
		return nil
	}

	level.Info(t.logger).Log("msg", "starting kafka client")

	// One tenth of the normal ingest-storage max bytes per fetch, hardcoded.
	// Smaller fetches reduce memory variance.
	const fetchMaxBytes = 10_000_000

	kc, err := ingest.NewKafkaReaderClient(
		t.cfg.Kafka,
		ingest.NewKafkaReaderClientMetrics(ingest.ReaderMetricsPrefix, "record-continuous-test", t.reg),
		t.logger,
		kgo.ConsumeTopics(t.cfg.Kafka.Topic),
		kgo.ConsumerGroup(t.cfg.ConsumerGroup),
		kgo.FetchMaxBytes(fetchMaxBytes),
	)
	if err != nil {
		return fmt.Errorf("creating kafka reader: %w", err)
	}

	t.client = kc
	t.adminClient = kadm.NewClient(kc)

	return nil
}

// Run implements Test.
func (t *IngestStorageRecordTest) Run(ctx context.Context, now time.Time) error {
	if !t.cfg.Enabled {
		return nil
	}

	// Find where the last run stopped at, if any.
	offResp, err := t.adminClient.FetchOffsetsForTopics(ctx, t.cfg.ConsumerGroup, t.cfg.Kafka.Topic)
	if err != nil {
		if errors.Is(err, kerr.GroupIDNotFound) {
			// ignore.
			err = nil
		}
		return fmt.Errorf("fetch offsets error: %w", err)
	}
	if err := offResp.Error(); err != nil {
		return fmt.Errorf("fetch offsets response error: %w", err)
	}
	committedOffsets := offResp.Offsets()

	// Get the absolute latest offsets, so we can look at the total amount to process for the current run.
	endOffsetsResp, err := t.adminClient.ListEndOffsets(ctx, t.cfg.Kafka.Topic)
	if err != nil {
		return fmt.Errorf("fetch end offsets error: %w", err)
	}
	if endOffsetsResp.Error() != nil {
		return fmt.Errorf("fetch end offsets response error: %w", err)
	}
	allPartitionEndOffsets := endOffsetsResp.Offsets()
	endOffsets := allPartitionEndOffsets[t.cfg.Kafka.Topic]

	// Build a set of offsets to start reading from.
	startPartitions := []int32{}
	startOffsets := map[string]map[int32]kgo.Offset{t.cfg.Kafka.Topic: {}}
	totalOffsetDiff := int64(0)
	for partition, endOffset := range endOffsets {
		// If we never committed an offset for this partition yet, we just ignore it on the current run.
		// It likely has a ton of history that hasn't been processed yet. We'll still seek to the end later on and pick up on the next time around.
		committedOffset, ok := committedOffsets.Lookup(t.cfg.Kafka.Topic, partition)
		if !ok {
			continue
		}

		// If there are a huge number of records written to a partition, skip it. We'll seek to the end and process it next run.
		diff := endOffset.At - committedOffset.At
		if diff > int64(t.cfg.MaxJumpLimitPerPartition) {
			level.Debug(t.logger).Log(
				"msg", "skipping partition because it jumped by an amount greater than the limit per run",
				"partition", partition,
				"limit", t.cfg.MaxJumpLimitPerPartition,
				"actual", diff,
			)
			continue
		}
		totalOffsetDiff += diff
		startOffsets[t.cfg.Kafka.Topic][partition] = kgo.NewOffset().At(committedOffset.At)
		startPartitions = append(startPartitions, partition)
	}

	recordsRemainingInBatch := (totalOffsetDiff / 100) * int64(t.cfg.RecordsProcessedPercent)
	if recordsRemainingInBatch > int64(t.cfg.MaxRecordsPerRun) {
		level.Info(t.logger).Log("msg", "the expected number of records is larger than the limit", "expectedRecordsToProcess", recordsRemainingInBatch, "limit", t.cfg.MaxRecordsPerRun)
		recordsRemainingInBatch = int64(t.cfg.MaxRecordsPerRun)
	}

	t.client.AddConsumePartitions(startOffsets)

	// Spin up workers to evaluate records in parallel.
	const numWorkers = 4
	jobs := make(chan kgo.Fetches)
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				err := t.testBatch(job)
				if err != nil {
					// Log errors, but don't fail them. We still want to commit the end offsets and continue,
					// that way a single failed record doesn't make the tool get stuck in time and stay there forever.
					level.Error(t.logger).Log("msg", "test failed", "reason", err)
				}
			}
		}()
	}

	// Poll fetches, until we've consumed a satisfactory number of records, and give work to the workers.
	recordsProcessedThisBatch := 0
	numFetches := 0
	for recordsRemainingInBatch > 0 {
		if ctx.Err() != nil {
			return err
		}
		fetches := t.client.PollRecords(ctx, t.cfg.MaxRecordsPerRun)
		if errs := fetches.Errors(); len(errs) > 0 {
			level.Error(t.logger).Log("msg", "fetch errors", "err", fetches.Err())
			break
		}
		numFetches++
		recordsRemainingInBatch -= int64(fetches.NumRecords())
		recordsProcessedThisBatch += fetches.NumRecords()

		jobs <- fetches
	}
	t.client.RemoveConsumePartitions(map[string][]int32{t.cfg.Kafka.Topic: startPartitions})
	level.Info(t.logger).Log("msg", "fetches for run complete, waiting for workers to finish processing", "count", numFetches)
	close(jobs)
	wg.Wait()

	timeTaken := time.Since(now)
	level.Info(t.logger).Log("msg", "run complete", "size", recordsProcessedThisBatch, "totalOffsetsIncrease", totalOffsetDiff, "time", timeTaken)

	// In case we stopped polling early due to volume, seek everything to the latest offsets. The next run wil pick up from there.
	// We seek to the end of all partitions, including the ones we skipped this run, so they will be included next time.
	_, err = t.adminClient.CommitOffsets(ctx, t.cfg.ConsumerGroup, allPartitionEndOffsets)
	if err != nil {
		level.Error(t.logger).Log("msg", "failed to commit offsets", "err", err)
		return err
	}

	return nil
}

// testBatch runs the test on each record in a fetch, stopping at the first record that fails.
func (t *IngestStorageRecordTest) testBatch(fetches kgo.Fetches) error {
	report := batchReport{}
	fetches.EachRecord(func(rec *kgo.Record) {
		// Only log the first failure in a batch, to keep the logs from being spammed.
		if report.err != nil {
			return
		}

		tenantID := string(rec.Key)
		t.metrics.recordsProcessedTotal.WithLabelValues(tenantID).Inc()
		report = t.testRec(rec, report)
		if report.err != nil {
			level.Error(t.logger).Log("msg", "a record failed the test", "user", tenantID, "err", report.err)
		}
	})

	report.Observe(t.metrics)
	return report.err
}

// batchReport contains the running results of testing a batch.
type batchReport struct {
	err                   error
	avgCompressionRatio   float64
	throughputBytes       int64
	recordsSeen           int
	recordsSizeMismatched int
}

func (b batchReport) Error(err error) batchReport {
	b.err = err
	return b
}

func (b batchReport) Track(v1buf, v2buf []byte) batchReport {
	ratio := float64(len(v1buf)) / float64(len(v2buf))

	// assume x is the current running avg, y is the number of values processed, and z is the new value.
	// then xy is the total sum of values processed, meaning (xy+z) is the updated sum and (y+1) is the updated count.
	// thus the updated running average is  (xy+z)/(y+1).
	b.avgCompressionRatio = ((b.avgCompressionRatio * float64(b.recordsSeen)) + ratio) / float64(b.recordsSeen+1)
	b.recordsSeen++

	b.throughputBytes += int64(len(v2buf))

	if len(v2buf) > len(v1buf) {
		b.recordsSizeMismatched++
	}

	return b
}

func (b batchReport) Observe(metrics *IngestStorageRecordTestMetrics) {
	metrics.batchesProcessedTotal.Inc()
	metrics.recordsWithGrowth.Add(float64(b.recordsSizeMismatched))
	if b.err != nil {
		metrics.batchesFailedTotal.Inc()
	}
	if b.err == nil {
		metrics.avgCompressionRatio.Set(b.avgCompressionRatio)
		metrics.throughputBytes.Add(float64(b.throughputBytes))
	}
}

func (t *IngestStorageRecordTest) testRec(rec *kgo.Record, report batchReport) batchReport {
	tenantID := string(rec.Key)
	req := mimirpb.PreallocWriteRequest{}
	defer mimirpb.ReuseSlice(req.Timeseries)

	version := ingest.ParseRecordVersion(rec)
	if version > ingest.LatestRecordVersion {
		return report.Error(fmt.Errorf("received a record with an unsupported version: %d, max supported version: %d", version, ingest.LatestRecordVersion))
	}

	err := ingest.DeserializeRecordContent(rec.Value, &req, version)
	if err != nil {
		return report.Error(fmt.Errorf("failed to unmarshal record from ingest topic: %w", err))
	}

	ser := ingest.RecordSerializerFromVersion(2)
	v2Records, err := ser.ToRecords(rec.Partition, string(rec.Key), &req.WriteRequest, t.cfg.Kafka.ProducerMaxRecordSizeBytes)
	if err != nil {
		return report.Error(fmt.Errorf("failed to serialize to v2: %w", err))
	}
	assert.Always(len(v2Records) == 1, "continuous-test: V1 record does not split into multiple V2 records", Details{
		"count":  len(v2Records),
		"tenant": tenantID,
	})
	if len(v2Records) == 0 {
		return report.Error(fmt.Errorf("no records returned after v2 conversion"))
	}
	if len(v2Records) > 1 {
		return report.Error(fmt.Errorf("a V1 record was split when converted to its smaller V2 counterpart. This is highly unusual"))
	}
	v2Rec := v2Records[0]

	keyMatches := string(rec.Key) == string(v2Rec.Key)
	assert.Always(keyMatches, "continuous-test: ingest record key survives V1-to-V2 roundtrip", Details{
		"original": string(rec.Key),
		"v2":       string(v2Rec.Key),
	})
	if !keyMatches {
		return report.Error(fmt.Errorf("key did not match, got: %s, expected: %s", string(v2Rec.Key), string(rec.Key)))
	}

	v2Req := mimirpb.PreallocWriteRequest{}
	defer mimirpb.ReuseSlice(v2Req.Timeseries)
	err = ingest.DeserializeRecordContent(v2Rec.Value, &v2Req, 2)
	if err != nil {
		return report.Error(fmt.Errorf("failed to unmarshal V2 record: %w", err))
	}

	assert.Always(v2Req.SkipLabelValidation == req.SkipLabelValidation, "continuous-test: SkipLabelValidation matches after V2 roundtrip", Details{
		"original": req.SkipLabelValidation,
		"v2":       v2Req.SkipLabelValidation,
		"tenant":   tenantID,
	})
	if v2Req.SkipLabelValidation != req.SkipLabelValidation {
		return report.Error(fmt.Errorf("skipLabelValidation did not match, original: %t, v2: %t", req.SkipLabelValidation, v2Req.SkipLabelValidation))
	}
	if v2Req.SkipLabelCountValidation != req.SkipLabelCountValidation {
		return report.Error(fmt.Errorf("skipLabelCountValidation did not match, original: %t, v2: %t", req.SkipLabelCountValidation, v2Req.SkipLabelCountValidation))
	}
	assert.Always(v2Req.Source == req.Source, "continuous-test: Source matches after V2 roundtrip", Details{
		"original": req.Source,
		"v2":       v2Req.Source,
		"tenant":   tenantID,
	})
	if v2Req.Source != req.Source {
		return report.Error(fmt.Errorf("source did not match, original: %d, v2: %d", req.Source, v2Req.Source))
	}
	if v2Req.SymbolsRW2 != nil {
		return report.Error(fmt.Errorf("v2 record had a SymbolsRW2 unmarshalling field left populated"))
	}
	if v2Req.TimeseriesRW2 != nil {
		return report.Error(fmt.Errorf("v2 record had a TimeseriesRW2 unmarshalling field left populated"))
	}

	assert.Sometimes(len(req.Metadata) > 0, "continuous-test: ingest records containing metadata are processed", Details{
		"tenant":        tenantID,
		"metadata_count": len(req.Metadata),
	})

	if len(req.Metadata) != 0 || len(v2Req.Metadata) != 0 {
		t.metrics.metadataProcessedTotal.WithLabelValues(tenantID).Add(float64(len(req.Metadata)))
		sortMetadata := cmpopts.SortSlices(func(m1, m2 *mimirpb.MetricMetadata) bool {
			return m1.MetricFamilyName < m2.MetricFamilyName
		})
		clean := func(m []*mimirpb.MetricMetadata) []*mimirpb.MetricMetadata {
			return dropExactDuplicates(dropMeaninglessMetadata(m))
		}
		if !cmp.Equal(clean(req.Metadata), clean(v2Req.Metadata), sortMetadata) {
			diff := cmp.Diff(clean(req.Metadata), clean(v2Req.Metadata), sortMetadata)
			return report.Error(fmt.Errorf("metadata did not match (adjusting for ordering, exact duplicates dropped). numTimeseries: %d, numMetadata: %d,\noriginalMetadata: %v,\nv2Metadata: %v\n Diff: %s", len(req.Timeseries), len(req.Metadata), req.Metadata, v2Req.Metadata, diff))
		}
	}

	req.ClearTimeseriesUnmarshalData() // We do not want to match on gRPC buffers used only in an optimization.
	if len(req.Timeseries) != 0 || len(v2Req.Timeseries) != 0 {
		t.metrics.timeseriesProcessedTotal.WithLabelValues(tenantID).Add(float64(len(req.Timeseries)))
		// No metadata -> Same number of timeseries must be in v2 as in the original.
		// Yes metadata -> There may be extra metadata carrier timeseries in the result, but they must be empty. We'll assert that later.
		if len(req.Metadata) == 0 {
			if len(req.Timeseries) != len(v2Req.Timeseries) {
				return report.Error(fmt.Errorf("mismatched count of timeseries on a request with no metadata, orig: %d, v2: %d", len(req.Timeseries), len(v2Req.Timeseries)))
			}
		} else {
			if len(req.Timeseries) > len(v2Req.Timeseries) {
				return report.Error(fmt.Errorf("too few timeseries returned on a request carrying metadata, orig: %d, v2: %d", len(req.Timeseries), len(v2Req.Timeseries)))
			}
		}
		extraTimeseriesCount := len(v2Req.Timeseries) - len(req.Timeseries)

		// For each original timeseries, the corresponding out timeseries should match.
		for i := range req.Timeseries {
			t.metrics.samplesProcessedTotal.WithLabelValues(tenantID).Add(float64(len(req.Timeseries[i].Samples)))
			t.metrics.exemplarsProcessedTotal.WithLabelValues(tenantID).Add(float64(len(req.Timeseries[i].Exemplars)))
			t.metrics.histogramsProcessedTotal.WithLabelValues(tenantID).Add(float64(len(req.Timeseries[i].Histograms)))
			tsEqual := TimeseriesEqual(req.Timeseries[i].TimeSeries, v2Req.Timeseries[i].TimeSeries)
			assert.Always(tsEqual, "continuous-test: timeseries data survives V1-to-V2 roundtrip", Details{
				"index":  i,
				"tenant": tenantID,
			})
			if !tsEqual {
				return report.Error(fmt.Errorf("timeseries do not match. Index: %d, orig: %v, v2: %v", i, req.Timeseries[i].TimeSeries, v2Req.Timeseries[i].TimeSeries))
			}
		}

		// All remaining timeseries in V2 should be empty as they were fabricated to carry metadata and appended to the end. If they contain any data, the result is wrong.
		if extraTimeseriesCount > 0 {
			extraTimeseries := v2Req.Timeseries[len(v2Req.Timeseries)-extraTimeseriesCount:]
			for _, extra := range extraTimeseries {
				assert.Always(len(extra.Samples) == 0, "continuous-test: metadata-carrier timeseries has no samples", Details{"tenant": tenantID})
				if len(extra.Samples) > 0 {
					return report.Error(fmt.Errorf("extra timeseries (that did not match to an input timeseries) contained samples: %v", extra))
				}
				assert.Always(len(extra.Exemplars) == 0, "continuous-test: metadata-carrier timeseries has no exemplars", Details{"tenant": tenantID})
				if len(extra.Exemplars) > 0 {
					return report.Error(fmt.Errorf("extra timeseries (that did not match to an input timeseries) contained exemplars: %v", extra))
				}
				assert.Always(len(extra.Histograms) == 0, "continuous-test: metadata-carrier timeseries has no histograms", Details{"tenant": tenantID})
				if len(extra.Histograms) > 0 {
					return report.Error(fmt.Errorf("extra timeseries (that did not match to an input timeseries) contained histograms: %v", extra))
				}
			}
		}
	}

	return report.Track(rec.Value, v2Rec.Value)
}

func dropExactDuplicates(metadata []*mimirpb.MetricMetadata) []*mimirpb.MetricMetadata {
	metadataByMetric := make(map[string][]*mimirpb.MetricMetadata, len(metadata))
	result := make([]*mimirpb.MetricMetadata, 0, len(metadata))

metaLoop:
	for _, meta := range metadata {
		seen, ok := metadataByMetric[meta.MetricFamilyName]
		if !ok {
			metadataByMetric[meta.MetricFamilyName] = append(metadataByMetric[meta.MetricFamilyName], meta)
			result = append(result, meta)
			continue
		}

		for _, seenMeta := range seen {
			if seenMeta.MetricFamilyName == meta.MetricFamilyName && seenMeta.Help == meta.Help && seenMeta.Type == meta.Type && seenMeta.Unit == meta.Unit {
				continue metaLoop
			}
		}

		metadataByMetric[meta.MetricFamilyName] = append(metadataByMetric[meta.MetricFamilyName], meta)
		result = append(result, meta)
	}

	return result
}

// A metadata that doesn't carry a unit, help, or type info, has no effect.
// There is no need to send such metadata through v2 in the first place.
func dropMeaninglessMetadata(metadata []*mimirpb.MetricMetadata) []*mimirpb.MetricMetadata {
	result := make([]*mimirpb.MetricMetadata, 0, len(metadata))
	for _, meta := range metadata {
		if meta.Unit != "" || meta.Help != "" || meta.Type != 0 {
			result = append(result, meta)
		}
	}
	return result
}

// TimeseriesEqual is a copy of mimirpb.TimeSeries.Equal that calls SampleEqual, ExemplarEqual, and HistogramEqual instead.
func TimeseriesEqual(this *mimirpb.TimeSeries, that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*mimirpb.TimeSeries)
	if !ok {
		that2, ok := that.(mimirpb.TimeSeries)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if this.CreatedTimestamp != that1.CreatedTimestamp {
		return false
	}
	if len(this.Labels) != len(that1.Labels) {
		return false
	}
	for i := range this.Labels {
		if !this.Labels[i].Equal(that1.Labels[i]) {
			return false
		}
	}
	if len(this.Samples) != len(that1.Samples) {
		return false
	}
	for i := range this.Samples {
		if !SampleEqual(&this.Samples[i], &that1.Samples[i]) {
			return false
		}
	}
	if len(this.Exemplars) != len(that1.Exemplars) {
		return false
	}
	for i := range this.Exemplars {
		if !ExemplarEqual(&this.Exemplars[i], &that1.Exemplars[i]) {
			return false
		}
	}
	if len(this.Histograms) != len(that1.Histograms) {
		return false
	}
	for i := range this.Histograms {
		if !HistogramEqual(&this.Histograms[i], &that1.Histograms[i]) {
			return false
		}
	}
	return true
}

// SampleEqual is a copy of mimirpb.Sample.Equal but equates NaN values.
func SampleEqual(this *mimirpb.Sample, that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*mimirpb.Sample)
	if !ok {
		that2, ok := that.(mimirpb.Sample)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if this.TimestampMs != that1.TimestampMs {
		return false
	}
	if !floatEqualsEquateNaN(this.Value, that1.Value) {
		return false
	}
	return true
}

// ExemplarEqual is a copy of mimirpb.Exemplar.Equal but equates NaN values.
func ExemplarEqual(this *mimirpb.Exemplar, that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*mimirpb.Exemplar)
	if !ok {
		that2, ok := that.(mimirpb.Exemplar)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if len(this.Labels) != len(that1.Labels) {
		return false
	}
	for i := range this.Labels {
		if !this.Labels[i].Equal(that1.Labels[i]) {
			return false
		}
	}
	if !floatEqualsEquateNaN(this.Value, that1.Value) {
		return false
	}
	if this.TimestampMs != that1.TimestampMs {
		return false
	}
	return true
}

// HistogramEqual is a copy of mimirpb.Histogram.Equal but equates NaN values.
func HistogramEqual(this *mimirpb.Histogram, that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*mimirpb.Histogram)
	if !ok {
		that2, ok := that.(mimirpb.Histogram)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if that1.Count == nil {
		if this.Count != nil {
			return false
		}
	} else if this.Count == nil {
		return false
	} else if !this.Count.Equal(that1.Count) {
		return false
	}
	if !floatEqualsEquateNaN(this.Sum, that1.Sum) {
		return false
	}
	if this.Schema != that1.Schema {
		return false
	}
	if !floatEqualsEquateNaN(this.ZeroThreshold, that1.ZeroThreshold) {
		return false
	}
	if that1.ZeroCount == nil {
		if this.ZeroCount != nil {
			return false
		}
	} else if this.ZeroCount == nil {
		return false
	} else if !this.ZeroCount.Equal(that1.ZeroCount) {
		return false
	}
	if len(this.NegativeSpans) != len(that1.NegativeSpans) {
		return false
	}
	for i := range this.NegativeSpans {
		if !this.NegativeSpans[i].Equal(&that1.NegativeSpans[i]) {
			return false
		}
	}
	if len(this.NegativeDeltas) != len(that1.NegativeDeltas) {
		return false
	}
	for i := range this.NegativeDeltas {
		if this.NegativeDeltas[i] != that1.NegativeDeltas[i] {
			return false
		}
	}
	if len(this.NegativeCounts) != len(that1.NegativeCounts) {
		return false
	}
	for i := range this.NegativeCounts {
		if !floatEqualsEquateNaN(this.NegativeCounts[i], that1.NegativeCounts[i]) {
			return false
		}
	}
	if len(this.PositiveSpans) != len(that1.PositiveSpans) {
		return false
	}
	for i := range this.PositiveSpans {
		if !this.PositiveSpans[i].Equal(&that1.PositiveSpans[i]) {
			return false
		}
	}
	if len(this.PositiveDeltas) != len(that1.PositiveDeltas) {
		return false
	}
	for i := range this.PositiveDeltas {
		if this.PositiveDeltas[i] != that1.PositiveDeltas[i] {
			return false
		}
	}
	if len(this.PositiveCounts) != len(that1.PositiveCounts) {
		return false
	}
	for i := range this.PositiveCounts {
		if !floatEqualsEquateNaN(this.PositiveCounts[i], that1.PositiveCounts[i]) {
			return false
		}
	}
	if this.ResetHint != that1.ResetHint {
		return false
	}
	if this.Timestamp != that1.Timestamp {
		return false
	}
	if len(this.CustomValues) != len(that1.CustomValues) {
		return false
	}
	for i := range this.CustomValues {
		if !floatEqualsEquateNaN(this.CustomValues[i], that1.CustomValues[i]) {
			return false
		}
	}
	return true
}

func floatEqualsEquateNaN(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}

	return a == b
}
