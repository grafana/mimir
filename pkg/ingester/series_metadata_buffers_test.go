// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// The ingester keeps nothing of the records it ingests, the series' own metadata and resources included: once it has
// ingested them, their buffers are collected, and reading what it keeps after they're freed doesn't fault. A canary
// that retains one help string fails both checks.
func TestIngesterRetainsNoRecordBuffers(t *testing.T) {
	skipIfSeriesstore(t, "it runs both engines itself")
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("freed buffers fault only on linux and darwin")
	}

	t.Run("collected", func(t *testing.T) {
		// Records aren't copied into instrumented pages, so the ingester gets the buffers allocated here.
		registerCodec(t, mimirpb.InstrumentRefLeaksConfig{})
		for _, canary := range []bool{false, true} {
			t.Run(fmt.Sprintf("canary=%t", canary), func(t *testing.T) {
				ingester, kafkaCfg := retainedBuffersIngester(t, mimir_tsdb.EnginePrometheus, 8)
				var pusher ingest.Pusher = ingester
				if canary {
					pusher = &retainingPusher{Ingester: ingester}
				}
				consumer := ingest.NewPusherConsumer(pusher, kafkaCfg, ingest.NewPusherConsumerMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())

				var records, collected atomic.Int64
				func() {
					for step := range 2 {
						batch := retainedBuffersRecords(t, step)
						for _, record := range batch {
							runtime.AddCleanup(unsafe.SliceData(record.Value), func(*atomic.Int64) { collected.Add(1) }, &collected)
						}
						records.Add(int64(len(batch)))
						require.NoError(t, consumer.Consume(context.Background(), slices.Values(batch)))
					}
				}()
				// Pooled objects that still point into a buffer are dropped by the second collection, with nothing
				// decoded in between to take them out of the pool.
				runtime.GC()
				runtime.GC()
				deadline := time.Now().Add(3 * time.Second)
				for collected.Load() < records.Load() && time.Now().Before(deadline) {
					runtime.GC()
					time.Sleep(10 * time.Millisecond)
				}
				// What retains a buffer has to outlive the check, as the ingester does.
				defer runtime.KeepAlive(pusher)
				if canary {
					require.Less(t, collected.Load(), records.Load(), "the retained help string keeps its buffer")
					return
				}
				require.Equal(t, records.Load(), collected.Load(), "buffers collected")
				requireIntactMetadata(t, ingester, true)
			})
		}
	})

	t.Run("freed", func(t *testing.T) {
		// Freed record buffers stay mapped but can't be read for the rest of the test, so that no other buffer takes
		// their place.
		registerCodec(t, mimirpb.InstrumentRefLeaksConfig{Percentage: 100, BeforeReusePeriod: time.Hour})
		for _, test := range []struct {
			engine string
			canary bool
		}{
			{engine: mimir_tsdb.EnginePrometheus},
			{engine: mimir_tsdb.EngineSeriesstore},
			{engine: mimir_tsdb.EnginePrometheus, canary: true},
		} {
			t.Run(fmt.Sprintf("engine=%s canary=%t", test.engine, test.canary), func(t *testing.T) {
				// The sequential pusher, and PushRecord, push on the calling goroutine, the only one where a fault
				// can be recovered from.
				ingester, kafkaCfg := retainedBuffersIngester(t, test.engine, 0)
				var pusher ingest.Pusher = ingester
				if test.canary {
					pusher = &retainingPusher{Ingester: ingester}
				}
				consumer := ingest.NewPusherConsumer(pusher, kafkaCfg, ingest.NewPusherConsumerMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
				ctx := user.InjectOrgID(context.Background(), "tenant")

				fault := func() (fault any) {
					prev := debug.SetPanicOnFault(true)
					defer debug.SetPanicOnFault(prev)
					defer func() { fault = recover() }()
					// The second push compares the series' metadata with what the first one left.
					for step := range 2 {
						for _, record := range retainedBuffersRecords(t, step) {
							if test.engine == mimir_tsdb.EngineSeriesstore {
								// The first record, before the tenant has a TSDB, is decoded, and the second read in
								// place.
								require.NoError(t, ingester.PushRecord(ctx, record.Value, ingest.ParseRecordVersion(record)))
								continue
							}
							require.NoError(t, consumer.Consume(context.Background(), slices.Values([]*kgo.Record{record})))
						}
						readEverything(t, ingester)
						if test.canary {
							retainedCopy = strings.Clone(pusher.(*retainingPusher).retained)
						}
					}
					return nil
				}()
				if test.canary {
					require.NotNil(t, fault, "reading the retained help string faults")
					return
				}
				require.Nil(t, fault)
				requireIntactMetadata(t, ingester, true)
			})
		}
	})
}

// retainedCopy is where the canary reads the retained help string to.
var retainedCopy string

// retainingPusher retains one help string of the series' own metadata, a canary for the checks that nothing is.
type retainingPusher struct {
	*Ingester
	retained string
}

func (p *retainingPusher) PushToStorageAndReleaseRequest(ctx context.Context, req *mimirpb.WriteRequest) error {
	if p.retained == "" && len(req.Timeseries) > 0 {
		p.retained = req.Timeseries[0].SeriesMetadata.Help
	}
	return p.Ingester.PushToStorageAndReleaseRequest(ctx, req)
}

// registerCodec registers the gRPC codec with cfg for the test, and the test binary's codec again after it.
func registerCodec(t *testing.T, cfg mimirpb.InstrumentRefLeaksConfig) {
	mimirpb.CustomCodecConfig{InstrumentRefLeaksConfig: cfg}.RegisterGlobally(prometheus.NewRegistry())
	t.Cleanup(func() {
		mimirpb.CustomCodecConfig{InstrumentRefLeaksConfig: mimirpb.InstrumentRefLeaksConfig{Percentage: 100}}.RegisterGlobally(prometheus.NewRegistry())
	})
}

// retainedBuffersIngester is a running ingester on the engine that stores the tenant's native metric metadata and
// resources, and the Kafka config to push records to it with, concurrently or not.
func retainedBuffersIngester(t *testing.T, engine string, concurrency int) (*Ingester, ingest.KafkaConfig) {
	limits := defaultLimitsTestConfig()
	limits.NativeMetricMetadataEnabled = true
	limits.NativeResourcesEnabled = true
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.Engine = engine
	cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyMax = concurrency
	if concurrency > 0 {
		cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyBatchSize = 150
		cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyEstimatedBytesPerSample = 200
		cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyQueueCapacity = 3
		cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyTargetFlushesPerShard = 40
	}
	ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, validation.NewOverrides(limits, nil), nil, nil, nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })
	return ingester, cfg.IngestStorageConfig.KafkaConfig
}

const retainedBuffersSeries = 300

// retainedBuffersRecords are the records of a request a minute after the previous step's: series of two families,
// with the same metadata and resource of their own at every step, in buffers of their own.
func retainedBuffersRecords(t *testing.T, step int) []*kgo.Record {
	timestamp := time.Now().Truncate(time.Minute).Add(-time.Hour).UnixMilli() + int64(step)*60_000
	req := &mimirpb.WriteRequest{Source: mimirpb.OTLP, ResourceTable: []mimirpb.ResourceEntry{retainedBuffersResource(0), retainedBuffersResource(1)}}
	for n := range retainedBuffersSeries {
		family, metadata := "requests_total", mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Requests served.", Unit: "requests"}
		if n%2 == 1 {
			family, metadata = "errors_total", mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Errors returned."}
		}
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:         []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: family}, {Name: "pod", Value: fmt.Sprintf("pod-%03d", n)}},
			Samples:        []mimirpb.Sample{{TimestampMs: timestamp, Value: float64(n)}},
			SeriesMetadata: metadata,
			ResourceRef:    uint32(n%3%2 + 1),
		}})
	}
	req.Metadata = []*mimirpb.MetricMetadata{
		{MetricFamilyName: "requests_total", Type: mimirpb.COUNTER, Help: "Requests served.", Unit: "requests"},
		{MetricFamilyName: "errors_total", Type: mimirpb.COUNTER, Help: "Errors returned."},
	}
	records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, "tenant", req, 1<<20)
	require.NoError(t, err)
	for _, record := range records {
		record.Value = bytes.Clone(record.Value)
		record.Context = context.Background()
	}
	return records
}

// retainedBuffersResource is the series' resource: of the first instance, or of the second.
func retainedBuffersResource(instance int) mimirpb.ResourceEntry {
	return mimirpb.ResourceEntry{Attributes: []mimirpb.ResourceAttribute{
		{Key: "service.name", StringValue: "checkout"},
		{Key: "service.instance.id", StringValue: fmt.Sprintf("checkout-%d", instance)},
		{Key: "host.name", StringValue: fmt.Sprintf("node-%d", instance)},
		{Key: "host.cores", Type: mimirpb.RESOURCE_ATTRIBUTE_INT, IntValue: 8},
	}}
}

// readEverything reads all that the ingester keeps of the tenant's series, through the engine and on the calling
// goroutine: their labels, the families' and the series' own metadata, and the series' resources.
func readEverything(t *testing.T, ingester *Ingester) {
	db := ingester.getTSDB("tenant")
	require.NotNil(t, db)
	querier, err := db.db.Querier(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	var read strings.Builder
	set := querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"))
	series := 0
	for set.Next() {
		read.WriteString(set.At().Labels().String())
		series++
	}
	require.NoError(t, set.Err())
	require.NoError(t, querier.Close())
	require.Equal(t, retainedBuffersSeries, series)

	response, err := ingester.MetricsMetadata(user.InjectOrgID(context.Background(), "tenant"), &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1})
	require.NoError(t, err)
	for _, m := range response.Metadata {
		read.WriteString(m.MetricFamilyName + m.Help + m.Unit)
	}

	result, err := db.nativeMetadata.nativeMetricMetadata(context.Background(), [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")}}, 0)
	if err == nil {
		for _, series := range result.Series {
			for _, version := range series.Versions {
				read.WriteString(string(version.Metadata.Type) + version.Metadata.Help + version.Metadata.Unit)
			}
		}
	} else {
		require.ErrorIs(t, err, tsdb.ErrNativeMetadataDisabled)
	}

	resources, err := db.nativeResources.nativeResources(context.Background(), [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")}}, math.MinInt64, math.MaxInt64, 0)
	require.NoError(t, err)
	for _, series := range resources.Series {
		for _, version := range series.Versions {
			for _, attributes := range []map[string]string{version.Identifying, version.Descriptive} {
				for key, value := range attributes {
					read.WriteString(key + value)
				}
			}
		}
	}
	require.NotZero(t, read.Len())
}

// requireIntactMetadata checks the families' metadata the ingester keeps, and with native, the series' own metadata
// and resources.
func requireIntactMetadata(t *testing.T, ingester *Ingester, native bool) {
	response, err := ingester.MetricsMetadata(user.InjectOrgID(context.Background(), "tenant"), &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1})
	require.NoError(t, err)
	var families []string
	for _, m := range response.Metadata {
		families = append(families, fmt.Sprintf("%s|%s|%s|%s", m.MetricFamilyName, m.Type, m.Help, m.Unit))
	}
	slices.Sort(families)
	require.Equal(t, []string{"errors_total|COUNTER|Errors returned.|", "requests_total|COUNTER|Requests served.|requests"}, families)
	if !native {
		return
	}

	result, err := ingester.getTSDB("tenant").nativeMetadata.nativeMetricMetadata(context.Background(), [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")}}, 0)
	require.NoError(t, err)
	require.Len(t, result.Series, retainedBuffersSeries)
	for _, series := range result.Series {
		expected := metadata.Metadata{Type: model.MetricTypeCounter, Help: "Requests served.", Unit: "requests"}
		if series.Labels.Get(model.MetricNameLabel) == "errors_total" {
			expected = metadata.Metadata{Type: model.MetricTypeCounter, Help: "Errors returned."}
		}
		require.Len(t, series.Versions, 1, series.Labels.String())
		require.Equal(t, expected, series.Versions[0].Metadata)
	}

	resources, err := ingester.getTSDB("tenant").nativeResources.nativeResources(context.Background(), [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")}}, math.MinInt64, math.MaxInt64, 0)
	require.NoError(t, err)
	require.Len(t, resources.Series, retainedBuffersSeries)
	for _, series := range resources.Series {
		var pod int
		_, err := fmt.Sscanf(series.Labels.Get("pod"), "pod-%d", &pod)
		require.NoError(t, err)
		instance := pod % 3 % 2
		require.Len(t, series.Versions, 1, series.Labels.String())
		require.Equal(t, map[string]string{"service.name": "checkout", "service.instance.id": fmt.Sprintf("checkout-%d", instance)}, series.Versions[0].Identifying)
		require.Equal(t, map[string]string{"host.name": fmt.Sprintf("node-%d", instance), "host.cores": "8"}, series.Versions[0].Descriptive)
	}
}
