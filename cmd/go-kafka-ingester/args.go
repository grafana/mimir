// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"flag"
	"runtime"
	"strings"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/limits"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/protection"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/store"
)

type serveArgs struct {
	brokers                string
	topic                  string
	additionalKafkaCluster limits.StringSlice
	partition              int
	dataDir                string
	segmentSyncIntervalMs  uint64
	listen                 string
	profileListen          string
	// With no persisted offset: `earliest`, `latest` or an offset. Without it, the pod starts
	// --retention-seconds back, or at the earliest offset without a retention.
	startOffset         string
	activeWindowSeconds int64
	retentionSeconds    optionalInt
	// With no persisted offset, start at the first Kafka record at most this many seconds old;
	// takes precedence over --start-offset.
	bootstrapLookbackSeconds optionalInt
	// Goroutines that apply records to store shards in parallel; defaults to the available CPUs.
	ingestThreads int
	// Store shards; a restored head snapshot keeps the shard count it was written with.
	storeShards               int
	readCompartment           int
	consistencyTimeoutSeconds uint64
	saslUsername              string
	saslPassword              string
	saslMechanism             string
	kafkaTLS                  bool
	grpcTLSCert               string
	grpcTLSKey                string
	// Serves Prometheus metrics on /metrics and the cost attribution registry path.
	metricsListen                string
	costAttributionRegistry      string
	metadataRetainPeriod         string
	activeSeriesUpdatePeriod     string
	activePartitionsURL          string
	ownedTokenRangesURL          string
	sidecarMetricsURL            string
	activeSeriesIdleTimeout      string
	ownedSeriesUpdateInterval    string
	trackOwnedSeries             bool
	useOwnedSeriesForLimits      bool
	headCompactionInterval       string
	ingestionConcurrencyMax      int
	ingestionConcurrencyBatch    int
	ingestionBytesPerSample      int
	ingestionTargetFlushes       int
	earlyCompactionMinSeries     uint64
	earlyCompactionMinPercent    uint64
	nonOwnedCompactionEnabled    bool
	nonOwnedMinGracePeriod       string
	nonOwnedMaxGracePeriod       string
	gracefulShutdownTimeout      string
	costAttributionCleanup       string
	costAttributionEviction      string
	limits                       limits.Args
	instanceLimits               limits.InstanceLimits
	runtimeConfig                limits.RuntimeConfigArgs
	protection                   protection.Args
	saslUsernameSet, passwordSet bool
}

// optionalInt is an integer flag that may be left unset.
type optionalInt struct {
	value int64
	set   bool
}

func (o *optionalInt) String() string {
	if !o.set {
		return ""
	}
	return formatInt(o.value)
}

func (o *optionalInt) Set(value string) error {
	parsed, err := parseInt(value)
	if err != nil {
		return err
	}
	o.value, o.set = parsed, true
	return nil
}

func parseServeArgs(args []string) (serveArgs, error) {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	parsed := serveArgs{limits: limits.DefaultArgs(), instanceLimits: limits.DefaultInstanceLimits()}
	flags.StringVar(&parsed.brokers, "brokers", "", "")
	flags.StringVar(&parsed.topic, "topic", "", "")
	flags.Var(&parsed.additionalKafkaCluster, "additional-kafka-cluster", "TOPIC=BROKER[,BROKER...]")
	flags.IntVar(&parsed.partition, "partition", -1, "")
	flags.StringVar(&parsed.dataDir, "data-dir", "", "")
	flags.Uint64Var(&parsed.segmentSyncIntervalMs, "segment-sync-interval-ms", 60_000, "")
	flags.StringVar(&parsed.listen, "listen", "0.0.0.0:9095", "")
	flags.StringVar(&parsed.profileListen, "profile-listen", "", "")
	flags.StringVar(&parsed.startOffset, "start-offset", "", "")
	flags.Int64Var(&parsed.activeWindowSeconds, "active-window-seconds", 1200, "")
	flags.Var(&parsed.retentionSeconds, "retention-seconds", "")
	flags.Var(&parsed.bootstrapLookbackSeconds, "bootstrap-lookback-seconds", "")
	flags.IntVar(&parsed.ingestThreads, "ingest-threads", 0, "")
	flags.IntVar(&parsed.storeShards, "store-shards", store.DefaultShards, "")
	flags.IntVar(&parsed.readCompartment, "read-compartment", 0, "")
	flags.Uint64Var(&parsed.consistencyTimeoutSeconds, "consistency-timeout-seconds", 30, "")
	flags.StringVar(&parsed.saslUsername, "sasl-username", "", "")
	flags.StringVar(&parsed.saslPassword, "sasl-password", "", "")
	flags.StringVar(&parsed.saslMechanism, "sasl-mechanism", "plain", "")
	flags.BoolVar(&parsed.kafkaTLS, "kafka-tls", false, "")
	flags.StringVar(&parsed.grpcTLSCert, "grpc-tls-cert", "", "")
	flags.StringVar(&parsed.grpcTLSKey, "grpc-tls-key", "", "")
	flags.StringVar(&parsed.metricsListen, "metrics-listen", "", "")
	flags.StringVar(&parsed.costAttributionRegistry, "cost-attribution.registry-path", "", "")
	flags.StringVar(&parsed.metadataRetainPeriod, "ingester.metadata-retain-period", "10m", "")
	flags.StringVar(&parsed.activeSeriesUpdatePeriod, "ingester.active-series-metrics-update-period", "1m", "")
	// Returns the number of active partitions, to convert global limits to local ones.
	flags.StringVar(&parsed.activePartitionsURL, "active-partitions-url", "", "")
	// Returns this partition's token ranges per tenant, for owned series.
	flags.StringVar(&parsed.ownedTokenRangesURL, "owned-token-ranges-url", "", "")
	// The ring sidecar's metrics, served with the ingester's so one scrape covers the pod.
	flags.StringVar(&parsed.sidecarMetricsURL, "sidecar-metrics-url", "", "")
	// Mimir's name for --active-window-seconds, which it replaces when set.
	flags.StringVar(&parsed.activeSeriesIdleTimeout, "ingester.active-series-metrics-idle-timeout", "", "")
	flags.StringVar(&parsed.ownedSeriesUpdateInterval, "ingester.owned-series-update-interval", "15s", "")
	// Like Mimir, `cortex_ingester_owned_series` is only exported with one of these.
	flags.BoolVar(&parsed.trackOwnedSeries, "ingester.track-ingester-owned-series", false, "")
	flags.BoolVar(&parsed.useOwnedSeriesForLimits, "ingester.use-ingester-owned-series-for-limits", false, "")
	// How often the emulated Go head compacts, which bounds the memory and owned series metrics.
	flags.StringVar(&parsed.headCompactionInterval, "blocks-storage.tsdb.head-compaction-interval", "1m", "")
	// With the batch size, how Mimir groups records into head appends, which decides the
	// out-of-order checks.
	flags.IntVar(&parsed.ingestionConcurrencyMax, "ingest-storage.kafka.ingestion-concurrency-max", 8, "")
	flags.IntVar(&parsed.ingestionConcurrencyBatch, "ingest-storage.kafka.ingestion-concurrency-batch-size", 150, "")
	flags.IntVar(&parsed.ingestionBytesPerSample, "ingest-storage.kafka.ingestion-concurrency-estimated-bytes-per-sample", 200, "")
	flags.IntVar(&parsed.ingestionTargetFlushes, "ingest-storage.kafka.ingestion-concurrency-target-flushes-per-shard", 40, "")
	flags.Uint64Var(&parsed.earlyCompactionMinSeries, "blocks-storage.tsdb.early-head-compaction-min-in-memory-series", 0, "")
	flags.Uint64Var(&parsed.earlyCompactionMinPercent, "blocks-storage.tsdb.early-head-compaction-min-estimated-series-reduction-percentage", 15, "")
	flags.BoolVar(&parsed.nonOwnedCompactionEnabled, "ingester.early-compaction-non-owned-series-enabled", false, "")
	flags.StringVar(&parsed.nonOwnedMinGracePeriod, "ingester.early-compaction-non-owned-series-min-grace-period", "30s", "")
	flags.StringVar(&parsed.nonOwnedMaxGracePeriod, "ingester.early-compaction-non-owned-series-max-grace-period", "5m", "")
	// Like Mimir's, how long a shutdown waits for in-flight requests before closing them.
	flags.StringVar(&parsed.gracefulShutdownTimeout, "server.graceful-shutdown-timeout", "30s", "")
	flags.StringVar(&parsed.costAttributionCleanup, "cost-attribution.cleanup-interval", "3m", "")
	flags.StringVar(&parsed.costAttributionEviction, "cost-attribution.eviction-interval", "20m", "")
	parsed.limits.RegisterFlags(flags)
	parsed.instanceLimits.RegisterFlags(flags)
	parsed.runtimeConfig.RegisterFlags(flags)
	parsed.protection.RegisterFlags(flags)
	if err := parseFlags(flags, args); err != nil {
		return serveArgs{}, err
	}
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "sasl-username":
			parsed.saslUsernameSet = true
		case "sasl-password":
			parsed.passwordSet = true
		}
	})
	for _, required := range []string{"brokers", "topic", "partition", "data-dir"} {
		if value := flags.Lookup(required).Value.String(); value == "" || (required == "partition" && strings.HasPrefix(value, "-")) {
			return serveArgs{}, errors.New("the following required arguments were not provided: --" + required)
		}
	}
	if parsed.ingestThreads == 0 {
		parsed.ingestThreads = runtime.GOMAXPROCS(0)
	}
	return parsed, nil
}
