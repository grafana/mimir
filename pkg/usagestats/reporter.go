// SPDX-License-Identifier: AGPL-3.0-only

package usagestats

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util"
)

const (
	// DefaultReportSendInterval is the interval at which anonymous usage statistics are reported.
	DefaultReportSendInterval = 4 * time.Hour

	defaultReportCheckInterval    = time.Minute
	defaultCPUUsageSampleInterval = time.Minute

	defaultStatsServerURL = "https://stats.grafana.org/mimir-usage-report"
)

const (
	installationModeCustom  = "custom"
	installationModeHelm    = "helm"
	installationModeJsonnet = "jsonnet"
)

var (
	supportedInstallationModes = []string{installationModeCustom, installationModeHelm, installationModeJsonnet}
)

type Config struct {
	Enabled          bool   `yaml:"enabled"`
	InstallationMode string `yaml:"installation_mode"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "usage-stats.enabled", true, "Enable anonymous usage reporting.")
	f.StringVar(&cfg.InstallationMode, "usage-stats.installation-mode", installationModeCustom, fmt.Sprintf("Installation mode. Supported values: %s.", strings.Join(supportedInstallationModes, ", ")))
}

func (cfg *Config) Validate() error {
	if !util.StringsContain(supportedInstallationModes, cfg.InstallationMode) {
		return errors.Errorf("unsupported installation mode: %q", cfg.InstallationMode)

	}

	return nil
}

type Reporter struct {
	logger log.Logger
	bucket objstore.InstrumentedBucket

	// How frequently check if there's a report to send.
	reportCheckInterval time.Duration

	// How frequently send a new report.
	reportSendInterval time.Duration

	// How long to wait for a cluster seed file creation before using it.
	seedFileMinStability time.Duration

	// How frequently to sample CPU usage.
	cpuUsageSampleInterval time.Duration
	cpuUsageProc           *process.Process

	client    http.Client
	serverURL string

	services.Service

	// Metrics.
	requestsTotal       prometheus.Counter
	requestsFailedTotal prometheus.Counter
	requestsLatency     prometheus.Histogram
}

func NewReporter(bucketClient objstore.InstrumentedBucket, logger log.Logger, reg prometheus.Registerer) *Reporter {
	// The cluster seed file is stored in a prefix dedicated to Mimir internals.
	bucketClient = bucket.NewPrefixedBucketClient(bucketClient, bucket.MimirInternalsPrefix)
	proc, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		level.Debug(logger).Log("msg", "failed to get process for usage reporting; will not report CPU", "err", err)
		proc = nil // the docs on process.NewProcess doesn't specify what's returned with an error, so we make sure it's nil
	}
	r := &Reporter{
		logger:               logger,
		bucket:               bucketClient,
		client:               http.Client{Timeout: 5 * time.Second},
		serverURL:            defaultStatsServerURL,
		reportCheckInterval:  defaultReportCheckInterval,
		reportSendInterval:   DefaultReportSendInterval,
		seedFileMinStability: clusterSeedFileMinStability,

		cpuUsageSampleInterval: defaultCPUUsageSampleInterval,
		cpuUsageProc:           proc,

		requestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_stats_report_sends_total",
			Help: "The total number of attempted send requests.",
		}),
		requestsFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_stats_report_sends_failed_total",
			Help: "The total number of failed send requests.",
		}),
		requestsLatency: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_usage_stats_report_sends_latency_seconds",
			Help:    "The latency of report send requests in seconds (include both succeeded and failed requests).",
			Buckets: prometheus.DefBuckets,
		}),
	}
	r.Service = services.NewBasicService(nil, r.running, nil)
	return r
}

func (r *Reporter) running(ctx context.Context) error {
	// Init or get the cluster seed.
	seed, err := initSeedFile(ctx, r.bucket, r.seedFileMinStability, r.logger)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	if err != nil {
		return err
	}

	level.Info(r.logger).Log("msg", "usage stats reporter initialized", "cluster_id", seed.UID)

	// Keep track of the next report to send, so that we reuse the same on retries after send failures.
	var nextReport *Report
	var nextReportAt time.Time

	// We define a function to update the timestamp of the next report to make sure
	// we also reset the next report when doing it.
	scheduleNextReport := func() {
		nextReportAt = getNextReportAt(r.reportSendInterval, seed.CreatedAt, time.Now())
		nextReport = nil
	}

	// Find when to send the next report.
	scheduleNextReport()

	checkTicker := time.NewTicker(r.reportCheckInterval)
	defer checkTicker.Stop()

	cpuUsageTicker := time.NewTicker(r.cpuUsageSampleInterval)
	defer cpuUsageTicker.Stop()

	for {
		select {
		case <-cpuUsageTicker.C:
			r.recordCPUUsage(ctx)
		case <-checkTicker.C:
			if time.Now().Before(nextReportAt) {
				continue
			}

			// If the send is failing since a long time and the report is falling behind,
			// we'll skip this one and try to send the next one.
			if time.Since(nextReportAt) >= r.reportSendInterval {
				scheduleNextReport()
				level.Info(r.logger).Log("msg", "failed to send anonymous usage stats report for too long, skipping to next report", "next_report_at", nextReportAt.String())
				continue
			}

			// We're going to send the report. If we already have it, then it means it's a retry after a failure,
			// otherwise we have to generate a new one.
			if nextReport == nil {
				nextReport = buildReport(seed, nextReportAt, r.reportSendInterval)
			}

			level.Debug(r.logger).Log("msg", "sending anonymous usage stats report")
			if err := r.sendReport(ctx, nextReport); err != nil {
				level.Info(r.logger).Log("msg", "failed to send anonymous usage stats report", "err", err)

				// We'll try at the next check interval.
				continue
			}

			scheduleNextReport()
		case <-ctx.Done():
			if err := ctx.Err(); !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}
	}
}

var cpuUsage = GetAndResetFloat("cpu_usage")

func (r *Reporter) recordCPUUsage(ctx context.Context) {
	if r.cpuUsageProc == nil {
		return
	}
	percent, err := r.cpuUsageProc.CPUPercentWithContext(ctx)
	if err != nil {
		level.Debug(r.logger).Log("msg", "failed to get cpu percent for usage reporting", "err", err)
		return
	}
	if cpuUsage.Value() < percent {
		cpuUsage.Set(percent)
	}
}

// sendReport sends the report to the stats server.
func (r *Reporter) sendReport(ctx context.Context, report *Report) (returnErr error) {
	if report == nil {
		return errors.New("no report provided")
	}

	startTime := time.Now()
	r.requestsTotal.Inc()

	defer func() {
		r.requestsLatency.Observe(time.Since(startTime).Seconds())
		if returnErr != nil {
			r.requestsFailedTotal.Inc()
		}
	}()

	data, err := json.Marshal(report)
	if err != nil {
		return errors.Wrap(err, "marshal the report")
	}
	req, err := http.NewRequest(http.MethodPost, r.serverURL, bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "create the request")
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "send the report to the stats server")
	}

	// Ensure the body reader is always closed.
	defer resp.Body.Close()

	// Consume all the response.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "read the response from the stats server")
	}

	if resp.StatusCode/100 != 2 {
		// Limit the body response that we log.
		maxBodyLength := 128
		if len(body) > maxBodyLength {
			body = body[:maxBodyLength]
		}
		return fmt.Errorf("received status code: %s and body: %q", resp.Status, string(body))
	}

	return nil
}

// getNextReportAt compute the next report time based on the interval.
// The interval is based off the creation of the cluster seed to avoid all cluster reporting at the same time.
// The returned value is guaranteed to be computed the same for all instances of the same Mimir cluster.
func getNextReportAt(interval time.Duration, createdAt, now time.Time) time.Time {
	// createdAt * (x * interval ) >= now
	return createdAt.Add(time.Duration(math.Ceil(float64(now.Sub(createdAt))/float64(interval))) * interval)
}
