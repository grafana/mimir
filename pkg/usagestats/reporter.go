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
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

const (
	// DefaultReportSendInterval is the interval at which anonymous usage statistics are reported.
	DefaultReportSendInterval = 4 * time.Hour

	defaultReportCheckInterval = time.Minute
	defaultStatsServerURL      = "https://stats.grafana.org/mimir-usage-report"
)

type Config struct {
	Enabled bool `yaml:"enabled" category:"experimental"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "usage-stats.enabled", false, "Enable anonymous usage reporting.")
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

	r := &Reporter{
		logger:               logger,
		bucket:               bucketClient,
		client:               http.Client{Timeout: 5 * time.Second},
		serverURL:            defaultStatsServerURL,
		reportCheckInterval:  defaultReportCheckInterval,
		reportSendInterval:   DefaultReportSendInterval,
		seedFileMinStability: clusterSeedFileMinStability,

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

	// Find when to send the next report. We want all instances of the same Mimir cluster computing the same value.
	nextReportAt := nextReport(r.reportSendInterval, seed.CreatedAt, time.Now())

	ticker := time.NewTicker(r.reportCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if time.Now().Before(nextReportAt) {
				continue
			}

			// If the send is failing since a long time and the report is falling behind,
			// we'll skip this one and try to send the next one.
			if time.Since(nextReportAt) >= r.reportSendInterval {
				nextReportAt = nextReport(r.reportSendInterval, seed.CreatedAt, time.Now())
				level.Info(r.logger).Log("msg", "failed to send anonymous usage stats report for too long, skipping to next report", "next_report_at", nextReportAt.String())
				continue
			}

			level.Debug(r.logger).Log("msg", "sending anonymous usage stats report")
			if err := r.sendReport(ctx, buildReport(seed, nextReportAt, r.reportSendInterval)); err != nil {
				level.Info(r.logger).Log("msg", "failed to send anonymous usage stats report", "err", err)

				// We'll try at the next check interval.
				continue
			}

			nextReportAt = nextReport(r.reportSendInterval, seed.CreatedAt, time.Now())
		case <-ctx.Done():
			if err := ctx.Err(); !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}
	}
}

// sendReport sends the report to the stats server.
func (r *Reporter) sendReport(ctx context.Context, report Report) (returnErr error) {
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

// nextReport compute the next report time based on the interval.
// The interval is based off the creation of the cluster seed to avoid all cluster reporting at the same time.
func nextReport(interval time.Duration, createdAt, now time.Time) time.Time {
	// createdAt * (x * interval ) >= now
	return createdAt.Add(time.Duration(math.Ceil(float64(now.Sub(createdAt))/float64(interval))) * interval)
}
