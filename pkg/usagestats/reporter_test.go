// SPDX-License-Identifier: AGPL-3.0-only

package usagestats

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
)

func TestGetNextReportAt(t *testing.T) {
	fixtures := map[string]struct {
		interval  time.Duration
		createdAt time.Time
		now       time.Time

		next time.Time
	}{
		"createdAt aligned with interval and now": {
			interval:  1 * time.Hour,
			createdAt: time.Unix(0, time.Hour.Nanoseconds()),
			now:       time.Unix(0, 2*time.Hour.Nanoseconds()),
			next:      time.Unix(0, 2*time.Hour.Nanoseconds()),
		},
		"createdAt aligned with interval": {
			interval:  1 * time.Hour,
			createdAt: time.Unix(0, time.Hour.Nanoseconds()),
			now:       time.Unix(0, 2*time.Hour.Nanoseconds()+1),
			next:      time.Unix(0, 3*time.Hour.Nanoseconds()),
		},
		"createdAt not aligned": {
			interval:  1 * time.Hour,
			createdAt: time.Unix(0, time.Hour.Nanoseconds()+18*time.Minute.Nanoseconds()+20*time.Millisecond.Nanoseconds()),
			now:       time.Unix(0, 2*time.Hour.Nanoseconds()+1),
			next:      time.Unix(0, 2*time.Hour.Nanoseconds()+18*time.Minute.Nanoseconds()+20*time.Millisecond.Nanoseconds()),
		},
	}
	for name, f := range fixtures {
		t.Run(name, func(t *testing.T) {
			next := getNextReportAt(f.interval, f.createdAt, f.now)
			require.Equal(t, f.next, next)
		})
	}
}

func TestSendReport(t *testing.T) {
	serverInvoked := atomic.NewBool(false)

	// Create a local HTTP server.
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		serverInvoked.Store(true)

		switch request.URL.Path {
		case "/success":
			writer.WriteHeader(http.StatusOK)
		default:
			writer.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()

	tests := map[string]func(t *testing.T){
		"server returns 2xx": func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			reporter := NewReporter(prepareLocalBucketClient(t), log.NewNopLogger(), reg)
			reporter.serverURL = server.URL + "/success"

			err := reporter.sendReport(context.Background(), buildReport(newClusterSeed(), time.Now(), time.Hour))
			require.NoError(t, err)
			require.True(t, serverInvoked.Load())

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_usage_stats_report_sends_total The total number of attempted send requests.
				# TYPE cortex_usage_stats_report_sends_total counter
				cortex_usage_stats_report_sends_total 1

				# HELP cortex_usage_stats_report_sends_failed_total The total number of failed send requests.
				# TYPE cortex_usage_stats_report_sends_failed_total counter
				cortex_usage_stats_report_sends_failed_total 0
			`), "cortex_usage_stats_report_sends_total", "cortex_usage_stats_report_sends_failed_total"))
		},
		"server returns 5xx": func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			reporter := NewReporter(prepareLocalBucketClient(t), log.NewNopLogger(), reg)
			reporter.serverURL = server.URL + "/failure"

			err := reporter.sendReport(context.Background(), buildReport(newClusterSeed(), time.Now(), time.Hour))
			require.Error(t, err)
			require.Contains(t, err.Error(), "received status code: 503")
			require.True(t, serverInvoked.Load())

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_usage_stats_report_sends_total The total number of attempted send requests.
				# TYPE cortex_usage_stats_report_sends_total counter
				cortex_usage_stats_report_sends_total 1

				# HELP cortex_usage_stats_report_sends_failed_total The total number of failed send requests.
				# TYPE cortex_usage_stats_report_sends_failed_total counter
				cortex_usage_stats_report_sends_failed_total 1
			`), "cortex_usage_stats_report_sends_total", "cortex_usage_stats_report_sends_failed_total"))
		},
		"server is not running": func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			reporter := NewReporter(prepareLocalBucketClient(t), log.NewNopLogger(), reg)
			reporter.serverURL = "http://127.0.0.1:12345"

			err := reporter.sendReport(context.Background(), buildReport(newClusterSeed(), time.Now(), time.Hour))
			require.Error(t, err)
			require.Contains(t, err.Error(), "connection refused")
			require.False(t, serverInvoked.Load())

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_usage_stats_report_sends_total The total number of attempted send requests.
				# TYPE cortex_usage_stats_report_sends_total counter
				cortex_usage_stats_report_sends_total 1

				# HELP cortex_usage_stats_report_sends_failed_total The total number of failed send requests.
				# TYPE cortex_usage_stats_report_sends_failed_total counter
				cortex_usage_stats_report_sends_failed_total 1
			`), "cortex_usage_stats_report_sends_total", "cortex_usage_stats_report_sends_failed_total"))
		},
	}

	for testName, testRun := range tests {
		t.Run(testName, func(t *testing.T) {
			// Reset.
			serverInvoked.Store(false)

			testRun(t)
		})
	}
}

func TestReporter_SendReportPeriodically(t *testing.T) {
	tests := map[string]struct {
		failOneRequestEvery int
	}{
		"all requests to stats server succeed": {
			failOneRequestEvery: 0,
		},
		"some requests to stats server fail and they get retried": {
			failOneRequestEvery: 2,
		},
	}

	type receivedReport struct {
		report Report
		failed bool
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			var (
				ctx             = context.Background()
				bucketClient    = prepareLocalBucketClient(t)
				reportsMx       sync.Mutex
				reportsReceived []receivedReport
				reportsAccepted []Report
				requestsCount   = atomic.NewInt32(0)
			)

			// Mock the stats server.
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requestsCount.Inc()

				data, err := io.ReadAll(request.Body)
				require.NoError(t, err)

				report := Report{}
				require.NoError(t, json.Unmarshal(data, &report))

				shouldFail := testData.failOneRequestEvery > 0 && requestsCount.Load()%int32(testData.failOneRequestEvery) == 0

				reportsMx.Lock()
				reportsReceived = append(reportsReceived, receivedReport{report: report, failed: shouldFail})
				reportsMx.Unlock()

				if shouldFail {
					writer.WriteHeader(http.StatusServiceUnavailable)
					return
				}

				reportsMx.Lock()
				reportsAccepted = append(reportsAccepted, report)
				reportsMx.Unlock()

				writer.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(server.Close)

			r := NewReporter(bucketClient, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			r.serverURL = server.URL
			r.reportCheckInterval = 100 * time.Millisecond
			r.reportSendInterval = time.Second

			// Upload the seed file.
			seed := newClusterSeed()
			seed.CreatedAt = time.Now().Add(-time.Hour)
			require.NoError(t, writeSeedFile(ctx, r.bucket, seed))

			// Start the reporter.
			require.NoError(t, services.StartAndAwaitRunning(ctx, r))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, r))
			})

			// We expect to have received and accepted a report per second.
			test.Poll(t, 10*time.Second, true, func() interface{} {
				reportsMx.Lock()
				defer reportsMx.Unlock()
				return len(reportsAccepted) >= 4
			})

			reportsMx.Lock()
			defer reportsMx.Unlock()

			// We expect each report interval to be exactly at 1s apart from the previous one.
			for i := 1; i < len(reportsAccepted); i++ {
				require.Equal(t, reportsAccepted[i-1].Interval.Add(time.Second), reportsAccepted[i].Interval)
			}

			// We expect that a report doesn't change when its delivery is retried.
			if testData.failOneRequestEvery > 0 {
				for i, report := range reportsReceived {
					// If the request has failed, then we expect the next one to succeed and have the same exact report.
					if report.failed {
						require.GreaterOrEqual(t, len(reportsReceived), i)
						assert.False(t, reportsReceived[i+1].failed)
						assert.Equal(t, reportsReceived[i].report, reportsReceived[i+1].report)
					}
				}
			}
		})
	}
}

func TestReporter_SendReportShouldSkipToNextReportOnLongFailure(t *testing.T) {
	t.Parallel()

	var (
		ctx           = context.Background()
		bucketClient  = prepareLocalBucketClient(t)
		reportsMx     sync.Mutex
		reports       []Report
		requestsCount = atomic.NewInt32(0)
	)

	// Mock the stats server.
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestsCount.Inc()

		// Fail 10 requests after the 1st one (we expect 1 retry every 100ms in this test).
		if requestsCount.Load() > 1 && requestsCount.Load() <= 11 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		data, err := io.ReadAll(request.Body)
		require.NoError(t, err)

		report := Report{}
		require.NoError(t, json.Unmarshal(data, &report))

		reportsMx.Lock()
		reports = append(reports, report)
		reportsMx.Unlock()

		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	r := NewReporter(bucketClient, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	r.serverURL = server.URL
	r.reportCheckInterval = 100 * time.Millisecond
	r.reportSendInterval = time.Second

	// Upload the seed file.
	seed := newClusterSeed()
	seed.CreatedAt = time.Now().Add(-time.Hour)
	require.NoError(t, writeSeedFile(ctx, r.bucket, seed))

	// Start the reporter.
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, r))
	})

	// Wait util we have received at least 2 reports.
	test.Poll(t, 10*time.Second, true, func() interface{} {
		reportsMx.Lock()
		defer reportsMx.Unlock()
		return len(reports) >= 2
	})

	reportsMx.Lock()
	defer reportsMx.Unlock()

	// We expect the report timestamp to have been updated during the long failure.
	require.GreaterOrEqual(t, reports[1].Interval, reports[0].Interval.Add(2*time.Second))
}

func prepareLocalBucketClient(t *testing.T) objstore.InstrumentedBucket {
	bucketClient, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	return objstore.BucketWithMetrics("", bucketClient, nil)
}
