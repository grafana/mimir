// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/transport/handler.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package transport

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"

	apierror "github.com/grafana/mimir/pkg/api/error"
	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/activitytracker"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	// StatusClientClosedRequest is the status code for when a client request cancellation of an http request
	StatusClientClosedRequest = 499
	ServiceTimingHeaderName   = "Server-Timing"
)

var (
	errCanceled              = httpgrpc.Errorf(StatusClientClosedRequest, context.Canceled.Error())
	errDeadlineExceeded      = httpgrpc.Errorf(http.StatusGatewayTimeout, context.DeadlineExceeded.Error())
	errRequestEntityTooLarge = httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "http: request body too large")
)

// Config for a Handler.
type HandlerConfig struct {
	LogQueriesLongerThan   time.Duration          `yaml:"log_queries_longer_than"`
	LogQueryRequestHeaders flagext.StringSliceCSV `yaml:"log_query_request_headers" category:"advanced"`
	MaxBodySize            int64                  `yaml:"max_body_size" category:"advanced"`
	QueryStatsEnabled      bool                   `yaml:"query_stats_enabled" category:"advanced"`
}

func (cfg *HandlerConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.LogQueriesLongerThan, "query-frontend.log-queries-longer-than", 0, "Log queries that are slower than the specified duration. Set to 0 to disable. Set to < 0 to enable on all queries.")
	f.Var(&cfg.LogQueryRequestHeaders, "query-frontend.log-query-request-headers", "Comma-separated list of request header names to include in query logs. Applies to both query stats and slow queries logs.")
	f.Int64Var(&cfg.MaxBodySize, "query-frontend.max-body-size", 10*1024*1024, "Max body size for downstream prometheus.")
	f.BoolVar(&cfg.QueryStatsEnabled, "query-frontend.query-stats-enabled", true, "False to disable query statistics tracking. When enabled, a message with some statistics is logged for every query.")
}

// Handler accepts queries and forwards them to RoundTripper. It can wait on in-flight requests and log slow queries,
// all other logic is inside the RoundTripper.
type Handler struct {
	cfg          HandlerConfig
	log          log.Logger
	roundTripper http.RoundTripper
	at           *activitytracker.ActivityTracker

	// Metrics.
	querySeconds    *prometheus.CounterVec
	querySeries     *prometheus.CounterVec
	queryChunkBytes *prometheus.CounterVec
	queryChunks     *prometheus.CounterVec
	queryIndexBytes *prometheus.CounterVec
	activeUsers     *util.ActiveUsersCleanupService

	mtx              sync.Mutex
	inflightRequests int
	stopped          bool
	cond             *sync.Cond
}

// NewHandler creates a new frontend handler.
func NewHandler(cfg HandlerConfig, roundTripper http.RoundTripper, log log.Logger, reg prometheus.Registerer, at *activitytracker.ActivityTracker) *Handler {
	h := &Handler{
		cfg:          cfg,
		log:          log,
		roundTripper: roundTripper,
		at:           at,
	}
	h.cond = sync.NewCond(&h.mtx)

	if cfg.QueryStatsEnabled {
		h.querySeconds = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_seconds_total",
			Help: "Total amount of wall clock time spend processing queries.",
		}, []string{"user", "sharded"})

		h.querySeries = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_fetched_series_total",
			Help: "Number of series fetched to execute a query.",
		}, []string{"user"})

		h.queryChunkBytes = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_fetched_chunk_bytes_total",
			Help: "Number of chunk bytes fetched to execute a query.",
		}, []string{"user"})

		h.queryChunks = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_fetched_chunks_total",
			Help: "Number of chunks fetched to execute a query.",
		}, []string{"user"})

		h.queryIndexBytes = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_fetched_index_bytes_total",
			Help: "Number of TSDB index bytes fetched from store-gateway to execute a query.",
		}, []string{"user"})

		h.activeUsers = util.NewActiveUsersCleanupWithDefaultValues(func(user string) {
			h.querySeconds.DeleteLabelValues(user, "true")
			h.querySeconds.DeleteLabelValues(user, "false")
			h.querySeries.DeleteLabelValues(user)
			h.queryChunkBytes.DeleteLabelValues(user)
			h.queryChunks.DeleteLabelValues(user)
			h.queryIndexBytes.DeleteLabelValues(user)
		})
		// If cleaner stops or fail, we will simply not clean the metrics for inactive users.
		_ = h.activeUsers.StartAsync(context.Background())
	}

	return h
}

// Stop makes f enter stopped mode and wait on in-flight requests.
func (f *Handler) Stop() {
	f.mtx.Lock()
	f.stopped = true

	level.Info(f.log).Log("msg", "waiting on in-flight requests", "requests", f.inflightRequests)
	for f.inflightRequests > 0 {
		f.cond.Wait()
	}
	f.mtx.Unlock()
	level.Info(f.log).Log("msg", "done waiting on in-flight requests")
}

func (f *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mtx.Lock()
	if f.stopped {
		f.mtx.Unlock()
		writeError(w, fmt.Errorf("frontend not running"))
		return
	}
	f.inflightRequests++
	f.mtx.Unlock()

	defer func() {
		f.mtx.Lock()
		f.inflightRequests--
		f.cond.Broadcast()
		f.mtx.Unlock()
	}()

	var stats *querier_stats.Stats

	// Initialise the stats in the context and make sure it's propagated
	// down the request chain.
	if f.cfg.QueryStatsEnabled {
		var ctx context.Context
		stats, ctx = querier_stats.ContextWithEmptyStats(r.Context())
		r = r.WithContext(ctx)
	}

	defer func() { _ = r.Body.Close() }()

	// Store the body contents, so we can read it multiple times.
	bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, r.Body, f.cfg.MaxBodySize))
	if err != nil {
		writeError(w, err)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	// Parse the form, as it's needed to build the activity for the activity-tracker.
	if err := r.ParseForm(); err != nil {
		writeError(w, apierror.New(apierror.TypeBadData, err.Error()))
		return
	}

	// Store a copy of the params and restore the request state.
	// Restore the body, so it can be read again if it's used to forward the request through a roundtripper.
	// Restore the Form and PostForm, to avoid subtle bugs in middlewares, as they were set by ParseForm.
	params := copyValues(r.Form)
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	r.Form, r.PostForm = nil, nil

	activityIndex := f.at.Insert(func() string { return httpRequestActivity(r, params) })
	defer f.at.Delete(activityIndex)

	startTime := time.Now()
	resp, err := f.roundTripper.RoundTrip(r)
	queryResponseTime := time.Since(startTime)

	if err != nil {
		writeError(w, err)
		f.reportQueryStats(r, params, queryResponseTime, 0, stats, err)
		return
	}

	hs := w.Header()
	for h, vs := range resp.Header {
		hs[h] = vs
	}

	if f.cfg.QueryStatsEnabled {
		writeServiceTimingHeader(queryResponseTime, hs, stats)
	}

	w.WriteHeader(resp.StatusCode)
	// we don't check for copy error as there is no much we can do at this point
	queryResponseSize, _ := io.Copy(w, resp.Body)

	if f.cfg.LogQueriesLongerThan > 0 && queryResponseTime > f.cfg.LogQueriesLongerThan {
		f.reportSlowQuery(r, params, queryResponseTime)
	}
	if f.cfg.QueryStatsEnabled {
		f.reportQueryStats(r, params, queryResponseTime, queryResponseSize, stats, nil)
	}
}

// reportSlowQuery reports slow queries.
func (f *Handler) reportSlowQuery(r *http.Request, queryString url.Values, queryResponseTime time.Duration) {
	logMessage := append([]interface{}{
		"msg", "slow query detected",
		"method", r.Method,
		"host", r.Host,
		"path", r.URL.Path,
		"time_taken", queryResponseTime.String(),
	}, formatQueryString(queryString)...)

	if len(f.cfg.LogQueryRequestHeaders) != 0 {
		logMessage = append(logMessage, formatRequestHeaders(&r.Header, f.cfg.LogQueryRequestHeaders)...)
	}

	level.Info(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)
}

func (f *Handler) reportQueryStats(r *http.Request, queryString url.Values, queryResponseTime time.Duration, queryResponseSizeBytes int64, stats *querier_stats.Stats, queryErr error) {
	tenantIDs, err := tenant.TenantIDs(r.Context())
	if err != nil {
		return
	}
	userID := tenant.JoinTenantIDs(tenantIDs)
	wallTime := stats.LoadWallTime()
	numSeries := stats.LoadFetchedSeries()
	numBytes := stats.LoadFetchedChunkBytes()
	numChunks := stats.LoadFetchedChunks()
	numIndexBytes := stats.LoadFetchedIndexBytes()
	sharded := strconv.FormatBool(stats.GetShardedQueries() > 0)

	if stats != nil {
		// Track stats.
		f.querySeconds.WithLabelValues(userID, sharded).Add(wallTime.Seconds())
		f.querySeries.WithLabelValues(userID).Add(float64(numSeries))
		f.queryChunkBytes.WithLabelValues(userID).Add(float64(numBytes))
		f.queryChunks.WithLabelValues(userID).Add(float64(numChunks))
		f.queryIndexBytes.WithLabelValues(userID).Add(float64(numIndexBytes))
		f.activeUsers.UpdateUserTimestamp(userID, time.Now())
	}

	// Log stats.
	logMessage := append([]interface{}{
		"msg", "query stats",
		"component", "query-frontend",
		"method", r.Method,
		"path", r.URL.Path,
		"user_agent", r.UserAgent(),
		"response_time", queryResponseTime,
		"response_size_bytes", queryResponseSizeBytes,
		"query_wall_time_seconds", wallTime.Seconds(),
		"fetched_series_count", numSeries,
		"fetched_chunk_bytes", numBytes,
		"fetched_chunks_count", numChunks,
		"fetched_index_bytes", numIndexBytes,
		"sharded_queries", stats.LoadShardedQueries(),
		"split_queries", stats.LoadSplitQueries(),
		"estimated_series_count", stats.GetEstimatedSeriesCount(),
	}, formatQueryString(queryString)...)

	if len(f.cfg.LogQueryRequestHeaders) != 0 {
		logMessage = append(logMessage, formatRequestHeaders(&r.Header, f.cfg.LogQueryRequestHeaders)...)
	}

	if queryErr != nil {
		logStatus := "failed"
		if errors.Is(queryErr, context.Canceled) {
			logStatus = "canceled"
		} else if errors.Is(queryErr, context.DeadlineExceeded) {
			logStatus = "timeout"
		}

		logMessage = append(logMessage,
			"status", logStatus,
			"err", queryErr)
	} else {
		logMessage = append(logMessage,
			"status", "success")
	}

	level.Info(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)
}

func formatQueryString(queryString url.Values) (fields []interface{}) {
	for k, v := range queryString {
		fields = append(fields, fmt.Sprintf("param_%s", k), strings.Join(v, ","))
	}
	return fields
}

func formatRequestHeaders(h *http.Header, headersToLog []string) (fields []interface{}) {
	for _, s := range headersToLog {
		if v := h.Get(s); v != "" {
			fields = append(fields, fmt.Sprintf("header_%s", strings.ReplaceAll(strings.ToLower(s), "-", "_")), v)
		}
	}
	return fields
}

func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, context.Canceled):
		err = errCanceled
	case errors.Is(err, context.DeadlineExceeded):
		err = errDeadlineExceeded
	default:
		if util.IsRequestBodyTooLarge(err) {
			err = errRequestEntityTooLarge
		}
	}

	// if the error is an APIError, ensure it gets written as a JSON response
	if resp, ok := apierror.HTTPResponseFromError(err); ok {
		_ = server.WriteResponse(w, resp)
		return
	}

	server.WriteError(w, err)
}

func writeServiceTimingHeader(queryResponseTime time.Duration, headers http.Header, stats *querier_stats.Stats) {
	if stats != nil {
		parts := make([]string, 0)
		parts = append(parts, statsValue("querier_wall_time", stats.LoadWallTime()))
		parts = append(parts, statsValue("response_time", queryResponseTime))
		headers.Set(ServiceTimingHeaderName, strings.Join(parts, ", "))
	}
}

func statsValue(name string, d time.Duration) string {
	durationInMs := strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', -1, 64)
	return name + ";dur=" + durationInMs
}

func httpRequestActivity(request *http.Request, requestParams url.Values) string {
	tenantID := "(unknown)"
	if tenantIDs, err := tenant.TenantIDs(request.Context()); err == nil {
		tenantID = tenant.JoinTenantIDs(tenantIDs)
	}

	params := requestParams.Encode()
	if params == "" {
		params = "(no params)"
	}

	// This doesn't have to be pretty, just useful for debugging, so prioritize efficiency.
	return strings.Join([]string{tenantID, request.Method, request.URL.Path, params}, " ")
}

func copyValues(src url.Values) url.Values {
	dst := make(url.Values, len(src))
	for k, vs := range src {
		dst[k] = append([]string(nil), vs...)
	}
	return dst
}
