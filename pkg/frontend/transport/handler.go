// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/transport/handler.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package transport

import (
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
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/activitytracker"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	// StatusClientClosedRequest is the status code for when a client request cancellation of an http request
	StatusClientClosedRequest = 499
	ServiceTimingHeaderName   = "Server-Timing"
	cacheControlHeader        = "Cache-Control"
	cacheControlLogField      = "header_cache_control"
)

var (
	errCanceled              = httpgrpc.Error(StatusClientClosedRequest, context.Canceled.Error())
	errDeadlineExceeded      = httpgrpc.Error(http.StatusGatewayTimeout, context.DeadlineExceeded.Error())
	errRequestEntityTooLarge = httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "http: request body too large")
)

// HandlerConfig is a config for the handler.
type HandlerConfig struct {
	LogQueriesLongerThan     time.Duration          `yaml:"log_queries_longer_than"`
	LogQueryRequestHeaders   flagext.StringSliceCSV `yaml:"log_query_request_headers" category:"advanced"`
	MaxBodySize              int64                  `yaml:"max_body_size" category:"advanced"`
	QueryStatsEnabled        bool                   `yaml:"query_stats_enabled" category:"advanced"`
	ActiveSeriesWriteTimeout time.Duration          `yaml:"active_series_write_timeout" category:"experimental"`
}

func (cfg *HandlerConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.LogQueriesLongerThan, "query-frontend.log-queries-longer-than", 0, "Log queries that are slower than the specified duration. Set to 0 to disable. Set to < 0 to enable on all queries.")
	f.Var(&cfg.LogQueryRequestHeaders, "query-frontend.log-query-request-headers", "Comma-separated list of request header names to include in query logs. Applies to both query stats and slow queries logs.")
	f.Int64Var(&cfg.MaxBodySize, "query-frontend.max-body-size", 10*1024*1024, "Max body size for downstream prometheus.")
	f.BoolVar(&cfg.QueryStatsEnabled, "query-frontend.query-stats-enabled", true, "False to disable query statistics tracking. When enabled, a message with some statistics is logged for every query.")
	f.DurationVar(&cfg.ActiveSeriesWriteTimeout, "query-frontend.active-series-write-timeout", 5*time.Minute, "Timeout for writing active series responses. 0 means the value from `-server.http-write-timeout` is used.")
}

// Handler accepts queries and forwards them to RoundTripper. It can wait on in-flight requests and log slow queries,
// all other logic is inside the RoundTripper.
type Handler struct {
	cfg          HandlerConfig
	headersToLog []string
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
		headersToLog: filterHeadersToLog(cfg.LogQueryRequestHeaders),
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
		http.Error(w, "frontend stopped", http.StatusServiceUnavailable)
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

	var queryDetails *querymiddleware.QueryDetails

	// Initialise the queryDetails in the context and make sure it's propagated
	// down the request chain.
	if f.cfg.QueryStatsEnabled {
		var ctx context.Context
		queryDetails, ctx = querymiddleware.ContextWithEmptyDetails(r.Context())
		r = r.WithContext(ctx)
	}

	// Ensure to close the request body reader.
	defer func() { _ = r.Body.Close() }()

	// Limit the read body size.
	r.Body = http.MaxBytesReader(w, r.Body, f.cfg.MaxBodySize)

	var params url.Values
	var err error

	if r.Header.Get("Content-Type") == "application/x-protobuf" && querymiddleware.IsRemoteReadQuery(r.URL.Path) {
		params, err = querymiddleware.ParseRemoteReadRequestValuesWithoutConsumingBody(r)
	} else {
		params, err = util.ParseRequestFormWithoutConsumingBody(r)
	}

	if err != nil {
		writeError(w, apierror.New(apierror.TypeBadData, err.Error()))
		return
	}

	activityIndex := f.at.Insert(func() string { return httpRequestActivity(r, r.Header.Get("User-Agent"), params) })
	defer f.at.Delete(activityIndex)

	if isActiveSeriesEndpoint(r) && f.cfg.ActiveSeriesWriteTimeout > 0 {
		deadline := time.Now().Add(f.cfg.ActiveSeriesWriteTimeout)
		err = http.NewResponseController(w).SetWriteDeadline(deadline)
		if err != nil {
			err := fmt.Errorf("failed to set write deadline for response writer: %w", err)
			writeError(w, apierror.New(apierror.TypeInternal, err.Error()))
			return
		}
		ctx, _ := context.WithDeadlineCause(r.Context(), deadline,
			cancellation.NewErrorf("write deadline exceeded (timeout: %v)", f.cfg.ActiveSeriesWriteTimeout))
		r = r.WithContext(ctx)
	}

	startTime := time.Now()
	resp, err := f.roundTripper.RoundTrip(r)
	queryResponseTime := time.Since(startTime)

	if err != nil {
		statusCode := writeError(w, err)
		f.reportQueryStats(r, params, startTime, queryResponseTime, 0, queryDetails, statusCode, err)
		return
	}

	// Make sure to close the response body to release resources associated with this request.
	defer func() {
		if resp.Body != nil {
			err = resp.Body.Close()
			if err != nil {
				level.Warn(f.log).Log("msg", "failed to close response body", "err", err)
			}
		}
	}()

	hs := w.Header()
	for h, vs := range resp.Header {
		hs[h] = vs
	}

	if f.cfg.QueryStatsEnabled {
		writeServiceTimingHeader(queryResponseTime, hs, queryDetails.QuerierStats)
	}

	w.WriteHeader(resp.StatusCode)
	// we don't check for copy error as there is no much we can do at this point
	queryResponseSize, _ := io.Copy(w, resp.Body)

	if f.cfg.LogQueriesLongerThan > 0 && queryResponseTime > f.cfg.LogQueriesLongerThan {
		f.reportSlowQuery(r, params, queryResponseTime, queryDetails)
	}
	if f.cfg.QueryStatsEnabled {
		f.reportQueryStats(r, params, startTime, queryResponseTime, queryResponseSize, queryDetails, resp.StatusCode, nil)
	}
}

// reportSlowQuery reports slow queries.
func (f *Handler) reportSlowQuery(r *http.Request, queryString url.Values, queryResponseTime time.Duration, details *querymiddleware.QueryDetails) {
	logMessage := append([]any{
		"msg", "slow query detected",
		"method", r.Method,
		"host", r.Host,
		"path", r.URL.Path,
		"time_taken", queryResponseTime.String(),
	}, formatQueryString(details, queryString)...)

	logMessage = append(logMessage, formatRequestHeaders(&r.Header, f.headersToLog)...)

	level.Info(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)
}

func (f *Handler) reportQueryStats(
	r *http.Request,
	queryString url.Values,
	queryStartTime time.Time,
	queryResponseTime time.Duration,
	queryResponseSizeBytes int64,
	details *querymiddleware.QueryDetails,
	queryResponseStatusCode int,
	queryErr error,
) {
	tenantIDs, err := tenant.TenantIDs(r.Context())
	if err != nil {
		return
	}
	userID := tenant.JoinTenantIDs(tenantIDs)
	var stats *querier_stats.Stats
	if details != nil {
		stats = details.QuerierStats
	}
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
	logMessage := append([]any{
		"msg", "query stats",
		"component", "query-frontend",
		"method", r.Method,
		"path", r.URL.Path,
		"route_name", middleware.ExtractRouteName(r.Context()),
		"user_agent", r.UserAgent(),
		"status_code", queryResponseStatusCode,
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
		"queue_time_seconds", stats.LoadQueueTime().Seconds(),
		"encode_time_seconds", stats.LoadEncodeTime().Seconds(),
		"samples_processed", stats.LoadSamplesProcessed(),
	}, formatQueryString(details, queryString)...)

	if details != nil {
		// Start and End may be zero when the request wasn't a query (e.g. /metadata)
		// or if the query was a constant expression and didn't need to process samples.
		if !details.MinT.IsZero() && !details.MaxT.IsZero() {
			logMessage = append(logMessage, "length", details.MaxT.Sub(details.MinT).String())
		}
		if !details.MinT.IsZero() {
			logMessage = append(logMessage, "time_since_min_time", queryStartTime.Sub(details.MinT))
		}
		if !details.MaxT.IsZero() {
			logMessage = append(logMessage, "time_since_max_time", queryStartTime.Sub(details.MaxT))
		}
		logMessage = append(logMessage,
			"results_cache_hit_bytes", details.ResultsCacheHitBytes,
			"results_cache_miss_bytes", details.ResultsCacheMissBytes,
		)
	}

	// Log the read consistency only when explicitly defined.
	if consistency, ok := querierapi.ReadConsistencyLevelFromContext(r.Context()); ok {
		logMessage = append(logMessage, "read_consistency", consistency)
	}

	logMessage = append(logMessage, formatRequestHeaders(&r.Header, f.headersToLog)...)

	if queryErr == nil && queryResponseStatusCode/100 != 2 {
		// If downstream replied with non-2xx, log this as a failure.
		queryErr = fmt.Errorf("downstream replied with %s", http.StatusText(queryResponseStatusCode))
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

// formatQueryString prefers printing start, end, and step from details if they are not nil.
func formatQueryString(details *querymiddleware.QueryDetails, queryString url.Values) (fields []any) {
	for k, v := range queryString {
		var formattedValue string
		if details != nil {
			formattedValue = paramValueFromDetails(details, k)
		}

		if formattedValue == "" {
			formattedValue = strings.Join(v, ",")
		}
		fields = append(fields, fmt.Sprintf("param_%s", k), formattedValue)
	}
	return fields
}

// paramValueFromDetails returns the value of the parameter from details if the value there is non-zero.
// Otherwise, it returns an empty string.
// One reason why details field may be zero-values is if the value was not parseable.
func paramValueFromDetails(details *querymiddleware.QueryDetails, paramName string) string {
	switch paramName {
	case "start", "time":
		if !details.Start.IsZero() {
			return details.Start.Format(time.RFC3339Nano)
		}
	case "end":
		if !details.End.IsZero() {
			return details.End.Format(time.RFC3339Nano)
		}
	case "step":
		if details.Step != 0 {
			return strconv.FormatInt(details.Step.Milliseconds(), 10)
		}
	}
	return ""
}

func filterHeadersToLog(headersToLog []string) (filtered []string) {
	for _, h := range headersToLog {
		if strings.EqualFold(h, cacheControlHeader) {
			continue
		}
		filtered = append(filtered, h)
	}
	return filtered
}

func formatRequestHeaders(h *http.Header, headersToLog []string) (fields []any) {
	fields = append(fields, cacheControlLogField, h.Get(cacheControlHeader))
	for _, s := range headersToLog {
		if v := h.Get(s); v != "" {
			fields = append(fields, fmt.Sprintf("header_%s", strings.ReplaceAll(strings.ToLower(s), "-", "_")), v)
		}
	}
	return fields
}

// writeError writes the error response to http.ResponseWriter, and returns the response HTTP status code.
func writeError(w http.ResponseWriter, err error) int {
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

	var (
		res      *httpgrpc.HTTPResponse
		resFound bool
	)

	// If the error is an APIError, ensure it gets written as a JSON response.
	// Otherwise, check if there's a response encoded in the gRPC error.
	res, resFound = apierror.HTTPResponseFromError(err)
	if !resFound {
		res, resFound = httpgrpc.HTTPResponseFromError(err)
	}

	// If we've been able to get the HTTP response from the error, then we send
	// it with the right status code and response body content.
	if resFound {
		_ = httpgrpc.WriteResponse(w, res)
		return int(res.Code)
	}

	// Otherwise, we do fallback to a 5xx error, returning the non-formatted error
	// message in the response body.
	statusCode := http.StatusInternalServerError
	http.Error(w, err.Error(), statusCode)
	return statusCode
}

func writeServiceTimingHeader(queryResponseTime time.Duration, headers http.Header, stats *querier_stats.Stats) {
	if stats != nil {
		parts := make([]string, 0)
		parts = append(parts, statsValue("querier_wall_time", stats.LoadWallTime()))
		parts = append(parts, statsValue("response_time", queryResponseTime))
		parts = append(parts, statsValue("bytes_processed", stats.LoadFetchedChunkBytes()+stats.LoadFetchedIndexBytes()))
		parts = append(parts, statsValue("samples_processed", stats.GetSamplesProcessed()))
		headers.Set(ServiceTimingHeaderName, strings.Join(parts, ", "))
	}
}

func statsValue(name string, val interface{}) string {
	switch v := val.(type) {
	case time.Duration:
		durationInMs := strconv.FormatFloat(float64(v)/float64(time.Millisecond), 'f', -1, 64)
		return name + ";dur=" + durationInMs
	case uint64:
		return name + ";val=" + strconv.FormatUint(v, 10)
	default:
		return name + ";val=" + fmt.Sprintf("%v", v)
	}
}

func httpRequestActivity(request *http.Request, userAgent string, requestParams url.Values) string {
	tenantID := "(unknown)"
	if tenantIDs, err := tenant.TenantIDs(request.Context()); err == nil {
		tenantID = tenant.JoinTenantIDs(tenantIDs)
	}

	params := requestParams.Encode()
	if params == "" {
		params = "(no params)"
	}

	// This doesn't have to be pretty, just useful for debugging, so prioritize efficiency.
	return fmt.Sprintf("user:%s UA:%s req:%s %s %s", tenantID, userAgent, request.Method, request.URL.Path, params)
}

func isActiveSeriesEndpoint(r *http.Request) bool {
	return strings.HasSuffix(r.URL.Path, "api/v1/cardinality/active_series")
}
