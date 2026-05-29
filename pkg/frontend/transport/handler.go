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
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	// StatusClientClosedRequest is the status code for when a client request cancellation of an http request
	StatusClientClosedRequest    = 499
	ServiceTimingHeaderName      = "Server-Timing"
	cacheControlSafeHeader       = safeHeader(requestoptions.CacheControlHeader)
	responseQueryStatsHeaderName = "X-Mimir-Response-Query-Stats"
	encodeTimeSeconds            = "encode_time_seconds"
	estimatedSeriesCount         = "estimated_series_count"
	fetchedChunkBytes            = "fetched_chunk_bytes"
	fetchedChunksCount           = "fetched_chunks_count"
	fetchedIndexBytes            = "fetched_index_bytes"
	fetchedSeriesCount           = "fetched_series_count"
	queryWallTimeSeconds         = "query_wall_time_seconds"
	queueTimeSeconds             = "queue_time_seconds"
	responseSizeBytes            = "response_size_bytes"
	responseTime                 = "response_time"
	resultsCacheHitBytes         = "results_cache_hit_bytes"
	resultsCacheMissBytes        = "results_cache_miss_bytes"
	shardedQueries               = "sharded_queries"
	splitQueries                 = "split_queries"
	remoteExecutionRequestCount  = "remote_execution_request_count"
)

var (
	errCanceled              = httpgrpc.Error(StatusClientClosedRequest, context.Canceled.Error())
	errDeadlineExceeded      = httpgrpc.Error(http.StatusGatewayTimeout, context.DeadlineExceeded.Error())
	errRequestEntityTooLarge = httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "http: request body too large")

	// sensitiveHeaderNames is the deny list of HTTP header names whose values
	// carry credentials or session material and must never appear in logs.
	sensitiveHeaderNames = []string{
		"authorization",
		"proxy-authorization",
		"cookie",
		"set-cookie",
		"x-api-key",
		"x-auth-token",
		"x-amz-security-token",
		"authentication-info",
		"www-authenticate",
		"proxy-authenticate",
		"x-csrf-token",
		"x-xsrf-token",
		"x-access-token",
		"x-session-token",
		"x-forwarded-authorization",
		"x-auth-request-access-token",
		"x-id-token",
	}
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

// Validate the HandlerConfig.
func (cfg *HandlerConfig) Validate() error {
	var rejected []string
	for _, h := range cfg.LogQueryRequestHeaders {
		if isSensitiveHeaderName(h) {
			rejected = append(rejected, h)
		}
	}
	if len(rejected) > 0 {
		return fmt.Errorf("-query-frontend.log-query-request-headers must not contain headers that carry credentials or session material: %s", strings.Join(rejected, ", "))
	}
	return nil
}

// Handler accepts queries and forwards them to RoundTripper. It can wait on in-flight requests and log slow queries,
// all other logic is inside the RoundTripper.
type Handler struct {
	cfg              HandlerConfig
	safeHeadersToLog []safeHeader
	log              log.Logger
	roundTripper     http.RoundTripper

	// Metrics.
	querySeconds               *prometheus.CounterVec
	querySeries                *prometheus.CounterVec
	queryChunkBytes            *prometheus.CounterVec
	queryChunks                *prometheus.CounterVec
	queryIndexBytes            *prometheus.CounterVec
	querySamplesProcessed      *prometheus.CounterVec
	queryPhysicalSamplesRead   *prometheus.CounterVec
	queryEquivalentSamplesRead *prometheus.CounterVec
	activeUsers                *util.ActiveUsersCleanupService

	mtx              sync.Mutex
	inflightRequests int
	stopped          bool
	cond             *sync.Cond
}

// NewHandler creates a new frontend handler.
func NewHandler(cfg HandlerConfig, roundTripper http.RoundTripper, log log.Logger, reg prometheus.Registerer) *Handler {
	h := &Handler{
		cfg:              cfg,
		safeHeadersToLog: safeHeadersToLog(cfg.LogQueryRequestHeaders),
		log:              log,
		roundTripper:     roundTripper,
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

		h.querySamplesProcessed = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_samples_processed_total",
			Help: "Number of samples processed to execute a query.",
		}, []string{"user"})

		h.queryPhysicalSamplesRead = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_physical_samples_read_total",
			Help: "Number of samples read from storage to execute a query. This excludes any samples that were read from a cache or otherwise not read due to query optimizations.",
		}, []string{"user"})

		h.queryEquivalentSamplesRead = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_equivalent_samples_read_total",
			Help: "Equivalent number of samples that would have been read from storage to execute a query, if no caching or other optimizations were applied to the query.",
		}, []string{"user"})

		h.activeUsers = util.NewActiveUsersCleanupWithDefaultValues(func(user string) {
			h.querySeconds.DeleteLabelValues(user, "true")
			h.querySeconds.DeleteLabelValues(user, "false")
			h.querySeries.DeleteLabelValues(user)
			h.queryChunkBytes.DeleteLabelValues(user)
			h.queryChunks.DeleteLabelValues(user)
			h.queryIndexBytes.DeleteLabelValues(user)
			h.querySamplesProcessed.DeleteLabelValues(user)
			h.queryPhysicalSamplesRead.DeleteLabelValues(user)
			h.queryEquivalentSamplesRead.DeleteLabelValues(user)
		})
		// If cleaner stops or fail, we will simply not clean the metrics for inactive users.
		_ = h.activeUsers.StartAsync(context.Background())
	}

	return h
}

// Stop makes f enter stopped mode and wait on in-flight requests.
func (h *Handler) Stop() {
	h.mtx.Lock()
	h.stopped = true

	level.Info(h.log).Log("msg", "waiting on in-flight requests", "requests", h.inflightRequests)
	for h.inflightRequests > 0 {
		h.cond.Wait()
	}
	h.mtx.Unlock()
	level.Info(h.log).Log("msg", "done waiting on in-flight requests")
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mtx.Lock()
	if h.stopped {
		h.mtx.Unlock()
		http.Error(w, "frontend stopped", http.StatusServiceUnavailable)
		return
	}
	h.inflightRequests++
	h.mtx.Unlock()

	defer func() {
		h.mtx.Lock()
		h.inflightRequests--
		h.cond.Broadcast()
		h.mtx.Unlock()
	}()

	var queryDetails *querymiddleware.QueryDetails

	// Initialise the queryDetails in the context and make sure it's propagated
	// down the request chain.
	queryStatsHeaderNameOk, _ := strconv.ParseBool(r.Header.Get(responseQueryStatsHeaderName))
	if h.cfg.QueryStatsEnabled || queryStatsHeaderNameOk {
		var ctx context.Context
		queryDetails, ctx = querymiddleware.ContextWithEmptyDetails(r.Context())
		r = r.WithContext(ctx)
	}

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

	if isActiveSeriesEndpoint(r) && h.cfg.ActiveSeriesWriteTimeout > 0 {
		deadline := time.Now().Add(h.cfg.ActiveSeriesWriteTimeout)
		err = http.NewResponseController(w).SetWriteDeadline(deadline)
		if err != nil {
			err := fmt.Errorf("failed to set write deadline for response writer: %w", err)
			writeError(w, apierror.New(apierror.TypeInternal, err.Error()))
			return
		}
		ctx, cancel := context.WithDeadlineCause(r.Context(), deadline,
			cancellation.NewErrorf("write deadline exceeded (timeout: %v)", h.cfg.ActiveSeriesWriteTimeout))
		defer cancel()
		r = r.WithContext(ctx)
	}

	startTime := time.Now()
	resp, err := h.roundTripper.RoundTrip(r)
	queryResponseTime := time.Since(startTime)

	if err != nil {
		statusCode := writeError(w, err)
		h.reportQueryStats(r, params, startTime, queryResponseTime, 0, queryDetails, statusCode, err)
		return
	}

	// Make sure to close the response body to release resources associated with this request.
	defer func() {
		if resp.Body != nil {
			err = resp.Body.Close()
			if err != nil {
				level.Warn(h.log).Log("msg", "failed to close response body", "err", err)
			}
		}
	}()

	hs := w.Header()
	for h, vs := range resp.Header {
		hs[h] = vs
	}

	var parts []string
	if h.cfg.QueryStatsEnabled {
		parts = getQueryStats(queryResponseTime, queryDetails)
	}
	if queryStatsHeaderNameOk {
		parts = append(parts, getResponseQueryStats(queryResponseTime, resp.ContentLength, queryDetails)...)
	}

	if len(parts) > 0 {
		hs.Set(ServiceTimingHeaderName, strings.Join(parts, ", "))
	}

	w.WriteHeader(resp.StatusCode)
	// we don't check for copy error as there is no much we can do at this point
	queryResponseSize, _ := io.Copy(w, resp.Body)

	if h.cfg.LogQueriesLongerThan > 0 && queryResponseTime > h.cfg.LogQueriesLongerThan {
		h.reportSlowQuery(r, params, queryResponseTime, queryDetails)
	}
	if h.cfg.QueryStatsEnabled {
		h.reportQueryStats(r, params, startTime, queryResponseTime, queryResponseSize, queryDetails, resp.StatusCode, nil)
	}
}

// reportSlowQuery reports slow queries.
func (h *Handler) reportSlowQuery(r *http.Request, queryString url.Values, queryResponseTime time.Duration, details *querymiddleware.QueryDetails) {
	logMessage := []any{
		"msg", "slow query detected",
		"method", r.Method,
		"host", r.Host,
		"path", r.URL.Path,
		"time_taken", queryResponseTime.String(),
	}

	logMessage = append(logMessage, h.formatRequestHeaders(&r.Header)...)

	// Append query string params last so that, if the log line is truncated downstream,
	// the long param_query value is what gets cut rather than other fields.
	logMessage = append(logMessage, formatQueryString(details, queryString)...)

	level.Info(util_log.WithContext(r.Context(), h.log)).Log(logMessage...)
}

func (h *Handler) reportQueryStats(
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
	var stats *querier_stats.SafeStats
	if details != nil {
		stats = details.QuerierStats
	}
	wallTime := stats.LoadWallTime()
	numSeries := stats.LoadFetchedSeries()
	numBytes := stats.LoadFetchedChunkBytes()
	numChunks := stats.LoadFetchedChunks()
	numIndexBytes := stats.LoadFetchedIndexBytes()
	sharded := strconv.FormatBool(stats.LoadShardedQueries() > 0)
	samplesProcessed := stats.LoadSamplesProcessed()
	equivalentSamplesRead := stats.LoadEquivalentSamplesRead()
	physicalSamplesRead := stats.LoadPhysicalSamplesRead()
	if stats != nil {
		// Track stats.
		h.querySeconds.WithLabelValues(userID, sharded).Add(wallTime.Seconds())
		h.querySeries.WithLabelValues(userID).Add(float64(numSeries))
		h.queryChunkBytes.WithLabelValues(userID).Add(float64(numBytes))
		h.queryChunks.WithLabelValues(userID).Add(float64(numChunks))
		h.queryIndexBytes.WithLabelValues(userID).Add(float64(numIndexBytes))
		h.querySamplesProcessed.WithLabelValues(userID).Add(float64(samplesProcessed))
		h.queryPhysicalSamplesRead.WithLabelValues(userID).Add(float64(physicalSamplesRead))
		h.queryEquivalentSamplesRead.WithLabelValues(userID).Add(float64(equivalentSamplesRead))
		h.activeUsers.UpdateUserTimestamp(userID, time.Now())
	}

	// Log stats.
	logMessage := []any{
		"msg", "query stats",
		"component", "query-frontend",
		"method", r.Method,
		"path", r.URL.Path,
		"route_name", middleware.ExtractRouteName(r.Context()),
		"user_agent", r.UserAgent(),
		"status_code", queryResponseStatusCode,
		responseTime, queryResponseTime,
		responseSizeBytes, queryResponseSizeBytes,
		queryWallTimeSeconds, wallTime.Seconds(),
		fetchedSeriesCount, numSeries,
		fetchedChunkBytes, numBytes,
		fetchedChunksCount, numChunks,
		fetchedIndexBytes, numIndexBytes,
		shardedQueries, stats.LoadShardedQueries(),
		splitQueries, stats.LoadSplitQueries(),
		"spun_off_subqueries", stats.LoadSpunOffSubqueries(),
		"split_range_vectors", stats.LoadSplitRangeVectors(),
		estimatedSeriesCount, stats.LoadEstimatedSeriesCount(),
		queueTimeSeconds, stats.LoadQueueTime().Seconds(),
		encodeTimeSeconds, stats.LoadEncodeTime().Seconds(),
		remoteExecutionRequestCount, stats.LoadRemoteExecutionRequestCount(),
		"samples_processed", samplesProcessed,
		"equivalent_samples_read", equivalentSamplesRead,
		"physical_samples_read", physicalSamplesRead,
	}

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
			resultsCacheHitBytes, details.ResultsCacheHitBytes,
			resultsCacheMissBytes, details.ResultsCacheMissBytes,
			"response_series_count", details.ResponseSeriesCount,
			"response_samples_count", details.ResponseSamplesCount,
		)
	}

	// Log the read consistency only when explicitly defined.
	if consistency, ok := querierapi.ReadConsistencyLevelFromContext(r.Context()); ok {
		logMessage = append(logMessage, "read_consistency", consistency)
	}
	if delay, ok := querierapi.ReadConsistencyMaxDelayFromContext(r.Context()); ok {
		logMessage = append(logMessage, "read_consistency_max_delay", delay)
	}

	logMessage = append(logMessage, h.formatRequestHeaders(&r.Header)...)

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

	// Append query string params last so that, if the log line is truncated downstream,
	// the long param_query value is what gets cut rather than other fields.
	logMessage = append(logMessage, formatQueryString(details, queryString)...)

	level.Info(util_log.WithContext(r.Context(), h.log)).Log(logMessage...)
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
	case "lookback_delta":
		if details.LookbackDelta != 0 {
			return details.LookbackDelta.String()
		}
	}
	return ""
}

// safeHeader is the name of an HTTP request header that has been validated as
// safe to log: it is not in the sensitive-header deny list. Values must be
// constructed via newSafeHeader so the deny-list check cannot be bypassed by
// callers outside the package.
type safeHeader string

// newSafeHeader returns a safeHeader wrapping name and true, or the zero value
// and false if name is in the sensitive-header deny list.
func newSafeHeader(name string) (safeHeader, bool) {
	if isSensitiveHeaderName(name) {
		return "", false
	}
	return safeHeader(name), true
}

func (s safeHeader) String() string {
	return string(s)
}

func (s safeHeader) log() string {
	return fmt.Sprintf("header_%s", strings.ReplaceAll(strings.ToLower(string(s)), "-", "_"))
}

// isSensitiveHeaderName reports whether the named header carries credentials
// or session material whose value must never be logged, regardless of operator
// allow-list configuration.
func isSensitiveHeaderName(name string) bool {
	lower := strings.ToLower(name)
	for _, h := range sensitiveHeaderNames {
		if h == lower {
			return true
		}
	}
	return false
}

func safeHeadersToLog(headersToLog []string) (safeHeaders []safeHeader) {
	for _, h := range headersToLog {
		if strings.EqualFold(h, requestoptions.CacheControlHeader) {
			continue
		}
		if s, ok := newSafeHeader(h); ok {
			safeHeaders = append(safeHeaders, s)
		}
	}
	return safeHeaders
}

// sanitizeHeaderValue returns value (and true) when headerName is a safeHeader
// and value is non-empty or acceptEmpty is true. The parameter is typed any
// rather than safeHeader so the runtime type assertion forms an explicit
// sanitizer barrier — both as defense in depth against in-package casts and
// as a node CodeQL can recognize between http.Header.Get and the logger.
func sanitizeHeaderValue(headerName any, value string, acceptEmpty bool) (string, bool) {
	if _, ok := headerName.(safeHeader); !ok {
		return "", false
	}
	return value, acceptEmpty || value != ""
}

func (h *Handler) formatRequestHeaders(header *http.Header) (fields []any) {
	if v, ok := sanitizeHeaderValue(cacheControlSafeHeader, header.Get(cacheControlSafeHeader.String()), true); ok {
		fields = append(fields, cacheControlSafeHeader.log(), v)
	}

	for _, s := range h.safeHeadersToLog {
		if v, ok := sanitizeHeaderValue(s, header.Get(s.String()), false); ok {
			fields = append(fields, s.log(), v)
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

func getQueryStats(queryResponseTime time.Duration, details *querymiddleware.QueryDetails) []string {
	if details == nil {
		return nil
	}
	stats := details.QuerierStats
	return []string{
		statsValue("querier_wall_time", stats.LoadWallTime()),
		statsValue("response_time", queryResponseTime),
		statsValue("bytes_processed", stats.LoadFetchedChunkBytes()+stats.LoadFetchedIndexBytes()),
		statsValue("samples_processed", stats.LoadSamplesProcessed()),
		statsValue("equivalent_samples_read", stats.LoadEquivalentSamplesRead()),
	}
}

// getResponseQueryStats returns the response query stats in the format of Server-Timing header.
// contentLengthBytes must be the http.Response.ContentLength field value; -1 means unknown (streaming response).
func getResponseQueryStats(queryResponseTime time.Duration, contentLengthBytes int64, details *querymiddleware.QueryDetails) []string {
	if details == nil {
		return nil
	}
	stats := details.QuerierStats

	statsResponse := []string{
		statsValue(estimatedSeriesCount, stats.LoadEstimatedSeriesCount()),
		statsValue(fetchedChunkBytes, stats.LoadFetchedChunkBytes()),
		statsValue(fetchedChunksCount, stats.LoadFetchedChunks()),
		statsValue(fetchedIndexBytes, stats.LoadFetchedIndexBytes()),
		statsValue(fetchedSeriesCount, stats.LoadFetchedSeries()),
		statsValue(queryWallTimeSeconds, stats.LoadWallTime().Seconds()),
		statsValue(queueTimeSeconds, stats.LoadQueueTime().Seconds()),
		statsValue(responseSizeBytes, contentLengthBytes),
		statsValue(responseTime, queryResponseTime),
		statsValue(resultsCacheHitBytes, details.ResultsCacheHitBytes),
		statsValue(resultsCacheMissBytes, details.ResultsCacheMissBytes),
		statsValue(shardedQueries, stats.LoadShardedQueries()),
		statsValue(splitQueries, stats.LoadSplitQueries()),
		statsValue(remoteExecutionRequestCount, stats.LoadRemoteExecutionRequestCount()),
		statsValue("physical_samples_read", stats.LoadPhysicalSamplesRead()), // The "equivalent samples read" count is added in getQueryStats above.
	}

	if contentLengthBytes >= 0 {
		// encode_time_seconds is always 0 for streaming responses: encoding runs concurrently with body
		// streaming, so the encode time is not available until after the headers have been sent.
		// We only insert this if we are in a non-streaming response.
		statsResponse = append(statsResponse, statsValue(encodeTimeSeconds, stats.LoadEncodeTime().Seconds()))
	}

	return statsResponse
}

func statsValue(name string, val interface{}) string {
	switch v := val.(type) {
	case time.Duration:
		durationInMs := strconv.FormatFloat(float64(v)/float64(time.Millisecond), 'f', -1, 64)
		return fmt.Sprintf("%s;dur=%s", name, durationInMs)
	case float64: // duration in seconds.
		return fmt.Sprintf("%s;dur=%v", name, val)
	case uint64:
		return fmt.Sprintf("%s;val=%s", name, strconv.FormatUint(v, 10))
	default:
		return fmt.Sprintf("%s;val=%v", name, val)
	}
}

func isActiveSeriesEndpoint(r *http.Request) bool {
	return strings.HasSuffix(r.URL.Path, "api/v1/cardinality/active_series")
}
