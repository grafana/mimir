// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier"
	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// Pusher is an ingester server that accepts pushes.
type Pusher interface {
	Push(context.Context, *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error)
}

type PusherAppender struct {
	failedWrites *prometheus.CounterVec
	totalWrites  *prometheus.CounterVec

	ctx             context.Context
	pusher          Pusher
	labels          [][]mimirpb.LabelAdapter
	samples         []mimirpb.Sample
	histogramLabels [][]mimirpb.LabelAdapter
	histograms      []mimirpb.Histogram
	userID          string
}

func (a *PusherAppender) SetOptions(*storage.AppendOptions) {
}

func (a *PusherAppender) Append(_ storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	a.labels = append(a.labels, mimirpb.FromLabelsToLabelAdapters(l))
	a.samples = append(a.samples, mimirpb.Sample{
		TimestampMs: t,
		Value:       v,
	})
	return 0, nil
}

func (a *PusherAppender) AppendExemplar(_ storage.SeriesRef, _ labels.Labels, _ exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, errors.New("exemplars are unsupported")
}

func (a *PusherAppender) UpdateMetadata(_ storage.SeriesRef, _ labels.Labels, _ metadata.Metadata) (storage.SeriesRef, error) {
	return 0, errors.New("metadata updates are unsupported")
}

func (a *PusherAppender) AppendHistogram(_ storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	a.histogramLabels = append(a.histogramLabels, mimirpb.FromLabelsToLabelAdapters(l))
	var hp mimirpb.Histogram
	if h != nil {
		hp = mimirpb.FromHistogramToHistogramProto(t, h)
	} else {
		hp = mimirpb.FromFloatHistogramToHistogramProto(t, fh)
	}
	a.histograms = append(a.histograms, hp)
	return 0, nil
}

func (a *PusherAppender) AppendCTZeroSample(_ storage.SeriesRef, _ labels.Labels, _, _ int64) (storage.SeriesRef, error) {
	return 0, errors.New("CT zero samples are unsupported")
}

func (a *PusherAppender) AppendHistogramCTZeroSample(storage.SeriesRef, labels.Labels, int64, int64, *histogram.Histogram, *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, errors.New("CT zero samples are unsupported")
}

func (a *PusherAppender) Commit() error {
	a.totalWrites.WithLabelValues(a.userID).Inc()

	// Since a.pusher is distributor, client.ReuseSlice will be called in a.pusher.Push.
	// We shouldn't call client.ReuseSlice here.
	req := mimirpb.ToWriteRequest(a.labels, a.samples, nil, nil, mimirpb.RULE)
	req.AddHistogramSeries(a.histogramLabels, a.histograms, nil)
	_, err := a.pusher.Push(user.InjectOrgID(a.ctx, a.userID), req)

	if err != nil {
		failureReason := "error"
		if mimirpb.IsClientError(err) {
			// Client errors, which are the same ones that would be reported with 4xx HTTP status code
			// (e.g. series limits, duplicate samples, out of order, etc.) are reported with a separate reason.
			failureReason = "4xx"
		}
		a.failedWrites.WithLabelValues(a.userID, failureReason).Inc()
	}

	a.labels = nil
	a.samples = nil
	return err
}

func (a *PusherAppender) Rollback() error {
	a.labels = nil
	a.samples = nil
	return nil
}

// PusherAppendable fulfills the storage.Appendable interface for prometheus manager
type PusherAppendable struct {
	pusher Pusher
	userID string

	totalWrites  *prometheus.CounterVec
	failedWrites *prometheus.CounterVec
}

func NewPusherAppendable(pusher Pusher, userID string, totalWrites, failedWrites *prometheus.CounterVec) *PusherAppendable {
	return &PusherAppendable{
		pusher:       pusher,
		userID:       userID,
		totalWrites:  totalWrites,
		failedWrites: failedWrites,
	}
}

// Appender returns a storage.Appender
func (t *PusherAppendable) Appender(ctx context.Context) storage.Appender {
	return &PusherAppender{
		failedWrites: t.failedWrites,
		totalWrites:  t.totalWrites,

		ctx:    ctx,
		pusher: t.pusher,
		userID: t.userID,
	}
}

type NoopAppender struct{}

func (a *NoopAppender) SetOptions(*storage.AppendOptions) {
}

func (a *NoopAppender) Append(_ storage.SeriesRef, _ labels.Labels, _ int64, _ float64) (storage.SeriesRef, error) {
	return 0, nil
}

func (a *NoopAppender) AppendExemplar(_ storage.SeriesRef, _ labels.Labels, _ exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, errors.New("exemplars are unsupported")
}

func (a *NoopAppender) UpdateMetadata(_ storage.SeriesRef, _ labels.Labels, _ metadata.Metadata) (storage.SeriesRef, error) {
	return 0, errors.New("metadata updates are unsupported")
}

func (a *NoopAppender) AppendHistogram(_ storage.SeriesRef, _ labels.Labels, _ int64, _ *histogram.Histogram, _ *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, nil
}

func (a *NoopAppender) AppendCTZeroSample(_ storage.SeriesRef, _ labels.Labels, _, _ int64) (storage.SeriesRef, error) {
	return 0, errors.New("CT zero samples are unsupported")
}

func (a *NoopAppender) AppendHistogramCTZeroSample(storage.SeriesRef, labels.Labels, int64, int64, *histogram.Histogram, *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, errors.New("CT zero samples are unsupported")
}

func (a *NoopAppender) Commit() error {
	return nil
}

func (a *NoopAppender) Rollback() error {
	return nil
}

type NoopAppendable struct{}

func NewNoopAppendable() *NoopAppendable {
	return &NoopAppendable{}
}

// Appender returns a storage.Appender.
func (t *NoopAppendable) Appender(_ context.Context) storage.Appender {
	return &NoopAppender{}
}

// RulesLimits defines limits used by Ruler.
type RulesLimits interface {
	EvaluationDelay(userID string) time.Duration
	RulerTenantShardSize(userID string) int
	RulerMaxRuleGroupsPerTenant(userID, namespace string) int
	RulerMaxRulesPerRuleGroup(userID, namespace string) int
	RulerRecordingRulesEvaluationEnabled(userID string) bool
	RulerAlertingRulesEvaluationEnabled(userID string) bool
	RulerSyncRulesOnChangesEnabled(userID string) bool
	RulerProtectedNamespaces(userID string) []string
	RulerMaxIndependentRuleEvaluationConcurrencyPerTenant(userID string) int64
}

func MetricsQueryFunc(qf rules.QueryFunc, userID string, queries, failedQueries *prometheus.CounterVec, remoteQuerier bool) rules.QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
		queries.WithLabelValues(userID).Inc()

		result, err := qf(ctx, qs, t)
		if err == nil {
			return result, nil
		}

		failureReason := "error"
		qerr := QueryableError{}
		if errors.As(err, &qerr) {
			origErr := qerr.Unwrap()

			// Not all errors returned by Queryable are interesting, only those that would result in 500 status code.
			//
			// We rely on TranslateToPromqlApiError to do its job here... it returns nil, if err is nil.
			// It returns promql.ErrStorage, if error should be reported back as 500.
			// Other errors it returns are either for canceled or timed-out queriers (we're not reporting those as failures),
			// or various user-errors (limits, duplicate samples, etc. ... also not failures).
			//
			// All errors will still be counted towards "evaluation failures" metrics and logged by Prometheus Ruler,
			// but we only want internal errors here.
			if _, ok := querier.TranslateToPromqlAPIError(origErr).(promql.ErrStorage); ok {
				failedQueries.WithLabelValues(userID, failureReason).Inc()
			}

			// Return unwrapped error.
			return result, origErr
		} else if remoteQuerier {
			// When remote querier enabled, consider anything an "error" except those with 4xx status code.
			if mimirpb.IsClientError(err) {
				failureReason = "4xx"
			}
			failedQueries.WithLabelValues(userID, failureReason).Inc()
		}
		return result, err
	}
}

func RecordAndReportRuleQueryMetrics(qf rules.QueryFunc, queryTime, zeroFetchedSeriesCount prometheus.Counter, logger log.Logger) rules.QueryFunc {
	if queryTime == nil || zeroFetchedSeriesCount == nil {
		return qf
	}

	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
		// Inject a new stats object in the context to be updated by various queryables used to execute
		// the query (blocks store queryable, distributor queryable, etc.). When used by the query-frontend
		// this is normally handled by middleware: instrumenting a QueryFunc is the ruler equivalent.
		stats, ctx := querier_stats.ContextWithEmptyStats(ctx)
		// If we've been passed a counter we want to record the wall time spent executing this request.
		timer := prometheus.NewTimer(nil)
		var err error
		defer func() {
			// Update stats wall time based on the timer created above.
			stats.AddWallTime(timer.ObserveDuration())

			wallTime := stats.LoadWallTime()
			numSeries := stats.LoadFetchedSeries()
			numBytes := stats.LoadFetchedChunkBytes()
			numChunks := stats.LoadFetchedChunks()
			shardedQueries := stats.LoadShardedQueries()

			queryTime.Add(wallTime.Seconds())
			// Do not count queries with errors for zero fetched series, or queries
			// with no selectors that are not meant to fetch any series.
			if err == nil && numSeries == 0 {
				if expr, err := parser.ParseExpr(qs); err == nil {
					if len(parser.ExtractSelectors(expr)) > 0 {
						zeroFetchedSeriesCount.Add(1)
					}
				}
			}

			// Log ruler query stats.
			logMessage := []interface{}{
				"msg", "query stats",
				"component", "ruler",
				"query_wall_time_seconds", wallTime.Seconds(),
				"fetched_series_count", numSeries,
				"fetched_chunk_bytes", numBytes,
				"fetched_chunks_count", numChunks,
				"sharded_queries", shardedQueries,
				"query", qs,
			}
			level.Info(util_log.WithContext(ctx, logger)).Log(logMessage...)
		}()

		result, err := qf(ctx, qs, t)
		return result, err
	}
}

// RulesManager mimics rules.Manager API. Interface is used to simplify tests.
type RulesManager interface {
	// Run starts the rules manager. Blocks until Stop is called.
	Run()

	// Stop rules manager. (Unblocks Run.)
	Stop()

	// Update rules manager state.
	Update(interval time.Duration, files []string, externalLabels labels.Labels, externalURL string, groupEvalIterationFunc rules.GroupEvalIterationFunc) error

	// RuleGroups returns current rules groups.
	RuleGroups() []*rules.Group
}

// ManagerFactory is a function that creates new RulesManager for given user and notifier.Manager.
type ManagerFactory func(ctx context.Context, userID string, notifier *notifier.Manager, logger log.Logger, reg prometheus.Registerer) RulesManager

func DefaultTenantManagerFactory(
	cfg Config,
	pusher Pusher,
	queryable storage.Queryable,
	queryFunc rules.QueryFunc,
	concurrencyController MultiTenantRuleConcurrencyController,
	overrides RulesLimits,
	reg prometheus.Registerer,
) ManagerFactory {
	totalWrites := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ruler_write_requests_total",
		Help: "Number of write requests to ingesters.",
	}, []string{"user"})
	failedWrites := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ruler_write_requests_failed_total",
		Help: "Number of failed write requests to ingesters.",
	}, []string{"user", "reason"})

	totalQueries := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ruler_queries_total",
		Help: "Number of queries executed by ruler.",
	}, []string{"user"})
	failedQueries := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ruler_queries_failed_total",
		Help: "Number of failed queries by ruler.",
	}, []string{"user", "reason"})
	var rulerQuerySeconds *prometheus.CounterVec
	var zeroFetchedSeriesQueries *prometheus.CounterVec
	if cfg.EnableQueryStats {
		rulerQuerySeconds = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ruler_query_seconds_total",
			Help: "Total amount of wall clock time spent processing queries by the ruler.",
		}, []string{"user"})
		zeroFetchedSeriesQueries = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ruler_queries_zero_fetched_series_total",
			Help: "Number of queries that did not fetch any series by ruler.",
		}, []string{"user"})
	}
	return func(ctx context.Context, userID string, notifier *notifier.Manager, logger log.Logger, reg prometheus.Registerer) RulesManager {
		var queryTime prometheus.Counter
		var zeroFetchedSeriesCount prometheus.Counter
		if rulerQuerySeconds != nil {
			queryTime = rulerQuerySeconds.WithLabelValues(userID)
			zeroFetchedSeriesCount = zeroFetchedSeriesQueries.WithLabelValues(userID)
		}

		// Wrap the query function with our custom logic.
		wrappedQueryFunc := WrapQueryFuncWithReadConsistency(queryFunc, logger)
		wrappedQueryFunc = MetricsQueryFunc(wrappedQueryFunc, userID, totalQueries, failedQueries, cfg.QueryFrontend.Address != "")
		wrappedQueryFunc = RecordAndReportRuleQueryMetrics(wrappedQueryFunc, queryTime, zeroFetchedSeriesCount, logger)

		// Wrap the queryable with our custom logic.
		wrappedQueryable := WrapQueryableWithReadConsistency(queryable, logger)

		var appendeable storage.Appendable
		if cfg.RuleEvaluationWriteEnabled {
			appendeable = NewPusherAppendable(pusher, userID, totalWrites, failedWrites)
		} else {
			appendeable = NewNoopAppendable()
		}

		return rules.NewManager(&rules.ManagerOptions{
			Appendable:                 appendeable,
			Queryable:                  wrappedQueryable,
			QueryFunc:                  wrappedQueryFunc,
			Context:                    user.InjectOrgID(ctx, userID),
			GroupEvaluationContextFunc: FederatedGroupContextFunc,
			ExternalURL:                cfg.ExternalURL.URL,
			NotifyFunc:                 rules.SendAlerts(notifier, cfg.ExternalURL.String()),
			Logger:                     util_log.SlogFromGoKit(log.With(logger, "component", "ruler", "insight", true, "user", userID)),
			Registerer:                 reg,
			OutageTolerance:            cfg.OutageTolerance,
			ForGracePeriod:             cfg.ForGracePeriod,
			ResendDelay:                cfg.ResendDelay,
			AlwaysRestoreAlertState:    true,
			DefaultRuleQueryOffset: func() time.Duration {
				// Delay the evaluation of all rules by a set interval to give a buffer
				// to metric that haven't been forwarded to Mimir yet.
				return overrides.EvaluationDelay(userID)
			},
			RuleConcurrencyController: concurrencyController.NewTenantConcurrencyControllerFor(userID),
		})
	}
}

type QueryableError struct {
	err error
}

func (q QueryableError) Unwrap() error {
	return q.err
}

func (q QueryableError) Error() string {
	return q.err.Error()
}

func WrapQueryableErrors(err error) error {
	if err == nil {
		return err
	}

	return QueryableError{err: err}
}
