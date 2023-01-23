// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/extract"
	util_kafka "github.com/grafana/mimir/pkg/util/kafka"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type Forwarder interface {
	services.Service
	Forward(ctx context.Context, dontForwardBefore int64, forwardingRules validation.ForwardingRules, ts []mimirpb.PreallocTimeseries, user string) ([]mimirpb.PreallocTimeseries, chan error)
	DeleteMetricsForUser(user string)
}

var errSamplesTooOld = httpgrpc.Errorf(http.StatusBadRequest, "dropped sample(s) because too old to forward")

type forwarder struct {
	services.Service

	cfg          Config
	pools        *pools
	kafkaWriter  *kafka.Writer
	log          log.Logger
	limits       *validation.Overrides
	workerWg     sync.WaitGroup
	reqCh        chan *request
	activeGroups *util.ActiveGroupsCleanupService

	requestsTotal           prometheus.Counter
	errorsTotal             *prometheus.CounterVec
	samplesTotal            prometheus.Counter
	exemplarsTotal          prometheus.Counter
	requestLatencyHistogram prometheus.Histogram
	grpcClientsGauge        prometheus.Gauge

	discardedSamplesTooOld *prometheus.CounterVec
}

// NewForwarder returns a new forwarder, if forwarding is disabled it returns nil.
func NewForwarder(cfg Config, reg prometheus.Registerer, log log.Logger, limits *validation.Overrides, activeGroupsCleanupService *util.ActiveGroupsCleanupService) Forwarder {
	if !cfg.Enabled {
		return nil
	}

	f := &forwarder{
		cfg:          cfg,
		pools:        newPools(),
		log:          log,
		limits:       limits,
		reqCh:        make(chan *request, cfg.RequestConcurrency),
		activeGroups: activeGroupsCleanupService,
		requestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_forward_requests_total",
			Help:      "The total number of requests the Distributor made to forward samples.",
		}),
		errorsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_forward_errors_total",
			Help:      "The total number of errors that the distributor received from forwarding targets when trying to send samples to them.",
		}, []string{"status_code"}),
		samplesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_forward_samples_total",
			Help:      "The total number of samples the Distributor forwarded.",
		}),
		exemplarsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_forward_exemplars_total",
			Help:      "The total number of exemplars the Distributor forwarded.",
		}),
		requestLatencyHistogram: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "distributor_forward_requests_latency_seconds",
			Help:      "The client-side latency of requests to forward metrics made by the Distributor.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
		}),
		grpcClientsGauge: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_distributor_forward_grpc_clients",
			Help: "Number of gRPC clients used by Distributor forwarder.",
		}),
		discardedSamplesTooOld: validation.DiscardedSamplesCounter(reg, "forwarded-sample-too-old"),
	}

	f.kafkaWriter = &kafka.Writer{
		Addr:        kafka.TCP(cfg.kafkaBrokers...),
		Topic:       cfg.KafkaTopic,
		Compression: kafka.Snappy,
		Balancer:    kafka.Murmur2Balancer{},
	}

	f.Service = services.NewIdleService(f.start, f.stop)

	return f
}

func (f *forwarder) DeleteMetricsForUser(user string) {
	f.discardedSamplesTooOld.DeleteLabelValues(user)
}

func (f *forwarder) start(ctx context.Context) error {
	f.workerWg.Add(f.cfg.RequestConcurrency)

	for i := 0; i < f.cfg.RequestConcurrency; i++ {
		go f.worker()
	}

	return nil
}

func (f *forwarder) stop(_ error) error {
	close(f.reqCh)
	f.workerWg.Wait()

	return nil
}

// worker is a worker go routine which performs the forwarding requests that it receives through a channel.
func (f *forwarder) worker() {
	defer f.workerWg.Done()

	for req := range f.reqCh {
		req.do()
	}
}

// Forward takes a set of forwarding rules and a slice of time series, it forwards the time series according to the rules.
// This function may return before the forwarding requests have completed, the caller can use the returned chan of errors
// to determine whether all forwarding requests have completed by checking if it is closed.
//
// The forwarding requests get executed with a limited concurrency which is configurable, in a situation where the
// concurrency limit is exhausted this function will block until a go routine is available to execute the requests.
// The slice of time series which gets passed into this function must not be returned to the pool by the caller, the
// returned slice of time series must be returned to the pool by the caller once it is done using it.
//
// Endpoint must be non-empty.
//
// The return values are:
//   - A slice of time series which should be sent to the ingesters, based on the given rule set.
//     The Forward() method does not send the time series to the ingesters itself, it expects the caller to do that.
//   - A chan of errors which resulted from forwarding the time series, the chan gets closed when all forwarding requests have completed.
func (f *forwarder) Forward(ctx context.Context, dontForwardBefore int64, rules validation.ForwardingRules, in []mimirpb.PreallocTimeseries, user string) ([]mimirpb.PreallocTimeseries, chan error) {
	if !f.cfg.Enabled {
		errCh := make(chan error)
		close(errCh)
		return in, errCh
	}

	spanlog, ctx := spanlogger.NewWithLogger(ctx, f.log, "forwarder.Forward")
	finishSpanlogInDefer := true
	defer func() {
		if finishSpanlogInDefer {
			spanlog.Finish()
		}
	}()

	toIngest, toForward, counts, err := f.splitToIngestedAndForwardedTimeseries(in, rules, dontForwardBefore, user)
	errCh := make(chan error, 2) // 1 for result of forwarding, 1 for possible error
	if err != nil {
		errCh <- err
	}

	if len(toForward) > 0 {
		var requestWg sync.WaitGroup
		requestWg.Add(1)

		f.submitForwardingRequest(ctx, user, rules, toForward, counts, &requestWg, errCh)

		// keep span running until goroutine finishes.
		finishSpanlogInDefer = false
		go func() {
			// Waiting for the requestWg in a go routine allows Forward() to return early, that way the call site can
			// continue doing whatever it needs to do and read the returned errCh later to wait for the forwarding requests
			// to complete and handle potential errors yielded by the forwarding requests.
			requestWg.Wait()
			close(errCh)
			spanlog.Finish()
		}()
	} else {
		f.pools.putTsSlice(toForward)
		close(errCh)
	}

	return toIngest, errCh
}

// filterAndCopyTimeseries makes a copy of the timeseries with old samples filtered out. Original timeseries is unchanged.
// The time series is deep-copied, so the passed in time series can be returned to the pool without affecting the copy.
func (f *forwarder) filterAndCopyTimeseries(ts mimirpb.PreallocTimeseries, dontForwardBeforeTimestamp int64) (_ mimirpb.PreallocTimeseries, filteredSamplesCount int) {
	if dontForwardBeforeTimestamp > 0 {
		samplesUnfiltered := ts.TimeSeries.Samples
		defer func() {
			// Before this function returns we need to restore the original sample slice because
			// we might still want to ingest it into the ingesters.
			ts.TimeSeries.Samples = samplesUnfiltered
		}()

		samplesFiltered := dropSamplesBefore(samplesUnfiltered, dontForwardBeforeTimestamp)
		if len(samplesFiltered) < len(samplesUnfiltered) {
			filteredSamplesCount = len(samplesUnfiltered) - len(samplesFiltered)
		}

		// Prepare ts for deep copy. Original samples will be set back via defer function.
		ts.TimeSeries.Samples = samplesFiltered
	}

	result := mimirpb.PreallocTimeseries{TimeSeries: f.pools.getTs()}
	if len(ts.TimeSeries.Samples) > 0 {
		// We don't keep exemplars when forwarding.
		result = mimirpb.DeepCopyTimeseries(result, ts, false)
	}
	return result, filteredSamplesCount
}

type TimeseriesCounts struct {
	SampleCount   int
	ExemplarCount int
}

func (t *TimeseriesCounts) count(ts mimirpb.PreallocTimeseries) {
	t.SampleCount += len(ts.TimeSeries.Samples)
	t.ExemplarCount += len(ts.TimeSeries.Exemplars)
}

// splitToIngestedAndForwardedTimeseries takes a slice of time series and a set of forwarding rules, then it divides
// the given time series into series that should be ingested and series that should be forwarded.
//
// It returns the following values:
//   - A slice of time series to ingest into the ingesters.
//   - A slice of time series to forward
//   - TimeseriesCounts
//   - An error if any occurred.
func (f *forwarder) splitToIngestedAndForwardedTimeseries(tsSliceIn []mimirpb.PreallocTimeseries, rules validation.ForwardingRules, dontForwardBefore int64, user string) (tsToIngest, tsToForward []mimirpb.PreallocTimeseries, _ TimeseriesCounts, _ error) {
	// This functions copies all the entries of tsSliceIn into new slices so tsSliceIn can be recycled,
	// we adjust the length of the slice to 0 to prevent that the contained *mimirpb.TimeSeries objects that have been
	// reassigned (not deep copied) get returned while they are still referred to by another slice.
	defer f.pools.putTsSlice(tsSliceIn[:0])

	tsToIngest = f.pools.getTsSlice()
	tsToForward = f.pools.getTsSlice()
	counts := TimeseriesCounts{}
	group := f.activeGroups.UpdateActiveGroupTimestamp(user, validation.GroupLabel(f.limits, user, tsSliceIn), time.Now())
	var err error

	for _, ts := range tsSliceIn {
		forward, ingest := shouldForwardAndIngest(ts.Labels, rules)
		if forward {
			tsCopy, filteredSamples := f.filterAndCopyTimeseries(ts, dontForwardBefore)
			if filteredSamples > 0 {
				err = errSamplesTooOld
				if !ingest {
					f.discardedSamplesTooOld.WithLabelValues(user, group).Add(float64(filteredSamples))
				}
			}

			if len(tsCopy.TimeSeries.Samples) > 0 {
				tsToForward = append(tsToForward, tsCopy)
				counts.count(tsCopy)
			} else {
				// We're not going to use this timeseries, put it back to pool.
				f.pools.putTs(tsCopy.TimeSeries)
			}
		}

		if ingest {
			// Only when reassigning time series to tsToIngest we don't deep copy them,
			// the distributor will return them to the pool when it is done sending them to the ingesters.
			tsToIngest = append(tsToIngest, ts)
		} else {
			// This ts won't be returned to the distributor because it should not be ingested according to the rules,
			// so we have to return it to the pool now to prevent that its reference gets lost.
			f.pools.putTs(ts.TimeSeries)
		}
	}

	return tsToIngest, tsToForward, counts, err
}

// dropSamplesBefore filters a given slice of samples to only contain samples that have timestamps newer or equal to
// the given timestamp. It relies on the samples being sorted by timestamp.
func dropSamplesBefore(samples []mimirpb.Sample, ts int64) []mimirpb.Sample {
	for sampleIdx := len(samples) - 1; sampleIdx >= 0; sampleIdx-- {
		if samples[sampleIdx].TimestampMs < ts {
			return samples[sampleIdx+1:]
		}
	}

	return samples
}

func shouldForwardAndIngest(labels []mimirpb.LabelAdapter, rules validation.ForwardingRules) (forward, ingest bool) {
	metric, err := extract.UnsafeMetricNameFromLabelAdapters(labels)
	if err != nil {
		// Can't check whether a timeseries should be forwarded if it has no metric name.
		// Ingest it and don't forward it.
		return false, true
	}

	rule, ok := rules[metric]
	if !ok {
		// There is no forwarding rule for this metric, ingest it and don't forward it.
		return false, true
	}

	return true, rule.Ingest
}

type request struct {
	pools       *pools
	kafkaWriter *kafka.Writer
	log         log.Logger

	ctx             context.Context
	timeout         time.Duration
	propagateErrors bool
	errCh           chan error
	requestWg       *sync.WaitGroup

	user     string
	rules    validation.ForwardingRules
	ts       []mimirpb.PreallocTimeseries
	messages []kafka.Message
	counts   TimeseriesCounts

	requests  prometheus.Counter
	errors    *prometheus.CounterVec
	samples   prometheus.Counter
	exemplars prometheus.Counter
	latency   prometheus.Histogram
}

// submitForwardingRequest launches a new forwarding request and sends it to a worker via a channel.
// It might block if all the workers are busy.
func (f *forwarder) submitForwardingRequest(ctx context.Context, user string, rules validation.ForwardingRules, ts []mimirpb.PreallocTimeseries, counts TimeseriesCounts, requestWg *sync.WaitGroup, errCh chan error) {
	req := f.pools.getReq()

	req.pools = f.pools
	req.kafkaWriter = f.kafkaWriter
	req.log = f.log
	req.ctx = ctx
	req.timeout = f.cfg.RequestTimeout
	req.propagateErrors = f.cfg.PropagateErrors
	req.errCh = errCh
	req.requestWg = requestWg

	// Request parameters.
	req.user = user
	req.ts = ts
	req.counts = counts
	req.rules = rules

	// Metrics.
	req.requests = f.requestsTotal
	req.errors = f.errorsTotal
	req.samples = f.samplesTotal
	req.exemplars = f.exemplarsTotal
	req.latency = f.requestLatencyHistogram

	select {
	case <-ctx.Done():
	case f.reqCh <- req:
	}
}

// do performs a forwarding request.
func (r *request) do() {
	spanlog, ctx := spanlogger.NewWithLogger(r.ctx, r.log, "request.do")
	defer spanlog.Finish()

	defer r.cleanup()

	err := r.buildKafkaMessages()
	if err != nil {
		r.errors.WithLabelValues("failed").Inc()

		r.handleError(ctx, http.StatusInternalServerError, err)
	}

	r.requests.Inc()
	r.samples.Add(float64(r.counts.SampleCount))
	r.exemplars.Add(float64(r.counts.ExemplarCount))

	beforeTs := time.Now()
	err = r.kafkaWriter.WriteMessages(ctx, r.messages...)
	r.latency.Observe(time.Since(beforeTs).Seconds())
	if err != nil {
		r.handleError(ctx, http.StatusInternalServerError, err)
	}
}

func (r *request) buildKafkaMessages() error {
	if cap(r.messages) < len(r.ts) {
		r.messages = make([]kafka.Message, len(r.ts))
	} else {
		r.messages = r.messages[:len(r.ts)]
	}

	for i, t := range r.ts {
		protoBuf := proto.NewBuffer(r.messages[i].Value[:0])
		err := protoBuf.Marshal(t)
		if err != nil {
			return errors.Wrap(err, "failed to marshal time series for submitting to Kafka")
		}
		r.messages[i].Key, err = util_kafka.ComposeKafkaKey(r.messages[i].Key, []byte(r.user), t.Labels, r.rules)
		if err != nil {
			return errors.Wrap(err, "failed to compose kafka key")
		}
	}

	return nil
}

func (r *request) handleError(ctx context.Context, status int, err error) {
	logger := spanlogger.FromContext(ctx, r.log)
	level.Warn(logger).Log("msg", "error in forwarding request", "statusCode", status, "err", err)
	if r.propagateErrors {
		r.errCh <- httpgrpc.Errorf(status, err.Error())
	}
}

func (r *request) cleanup() {
	r.pools.putTsSlice(r.ts)

	// Ensure that we don't modify a request property after returning it to the pool by calling .Done() on the wg.
	wg := r.requestWg
	r.requestWg = nil

	for i := range r.messages {
		r.messages[i].Topic = ""
		r.messages[i].Partition = 0
		r.messages[i].Offset = 0
		r.messages[i].HighWaterMark = 0
		r.messages[i].Key = r.messages[i].Key[:0]
		r.messages[i].Value = r.messages[i].Value[:0]
		r.messages[i].Headers = r.messages[i].Headers[:0]
		r.messages[i].Time = time.Time{}
	}
	r.messages = r.messages[:0]

	// Return request to the pool before calling wg.Done() because otherwise the tests can't wait for the request to
	// be completely done in order to validate the pool usage.
	r.pools.putReq(r)

	wg.Done()
}
