// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/validation"
)

type Forwarder interface {
	services.Service
	Forward(ctx context.Context, targetEndpoint string, forwardingRules validation.ForwardingRules, ts []mimirpb.PreallocTimeseries) ([]mimirpb.PreallocTimeseries, chan error)
}

type forwarder struct {
	services.Service

	cfg      Config
	pools    *pools
	client   http.Client
	log      log.Logger
	workerWg sync.WaitGroup
	reqCh    chan *request

	requestsTotal           prometheus.Counter
	errorsTotal             *prometheus.CounterVec
	samplesTotal            prometheus.Counter
	exemplarsTotal          prometheus.Counter
	requestLatencyHistogram prometheus.Histogram
}

// NewForwarder returns a new forwarder, if forwarding is disabled it returns nil.
func NewForwarder(cfg Config, reg prometheus.Registerer, log log.Logger) Forwarder {
	if !cfg.Enabled {
		return nil
	}

	f := &forwarder{
		cfg:   cfg,
		pools: newPools(),
		log:   log,
		reqCh: make(chan *request, cfg.RequestConcurrency),

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
	}

	f.Service = services.NewIdleService(f.start, f.stop)

	return f
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
// If endpoint is not empty, it is used instead of any rule-specific endpoints.
//
// The return values are:
//   - A slice of time series which should be sent to the ingesters, based on the given rule set.
//     The Forward() method does not send the time series to the ingesters itself, it expects the caller to do that.
//   - A chan of errors which resulted from forwarding the time series, the chan gets closed when all forwarding requests have completed.
func (f *forwarder) Forward(ctx context.Context, endpoint string, rules validation.ForwardingRules, in []mimirpb.PreallocTimeseries) ([]mimirpb.PreallocTimeseries, chan error) {
	if !f.cfg.Enabled {
		errCh := make(chan error)
		close(errCh)
		return in, errCh
	}

	toIngest, tsByTargets := f.splitByTargets(endpoint, in, rules)
	defer f.pools.putTsByTargets(tsByTargets)

	var requestWg sync.WaitGroup
	requestWg.Add(len(tsByTargets))
	errCh := make(chan error, len(tsByTargets))
	for endpoint, ts := range tsByTargets {
		f.submitForwardingRequest(ctx, endpoint, ts, &requestWg, errCh)
	}

	go func() {
		// Waiting for the requestWg in a go routine allows Forward() to return early, that way the call site can
		// continue doing whatever it needs to do and read the returned errCh later to wait for the forwarding requests
		// to complete and handle potential errors yielded by the forwarding requests.
		requestWg.Wait()
		close(errCh)
	}()

	return toIngest, errCh
}

type tsWithSampleCount struct {
	ts     []mimirpb.PreallocTimeseries
	counts TimeseriesCounts
}

type tsByTargets map[string]tsWithSampleCount

// copyToTarget copies the given time series into the given target and does the necessary accounting.
// The time series is deep-copied, so the passed in time series can be returned to the pool without affecting the copy.
func (t tsByTargets) copyToTarget(target string, ts mimirpb.PreallocTimeseries, pool *pools) {
	tsByTarget, ok := t[target]
	if !ok {
		tsByTarget.ts = pool.getTsSlice()
	}

	tsIdx := len(tsByTarget.ts)
	tsByTarget.ts = append(tsByTarget.ts, mimirpb.PreallocTimeseries{TimeSeries: pool.getTs()})

	// We don't keep exemplars when forwarding.
	tsByTarget.ts[tsIdx] = mimirpb.DeepCopyTimeseries(tsByTarget.ts[tsIdx], ts, false)
	tsByTarget.counts.count(tsByTarget.ts[tsIdx])

	t[target] = tsByTarget
}

type TimeseriesCounts struct {
	SampleCount   int
	ExemplarCount int
}

func (t *TimeseriesCounts) count(ts mimirpb.PreallocTimeseries) {
	t.SampleCount += len(ts.TimeSeries.Samples)
	t.ExemplarCount += len(ts.TimeSeries.Exemplars)
}

// splitByTargets takes a slice of time series and a set of forwarding rules, then it divides the given time series by
// the target to which each of them should be forwarded according to the forwarding rules.
// It returns the following values:
//
// - A slice of time series to ingest into the ingesters.
// - A map of slices of time series which is keyed by the target to which they should be forwarded.
func (f *forwarder) splitByTargets(targetEndpoint string, tsSliceIn []mimirpb.PreallocTimeseries, rules validation.ForwardingRules) ([]mimirpb.PreallocTimeseries, tsByTargets) {
	// This functions copies all the entries of tsSliceIn into new slices so tsSliceIn can be recycled,
	// we adjust the length of the slice to 0 to prevent that the contained *mimirpb.TimeSeries objects that have been
	// reassigned (not deep copied) get returned while they are still referred to by another slice.
	defer f.pools.putTsSlice(tsSliceIn[:0])

	tsToIngest := f.pools.getTsSlice()
	tsByTargets := f.pools.getTsByTargets()
	for _, ts := range tsSliceIn {
		forwardingTarget, ingest := findTargetForLabels(targetEndpoint, ts.Labels, rules)
		if forwardingTarget != "" {
			tsByTargets.copyToTarget(forwardingTarget, ts, f.pools)
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

	return tsToIngest, tsByTargets
}

func findTargetForLabels(targetEndpoint string, labels []mimirpb.LabelAdapter, rules validation.ForwardingRules) (string, bool) {
	metric, err := extract.UnsafeMetricNameFromLabelAdapters(labels)
	if err != nil {
		// Can't check whether a timeseries should be forwarded if it has no metric name.
		// Ingest it and don't forward it.
		return "", true
	}

	rule, ok := rules[metric]
	if !ok {
		// There is no forwarding rule for this metric, ingest it and don't forward it.
		return "", true
	}

	// Target endpoint is set, use it.
	if targetEndpoint != "" {
		return targetEndpoint, rule.Ingest
	}
	return rule.Endpoint, rule.Ingest
}

type request struct {
	pools  *pools
	client *http.Client
	log    log.Logger

	ctx             context.Context
	timeout         time.Duration
	propagateErrors bool
	errCh           chan error
	requestWg       *sync.WaitGroup

	endpoint string
	ts       tsWithSampleCount

	requests  prometheus.Counter
	errors    *prometheus.CounterVec
	samples   prometheus.Counter
	exemplars prometheus.Counter
	latency   prometheus.Histogram
}

// submitForwardingRequest launches a new forwarding request and sends it to a worker via a channel.
// It might block if all the workers are busy.
func (f *forwarder) submitForwardingRequest(ctx context.Context, endpoint string, ts tsWithSampleCount, requestWg *sync.WaitGroup, errCh chan error) {
	req := f.pools.getReq()

	req.pools = f.pools
	req.client = &f.client // http client should be re-used so open connections get re-used.
	req.log = f.log
	req.ctx = ctx
	req.timeout = f.cfg.RequestTimeout
	req.propagateErrors = f.cfg.PropagateErrors
	req.errCh = errCh
	req.requestWg = requestWg

	// Target endpoint and TimeSeries to forward.
	req.endpoint = endpoint
	req.ts = ts

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
	defer r.cleanup()

	protoBufBytesRef := r.pools.getProtobuf()
	protoBufBytes := (*protoBufBytesRef)[:0]
	defer func() {
		*protoBufBytesRef = protoBufBytes // just in case we increased its capacity
		r.pools.putProtobuf(protoBufBytesRef)
	}()

	protoBuf := proto.NewBuffer(protoBufBytes)
	err := protoBuf.Marshal(&mimirpb.WriteRequest{Timeseries: r.ts.ts})
	if err != nil {
		r.handleError(http.StatusBadRequest, errors.Wrap(err, "failed to marshal write request for forwarding"))
		return
	}

	snappyBuf := *r.pools.getSnappy()
	defer r.pools.putSnappy(&snappyBuf)

	protoBufBytes = protoBuf.Bytes()
	snappyBuf = snappy.Encode(snappyBuf[:cap(snappyBuf)], protoBufBytes)

	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	defer cancel()

	bytesReader := r.pools.getBytesReader()
	defer r.pools.putBytesReader(bytesReader)

	bytesReader.Reset(snappyBuf)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", r.endpoint, bytesReader)
	if err != nil {
		// Errors from NewRequest are from unparsable URLs being configured, so this is an internal server error.
		r.handleError(http.StatusInternalServerError, errors.Wrap(err, "failed to create HTTP request for forwarding"))
		return
	}

	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")

	r.requests.Inc()
	r.samples.Add(float64(r.ts.counts.SampleCount))
	r.exemplars.Add(float64(r.ts.counts.ExemplarCount))

	beforeTs := time.Now()
	httpResp, err := r.client.Do(httpReq)
	r.latency.Observe(time.Since(beforeTs).Seconds())
	if err != nil {
		// Errors from Client.Do are from (for example) network errors, so we want the client to retry.
		r.handleError(http.StatusInternalServerError, errors.Wrap(err, "failed to send HTTP request for forwarding"))
		return
	}
	defer func() {
		io.Copy(ioutil.Discard, httpResp.Body)
		httpResp.Body.Close()
	}()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, 1024))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		r.errors.WithLabelValues(strconv.Itoa(httpResp.StatusCode)).Inc()
		err := errors.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
		if httpResp.StatusCode/100 == 5 || httpResp.StatusCode == http.StatusTooManyRequests {
			// The forwarding endpoint has returned a retriable error, so we want the client to retry.
			r.handleError(http.StatusInternalServerError, err)
			return
		}
		r.handleError(http.StatusBadRequest, err)
	}
}

func (r *request) handleError(status int, err error) {
	errMsg := err.Error()
	level.Warn(r.log).Log("msg", "error in forwarding request", "err", errMsg)
	if r.propagateErrors {
		r.errCh <- httpgrpc.Errorf(status, errMsg)
	}
}

func (r *request) cleanup() {
	ts := r.ts.ts
	r.pools.putTsSlice(ts)

	// Ensure that we don't modify a request property after returning it to the pool by calling .Done() on the wg.
	wg := r.requestWg
	r.requestWg = nil

	// Return request to the pool before calling wg.Done() because otherwise the tests can't wait for the request to
	// be completely done in order to validate the pool usage.
	r.pools.putReq(r)

	wg.Done()
}
