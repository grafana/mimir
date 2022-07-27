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
	Forward(ctx context.Context, forwardingRules validation.ForwardingRules, ts []mimirpb.PreallocTimeseries) (TimeseriesCounts, []mimirpb.PreallocTimeseries, chan error)
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

	f.Service = services.NewBasicService(f.start, f.loop, f.stop)

	return f
}

func (f *forwarder) start(ctx context.Context) error {
	f.workerWg.Add(f.cfg.RequestConcurrency)

	for i := 0; i < f.cfg.RequestConcurrency; i++ {
		go f.worker()
	}

	return nil
}

func (f *forwarder) loop(ctx context.Context) error {
	<-ctx.Done()
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

func (f *forwarder) Forward(ctx context.Context, rules validation.ForwardingRules, in []mimirpb.PreallocTimeseries) (TimeseriesCounts, []mimirpb.PreallocTimeseries, chan error) {
	if !f.cfg.Enabled {
		errCh := make(chan error)
		close(errCh)
		return TimeseriesCounts{}, in, errCh
	}

	notIngestedCounts, toIngest, tsByTargets := f.splitByTargets(in, rules)
	defer f.pools.putTsByTargets(tsByTargets)

	var requestWg sync.WaitGroup
	requestWg.Add(len(tsByTargets))
	errCh := make(chan error, len(tsByTargets))
	for endpoint, ts := range tsByTargets {
		f.newRequest(ctx, endpoint, ts, &requestWg, errCh)
	}

	go func() {
		// Waiting for the requestWg in a go routine allows Forward() to return early, that way the call site can
		// continue doing whatever it needs to do and read the returned errCh later to wait for the forwarding requests
		// to complete and handle potential errors yielded by the forwarding requests.
		requestWg.Wait()
		close(errCh)
	}()

	return notIngestedCounts, toIngest, errCh
}

type tsWithSampleCount struct {
	ts     []mimirpb.PreallocTimeseries
	counts TimeseriesCounts
}

type tsByTargets map[string]tsWithSampleCount

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
// - The counts of samples and exemplars that will not be ingested by the ingesters.
// - A slice of time series to ingest into the ingesters.
// - A map of slices of time series which is keyed by the target to which they should be forwarded.
func (f *forwarder) splitByTargets(tsSlice []mimirpb.PreallocTimeseries, rules validation.ForwardingRules) (TimeseriesCounts, []mimirpb.PreallocTimeseries, tsByTargets) {
	// notIngestedCounts keeps track of the number of samples and exemplars that we don't send to the ingesters,
	// we need to count these in order to later update some of the distributor's metrics correctly.
	var notIngestedCounts TimeseriesCounts

	// tsSliceWriteIdx is the index in the toIngest slice where we are writing TimeSeries to.
	tsSliceWriteIdx := 0

	tsByTargets := f.pools.getTsByTargets()
	for tsSliceReadIdx, ts := range tsSlice {
		ingest := false
		var forwardingTarget string

		metric, err := extract.UnsafeMetricNameFromLabelAdapters(ts.Labels)
		if err != nil {
			// Can't check whether a timeseries should be forwarded if it has no metric name.
			// Send it to the Ingesters and don't forward it.
			ingest = true
		} else {
			rule, ok := rules[metric]
			if !ok {
				// There is no forwarding rule for this metric, send it to the Ingesters and don't forward it.
				ingest = true
			} else {
				forwardingTarget = rule.Endpoint
				ingest = rule.Ingest
			}
		}

		if forwardingTarget != "" {
			tsByTarget, ok := tsByTargets[forwardingTarget]
			if !ok {
				tsByTarget.ts = f.pools.getTsSlice()
			}

			tsByTarget.ts = f.growTimeseriesSlice(tsByTarget.ts)
			tsWriteIdx := len(tsByTarget.ts) - 1
			mimirpb.DeepCopyTimeseries(tsByTarget.ts[tsWriteIdx].TimeSeries, ts.TimeSeries)
			tsByTarget.counts.count(tsByTarget.ts[tsWriteIdx])

			tsByTargets[forwardingTarget] = tsByTarget
		}

		if ingest {
			if tsSliceWriteIdx != tsSliceReadIdx {
				// Swap the timeseries at the reading and writing indices,
				// later we'll return all timeseries beyond the write index to the pool.
				tsSlice[tsSliceWriteIdx], tsSlice[tsSliceReadIdx] = tsSlice[tsSliceReadIdx], tsSlice[tsSliceWriteIdx]
			}
			tsSliceWriteIdx++
		} else {
			notIngestedCounts.count(ts)
		}
	}

	// Truncate the toIngest slice to the index up to which we wrote TimeSeries data into it,
	// all the TimeSeries objects beyond the write index must be returned to the pool.
	for _, ts := range tsSlice[tsSliceWriteIdx:] {
		f.pools.putTs(ts.TimeSeries)
	}
	tsSlice = tsSlice[:tsSliceWriteIdx]

	return notIngestedCounts, tsSlice, tsByTargets
}

func (f *forwarder) growTimeseriesSlice(ts []mimirpb.PreallocTimeseries) []mimirpb.PreallocTimeseries {
	newPos := len(ts)

	if cap(ts) > len(ts) {
		ts = ts[:newPos+1]
	} else {
		ts = append(ts, mimirpb.PreallocTimeseries{})
	}

	ts[newPos].TimeSeries = f.pools.getTs()

	return ts
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

// newRequest launches a new forwarding request and sends it to a worker via a channel.
// It might block if all the workers are busy.
func (f *forwarder) newRequest(ctx context.Context, endpoint string, ts tsWithSampleCount, requestWg *sync.WaitGroup, errCh chan error) {
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

	protoBufBytes := (*r.pools.getProtobuf())[:0]
	defer r.pools.putProtobuf(&protoBufBytes)

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
	r.pools.putTsSlice(r.ts.ts)

	// Ensure we don't modify a request property after returning it to the pool by calling .Done() on the wait group.
	wg := r.requestWg
	r.requestWg = nil

	r.pools.putReq(r)

	wg.Done()
}
