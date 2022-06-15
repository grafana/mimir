// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/validation"
)

var errBadEndpointConfiguration = errors.New("bad endpoint configuration")

type Forwarder interface {
	NewRequest(ctx context.Context, tenant string, rules validation.ForwardingRules) Request
}

type Request interface {
	// Add adds a timeseries to the forwarding request.
	// Samples which don't match any forwarding rule won't be added to the request.
	// It returns a bool which indicates whether this timeseries should be sent to the Ingesters.
	// A timeseries should be sent to the Ingester if any of the following conditions are true:
	// - It has a labelset without a metric name, hence it can't match a forwarding rule.
	// - There is no matching forwarding rule for the metric name of the timeseries.
	// - There is a forwarding rule which defines that this metric should be forwarded and also pushed to the Ingesters.
	Add(sample mimirpb.PreallocTimeseries) bool

	// Send sends the timeseries which have been added to this forwarding request to the according endpoints.
	// All errors returned via the returned error chan are http grpc errors.
	// Send should only be called once, after it has been called this forwardingRequest must not be used anymore.
	Send(ctx context.Context) <-chan error
}

// pools is the collection of pools which the forwarding uses when building remote_write requests.
// Even though protobuf and snappy are both pools of []byte we keep them separate because the slices
// which they contain are likely to have very different sizes.
type pools struct {
	timeseries sync.Pool
	protobuf   sync.Pool
	snappy     sync.Pool
}

type forwarder struct {
	cfg    Config
	pools  pools
	client http.Client

	requestsTotal           prometheus.Counter
	errorsTotal             *prometheus.CounterVec
	samplesTotal            prometheus.Counter
	requestLatencyHistogram prometheus.Histogram
}

// NewForwarder returns a new forwarder, if forwarding is disabled it returns nil.
func NewForwarder(reg prometheus.Registerer, cfg Config) Forwarder {
	if !cfg.Enabled {
		return nil
	}

	return &forwarder{
		cfg: cfg,
		pools: pools{
			timeseries: sync.Pool{New: func() interface{} { return &[]mimirpb.PreallocTimeseries{} }},
			protobuf:   sync.Pool{New: func() interface{} { return &[]byte{} }},
			snappy:     sync.Pool{New: func() interface{} { return &[]byte{} }},
		},

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
		requestLatencyHistogram: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "distributor_forward_requests_latency_seconds",
			Help:      "The client-side latency of requests to forward metrics made by the Distributor.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
		}),
	}
}

func (r *forwarder) NewRequest(ctx context.Context, tenant string, rules validation.ForwardingRules) Request {
	return &request{
		ctx:    ctx,
		client: &r.client, // http client should be re-used so open connections get re-used.
		pools:  &r.pools,

		tsByEndpoint: make(map[string]*[]mimirpb.PreallocTimeseries),

		rules:           rules,
		timeout:         r.cfg.RequestTimeout,
		propagateErrors: r.cfg.PropagateErrors,

		requests: r.requestsTotal,
		errors:   r.errorsTotal,
		samples:  r.samplesTotal,
		latency:  r.requestLatencyHistogram,
	}
}

type request struct {
	ctx    context.Context
	client *http.Client
	pools  *pools

	tsByEndpoint map[string]*[]mimirpb.PreallocTimeseries

	// The rules which define:
	// - which metrics get forwarded
	// - where the metrics get forwarded to
	// - whether the forwarded metrics should also be ingested (sent to ingesters)
	rules           validation.ForwardingRules
	timeout         time.Duration
	propagateErrors bool

	requests prometheus.Counter
	errors   *prometheus.CounterVec
	samples  prometheus.Counter
	latency  prometheus.Histogram
}

func (r *request) Add(sample mimirpb.PreallocTimeseries) bool {
	metric, err := extract.UnsafeMetricNameFromLabelAdapters(sample.Labels)
	if err != nil {
		// The only possible error is due to no metric name being defined, in which case we won't forward the sample
		// to anywhere but we should still send it to the Ingesters.
		return true
	}

	rule, ok := r.rules[metric]
	if !ok {
		// There is no forwarding rule for this metric, send it to the Ingesters.
		return true
	}
	r.samples.Add(float64(len(sample.Samples)))

	ts, ok := r.tsByEndpoint[rule.Endpoint]
	if !ok {
		ts = r.pools.timeseries.Get().(*[]mimirpb.PreallocTimeseries)
		r.requests.Inc()
	}

	*ts = append(*ts, sample)
	r.tsByEndpoint[rule.Endpoint] = ts

	return rule.Ingest
}

type recoverableError struct {
	error
}

func (r *request) Send(ctx context.Context) <-chan error {
	errCh := make(chan error, 1)

	// Early return if there's no data to send.
	if len(r.tsByEndpoint) == 0 {
		close(errCh)
		return errCh
	}

	returnErr := func(err error) {
		if !r.propagateErrors {
			return
		}
		errCh <- err
	}

	var wg sync.WaitGroup
	wg.Add(len(r.tsByEndpoint))

	// Can't use concurrency.ForEachJob because we don't want to cancel the other jobs if one errors.
	errorsByEndpoint := make(map[string]error, len(r.tsByEndpoint))
	var errorsMtx sync.Mutex
	for endpoint, ts := range r.tsByEndpoint {
		go func(endpoint string, ts []mimirpb.PreallocTimeseries) {
			defer wg.Done()

			errorsMtx.Lock()
			defer errorsMtx.Unlock()
			errorsByEndpoint[endpoint] = r.sendToEndpoint(ctx, endpoint, ts)
		}(endpoint, *ts)
	}

	go func() {
		defer r.cleanup()
		defer close(errCh)

		wg.Wait()

		var nonRecoverable error
		// No need to get errorsMtx because we already waited for all routines which might modify it to end.
		for endpoint, err := range errorsByEndpoint {
			if err == nil {
				continue
			}

			if errors.As(err, &recoverableError{}) {
				// If there is at least one recoverable error we want to return the recoverable error.
				returnErr(httpgrpc.Errorf(http.StatusInternalServerError, "endpoint %s: %s", endpoint, err.Error()))
				return
			}

			nonRecoverable = httpgrpc.Errorf(http.StatusBadRequest, "endpoint %s: %s", endpoint, err.Error())
		}

		if nonRecoverable != nil {
			returnErr(nonRecoverable)
		}
	}()

	return errCh
}

// sendToEndpoint sends the given timeseries to the given endpoint.
// All returned errors which are recoverable are of the type recoverableError.
func (r *request) sendToEndpoint(ctx context.Context, endpoint string, ts []mimirpb.PreallocTimeseries) error {
	protoBufBytes := (*r.pools.protobuf.Get().(*[]byte))[:0]
	protoBuf := proto.NewBuffer(protoBufBytes)
	err := protoBuf.Marshal(&mimirpb.WriteRequest{Timeseries: ts})
	if err != nil {
		return err
	}
	protoBufBytes = protoBuf.Bytes()
	defer r.pools.protobuf.Put(&protoBufBytes)

	snappyBuf := *r.pools.snappy.Get().(*[]byte)
	snappyBuf = snappy.Encode(snappyBuf[:cap(snappyBuf)], protoBufBytes)
	defer r.pools.snappy.Put(&snappyBuf)

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(snappyBuf))
	if err != nil {
		// Errors from NewRequest are from unparsable URLs being configured.
		// Usually configuration errors should lead to recoverable errors (5xx), but this is an exception because we
		// don't want that a misconfigured forwarding rule can stop ingestion completely, so we return a non-recoverable
		// to make the client move on and not retry the request.
		return errBadEndpointConfiguration
	}

	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")

	beforeTs := time.Now()
	httpResp, err := r.client.Do(httpReq)
	r.latency.Observe(time.Since(beforeTs).Seconds())
	if err != nil {
		// Errors from Client.Do are from (for example) network errors, so are recoverable.
		return recoverableError{err}
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
			return recoverableError{err}
		}
		return err
	}

	return nil
}

// cleanup must be called to return the used buffers to their pools after a request has completed.
func (r *request) cleanup() {
	for _, ts := range r.tsByEndpoint {
		*ts = (*ts)[:0]
		r.pools.timeseries.Put(ts)
	}

	r.tsByEndpoint = nil
}
