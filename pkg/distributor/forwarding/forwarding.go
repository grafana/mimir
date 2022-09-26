// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"bufio"
	"bytes"
	"context"
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
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/validation"
)

type Forwarder interface {
	services.Service
	Forward(ctx context.Context, targetEndpoint string, dontForwardOlderThan time.Duration, forwardingRules validation.ForwardingRules, ts []mimirpb.PreallocTimeseries) ([]mimirpb.PreallocTimeseries, chan error)
}

// httpGrpcPrefix is URL prefix used by forwarder to check if it should use httpgrpc for sending request over to the target.
// Full URL is in the form: httpgrpc://hostname:port/some/path, where "hostname:port" will be used to establish gRPC connection
// (by adding "dns:///" prefix before passing it to grpc client, to enable client-side load balancing), and "/some/path" will be included in the "url"
// part of the message for "httpgrpc.HTTP/Handle" method.
const httpGrpcPrefix = "httpgrpc://"

var errSamplesTooOld = httpgrpc.Errorf(400, "dropped sample(s) because too old to forward")

type forwarder struct {
	services.Service

	cfg                Config
	pools              *pools
	client             http.Client
	log                log.Logger
	workerWg           sync.WaitGroup
	reqCh              chan *request
	httpGrpcClientPool *client.Pool

	requestsTotal           prometheus.Counter
	errorsTotal             *prometheus.CounterVec
	samplesTotal            prometheus.Counter
	exemplarsTotal          prometheus.Counter
	requestLatencyHistogram prometheus.Histogram
	grpcClientsGauge        prometheus.Gauge

	timeNow func() time.Time
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
		client: http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        0,                      // no limit
				MaxIdleConnsPerHost: cfg.RequestConcurrency, // if MaxIdleConnsPerHost is left as 0, default value of 2 is used.
				MaxConnsPerHost:     0,                      // no limit
				IdleConnTimeout:     10 * time.Second,       // don't keep unused connections for too long
			},
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

		timeNow: time.Now,
	}

	f.httpGrpcClientPool = f.newHTTPGrpcClientsPool()
	f.Service = services.NewIdleService(f.start, f.stop)

	return f
}

func (f *forwarder) newHTTPGrpcClientsPool() *client.Pool {
	poolConfig := client.PoolConfig{
		CheckInterval: 5 * time.Second,

		// There may be multiple hosts behind single PoolClient, we don't want to close entire client if one of hosts
		// is unhealthy.
		HealthCheckEnabled: false,
	}

	return client.NewPool("forwarding-httpgrpc", poolConfig, nil, f.createHTTPGrpcClient, f.grpcClientsGauge, f.log)
}

func (f *forwarder) createHTTPGrpcClient(addr string) (client.PoolClient, error) {
	opts, err := f.cfg.GRPCClientConfig.DialOption(nil, nil)

	if err != nil {
		return nil, err
	}

	const grpcServiceConfig = `{"loadBalancingPolicy":"round_robin"}`

	opts = append(opts,
		grpc.WithBlock(),
		grpc.WithDefaultServiceConfig(grpcServiceConfig),

		// These settings are quite low, and require server to be tolerant to them.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * 10,
			Timeout:             time.Second * 5,
			PermitWithoutStream: true,
		}),
	)

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &grpcHTTPClient{
		HTTPClient:   httpgrpc.NewHTTPClient(conn),
		HealthClient: grpc_health_v1.NewHealthClient(conn),
		conn:         conn,
	}, nil
}

func (f *forwarder) start(ctx context.Context) error {
	if err := services.StartAndAwaitRunning(ctx, f.httpGrpcClientPool); err != nil {
		return errors.Wrap(err, "failed to start grpc client pool")
	}

	f.workerWg.Add(f.cfg.RequestConcurrency)

	for i := 0; i < f.cfg.RequestConcurrency; i++ {
		go f.worker()
	}

	return nil
}

func (f *forwarder) stop(_ error) error {
	close(f.reqCh)
	f.workerWg.Wait()

	if err := services.StopAndAwaitTerminated(context.Background(), f.httpGrpcClientPool); err != nil {
		return errors.Wrap(err, "failed to stop grpc client pool")
	}
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
func (f *forwarder) Forward(ctx context.Context, endpoint string, dontForwardOlderThan time.Duration, rules validation.ForwardingRules, in []mimirpb.PreallocTimeseries) ([]mimirpb.PreallocTimeseries, chan error) {
	if !f.cfg.Enabled {
		errCh := make(chan error)
		close(errCh)
		return in, errCh
	}

	toIngest, tsByTargets, err := f.splitByTargets(endpoint, dontForwardOlderThan.Milliseconds(), in, rules)
	defer f.pools.putTsByTargets(tsByTargets)
	errCh := make(chan error, len(tsByTargets)+1)
	if err != nil {
		errCh <- err
	}

	var requestWg sync.WaitGroup
	requestWg.Add(len(tsByTargets))
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
func (t tsByTargets) copyToTarget(target string, ts mimirpb.PreallocTimeseries, dontForwardBeforeTs int64, pool *pools) bool {
	samplesTooOld := false

	if dontForwardBeforeTs > 0 && samplesNeedFiltering(ts.TimeSeries.Samples, dontForwardBeforeTs) {
		samplesUnfiltered := ts.TimeSeries.Samples
		defer func() {
			ts.TimeSeries.Samples = samplesUnfiltered
		}()

		samplesFiltered := filterSamplesBefore(samplesUnfiltered, dontForwardBeforeTs)
		if len(samplesFiltered) < len(samplesUnfiltered) {
			samplesTooOld = true
		}

		if len(samplesFiltered) == 0 {
			return samplesTooOld
		}

		ts.TimeSeries.Samples = samplesFiltered
	}

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

	return samplesTooOld
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
// - An error if any occurred.
func (f *forwarder) splitByTargets(targetEndpoint string, dontForwardOlderThan int64, tsSliceIn []mimirpb.PreallocTimeseries, rules validation.ForwardingRules) ([]mimirpb.PreallocTimeseries, tsByTargets, error) {
	// This functions copies all the entries of tsSliceIn into new slices so tsSliceIn can be recycled,
	// we adjust the length of the slice to 0 to prevent that the contained *mimirpb.TimeSeries objects that have been
	// reassigned (not deep copied) get returned while they are still referred to by another slice.
	defer f.pools.putTsSlice(tsSliceIn[:0])

	var dontForwardBeforeTs int64
	if dontForwardOlderThan > 0 {
		dontForwardBeforeTs = f.timeNow().UnixMilli() - dontForwardOlderThan
	}

	var err error
	tsToIngest := f.pools.getTsSlice()
	tsByTargets := f.pools.getTsByTargets()
	for _, ts := range tsSliceIn {
		forwardingTarget, ingest := findTargetForLabels(targetEndpoint, ts.Labels, rules)
		if forwardingTarget != "" {
			if tsByTargets.copyToTarget(forwardingTarget, ts, dontForwardBeforeTs, f.pools) {
				err = errSamplesTooOld
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

	return tsToIngest, tsByTargets, err
}

// samplesNeedFiltering takes a slice of samples and a timestamp before which samples should be filtered out,
// it returns true if some samples are older than the given timestamp or false otherwise.
// It assumes that the samples are sorted by timestamp.
func samplesNeedFiltering(samples []mimirpb.Sample, dontForwardBefore int64) bool {
	if len(samples) == 0 {
		return false
	}

	return samples[0].TimestampMs < dontForwardBefore
}

// filterSamplesBefore filters a given slice of samples to only contain samples that have timestamps newer or equal to
// the given timestamp. It relies on the samples being sorted by timestamp.
// If some samples have been filtered the second return value is true, otherwise it is false.
func filterSamplesBefore(samples []mimirpb.Sample, dontForwardBefore int64) []mimirpb.Sample {
	if dontForwardBefore == 0 {
		return samples
	}

	for sampleIdx, sample := range samples {
		if sample.TimestampMs >= dontForwardBefore {
			// In most cases the first sample should already meet this condition and we can return quickly.
			return samples[sampleIdx:]
		}
	}

	return samples[:0]
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
	pools              *pools
	client             *http.Client
	httpGrpcClientPool *client.Pool
	log                log.Logger

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
	req.httpGrpcClientPool = f.httpGrpcClientPool
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

	if strings.HasPrefix(r.endpoint, httpGrpcPrefix) {
		err = r.doHTTPGrpc(ctx, snappyBuf)
	} else {
		err = r.doHTTP(ctx, snappyBuf)
	}

	if err != nil {
		r.errors.WithLabelValues("failed").Inc()

		r.handleError(http.StatusInternalServerError, err)
	}
}

func (r *request) doHTTP(ctx context.Context, body []byte) error {
	httpReq, err := http.NewRequestWithContext(ctx, "POST", r.endpoint, bytes.NewReader(body))
	if err != nil {
		// Errors from NewRequest are from unparsable URLs being configured, so this is an internal server error.
		return errors.Wrap(err, "failed to create HTTP request for forwarding")
	}

	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	// Mark request as idempotent, so that http client can retry them on (some) errors.
	httpReq.Header.Set("Idempotency-Key", "true")

	r.requests.Inc()
	r.samples.Add(float64(r.ts.counts.SampleCount))
	r.exemplars.Add(float64(r.ts.counts.ExemplarCount))

	beforeTs := time.Now()
	httpResp, err := r.client.Do(httpReq)
	r.latency.Observe(time.Since(beforeTs).Seconds())
	if err != nil {
		// Errors from Client.Do are from (for example) network errors, so we want the client to retry.
		return errors.Wrap(err, "failed to send HTTP request for forwarding")
	}
	defer func() {
		io.Copy(io.Discard, httpResp.Body)
		httpResp.Body.Close()
	}()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, 1024))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}

		r.processHTTPResponse(httpResp.StatusCode, line)
	}
	return nil
}

func (r *request) processHTTPResponse(code int, message string) {
	r.errors.WithLabelValues(strconv.Itoa(code)).Inc()

	err := errors.Errorf("server returned HTTP status %d: %s", code, message)
	if code/100 == 5 || code == http.StatusTooManyRequests {
		// The forwarding endpoint has returned a retriable error, so we want the client to retry.
		r.handleError(http.StatusInternalServerError, err)
		return
	}
	r.handleError(http.StatusBadRequest, err)
}

var headers = []*httpgrpc.Header{
	{
		Key:    "Content-Encoding",
		Values: []string{"snappy"},
	},
	{
		Key:    "Content-Type",
		Values: []string{"application/x-protobuf"},
	},
}

func (r *request) doHTTPGrpc(ctx context.Context, body []byte) error {
	u, err := url.Parse(r.endpoint)
	if err != nil {
		return errors.Wrapf(err, "failed to parse URL for HTTP GRPC request forwarding: %s", r.endpoint)
	}

	req := &httpgrpc.HTTPRequest{
		Method:  "POST",
		Url:     u.Path,
		Body:    body,
		Headers: headers,
	}

	// Use dns:/// prefix to enable client-side load balancing inside gRPC client.
	// gRPC client interprets the address as "[scheme]://[authority]/endpoint, so technically we pass the host:port part to the endpoint.
	// Authority for "dns" would be DNS server.
	c, err := r.httpGrpcClientPool.GetClientFor(fmt.Sprintf("dns:///%s", u.Host))
	if err != nil {
		return errors.Wrap(err, "failed to get client for HTTP GRPC request forwarding")
	}

	h := c.(httpgrpc.HTTPClient)

	r.requests.Inc()
	r.samples.Add(float64(r.ts.counts.SampleCount))
	r.exemplars.Add(float64(r.ts.counts.ExemplarCount))

	beforeTs := time.Now()
	resp, err := h.Handle(ctx, req)
	r.latency.Observe(time.Since(beforeTs).Seconds())

	if err != nil {
		if r, ok := httpgrpc.HTTPResponseFromError(err); ok {
			resp = r
		} else {
			return errors.Wrap(err, "failed to send HTTP GRPC request for forwarding")
		}
	}

	if resp.Code/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(bytes.NewReader(resp.Body), 1024))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}

		r.processHTTPResponse(int(resp.Code), line)
	}
	return nil
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

type grpcHTTPClient struct {
	httpgrpc.HTTPClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (fc *grpcHTTPClient) Close() error {
	return fc.conn.Close()
}
