// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
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
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type Forwarder interface {
	services.Service
	Forward(ctx context.Context, targetEndpoint string, dontForwardBefore int64, forwardingRules validation.ForwardingRules, ts []mimirpb.PreallocTimeseries, user string) ([]mimirpb.PreallocTimeseries, chan error)
	DeleteMetricsForUser(user string)
}

// httpGrpcPrefix is URL prefix used by forwarder to check if it should use httpgrpc for sending request over to the target.
// Full URL is in the form: httpgrpc://hostname:port/some/path, where "hostname:port" will be used to establish gRPC connection
// (by adding "dns:///" prefix before passing it to grpc client, to enable client-side load balancing), and "/some/path" will be included in the "url"
// part of the message for "httpgrpc.HTTP/Handle" method.
const httpGrpcPrefix = "httpgrpc://"

var errSamplesTooOld = httpgrpc.Errorf(http.StatusBadRequest, "dropped sample(s) because too old to forward")

type forwarder struct {
	services.Service

	cfg                Config
	pools              *pools
	client             http.Client
	log                log.Logger
	limits             *validation.Overrides
	workerWg           sync.WaitGroup
	reqCh              chan *request
	httpGrpcClientPool *client.Pool
	activeGroups       *util.ActiveGroupsCleanupService

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
		cfg:    cfg,
		pools:  newPools(),
		log:    log,
		limits: limits,
		reqCh:  make(chan *request, cfg.RequestConcurrency),
		client: http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        0,                      // no limit
				MaxIdleConnsPerHost: cfg.RequestConcurrency, // if MaxIdleConnsPerHost is left as 0, default value of 2 is used.
				MaxConnsPerHost:     0,                      // no limit
				IdleConnTimeout:     10 * time.Second,       // don't keep unused connections for too long
			},
		},
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

	f.httpGrpcClientPool = f.newHTTPGrpcClientsPool()
	f.Service = services.NewIdleService(f.start, f.stop)

	return f
}

func (f *forwarder) DeleteMetricsForUser(user string) {
	f.discardedSamplesTooOld.DeleteLabelValues(user)
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
// Endpoint must be non-empty.
//
// The return values are:
//   - A slice of time series which should be sent to the ingesters, based on the given rule set.
//     The Forward() method does not send the time series to the ingesters itself, it expects the caller to do that.
//   - A chan of errors which resulted from forwarding the time series, the chan gets closed when all forwarding requests have completed.
func (f *forwarder) Forward(ctx context.Context, endpoint string, dontForwardBefore int64, rules validation.ForwardingRules, in []mimirpb.PreallocTimeseries, user string) ([]mimirpb.PreallocTimeseries, chan error) {
	if !f.cfg.Enabled || endpoint == "" {
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

		f.submitForwardingRequest(ctx, user, endpoint, toForward, counts, &requestWg, errCh)

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
	group := validation.GroupLabel(f.limits, user, tsSliceIn)
	var err error

	for _, ts := range tsSliceIn {
		forward, ingest := shouldForwardAndIngest(ts.Labels, rules)
		if forward {
			tsCopy, filteredSamples := f.filterAndCopyTimeseries(ts, dontForwardBefore)
			if filteredSamples > 0 {
				err = errSamplesTooOld
				if !ingest {
					f.activeGroups.UpdateActiveGroupTimestamp(user, group, time.Now())
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
	pools              *pools
	client             *http.Client
	httpGrpcClientPool *client.Pool
	log                log.Logger

	ctx             context.Context
	timeout         time.Duration
	propagateErrors bool
	errCh           chan error
	requestWg       *sync.WaitGroup

	user     string
	endpoint string
	ts       []mimirpb.PreallocTimeseries
	counts   TimeseriesCounts

	requests  prometheus.Counter
	errors    *prometheus.CounterVec
	samples   prometheus.Counter
	exemplars prometheus.Counter
	latency   prometheus.Histogram
}

// submitForwardingRequest launches a new forwarding request and sends it to a worker via a channel.
// It might block if all the workers are busy.
func (f *forwarder) submitForwardingRequest(ctx context.Context, user string, endpoint string, ts []mimirpb.PreallocTimeseries, counts TimeseriesCounts, requestWg *sync.WaitGroup, errCh chan error) {
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

	// Request parameters.
	req.user = user
	req.endpoint = endpoint
	req.ts = ts
	req.counts = counts

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

	protoBufBytesRef := r.pools.getProtobuf()
	protoBufBytes := (*protoBufBytesRef)[:0]
	defer func() {
		*protoBufBytesRef = protoBufBytes // just in case we increased its capacity
		r.pools.putProtobuf(protoBufBytesRef)
	}()

	protoBuf := proto.NewBuffer(protoBufBytes)
	err := protoBuf.Marshal(&mimirpb.WriteRequest{Timeseries: r.ts})
	if err != nil {
		r.handleError(ctx, http.StatusBadRequest, errors.Wrap(err, "failed to marshal write request for forwarding"))
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

		r.handleError(ctx, http.StatusInternalServerError, err)
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
	httpReq.Header.Set(user.OrgIDHeaderName, r.user)

	r.requests.Inc()
	r.samples.Add(float64(r.counts.SampleCount))
	r.exemplars.Add(float64(r.counts.ExemplarCount))

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

		r.processHTTPResponse(ctx, httpResp.StatusCode, line)
	}
	return nil
}

func (r *request) processHTTPResponse(ctx context.Context, code int, message string) {
	r.errors.WithLabelValues(strconv.Itoa(code)).Inc()

	err := errors.Errorf("server returned HTTP status %d: %s", code, message)
	if code/100 == 5 || code == http.StatusTooManyRequests {
		// The forwarding endpoint has returned a retriable error, so we want the client to retry.
		r.handleError(ctx, http.StatusInternalServerError, err)
		return
	}
	r.handleError(ctx, http.StatusBadRequest, err)
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
		Method: "POST",
		Url:    u.Path,
		Body:   body,
		Headers: append(headers, &httpgrpc.Header{
			Key:    textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName),
			Values: []string{r.user},
		}),
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
	r.samples.Add(float64(r.counts.SampleCount))
	r.exemplars.Add(float64(r.counts.ExemplarCount))

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

		r.processHTTPResponse(ctx, int(resp.Code), line)
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
