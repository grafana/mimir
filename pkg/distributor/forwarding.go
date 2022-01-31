package distributor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// forwardingPools is the collection of pools which the forwarding uses when building remote_write requests.
// Even though protobuf and snappy are both pools of []byte we keep them separate because the slices
// which they contain are likely to have very different sizes.
type forwardingPools struct {
	timeseries sync.Pool
	protobuf   sync.Pool
	snappy     sync.Pool
}

type forwarding struct {
	pools forwardingPools

	requestsTotal           *prometheus.CounterVec
	requestLatencyHistogram *prometheus.HistogramVec
	samplesTotal            *prometheus.CounterVec
}

func newForwarding(reg prometheus.Registerer) *forwarding {
	return &forwarding{
		pools: forwardingPools{
			timeseries: sync.Pool{New: func() interface{} { return []mimirpb.PreallocTimeseries{} }},
			protobuf:   sync.Pool{New: func() interface{} { return []byte{} }},
			snappy:     sync.Pool{New: func() interface{} { return []byte{} }},
		},

		requestsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_forward_requests_total",
			Help:      "The total number of requests the Distributor made to forward samples.",
		}, []string{"user"}),
		samplesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_forward_samples_total",
			Help:      "The total number of samples the Distributor forwarded.",
		}, []string{"user"}),
		requestLatencyHistogram: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "distributor_forward_requests_latency_seconds",
			Help:      "The client-side latency of requests to forward metrics made by the Distributor.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
		}, []string{"user"}),
	}
}

type requestMetrics struct {
	requestsTotal    prometheus.Counter
	samplesTotal     prometheus.Counter
	latencyHistogram prometheus.Observer
}

func (r *forwarding) newRequest(ctx context.Context, tenant string, rules validation.ForwardingRules) *forwardingRequest {
	requestMetrics := requestMetrics{
		requestsTotal:    r.requestsTotal.WithLabelValues(tenant),
		samplesTotal:     r.samplesTotal.WithLabelValues(tenant),
		latencyHistogram: r.requestLatencyHistogram.WithLabelValues(tenant),
	}
	return newRequest(ctx, tenant, rules, &r.pools, requestMetrics)
}

type forwardingRequest struct {
	ctx          context.Context
	client       http.Client
	rules        validation.ForwardingRules
	tsByEndpoint map[string][]mimirpb.PreallocTimeseries
	pools        *forwardingPools
	metrics      requestMetrics
}

func newRequest(ctx context.Context, tenant string, rules validation.ForwardingRules, pools *forwardingPools, metrics requestMetrics) *forwardingRequest {
	return &forwardingRequest{
		ctx:          ctx,
		rules:        rules,
		tsByEndpoint: make(map[string][]mimirpb.PreallocTimeseries),
		pools:        pools,
		metrics:      metrics,
	}
}

// add adds a timeseries to the forwarding request.
// It returns a bool which indicates whether this timeseries should be sent to the Ingester.
// A timeseries should be sent to the Ingester if any of the following conditions are true:
// - It has a labelset without a metric name.
// - There is no matching forwarding rule for the metric name of the timeseries.
// - There is a forwarding rule which defines that this metric should be forwarded and also pushed to the Ingesters.
func (r *forwardingRequest) add(sample mimirpb.PreallocTimeseries) bool {
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
	r.metrics.samplesTotal.Add(float64(len(sample.Samples)))

	ts, ok := r.tsByEndpoint[rule.Endpoint]
	if !ok {
		ts = r.pools.timeseries.Get().([]mimirpb.PreallocTimeseries)[:0]
		r.metrics.requestsTotal.Inc()
	}

	ts = append(ts, sample)
	r.tsByEndpoint[rule.Endpoint] = ts

	return rule.IngesterPush
}

type recoverableError struct {
	error
}

func (r *forwardingRequest) send(ctx context.Context) <-chan error {
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
		}(endpoint, ts)
	}

	errCh := make(chan error)
	go func() {
		defer r.cleanup()
		wg.Wait()

		var firstNonRecoverable error
		for endpoint, err := range errorsByEndpoint {
			if err == nil {
				continue
			}

			if errors.As(err, &recoverableError{}) {
				// If there is at least one recoverable error we want to return a recoverable error.
				errCh <- fmt.Errorf("endpoint %s returned %w", endpoint, err)
				return
			}

			if firstNonRecoverable == nil {
				firstNonRecoverable = fmt.Errorf("endpoint %s returned %w", endpoint, err)
			}
		}

		errCh <- firstNonRecoverable
	}()

	return errCh
}

func (r *forwardingRequest) sendToEndpoint(ctx context.Context, endpoint string, ts []mimirpb.PreallocTimeseries) error {
	protoBufBytes := r.pools.protobuf.Get().([]byte)[:0]
	protoBuf := proto.NewBuffer(protoBufBytes)
	err := protoBuf.Marshal(&mimirpb.WriteRequest{Timeseries: ts})
	if err != nil {
		return err
	}
	protoBufBytes = protoBuf.Bytes()
	defer r.pools.protobuf.Put(protoBufBytes)

	snappyBuf := r.pools.snappy.Get().([]byte)
	snappyBuf = snappy.Encode(snappyBuf[:cap(snappyBuf)], protoBufBytes)
	defer r.pools.snappy.Put(snappyBuf)

	httpReq, err := http.NewRequest("POST", endpoint, bytes.NewReader(snappyBuf))
	if err != nil {
		// Errors from NewRequest are from unparsable URLs, so are not
		// recoverable.
		return err
	}

	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	beforeTs := time.Now()
	httpResp, err := r.client.Do(httpReq.WithContext(ctx))
	r.metrics.latencyHistogram.Observe(time.Since(beforeTs).Seconds())
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
		err = errors.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
	}
	if httpResp.StatusCode/100 == 5 {
		return recoverableError{err}
	}
	if httpResp.StatusCode == http.StatusTooManyRequests {
		return recoverableError{err}
	}

	return err
}

// cleanup must be called to return the used buffers to their pools after a request has completed.
func (r *forwardingRequest) cleanup() {
	for _, ts := range r.tsByEndpoint {
		r.pools.timeseries.Put(ts)
	}
}
