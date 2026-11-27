// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
)

var errResponseLimit = errors.New("response exceeds comparison limit")

// WireCodec keeps pass-through RPCs independent of protobuf API changes and avoids
// retaining strings backed by the ingester client's pooled decode buffers.
type WireCodec struct{}

type frame []byte

func (WireCodec) Name() string { return "proto" }
func (WireCodec) Marshal(v any) ([]byte, error) {
	if f, ok := v.(*frame); ok {
		return *f, nil
	}
	data, err := encoding.GetCodecV2("proto").Marshal(v)
	if err != nil {
		return nil, err
	}
	defer data.Free()
	return data.Materialize(), nil
}
func (WireCodec) Unmarshal(data []byte, v any) error {
	if f, ok := v.(*frame); ok {
		*f = append((*f)[:0], data...)
		return nil
	}
	return encoding.GetCodecV2("proto").Unmarshal(mem.BufferSlice{mem.SliceBuffer(data)}, v)
}

type response struct {
	frames  []frame
	err     error
	bytes   int
	limited bool
}

func (r *response) add(f frame, limit int) {
	if r.limited {
		return
	}
	// Account for slice entries and spare capacity as well as payload bytes.
	size := len(f) + 64
	if size > limit-r.bytes {
		r.frames = nil
		r.limited = true
		return
	}
	r.bytes += size
	r.frames = append(r.frames, f)
}

type shadowBackend struct {
	name    string
	address string
	conn    grpc.ClientConnInterface
}

type capturedResponse struct {
	response
	done    chan struct{}
	pending atomic.Int32
}

type Proxy struct {
	cfg            Config
	primary        grpc.ClientConnInterface
	shadows        []shadowBackend
	ctx            context.Context
	logger         log.Logger
	slots          chan struct{}
	retrySlots     chan struct{}
	comparisons    *prometheus.CounterVec
	mirrored       *prometheus.CounterVec
	copies         *prometheus.CounterVec
	duration       *prometheus.HistogramVec
	diagnosticLogs *rate.Limiter
	slowLogs       *rate.Limiter
	hostname       string
}

func New(ctx context.Context, cfg Config, primary grpc.ClientConnInterface, shadows []grpc.ClientConnInterface, reg prometheus.Registerer, logger log.Logger) (*Proxy, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	addresses := cfg.ShadowAddresses
	if len(shadows) != len(addresses) {
		return nil, fmt.Errorf("expected %d shadow connections, got %d", len(addresses), len(shadows))
	}
	p := &Proxy{cfg: cfg, primary: primary, ctx: ctx, logger: logger, slots: make(chan struct{}, cfg.MaxConcurrent), retrySlots: make(chan struct{}, cfg.MaxConcurrent), diagnosticLogs: rate.NewLimiter(rate.Every(time.Second), 1), slowLogs: rate.NewLimiter(rate.Every(100*time.Millisecond), 10)}
	for i, shadow := range shadows {
		name := "shadow"
		if i > 0 {
			name = fmt.Sprintf("shadow%d", i+1)
		}
		p.shadows = append(p.shadows, shadowBackend{name: name, address: addresses[i], conn: shadow})
	}
	if hostname, err := os.Hostname(); err == nil {
		p.hostname = hostname
	}
	p.comparisons = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingester_query_tee_comparisons_total", Help: "Read comparisons by RPC, shadow backend, and outcome: match, match_after_retry, mismatch, skipped, unavailable, primary_error, shadow_error, comparison_error, or limit.",
	}, []string{"method", "backend", "result"})
	p.mirrored = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingester_query_tee_mirrored_requests_total", Help: "Shadow requests consumed by RPC, shadow backend, and outcome: success, unavailable, or shadow_error.",
	}, []string{"method", "backend", "result"})
	p.copies = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingester_query_tee_copies_total", Help: "Copies of reads sent by -amplification, by RPC, backend and outcome: success, unavailable, or error.",
	}, []string{"method", "backend", "result"})
	p.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ingester_query_tee_backend_duration_seconds", Help: "Time to consume each backend response: primary or shadow, with a _copy suffix for -amplification's copies.", Buckets: prometheus.DefBuckets,
	}, []string{"method", "backend"})
	var registered []prometheus.Collector
	for _, c := range []prometheus.Collector{p.comparisons, p.mirrored, p.copies, p.duration} {
		if err := reg.Register(c); err != nil {
			for _, r := range registered {
				reg.Unregister(r)
			}
			return nil, err
		}
		registered = append(registered, c)
	}
	return p, nil
}

func outgoingContext(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, incomingMetadata(ctx))
}

func incomingMetadata(ctx context.Context) metadata.MD {
	//lint:ignore faillint A transparent proxy must propagate all metadata, including keys added by future clients.
	md, _ := metadata.FromIncomingContext(ctx)
	return md.Copy()
}

func openStream(ctx context.Context, conn grpc.ClientConnInterface, method string, req *frame) (grpc.ClientStream, error) {
	s, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method, grpc.ForceCodec(WireCodec{}))
	if err != nil {
		return nil, err
	}
	if err = s.SendMsg(req); err != nil {
		return nil, err
	}
	//nolint:forbidigo // The raw stream must half-close its single request before forwarding responses.
	if err = s.CloseSend(); err != nil {
		return nil, err
	}
	return s, nil
}

func (p *Proxy) selectComparison(method string) bool {
	if sampleRequest(p.cfg.SampleRate) {
		select {
		case p.slots <- struct{}{}:
			return true
		default:
		}
	}
	for _, shadow := range p.shadows {
		p.comparisons.WithLabelValues(method, shadow.name, "skipped").Inc()
	}
	return false
}

func sampleRequest(rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	sample, ok := randomFraction()
	return ok && sample < rate
}

// randomFraction returns a random number in [0, 1), and false when none could be read.
func randomFraction() (float64, bool) {
	var data [8]byte
	if _, err := rand.Read(data[:]); err != nil {
		return 0, false
	}
	// Using 53 bits keeps the conversion exact and the sampled value strictly below one.
	return float64(binary.LittleEndian.Uint64(data[:])>>11) / (1 << 53), true
}

func (p *Proxy) Handler(_ any, downstream grpc.ServerStream) (retErr error) {
	fullMethod, ok := grpc.MethodFromServerStream(downstream)
	if !ok {
		return status.Error(codes.Internal, "missing gRPC method")
	}
	method := strings.TrimPrefix(fullMethod, "/cortex.Ingester/")
	var req frame
	if err := downstream.RecvMsg(&req); err != nil {
		return err
	}
	start := time.Now()
	var captured *capturedResponse
	var timing *readTiming
	var md metadata.MD
	if strings.HasPrefix(fullMethod, "/cortex.Ingester/") && comparable(method) && p.cfg.shadowed() {
		if p.selectComparison(method) {
			captured = &capturedResponse{done: make(chan struct{})}
			captured.pending.Store(int32(len(p.shadows)))
			defer func() {
				captured.err = retErr
				close(captured.done)
			}()
		}
		md = incomingMetadata(downstream.Context())
		if p.cfg.LogSlowResponseThreshold > 0 {
			timing = &readTiming{durations: make([]time.Duration, 1+len(p.shadows))}
		}
		for i, shadow := range p.shadows {
			go p.mirror(shadow, i+1, fullMethod, method, req, md, captured, start, timing)
		}
		p.amplify(fullMethod, method, req, md)
	}
	ctx, cancel := context.WithCancelCause(outgoingContext(downstream.Context()))
	defer cancel(nil)
	defer func() {
		if comparable(method) {
			took := time.Since(start)
			p.duration.WithLabelValues(method, "primary").Observe(took.Seconds())
			if timing != nil {
				p.finishTiming(timing, 0, took, method, req, md)
			}
		}
	}()
	upstream, err := openStream(ctx, p.primary, fullMethod, &req)
	if err != nil {
		return err
	}
	defer func() { downstream.SetTrailer(upstream.Trailer()) }()
	headers, err := upstream.Header()
	if err != nil {
		return err
	}
	if err := downstream.SendHeader(headers); err != nil {
		return err
	}
	for {
		var f frame
		if err := upstream.RecvMsg(&f); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := downstream.SendMsg(&f); err != nil {
			return err
		}
		if captured != nil {
			captured.add(f, p.cfg.MaxResponseBytes)
		}
	}
}

func (p *Proxy) mirror(backend shadowBackend, backendIndex int, fullMethod, method string, req frame, md metadata.MD, captured *capturedResponse, start time.Time, timing *readTiming) {
	if captured != nil {
		defer func() {
			if captured.pending.Add(-1) == 0 {
				<-p.slots
			}
		}()
	}
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.ShadowTimeout)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, md)
	shadow := response{}
	upstream, err := openStream(ctx, backend.conn, fullMethod, &req)
	if err == nil {
		// Uncompared responses reuse one buffer instead of retaining the whole stream.
		var f frame
		for {
			err = upstream.RecvMsg(&f)
			if err != nil {
				break
			}
			if captured != nil {
				shadow.add(f, p.cfg.MaxResponseBytes)
				if !shadow.limited {
					// Captured frames must not share the next receive buffer.
					f = nil
				}
			}
		}
	}
	if !errors.Is(err, io.EOF) {
		shadow.err = err
	}
	took := time.Since(start)
	p.duration.WithLabelValues(method, backend.name).Observe(took.Seconds())
	if timing != nil {
		p.finishTiming(timing, backendIndex, took, method, req, md)
	}
	mirrorResult := "success"
	if isUnavailable(shadow.err) {
		mirrorResult = "unavailable"
	} else if shadow.err != nil {
		mirrorResult = "shadow_error"
	}
	p.mirrored.WithLabelValues(method, backend.name, mirrorResult).Inc()
	if captured == nil {
		return
	}
	select {
	case <-captured.done:
	case <-p.ctx.Done():
		return
	}
	primary := captured.response
	result := "match"
	var comparisonErr error
	switch {
	case isUnavailable(primary.err) || isUnavailable(shadow.err):
		result = "unavailable"
	case primary.limited || shadow.limited:
		result = "limit"
	case primary.err != nil:
		result = "primary_error"
	case shadow.err != nil:
		result = "shadow_error"
	default:
		err = compareResponses(method, req, primary.frames, shadow.frames, p.cfg, start)
		comparisonErr = err
		if errors.Is(err, errResponseLimit) {
			result = "limit"
		} else if errors.Is(err, errMismatch) {
			result = "mismatch"
		} else if errors.Is(err, errBeyondRetention) {
			result = "beyond_retention"
		} else if err != nil {
			result = "comparison_error"
		}
	}
	var mismatch *mismatchError
	if result == "mismatch" && errors.As(comparisonErr, &mismatch) && len(mismatch.items) > 0 && p.cfg.MismatchRetries > 0 {
		select {
		case p.retrySlots <- struct{}{}:
			// Retries wait for minutes, so they don't hold the comparison slot.
			go p.retryMismatch(backend, fullMethod, method, req, md, mismatch, start, primary.frames, shadow.frames)
			return
		default:
		}
	}
	p.comparisons.WithLabelValues(method, backend.name, result).Inc()
	if result == "mismatch" || result == "comparison_error" || result == "shadow_error" {
		fields := []any{"msg", "ingester query comparison failed", "method", method, "result", result, "shadow_backend", backend.name}
		if p.cfg.LogMismatchedQueries && method == "QueryStream" {
			tenantID := strings.Join(md.Get("x-scope-orgid"), "|")
			if len(tenantID) > 256 {
				tenantID = tenantID[:256]
				fields = append(fields, "tenant_id_truncated", true)
			}
			fields = append(fields, "tenant_id", tenantID)
		}
		if mismatch != nil || errors.As(comparisonErr, &mismatch) {
			fields = append(fields, mismatch.fields...)
		}
		var decodeError *comparisonError
		if errors.As(comparisonErr, &decodeError) {
			fields = append(fields, "backend", decodeError.backend, "error", decodeError.Error(),
				"primary_frames", len(primary.frames), "shadow_frames", len(shadow.frames),
				"primary_bytes", primary.bytes, "shadow_bytes", shadow.bytes)
		}
		if method == "QueryStream" && (result == "mismatch" || result == "comparison_error") && (p.cfg.LogMismatchedQueries || p.cfg.LogMismatchedSamples) {
			if p.diagnosticLogs.Allow() {
				budget := diagnosticLogBytes
				if p.cfg.LogMismatchedQueries {
					var query client.QueryRequest
					comparisonEnd := int64(0)
					if query.Unmarshal(req) == nil {
						comparisonEnd = query.EndTimestampMs
						if p.cfg.SkipRecentSamples > 0 {
							comparisonEnd = min(comparisonEnd, start.Add(-p.cfg.SkipRecentSamples).UnixMilli())
						}
						fields = appendDiagnosticJSON(fields, "query", &query, &budget)
					}
					// Replay needs read-consistency metadata, never credentials or arbitrary headers.
					replayMetadata := map[string][]string{}
					for _, name := range []string{"x-scope-orgid", "__consistency_level__", "__consistency_offsets__", "__consistency_max_delay__", "__only_replica__", "x-cluster"} {
						if values := md.Get(name); len(values) > 0 {
							replayMetadata[name] = values
						}
					}
					fields = appendDiagnosticJSON(fields, "replay", map[string]any{
						"metadata": replayMetadata, "tenant_id": strings.Join(md.Get("x-scope-orgid"), "|"),
						"primary_address": p.cfg.PrimaryAddress, "shadow_address": backend.address, "tee_host": p.hostname,
						"request_start_ms": start.UnixMilli(), "comparison_end_ms": comparisonEnd,
						"skip_recent_samples": p.cfg.SkipRecentSamples.String(), "shadow_timeout": p.cfg.ShadowTimeout.String(),
					}, &budget)
				}
				if p.cfg.LogMismatchedSamples && mismatch != nil && mismatch.difference != nil {
					fields = appendDiagnosticJSON(fields, "difference", differenceDiagnostic(mismatch.difference, primary.frames, shadow.frames), &budget)
				}
			} else {
				fields = append(fields, "diagnostics_suppressed", true)
			}
		}
		level.Warn(p.logger).Log(fields...)
	}
}

// retryMismatch asks both backends again, up to -mismatch-retries times, for the items or samples that differed, and
// counts a mismatch only if some of the same differ in the same way every time. Samples are compared as of the first
// request, so the retries don't see the ones written since.
func (p *Proxy) retryMismatch(backend shadowBackend, fullMethod, method string, req frame, md metadata.MD, mismatch *mismatchError, start time.Time, primaryFrames, shadowFrames []frame) {
	defer func() { <-p.retrySlots }()
	differing := mismatch.items
	retries := 0
	for retries < p.cfg.MismatchRetries && len(differing) > 0 {
		select {
		case <-time.After(p.cfg.MismatchRetryDelay):
		case <-p.ctx.Done():
			return
		}
		retries++
		var primary, shadow response
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); primary = p.fetch(p.primary, fullMethod, req, md) }()
		go func() { defer wg.Done(); shadow = p.fetch(backend.conn, fullMethod, req, md) }()
		wg.Wait()
		if primary.err != nil || shadow.err != nil || primary.limited || shadow.limited {
			// What the items are now isn't known, so the first comparison stands.
			break
		}
		now, err := differences(method, req, primary.frames, shadow.frames, p.cfg, start)
		if err != nil {
			break
		}
		for item, count := range differing {
			// The same item or sample, missing from the same backend, or with values that still differ.
			if again := now[item]; again == 0 || (count > 0) != (again > 0) || (count == 2) != (again == 2) {
				delete(differing, item)
			}
		}
	}
	if len(differing) == 0 {
		p.comparisons.WithLabelValues(method, backend.name, "match_after_retry").Inc()
		return
	}
	p.comparisons.WithLabelValues(method, backend.name, "mismatch").Inc()
	var missing, extra, different int
	for _, count := range differing {
		switch {
		case count == 2:
			different++
		case count > 0:
			missing++
		default:
			extra++
		}
	}
	fields := []any{"msg", "ingester query comparison failed", "method", method, "result", "mismatch", "shadow_backend", backend.name}
	if p.cfg.LogMismatchedQueries && method == "QueryStream" {
		fields = append(fields, "tenant_id", strings.Join(md.Get("x-scope-orgid"), "|"))
	}
	fields = append(fields, mismatch.fields...)
	fields = append(fields, "retries", retries, "persistent_missing_from_shadow", missing, "persistent_extra_in_shadow", extra, "persistent_different_values", different)
	if method == "QueryStream" && (p.cfg.LogMismatchedQueries || p.cfg.LogMismatchedSamples) && p.diagnosticLogs.Allow() {
		budget := diagnosticLogBytes
		var query client.QueryRequest
		if p.cfg.LogMismatchedQueries && query.Unmarshal(req) == nil {
			fields = appendDiagnosticJSON(fields, "query", &query, &budget)
		}
		// The difference of the first comparison, which the retries still saw.
		if p.cfg.LogMismatchedSamples && mismatch.difference != nil {
			fields = appendDiagnosticJSON(fields, "difference", differenceDiagnostic(mismatch.difference, primaryFrames, shadowFrames), &budget)
		}
	}
	level.Warn(p.logger).Log(fields...)
}

// fetch sends the request to a backend again and keeps its response, within the shadow timeout.
func (p *Proxy) fetch(conn grpc.ClientConnInterface, fullMethod string, req frame, md metadata.MD) response {
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.ShadowTimeout)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, md)
	var r response
	stream, err := openStream(ctx, conn, fullMethod, &req)
	if err != nil {
		r.err = err
		return r
	}
	for {
		var f frame
		if err := stream.RecvMsg(&f); err != nil {
			if !errors.Is(err, io.EOF) {
				r.err = err
			}
			return r
		}
		r.add(f, p.cfg.MaxResponseBytes)
	}
}

// amplify sends both backends -amplification minus one copies of the read on average: the whole part, and one more as
// often as the fraction says. Copies are only timed and counted, and share the read's request and metadata, which
// nothing modifies.
func (p *Proxy) amplify(fullMethod, method string, req frame, md metadata.MD) {
	extra := p.cfg.Amplification - 1
	if extra <= 0 {
		return
	}
	copies := int(extra)
	if sampleRequest(extra - float64(copies)) {
		copies++
	}
	for range copies {
		go p.sendCopy(fullMethod, method, req, md)
	}
}

// sendCopy sends a copy of the read to all backends at once, after a random delay up to -amplification-delay, and
// drains their responses. The delay and the shadow timeout bound how many copies a tee holds, at its read rate.
func (p *Proxy) sendCopy(fullMethod, method string, req frame, md metadata.MD) {
	if p.cfg.AmplificationDelay > 0 {
		fraction, _ := randomFraction()
		select {
		case <-time.After(time.Duration(fraction * float64(p.cfg.AmplificationDelay))):
		case <-p.ctx.Done():
			return
		}
	}
	var wg sync.WaitGroup
	wg.Go(func() { p.drainCopy(p.primary, "primary", fullMethod, method, req, md) })
	for _, backend := range p.shadows {
		wg.Go(func() { p.drainCopy(backend.conn, backend.name, fullMethod, method, req, md) })
	}
	wg.Wait()
}

// drainCopy sends a copy of the read to the backend and consumes its response, within the shadow timeout.
func (p *Proxy) drainCopy(conn grpc.ClientConnInterface, backend, fullMethod, method string, req frame, md metadata.MD) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.ShadowTimeout)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, md)
	upstream, err := openStream(ctx, conn, fullMethod, &req)
	if err == nil {
		// One buffer for every frame: nothing compares a copy's response.
		var f frame
		for err == nil {
			err = upstream.RecvMsg(&f)
		}
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	p.duration.WithLabelValues(method, backend+"_copy").Observe(time.Since(start).Seconds())
	result := "success"
	if isUnavailable(err) {
		result = "unavailable"
	} else if err != nil {
		result = "error"
	}
	p.copies.WithLabelValues(method, backend, result).Inc()
}

type readTiming struct {
	mu        sync.Mutex
	durations []time.Duration
	finished  int
}

// The last backend to finish can compare durations without holding the lock.
func (p *Proxy) finishTiming(timing *readTiming, backend int, took time.Duration, method string, req frame, md metadata.MD) {
	timing.mu.Lock()
	timing.durations[backend] = took
	timing.finished++
	last := timing.finished == len(timing.durations)
	timing.mu.Unlock()
	if !last {
		return
	}
	slowestBackend, fastestBackend := "primary", "primary"
	slowest, fastest := timing.durations[0], timing.durations[0]
	for i, shadow := range p.shadows {
		duration := timing.durations[i+1]
		if duration > slowest {
			slowestBackend, slowest = shadow.name, duration
		}
		if duration < fastest {
			fastestBackend, fastest = shadow.name, duration
		}
	}
	if slowest-fastest < p.cfg.LogSlowResponseThreshold || !p.slowLogs.Allow() {
		return
	}
	fields := []any{
		"msg", "response time difference between backends exceeded threshold", "method", method,
		"slowest_backend", slowestBackend, "slowest_duration", slowest, "fastest_backend", fastestBackend, "fastest_duration", fastest,
	}
	if p.cfg.LogMismatchedQueries {
		fields = append(fields, "tenant_id", strings.Join(md.Get("x-scope-orgid"), "|"))
		if request, ok := decodeRequest(method, req); ok {
			budget := diagnosticLogBytes
			fields = appendDiagnosticJSON(fields, "request", request, &budget)
		}
	}
	level.Warn(p.logger).Log(fields...)
}

// decodeRequest decodes the request of a mirrored read, to log it.
func decodeRequest(method string, req frame) (any, bool) {
	var request interface{ Unmarshal([]byte) error }
	switch method {
	case "QueryStream":
		request = &client.QueryRequest{}
	case "QueryExemplars":
		request = &client.ExemplarQueryRequest{}
	case "LabelNames":
		request = &client.LabelNamesRequest{}
	case "LabelValues":
		request = &client.LabelValuesRequest{}
	case "MetricsForLabelMatchers":
		request = &client.MetricsForLabelMatchersRequest{}
	case "MetricsMetadata":
		request = &client.MetricsMetadataRequest{}
	case "LabelNamesAndValues":
		request = &client.LabelNamesAndValuesRequest{}
	case "LabelValuesCardinality":
		request = &client.LabelValuesCardinalityRequest{}
	case "ActiveSeries":
		request = &client.ActiveSeriesRequest{}
	case "SearchLabelNames":
		request = &client.SearchLabelNamesRequest{}
	case "SearchLabelValues":
		request = &client.SearchLabelValuesRequest{}
	default:
		return nil, false
	}
	if request.Unmarshal(req) != nil {
		return nil, false
	}
	return request, true
}

func isUnavailable(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded
}
