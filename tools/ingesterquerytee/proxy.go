// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
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

type Proxy struct {
	cfg            Config
	primary        grpc.ClientConnInterface
	shadow         grpc.ClientConnInterface
	ctx            context.Context
	logger         log.Logger
	slots          chan struct{}
	retrySlots     chan struct{}
	copySlots      chan struct{}
	comparisons    *prometheus.CounterVec
	mirrored       *prometheus.CounterVec
	copies         *prometheus.CounterVec
	duration       *prometheus.HistogramVec
	diagnosticLogs *rate.Limiter
	hostname       string

	// Copies waiting for their delay, at most delayedCopiesPerSlot per copy slot.
	delayedCopies atomic.Int64
}

func New(ctx context.Context, cfg Config, primary, shadow grpc.ClientConnInterface, reg prometheus.Registerer, logger log.Logger) (*Proxy, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	p := &Proxy{cfg: cfg, primary: primary, shadow: shadow, ctx: ctx, logger: logger, slots: make(chan struct{}, cfg.MaxConcurrent), retrySlots: make(chan struct{}, cfg.MaxConcurrent), copySlots: make(chan struct{}, max(1, cfg.MaxConcurrentCopies)), diagnosticLogs: rate.NewLimiter(rate.Every(time.Second), 1)}
	if hostname, err := os.Hostname(); err == nil {
		p.hostname = hostname
	}
	p.comparisons = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingester_query_tee_comparisons_total", Help: "Read comparisons by RPC and outcome: match, match_after_retry, mismatch, skipped, unavailable, primary_error, shadow_error, comparison_error, or limit.",
	}, []string{"method", "result"})
	p.mirrored = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingester_query_tee_mirrored_requests_total", Help: "Shadow requests consumed by RPC and outcome: success, unavailable, or shadow_error.",
	}, []string{"method", "result"})
	p.copies = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingester_query_tee_copies_total", Help: "Copies of reads sent by -amplification, by RPC, backend (primary or shadow) and outcome: success, unavailable, error, or dropped.",
	}, []string{"method", "backend", "result"})
	p.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ingester_query_tee_backend_duration_seconds", Help: "Time to consume each backend response: primary, shadow, or primary_copy and shadow_copy for -amplification's copies.", Buckets: prometheus.DefBuckets,
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
	if !comparable(method) {
		return false
	}
	if !sampleRequest(p.cfg.SampleRate) {
		p.comparisons.WithLabelValues(method, "skipped").Inc()
		return false
	}
	select {
	case p.slots <- struct{}{}:
		return true
	default:
		p.comparisons.WithLabelValues(method, "skipped").Inc()
		return false
	}
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
	var captured *response
	if strings.HasPrefix(fullMethod, "/cortex.Ingester/") && comparable(method) && p.cfg.shadowed() {
		var primaryDone chan response
		if p.selectComparison(method) {
			captured = &response{}
			primaryDone = make(chan response, 1)
			defer func() { captured.err = retErr; primaryDone <- *captured }()
		}
		md := incomingMetadata(downstream.Context())
		go p.mirror(fullMethod, method, req, md, primaryDone, start)
		p.amplify(fullMethod, method, req, md)
	}
	ctx, cancel := context.WithCancelCause(outgoingContext(downstream.Context()))
	defer cancel(nil)
	defer func() {
		if comparable(method) {
			p.duration.WithLabelValues(method, "primary").Observe(time.Since(start).Seconds())
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

func (p *Proxy) mirror(fullMethod, method string, req frame, md metadata.MD, primaryDone <-chan response, start time.Time) {
	if primaryDone != nil {
		defer func() { <-p.slots }()
	}
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.ShadowTimeout)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, md)
	shadow := response{}
	upstream, err := openStream(ctx, p.shadow, fullMethod, &req)
	if err == nil {
		// Uncompared responses reuse one buffer instead of retaining the whole stream.
		var f frame
		for {
			err = upstream.RecvMsg(&f)
			if err != nil {
				break
			}
			if primaryDone != nil {
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
	p.duration.WithLabelValues(method, "shadow").Observe(time.Since(start).Seconds())
	mirrorResult := "success"
	if isUnavailable(shadow.err) {
		mirrorResult = "unavailable"
	} else if shadow.err != nil {
		mirrorResult = "shadow_error"
	}
	p.mirrored.WithLabelValues(method, mirrorResult).Inc()
	if primaryDone == nil {
		return
	}
	var primary response
	select {
	case primary = <-primaryDone:
	case <-p.ctx.Done():
		return
	}
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
		} else if err != nil {
			result = "comparison_error"
		}
	}
	var mismatch *mismatchError
	if result == "mismatch" && errors.As(comparisonErr, &mismatch) && len(mismatch.items) > 0 && p.cfg.MismatchRetries > 0 {
		select {
		case p.retrySlots <- struct{}{}:
			// Retries wait for minutes, so they don't hold the comparison slot.
			go p.retryMismatch(fullMethod, method, req, md, mismatch, start, primary.frames, shadow.frames)
			return
		default:
		}
	}
	p.comparisons.WithLabelValues(method, result).Inc()
	if result == "mismatch" || result == "comparison_error" || result == "shadow_error" {
		fields := []any{"msg", "ingester query comparison failed", "method", method, "result", result}
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
						"primary_address": p.cfg.PrimaryAddress, "shadow_address": p.cfg.ShadowAddress, "tee_host": p.hostname,
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
func (p *Proxy) retryMismatch(fullMethod, method string, req frame, md metadata.MD, mismatch *mismatchError, start time.Time, primaryFrames, shadowFrames []frame) {
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
		go func() { defer wg.Done(); shadow = p.fetch(p.shadow, fullMethod, req, md) }()
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
		p.comparisons.WithLabelValues(method, "match_after_retry").Inc()
		return
	}
	p.comparisons.WithLabelValues(method, "mismatch").Inc()
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
	fields := []any{"msg", "ingester query comparison failed", "method", method, "result", "mismatch"}
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

// delayedCopiesPerSlot bounds the copies waiting for their delay, each a goroutine holding its request.
const delayedCopiesPerSlot = 16

// sendCopy sends a copy of the read to both backends at once, after a random delay up to -amplification-delay, and
// drains their responses. It's dropped for both when -max-concurrent-copies are in flight already, so that both see the
// same load.
func (p *Proxy) sendCopy(fullMethod, method string, req frame, md metadata.MD) {
	dropped := func() {
		p.copies.WithLabelValues(method, "primary", "dropped").Inc()
		p.copies.WithLabelValues(method, "shadow", "dropped").Inc()
	}
	if p.cfg.AmplificationDelay > 0 {
		if p.delayedCopies.Inc() > int64(delayedCopiesPerSlot*cap(p.copySlots)) {
			p.delayedCopies.Dec()
			dropped()
			return
		}
		fraction, _ := randomFraction()
		select {
		case <-time.After(time.Duration(fraction * float64(p.cfg.AmplificationDelay))):
			p.delayedCopies.Dec()
		case <-p.ctx.Done():
			p.delayedCopies.Dec()
			return
		}
	}
	select {
	case p.copySlots <- struct{}{}:
	default:
		dropped()
		return
	}
	defer func() { <-p.copySlots }()
	var wg sync.WaitGroup
	for _, backend := range []struct {
		name string
		conn grpc.ClientConnInterface
	}{{"primary", p.primary}, {"shadow", p.shadow}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.drainCopy(backend.conn, backend.name, fullMethod, method, req, md)
		}()
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

func isUnavailable(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded
}
