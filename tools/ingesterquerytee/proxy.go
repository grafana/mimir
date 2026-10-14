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

type Proxy struct {
	cfg            Config
	primary        grpc.ClientConnInterface
	shadow         grpc.ClientConnInterface
	ctx            context.Context
	logger         log.Logger
	slots          chan struct{}
	comparisons    *prometheus.CounterVec
	mirrored       *prometheus.CounterVec
	duration       *prometheus.HistogramVec
	diagnosticLogs *rate.Limiter
	hostname       string
}

func New(ctx context.Context, cfg Config, primary, shadow grpc.ClientConnInterface, reg prometheus.Registerer, logger log.Logger) (*Proxy, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	p := &Proxy{cfg: cfg, primary: primary, shadow: shadow, ctx: ctx, logger: logger, slots: make(chan struct{}, cfg.MaxConcurrent), diagnosticLogs: rate.NewLimiter(rate.Every(time.Second), 1)}
	if hostname, err := os.Hostname(); err == nil {
		p.hostname = hostname
	}
	p.comparisons = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingester_query_tee_comparisons_total", Help: "Read comparisons by RPC and outcome: match, mismatch, skipped, unavailable, primary_error, shadow_error, comparison_error, or limit.",
	}, []string{"method", "result"})
	p.mirrored = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingester_query_tee_mirrored_requests_total", Help: "Shadow requests consumed by RPC and outcome: success, unavailable, or shadow_error.",
	}, []string{"method", "result"})
	p.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ingester_query_tee_backend_duration_seconds", Help: "Time to consume each backend response.", Buckets: prometheus.DefBuckets,
	}, []string{"method", "backend"})
	if err := reg.Register(p.comparisons); err != nil {
		return nil, err
	}
	if err := reg.Register(p.mirrored); err != nil {
		reg.Unregister(p.comparisons)
		return nil, err
	}
	if err := reg.Register(p.duration); err != nil {
		reg.Unregister(p.comparisons)
		reg.Unregister(p.mirrored)
		return nil, err
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
	var data [8]byte
	if _, err := rand.Read(data[:]); err != nil {
		return false
	}
	// Using 53 bits keeps the conversion exact and the sampled value strictly below one.
	sample := float64(binary.LittleEndian.Uint64(data[:])>>11) / (1 << 53)
	return sample < rate
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
	if strings.HasPrefix(fullMethod, "/cortex.Ingester/") && comparable(method) {
		var primaryDone chan response
		if p.selectComparison(method) {
			captured = &response{}
			primaryDone = make(chan response, 1)
			defer func() { captured.err = retErr; primaryDone <- *captured }()
		}
		go p.mirror(fullMethod, method, req, incomingMetadata(downstream.Context()), primaryDone, start)
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
		var mismatch *mismatchError
		if errors.As(comparisonErr, &mismatch) {
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

func isUnavailable(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded
}
