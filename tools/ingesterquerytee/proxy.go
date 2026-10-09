// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	cfg         Config
	primary     grpc.ClientConnInterface
	shadow      grpc.ClientConnInterface
	ctx         context.Context
	logger      log.Logger
	slots       chan struct{}
	comparisons *prometheus.CounterVec
	duration    *prometheus.HistogramVec
}

func New(ctx context.Context, cfg Config, primary, shadow grpc.ClientConnInterface, reg prometheus.Registerer, logger log.Logger) (*Proxy, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	p := &Proxy{cfg: cfg, primary: primary, shadow: shadow, ctx: ctx, logger: logger, slots: make(chan struct{}, cfg.MaxConcurrent)}
	p.comparisons = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingester_query_tee_comparisons_total", Help: "Read comparisons by RPC and outcome: match, mismatch, skipped, primary_error, shadow_error, comparison_error, or limit.",
	}, []string{"method", "result"})
	p.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ingester_query_tee_backend_duration_seconds", Help: "Time to consume each backend response.", Buckets: prometheus.DefBuckets,
	}, []string{"method", "backend"})
	if err := reg.Register(p.comparisons); err != nil {
		return nil, err
	}
	if err := reg.Register(p.duration); err != nil {
		reg.Unregister(p.comparisons)
		return nil, err
	}
	return p, nil
}

func outgoingContext(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	return metadata.NewOutgoingContext(ctx, md.Copy())
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

func (p *Proxy) mirror(method string) bool {
	if !comparable(method) {
		return false
	}
	if rand.Float64() >= p.cfg.SampleRate {
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
	if strings.HasPrefix(fullMethod, "/cortex.Ingester/") && p.mirror(method) {
		captured = &response{}
		primaryDone := make(chan response, 1)
		md, _ := metadata.FromIncomingContext(downstream.Context())
		go p.compare(fullMethod, method, req, md.Copy(), primaryDone, start)
		defer func() { captured.err = retErr; primaryDone <- *captured }()
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

func (p *Proxy) compare(fullMethod, method string, req frame, md metadata.MD, primaryDone <-chan response, start time.Time) {
	defer func() { <-p.slots }()
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.ShadowTimeout)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, md)
	shadow := response{}
	upstream, err := openStream(ctx, p.shadow, fullMethod, &req)
	if err == nil {
		for {
			var f frame
			err = upstream.RecvMsg(&f)
			if err != nil {
				break
			}
			shadow.add(f, p.cfg.MaxResponseBytes)
			if shadow.limited {
				cancel()
				break
			}
		}
	}
	if !errors.Is(err, io.EOF) {
		shadow.err = err
	}
	p.duration.WithLabelValues(method, "shadow").Observe(time.Since(start).Seconds())
	var primary response
	select {
	case primary = <-primaryDone:
	case <-p.ctx.Done():
		return
	}
	result := "match"
	switch {
	case primary.limited || shadow.limited:
		result = "limit"
	case primary.err != nil:
		result = "primary_error"
	case shadow.err != nil:
		result = "shadow_error"
	default:
		err = compareResponses(method, req, primary.frames, shadow.frames, p.cfg, start)
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
		// Queries and returned values may contain tenant data; only log the RPC and classification.
		level.Warn(p.logger).Log("msg", "ingester query comparison failed", "method", method, "result", result)
	}
}
