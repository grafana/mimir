// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type Pusher interface {
	Push(context.Context, *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error)
}

type pusherConsumer struct {
	p Pusher

	processingTimeSeconds prometheus.Observer
	clientErrRequests     prometheus.Counter
	serverErrRequests     prometheus.Counter
	totalRequests         prometheus.Counter
	l                     log.Logger
}

func newPusherConsumer(p Pusher, reg prometheus.Registerer, l log.Logger) *pusherConsumer {
	errRequestsCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingest_storage_reader_records_failed_total",
		Help: "Number of writeRequests (write requests) which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.",
	}, []string{"cause"})

	return &pusherConsumer{
		p: p,
		l: l,
		processingTimeSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Name:       "cortex_ingest_storage_reader_processing_time_seconds",
			Help:       "Time taken to process a single record (write request).",
			Objectives: latencySummaryObjectives,
			MaxAge:     time.Minute,
			AgeBuckets: 10,
		}),
		clientErrRequests: errRequestsCounter.WithLabelValues("client"),
		serverErrRequests: errRequestsCounter.WithLabelValues("server"),
		totalRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_records_total",
			Help: "Number of attempted writeRequests (write requests).",
		}),
	}
}

func (c pusherConsumer) consume(ctx context.Context, segment *Segment) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(cancellation.NewErrorf("done consuming writeRequests"))

	err := c.pushRequests(ctx, segment)
	if err != nil {
		return err
	}
	return nil
}

func (c pusherConsumer) pushRequests(ctx context.Context, segment *Segment) error {
	for pieceIdx, piece := range segment.Data.Pieces {
		processingStart := time.Now()

		ctx := user.InjectOrgID(ctx, piece.TenantId)
		_, err := c.p.Push(ctx, piece.WriteRequests)

		c.processingTimeSeconds.Observe(time.Since(processingStart).Seconds())
		c.totalRequests.Inc()
		if err != nil {
			if !mimirpb.IsClientError(err) {
				c.serverErrRequests.Inc()
				return fmt.Errorf("consuming piece at index %d for tenant %s: %w", pieceIdx, piece.TenantId, err)
			}
			c.clientErrRequests.Inc()
			level.Warn(c.l).Log("msg", "detected a client error while ingesting write request (the request may have been partially ingested)", "err", err, "user", piece.TenantId)
		}
	}
	return nil
}
