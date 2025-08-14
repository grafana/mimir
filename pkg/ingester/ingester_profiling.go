// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"net/http"
	"runtime/pprof"

	"github.com/grafana/dskit/tenant"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/api"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// ProfilingWrapper is a wrapper around Ingester that adds tenant ID to pprof labels.
type ProfilingWrapper struct {
	ing api.Ingester
}

func NewIngesterProfilingWrapper(ing api.Ingester) *ProfilingWrapper {
	return &ProfilingWrapper{
		ing: ing,
	}
}

type baseCtxStream struct {
	ctx context.Context
}

func (s baseCtxStream) Context() context.Context {
	return s.ctx
}

// isTraceSampled checks if the current trace is sampled
func isTraceSampled(ctx context.Context) bool {
	return trace.SpanFromContext(ctx).SpanContext().IsSampled()
}

func (i *ProfilingWrapper) Push(ctx context.Context, request *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.Push", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.Push(ctx, request)
}

func (i *ProfilingWrapper) QueryStream(request *client.QueryRequest, server client.Ingester_QueryStreamServer) error {
	ctx := server.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.QueryStream", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)

		type queryStreamStream struct {
			baseCtxStream
			client.Ingester_QueryStreamServer
		}

		server = queryStreamStream{baseCtxStream{ctx}, server}
	}

	return i.ing.QueryStream(request, server)
}

func (i *ProfilingWrapper) QueryExemplars(ctx context.Context, request *client.ExemplarQueryRequest) (*client.ExemplarQueryResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.QueryExemplars", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.QueryExemplars(ctx, request)
}

func (i *ProfilingWrapper) LabelValues(ctx context.Context, request *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.LabelValues", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.LabelValues(ctx, request)
}

func (i *ProfilingWrapper) LabelNames(ctx context.Context, request *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.LabelNames", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.LabelNames(ctx, request)
}

func (i *ProfilingWrapper) UserStats(ctx context.Context, request *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.UserStats", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.UserStats(ctx, request)
}

func (i *ProfilingWrapper) AllUserStats(ctx context.Context, request *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.AllUserStats", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.AllUserStats(ctx, request)
}

func (i *ProfilingWrapper) MetricsForLabelMatchers(ctx context.Context, request *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.MetricsForLabelMatchers", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.MetricsForLabelMatchers(ctx, request)
}

func (i *ProfilingWrapper) MetricsMetadata(ctx context.Context, request *client.MetricsMetadataRequest) (*client.MetricsMetadataResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.MetricsMetadata", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.MetricsMetadata(ctx, request)
}

func (i *ProfilingWrapper) LabelNamesAndValues(request *client.LabelNamesAndValuesRequest, server client.Ingester_LabelNamesAndValuesServer) error {
	ctx := server.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.LabelNamesAndValues", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)

		type labelNamesAndValuesStream struct {
			baseCtxStream
			client.Ingester_LabelNamesAndValuesServer
		}

		server = labelNamesAndValuesStream{baseCtxStream{ctx}, server}
	}

	return i.ing.LabelNamesAndValues(request, server)
}

func (i *ProfilingWrapper) LabelValuesCardinality(request *client.LabelValuesCardinalityRequest, server client.Ingester_LabelValuesCardinalityServer) error {
	ctx := server.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.LabelValuesCardinality", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)

		type labelValuesCardinalityStream struct {
			baseCtxStream
			client.Ingester_LabelValuesCardinalityServer
		}

		server = labelValuesCardinalityStream{baseCtxStream{ctx}, server}
	}

	return i.ing.LabelValuesCardinality(request, server)
}

func (i *ProfilingWrapper) ActiveSeries(request *client.ActiveSeriesRequest, server client.Ingester_ActiveSeriesServer) error {
	ctx := server.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.ActiveSeries", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)

		type activeSeriesStream struct {
			baseCtxStream
			client.Ingester_ActiveSeriesServer
		}

		server = activeSeriesStream{baseCtxStream{ctx}, server}
	}

	return i.ing.ActiveSeries(request, server)
}

func (i *ProfilingWrapper) FlushHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.FlushHandler", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.FlushHandler(w, r)
}

func (i *ProfilingWrapper) PrepareShutdownHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.PrepareShutdownHandler", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.PrepareShutdownHandler(w, r)
}

func (i *ProfilingWrapper) PreparePartitionDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.PreparePartitionDownscaleHandler", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.PreparePartitionDownscaleHandler(w, r)
}

func (i *ProfilingWrapper) PrepareInstanceRingDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.PrepareInstanceRingDownscaleHandler", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.PrepareInstanceRingDownscaleHandler(w, r)
}

func (i *ProfilingWrapper) PrepareUnregisterHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.PrepareUnregisterHandler", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.PrepareUnregisterHandler(w, r)
}

func (i *ProfilingWrapper) ShutdownHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.ShutdownHandler", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.ShutdownHandler(w, r)
}

func (i *ProfilingWrapper) UserRegistryHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.UserRegistryHandler", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.UserRegistryHandler(w, r)
}

func (i *ProfilingWrapper) TenantsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.TenantsHandler", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.TenantsHandler(w, r)
}

func (i *ProfilingWrapper) TenantTSDBHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("method", "Ingester.TenantTSDBHandler", "userID", userID)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.TenantTSDBHandler(w, r)
}
