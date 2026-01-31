// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"net/http"
	"runtime/pprof"

	"github.com/grafana/dskit/tenant"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// ProfilingWrapper is a wrapper around Ingester that adds tenant ID to pprof labels.
type ProfilingWrapper struct {
	ing API
}

func NewIngesterProfilingWrapper(ing API) *ProfilingWrapper {
	return &ProfilingWrapper{
		ing: ing,
	}
}

type labelNamesAndValuesStream struct {
	ctx context.Context
	client.Ingester_LabelNamesAndValuesServer
}

func (s labelNamesAndValuesStream) Context() context.Context {
	return s.ctx
}

type labelValuesCardinalityStream struct {
	ctx context.Context
	client.Ingester_LabelValuesCardinalityServer
}

func (s labelValuesCardinalityStream) Context() context.Context {
	return s.ctx
}

type activeSeriesStream struct {
	ctx context.Context
	client.Ingester_ActiveSeriesServer
}

func (s activeSeriesStream) Context() context.Context {
	return s.ctx
}

type queryStreamStream struct {
	ctx context.Context
	client.Ingester_QueryStreamServer
}

func (s queryStreamStream) Context() context.Context {
	return s.ctx
}

type resourceAttributesStream struct {
	ctx context.Context
	client.Ingester_ResourceAttributesServer
}

func (s resourceAttributesStream) Context() context.Context {
	return s.ctx
}

// isTraceSampled checks if the current trace is sampled
func isTraceSampled(ctx context.Context) bool {
	return trace.SpanFromContext(ctx).SpanContext().IsSampled()
}

func (i *ProfilingWrapper) Push(ctx context.Context, request *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.Push(ctx, request)
}

func (i *ProfilingWrapper) PushToStorageAndReleaseRequest(ctx context.Context, request *mimirpb.WriteRequest) error {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.PushToStorageAndReleaseRequest(ctx, request)
}

func (i *ProfilingWrapper) NotifyPreCommit(ctx context.Context) error {
	return i.ing.NotifyPreCommit(ctx)
}

func (i *ProfilingWrapper) QueryStream(request *client.QueryRequest, server client.Ingester_QueryStreamServer) error {
	ctx := server.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		server = queryStreamStream{ctx, server}
	}

	return i.ing.QueryStream(request, server)
}

func (i *ProfilingWrapper) QueryExemplars(ctx context.Context, request *client.ExemplarQueryRequest) (*client.ExemplarQueryResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.QueryExemplars(ctx, request)
}

func (i *ProfilingWrapper) LabelValues(ctx context.Context, request *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.LabelValues(ctx, request)
}

func (i *ProfilingWrapper) LabelNames(ctx context.Context, request *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.LabelNames(ctx, request)
}

func (i *ProfilingWrapper) UserStats(ctx context.Context, request *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.UserStats(ctx, request)
}

func (i *ProfilingWrapper) AllUserStats(ctx context.Context, request *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.AllUserStats(ctx, request)
}

func (i *ProfilingWrapper) MetricsForLabelMatchers(ctx context.Context, request *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.MetricsForLabelMatchers(ctx, request)
}

func (i *ProfilingWrapper) MetricsMetadata(ctx context.Context, request *client.MetricsMetadataRequest) (*client.MetricsMetadataResponse, error) {
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
	}

	return i.ing.MetricsMetadata(ctx, request)
}

func (i *ProfilingWrapper) LabelNamesAndValues(request *client.LabelNamesAndValuesRequest, server client.Ingester_LabelNamesAndValuesServer) error {
	ctx := server.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		server = labelNamesAndValuesStream{ctx, server}
	}

	return i.ing.LabelNamesAndValues(request, server)
}

func (i *ProfilingWrapper) LabelValuesCardinality(request *client.LabelValuesCardinalityRequest, server client.Ingester_LabelValuesCardinalityServer) error {
	ctx := server.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		server = labelValuesCardinalityStream{ctx, server}
	}

	return i.ing.LabelValuesCardinality(request, server)
}

func (i *ProfilingWrapper) ActiveSeries(request *client.ActiveSeriesRequest, server client.Ingester_ActiveSeriesServer) error {
	ctx := server.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		server = activeSeriesStream{ctx, server}
	}

	return i.ing.ActiveSeries(request, server)
}

func (i *ProfilingWrapper) ResourceAttributes(request *client.ResourceAttributesRequest, server client.Ingester_ResourceAttributesServer) error {
	ctx := server.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		server = resourceAttributesStream{ctx, server}
	}

	return i.ing.ResourceAttributes(request, server)
}

func (i *ProfilingWrapper) FlushHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.FlushHandler(w, r)
}

func (i *ProfilingWrapper) PrepareShutdownHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.PrepareShutdownHandler(w, r)
}

func (i *ProfilingWrapper) PreparePartitionDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.PreparePartitionDownscaleHandler(w, r)
}

func (i *ProfilingWrapper) PrepareInstanceRingDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.PrepareInstanceRingDownscaleHandler(w, r)
}

func (i *ProfilingWrapper) PrepareUnregisterHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.PrepareUnregisterHandler(w, r)
}

func (i *ProfilingWrapper) ShutdownHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.ShutdownHandler(w, r)
}

func (i *ProfilingWrapper) UserRegistryHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.UserRegistryHandler(w, r)
}

func (i *ProfilingWrapper) TenantsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.TenantsHandler(w, r)
}

func (i *ProfilingWrapper) TenantTSDBHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isTraceSampled(ctx) {
		userID, _ := tenant.TenantID(ctx)
		labels := pprof.Labels("userID", userID)
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, labels)
		pprof.SetGoroutineLabels(ctx)
		r = r.WithContext(ctx)
	}

	i.ing.TenantTSDBHandler(w, r)
}
