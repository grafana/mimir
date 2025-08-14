// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"net/http"
	"runtime/pprof"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// ProfilingWrapper is a wrapper around Ingester that adds tenant ID to pprof labels.
type ProfilingWrapper struct {
	ing *Ingester
}

func NewIngesterProfilingWrapper(ing *Ingester) *ProfilingWrapper {
	return &ProfilingWrapper{
		ing: ing,
	}
}

func (i *ProfilingWrapper) Push(ctx context.Context, request *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	userID, _ := tenant.TenantID(ctx)
	var resp *mimirpb.WriteResponse
	var err error
	pprof.Do(ctx, pprof.Labels("method", "Ingester.Push", "userID", userID), func(ctx context.Context) {
		resp, err = i.ing.Push(ctx, request)
	})
	return resp, err
}

func (i *ProfilingWrapper) PushToStorageAndReleaseRequest(ctx context.Context, req *mimirpb.WriteRequest) error {
	userID, _ := tenant.TenantID(ctx)
	var err error
	pprof.Do(ctx, pprof.Labels("method", "Ingester.PushToStorageAndReleaseRequest", "userID", userID), func(ctx context.Context) {
		err = i.ing.PushToStorageAndReleaseRequest(ctx, req)
	})
	return err
}

func (i *ProfilingWrapper) QueryStream(request *client.QueryRequest, server client.Ingester_QueryStreamServer) error {
	userID, _ := tenant.TenantID(server.Context())
	var err error
	pprof.Do(server.Context(), pprof.Labels("method", "Ingester.QueryStream", "userID", userID), func(ctx context.Context) {
		err = i.ing.QueryStream(request, server)
	})
	return err
}

func (i *ProfilingWrapper) QueryExemplars(ctx context.Context, request *client.ExemplarQueryRequest) (*client.ExemplarQueryResponse, error) {
	userID, _ := tenant.TenantID(ctx)
	var resp *client.ExemplarQueryResponse
	var err error
	pprof.Do(ctx, pprof.Labels("method", "Ingester.QueryExemplars", "userID", userID), func(ctx context.Context) {
		resp, err = i.ing.QueryExemplars(ctx, request)
	})
	return resp, err
}

func (i *ProfilingWrapper) LabelValues(ctx context.Context, request *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	userID, _ := tenant.TenantID(ctx)
	var resp *client.LabelValuesResponse
	var err error
	pprof.Do(ctx, pprof.Labels("method", "Ingester.LabelValues", "userID", userID), func(ctx context.Context) {
		resp, err = i.ing.LabelValues(ctx, request)
	})
	return resp, err
}

func (i *ProfilingWrapper) LabelNames(ctx context.Context, request *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	userID, _ := tenant.TenantID(ctx)
	var resp *client.LabelNamesResponse
	var err error
	pprof.Do(ctx, pprof.Labels("method", "Ingester.LabelNames", "userID", userID), func(ctx context.Context) {
		resp, err = i.ing.LabelNames(ctx, request)
	})
	return resp, err
}

func (i *ProfilingWrapper) UserStats(ctx context.Context, request *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	userID, _ := tenant.TenantID(ctx)
	var resp *client.UserStatsResponse
	var err error
	pprof.Do(ctx, pprof.Labels("method", "Ingester.UserStats", "userID", userID), func(ctx context.Context) {
		resp, err = i.ing.UserStats(ctx, request)
	})
	return resp, err
}

func (i *ProfilingWrapper) AllUserStats(ctx context.Context, request *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	userID, _ := tenant.TenantID(ctx)
	var resp *client.UsersStatsResponse
	var err error
	pprof.Do(ctx, pprof.Labels("method", "Ingester.AllUserStats", "userID", userID), func(ctx context.Context) {
		resp, err = i.ing.AllUserStats(ctx, request)
	})
	return resp, err
}

func (i *ProfilingWrapper) MetricsForLabelMatchers(ctx context.Context, request *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	userID, _ := tenant.TenantID(ctx)
	var resp *client.MetricsForLabelMatchersResponse
	var err error
	pprof.Do(ctx, pprof.Labels("method", "Ingester.MetricsForLabelMatchers", "userID", userID), func(ctx context.Context) {
		resp, err = i.ing.MetricsForLabelMatchers(ctx, request)
	})
	return resp, err
}

func (i *ProfilingWrapper) MetricsMetadata(ctx context.Context, request *client.MetricsMetadataRequest) (*client.MetricsMetadataResponse, error) {
	userID, _ := tenant.TenantID(ctx)
	var resp *client.MetricsMetadataResponse
	var err error
	pprof.Do(ctx, pprof.Labels("method", "Ingester.MetricsMetadata", "userID", userID), func(ctx context.Context) {
		resp, err = i.ing.MetricsMetadata(ctx, request)
	})
	return resp, err
}

func (i *ProfilingWrapper) LabelNamesAndValues(request *client.LabelNamesAndValuesRequest, server client.Ingester_LabelNamesAndValuesServer) error {
	userID, _ := tenant.TenantID(server.Context())
	var err error
	pprof.Do(server.Context(), pprof.Labels("method", "Ingester.LabelNamesAndValues", "userID", userID), func(ctx context.Context) {
		err = i.ing.LabelNamesAndValues(request, server)
	})
	return err
}

func (i *ProfilingWrapper) LabelValuesCardinality(request *client.LabelValuesCardinalityRequest, server client.Ingester_LabelValuesCardinalityServer) error {
	userID, _ := tenant.TenantID(server.Context())
	var err error
	pprof.Do(server.Context(), pprof.Labels("method", "Ingester.LabelValuesCardinality", "userID", userID), func(ctx context.Context) {
		err = i.ing.LabelValuesCardinality(request, server)
	})
	return err
}

func (i *ProfilingWrapper) ActiveSeries(request *client.ActiveSeriesRequest, server client.Ingester_ActiveSeriesServer) error {
	userID, _ := tenant.TenantID(server.Context())
	var err error
	pprof.Do(server.Context(), pprof.Labels("method", "Ingester.ActiveSeries", "userID", userID), func(ctx context.Context) {
		err = i.ing.ActiveSeries(request, server)
	})
	return err
}

func (i *ProfilingWrapper) FlushHandler(w http.ResponseWriter, r *http.Request) {
	userID, _ := tenant.TenantID(r.Context())
	pprof.Do(r.Context(), pprof.Labels("method", "Ingester.FlushHandler", "userID", userID), func(ctx context.Context) {
		i.ing.FlushHandler(w, r)
	})
}

func (i *ProfilingWrapper) PrepareShutdownHandler(w http.ResponseWriter, r *http.Request) {
	userID, _ := tenant.TenantID(r.Context())
	pprof.Do(r.Context(), pprof.Labels("method", "Ingester.PrepareShutdownHandler", "userID", userID), func(ctx context.Context) {
		i.ing.PrepareShutdownHandler(w, r)
	})
}

func (i *ProfilingWrapper) PreparePartitionDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	userID, _ := tenant.TenantID(r.Context())
	pprof.Do(r.Context(), pprof.Labels("method", "Ingester.PreparePartitionDownscaleHandler", "userID", userID), func(ctx context.Context) {
		i.ing.PreparePartitionDownscaleHandler(w, r)
	})
}

func (i *ProfilingWrapper) PrepareInstanceRingDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	userID, _ := tenant.TenantID(r.Context())
	pprof.Do(r.Context(), pprof.Labels("method", "Ingester.PrepareInstanceRingDownscaleHandler", "userID", userID), func(ctx context.Context) {
		i.ing.PrepareInstanceRingDownscaleHandler(w, r)
	})
}

func (i *ProfilingWrapper) PrepareUnregisterHandler(w http.ResponseWriter, r *http.Request) {
	userID, _ := tenant.TenantID(r.Context())
	pprof.Do(r.Context(), pprof.Labels("method", "Ingester.PrepareUnregisterHandler", "userID", userID), func(ctx context.Context) {
		i.ing.PrepareUnregisterHandler(w, r)
	})
}

func (i *ProfilingWrapper) ShutdownHandler(w http.ResponseWriter, r *http.Request) {
	userID, _ := tenant.TenantID(r.Context())
	pprof.Do(r.Context(), pprof.Labels("method", "Ingester.ShutdownHandler", "userID", userID), func(ctx context.Context) {
		i.ing.ShutdownHandler(w, r)
	})
}

func (i *ProfilingWrapper) UserRegistryHandler(writer http.ResponseWriter, request *http.Request) {
	userID, _ := tenant.TenantID(request.Context())
	pprof.Do(request.Context(), pprof.Labels("method", "Ingester.UserRegistryHandler", "userID", userID), func(ctx context.Context) {
		i.ing.UserRegistryHandler(writer, request)
	})
}

func (i *ProfilingWrapper) TenantsHandler(w http.ResponseWriter, r *http.Request) {
	userID, _ := tenant.TenantID(r.Context())
	pprof.Do(r.Context(), pprof.Labels("method", "Ingester.TenantsHandler", "userID", userID), func(ctx context.Context) {
		i.ing.TenantsHandler(w, r)
	})
}

func (i *ProfilingWrapper) TenantTSDBHandler(w http.ResponseWriter, r *http.Request) {
	userID, _ := tenant.TenantID(r.Context())
	pprof.Do(r.Context(), pprof.Labels("method", "Ingester.TenantTSDBHandler", "userID", userID), func(ctx context.Context) {
		i.ing.TenantTSDBHandler(w, r)
	})
}
