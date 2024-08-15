// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/tracing"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/activitytracker"
)

// ActivityTrackerWrapper is a wrapper around Ingester that adds queries to activity tracker.
type ActivityTrackerWrapper struct {
	ing     *Ingester
	tracker *activitytracker.ActivityTracker
}

func NewIngesterActivityTracker(ing *Ingester, tracker *activitytracker.ActivityTracker) *ActivityTrackerWrapper {
	return &ActivityTrackerWrapper{
		ing:     ing,
		tracker: tracker,
	}
}

func (i *ActivityTrackerWrapper) Push(ctx context.Context, request *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	// No tracking in Push
	return i.ing.Push(ctx, request)
}

func (i *ActivityTrackerWrapper) QueryStream(request *client.QueryRequest, server client.Ingester_QueryStreamServer) error {
	ix := i.tracker.Insert(func() string {
		return requestActivity(server.Context(), "Ingester/QueryStream", request)
	})
	defer i.tracker.Delete(ix)

	return i.ing.QueryStream(request, server)
}

func (i *ActivityTrackerWrapper) QueryExemplars(ctx context.Context, request *client.ExemplarQueryRequest) (*client.ExemplarQueryResponse, error) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(ctx, "Ingester/QueryExemplars", request)
	})
	defer i.tracker.Delete(ix)

	return i.ing.QueryExemplars(ctx, request)
}

func (i *ActivityTrackerWrapper) LabelValues(ctx context.Context, request *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(ctx, "Ingester/LabelValues", request)
	})
	defer i.tracker.Delete(ix)

	return i.ing.LabelValues(ctx, request)
}

func (i *ActivityTrackerWrapper) LabelNames(ctx context.Context, request *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(ctx, "Ingester/LabelNames", request)
	})
	defer i.tracker.Delete(ix)

	return i.ing.LabelNames(ctx, request)
}

func (i *ActivityTrackerWrapper) UserStats(ctx context.Context, request *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(ctx, "Ingester/UserStats", request)
	})
	defer i.tracker.Delete(ix)

	return i.ing.UserStats(ctx, request)
}

func (i *ActivityTrackerWrapper) AllUserStats(ctx context.Context, request *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(ctx, "Ingester/AllUserStats", request)
	})
	defer i.tracker.Delete(ix)

	return i.ing.AllUserStats(ctx, request)
}

func (i *ActivityTrackerWrapper) MetricsForLabelMatchers(ctx context.Context, request *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(ctx, "Ingester/MetricsForLabelMatchers", request)
	})
	defer i.tracker.Delete(ix)

	return i.ing.MetricsForLabelMatchers(ctx, request)
}

func (i *ActivityTrackerWrapper) MetricsMetadata(ctx context.Context, request *client.MetricsMetadataRequest) (*client.MetricsMetadataResponse, error) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(ctx, "Ingester/MetricsMetadata", request)
	})
	defer i.tracker.Delete(ix)

	return i.ing.MetricsMetadata(ctx, request)
}

func (i *ActivityTrackerWrapper) LabelNamesAndValues(request *client.LabelNamesAndValuesRequest, server client.Ingester_LabelNamesAndValuesServer) error {
	ix := i.tracker.Insert(func() string {
		return requestActivity(server.Context(), "Ingester/LabelNamesAndValues", request)
	})
	defer i.tracker.Delete(ix)

	return i.ing.LabelNamesAndValues(request, server)
}

func (i *ActivityTrackerWrapper) LabelValuesCardinality(request *client.LabelValuesCardinalityRequest, server client.Ingester_LabelValuesCardinalityServer) error {
	ix := i.tracker.Insert(func() string {
		return requestActivity(server.Context(), "Ingester/LabelValuesCardinality", request)
	})
	defer i.tracker.Delete(ix)

	return i.ing.LabelValuesCardinality(request, server)
}

func (i *ActivityTrackerWrapper) ActiveSeries(request *client.ActiveSeriesRequest, server client.Ingester_ActiveSeriesServer) error {
	ix := i.tracker.Insert(func() string {
		return requestActivity(server.Context(), "Ingester/ActiveSeries", request)
	})
	defer i.tracker.Delete(ix)

	return i.ing.ActiveSeries(request, server)
}

func (i *ActivityTrackerWrapper) FlushHandler(w http.ResponseWriter, r *http.Request) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(r.Context(), "Ingester/FlushHandler", nil)
	})
	defer i.tracker.Delete(ix)

	i.ing.FlushHandler(w, r)
}

func (i *ActivityTrackerWrapper) PrepareShutdownHandler(w http.ResponseWriter, r *http.Request) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(r.Context(), "Ingester/PrepareShutdownHandler", nil)
	})
	defer i.tracker.Delete(ix)

	i.ing.PrepareShutdownHandler(w, r)
}

func (i *ActivityTrackerWrapper) PreparePartitionDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(r.Context(), "Ingester/PreparePartitionDownscaleHandler", nil)
	})
	defer i.tracker.Delete(ix)

	i.ing.PreparePartitionDownscaleHandler(w, r)
}

func (i *ActivityTrackerWrapper) PrepareInstanceRingDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(r.Context(), "Ingester/PrepareInstanceRingDownscaleHandler", nil)
	})
	defer i.tracker.Delete(ix)

	i.ing.PrepareInstanceRingDownscaleHandler(w, r)
}

func (i *ActivityTrackerWrapper) PrepareUnregisterHandler(w http.ResponseWriter, r *http.Request) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(r.Context(), "Ingester/PrepareUnregisterHandler", nil)
	})
	defer i.tracker.Delete(ix)

	i.ing.PrepareUnregisterHandler(w, r)
}

func (i *ActivityTrackerWrapper) ShutdownHandler(w http.ResponseWriter, r *http.Request) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(r.Context(), "Ingester/ShutdownHandler", nil)
	})
	defer i.tracker.Delete(ix)

	i.ing.ShutdownHandler(w, r)
}

func (i *ActivityTrackerWrapper) UserRegistryHandler(writer http.ResponseWriter, request *http.Request) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(request.Context(), "Ingester/UserRegistryHandler", nil)
	})
	defer i.tracker.Delete(ix)

	i.ing.UserRegistryHandler(writer, request)
}

func (i *ActivityTrackerWrapper) TenantsHandler(w http.ResponseWriter, r *http.Request) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(r.Context(), "Ingester/TenantsHandler", nil)
	})
	defer i.tracker.Delete(ix)

	i.ing.TenantsHandler(w, r)
}

func (i *ActivityTrackerWrapper) TenantTSDBHandler(w http.ResponseWriter, r *http.Request) {
	ix := i.tracker.Insert(func() string {
		return requestActivity(r.Context(), "Ingester/TenantTSDBHandler", nil)
	})
	defer i.tracker.Delete(ix)

	i.ing.TenantTSDBHandler(w, r)
}

func requestActivity(ctx context.Context, name string, req interface{}) string {
	userID, _ := tenant.TenantID(ctx)
	traceID, _ := tracing.ExtractSampledTraceID(ctx)

	switch r := req.(type) {
	case *client.QueryRequest:
		// To minimize memory allocation, make use of an optimized stringer implementation
		// for *client.QueryRequest type, as this request can be invoked multiple times per second.
		return queryRequestActivity(name, userID, traceID, r)

	default:
		return fmt.Sprintf("%s: user=%q trace=%q request=%v", name, userID, traceID, req)
	}
}

func queryRequestActivity(name, userID, traceID string, req *client.QueryRequest) string {
	sb := bytes.NewBuffer(
		make([]byte, 0, 8192),
	)
	sb.WriteString(name)

	sb.WriteString(`: user=`)
	b := strconv.AppendQuote(sb.Bytes(), userID)
	sb = bytes.NewBuffer(b)

	sb.WriteString(` trace=`)
	b = strconv.AppendQuote(sb.Bytes(), traceID)
	sb = bytes.NewBuffer(b)

	sb.WriteString(` request=`)
	queryRequestToString(sb, req)

	return sb.String()
}

func queryRequestToString(sb *bytes.Buffer, req *client.QueryRequest) {
	if req == nil {
		sb.WriteString("nil")
		return
	}
	b := make([]byte, 0, 32)

	sb.WriteString("&QueryRequest{")

	sb.WriteString("StartTimestampMs:")
	sb.Write(strconv.AppendInt(b, req.StartTimestampMs, 10))
	sb.WriteString(",")

	b = b[:0]
	sb.WriteString("EndTimestampMs:")
	sb.Write(strconv.AppendInt(b, req.EndTimestampMs, 10))
	sb.WriteString(",")

	sb.WriteString("Matchers:[]*LabelMatcher{")
	for _, m := range req.Matchers {
		labelMatcherToString(sb, m)
		sb.WriteString(",")
	}
	sb.WriteString("},")

	b = b[:0]
	sb.WriteString("StreamingChunksBatchSize:")
	sb.Write(strconv.AppendUint(b, req.StreamingChunksBatchSize, 10))
	sb.WriteString(",}")
}

func labelMatcherToString(sb *bytes.Buffer, m *client.LabelMatcher) {
	if m == nil {
		sb.WriteString("nil")
		return
	}
	sb.WriteString("&LabelMatcher{Type:")
	sb.WriteString(m.Type.String())
	sb.WriteString(",Name:")
	sb.WriteString(m.Name)
	sb.WriteString(",Value:")
	sb.WriteString(m.Value)
	sb.WriteString(",}")
}
