// SPDX-License-Identifier: AGPL-3.0-only

// Package service serves the store over the cortex.Ingester gRPC API, like the Go ingester.
package service

import (
	"cmp"
	"context"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/exemplars"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/kafka"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/metrics"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/store"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	responseTargetBytes = 1024 * 1024
	searchBatchSize     = 256
	seriesBatchSize     = 1024
)

// Service implements the ingester's read API; Push and the other write paths are unimplemented,
// since this ingester consumes Kafka.
type Service struct {
	client.UnimplementedIngesterServer
	store       *store.Store
	consistency *kafka.Consistency
}

func New(s *store.Store) *Service {
	return &Service{store: s}
}

func WithConsistency(s *store.Store, consistency *kafka.Consistency) *Service {
	return &Service{store: s, consistency: consistency}
}

var _ client.IngesterServer = (*Service)(nil)

func (s *Service) enforce(ctx context.Context) error {
	if s.consistency == nil {
		return nil
	}
	return s.consistency.Enforce(ctx)
}

func tenant(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("x-scope-orgid")
	if len(values) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing x-scope-orgid")
	}
	return values[0], nil
}

func internal(err error) error {
	return status.Error(codes.InvalidArgument, err.Error())
}

func toMatchers(matchers []*client.LabelMatcher) []store.LabelMatcher {
	converted := make([]store.LabelMatcher, len(matchers))
	for index, matcher := range matchers {
		converted[index] = store.LabelMatcher{Type: int32(matcher.Type), Name: matcher.Name, Value: matcher.Value}
	}
	return converted
}

func labelMatchers(matchers *client.LabelMatchers) []store.LabelMatcher {
	if matchers == nil {
		return nil
	}
	return toMatchers(matchers.Matchers)
}

// begin runs the checks every tenant read starts with.
func (s *Service) begin(ctx context.Context) (string, error) {
	if err := s.enforce(ctx); err != nil {
		return "", err
	}
	return tenant(ctx)
}

func (s *Service) QueryStream(request *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
	metrics.Queries.Inc()
	ctx := stream.Context()
	tenantID, err := s.begin(ctx)
	if err != nil {
		return err
	}
	selected, err := s.store.SelectChunks(tenantID, request.StartTimestampMs, request.EndTimestampMs, toMatchers(request.Matchers))
	if err != nil {
		return internal(err)
	}
	// Like Go, observed once the tenant has a TSDB.
	if s.store.HasTenant(tenantID) {
		metrics.QueriedSeries.WithLabelValues("merged_blocks").Observe(float64(len(selected)))
		samples := 0.0
		for _, series := range selected {
			for _, chunk := range series.Chunks {
				samples += float64(chunk.Samples)
			}
		}
		metrics.QueriedSamples.Observe(samples)
	}
	batchSize := int(min(request.StreamingChunksBatchSize, uint64(1<<62)))
	if batchSize == 0 {
		batchSize = 1024
	}
	for start := 0; start < len(selected); start += seriesBatchSize {
		end := min(start+seriesBatchSize, len(selected))
		if err := stream.SendMsg(RawMessage(seriesResponse(selected[start:end], end == len(selected)))); err != nil {
			return err
		}
	}
	if len(selected) == 0 {
		return stream.SendMsg(RawMessage{0x20, 0x01})
	}
	return sendChunks(selected, batchSize, func(encoded []byte) error {
		return stream.SendMsg(RawMessage(encoded))
	})
}

// seriesResponse is a QueryStreamResponse of the series' labels and chunk counts, the end of the
// series stream with the last batch.
func seriesResponse(series []store.QuerySeriesView, isEnd bool) []byte {
	size := 2
	for _, view := range series {
		size += len(view.EncodedLabels) + 16
	}
	encoded := make([]byte, 0, size)
	for _, view := range series {
		seriesSize := len(view.EncodedLabels)
		if len(view.Chunks) > 0 {
			seriesSize += 1 + protowire.SizeVarint(uint64(len(view.Chunks)))
		}
		encoded = protowire.AppendTag(encoded, 3, protowire.BytesType)
		encoded = protowire.AppendVarint(encoded, uint64(seriesSize))
		encoded = append(encoded, view.EncodedLabels...)
		if len(view.Chunks) > 0 {
			encoded = protowire.AppendTag(encoded, 2, protowire.VarintType)
			encoded = protowire.AppendVarint(encoded, uint64(len(view.Chunks)))
		}
	}
	if isEnd {
		encoded = append(encoded, 0x20, 0x01)
	}
	return encoded
}

func chunksItemSize(view store.QuerySeriesView, index int) int {
	size := 0
	if index > 0 {
		size += 1 + protowire.SizeVarint(uint64(index))
	}
	for _, chunk := range view.Chunks {
		size += 1 + protowire.SizeBytes(len(chunk.Wire))
	}
	return size
}

// sendChunks sends the series' chunks as QueryStreamResponses of at most batchSize series and
// about responseTargetBytes each.
func sendChunks(views []store.QuerySeriesView, batchSize int, send func([]byte) error) error {
	// Sized to what the responses hold, up to the target: a whole target zeroed for every query
	// would cost more than the query.
	remaining := 0
	for index, view := range views {
		remaining += 1 + protowire.SizeVarint(uint64(chunksItemSize(view, index))) + chunksItemSize(view, index)
	}
	newBuffer := func() []byte { return make([]byte, 0, min(remaining, responseTargetBytes)) }
	encoded := newBuffer()
	items := 0
	for index, view := range views {
		if len(view.Chunks) == 0 {
			continue
		}
		size := chunksItemSize(view, index)
		itemSize := 1 + protowire.SizeVarint(uint64(size)) + size
		if items > 0 && (items >= batchSize || len(encoded)+itemSize > responseTargetBytes) {
			if err := send(encoded); err != nil {
				return err
			}
			remaining -= len(encoded)
			encoded = newBuffer()
			items = 0
		}
		encoded = protowire.AppendTag(encoded, 5, protowire.BytesType)
		encoded = protowire.AppendVarint(encoded, uint64(size))
		if index > 0 {
			encoded = protowire.AppendTag(encoded, 1, protowire.VarintType)
			encoded = protowire.AppendVarint(encoded, uint64(index))
		}
		for _, chunk := range view.Chunks {
			encoded = protowire.AppendTag(encoded, 2, protowire.BytesType)
			encoded = protowire.AppendBytes(encoded, chunk.Wire)
		}
		items++
	}
	if items > 0 {
		return send(encoded)
	}
	return nil
}

func comparePairs(a, b [][2]string) int {
	for index := range min(len(a), len(b)) {
		if c := cmp.Compare(a[index][0], b[index][0]); c != 0 {
			return c
		}
		if c := cmp.Compare(a[index][1], b[index][1]); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(a), len(b))
}

func pairsKey(pairs [][2]string) string {
	var key strings.Builder
	for _, pair := range pairs {
		key.WriteString(pair[0])
		key.WriteByte(0xff)
		key.WriteString(pair[1])
		key.WriteByte(0xff)
	}
	return key.String()
}

func labelAdapters(pairs [][2]string) []mimirpb.LabelAdapter {
	adapters := make([]mimirpb.LabelAdapter, len(pairs))
	for index, pair := range pairs {
		adapters[index] = mimirpb.LabelAdapter{Name: pair[0], Value: pair[1]}
	}
	return adapters
}

func (s *Service) QueryExemplars(ctx context.Context, request *client.ExemplarQueryRequest) (*client.ExemplarQueryResponse, error) {
	metrics.Queries.Inc()
	tenantID, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	type group struct {
		labels    [][2]string
		exemplars []exemplars.Exemplar
	}
	groups := map[string]*group{}
	for _, matcherSet := range request.Matchers {
		selected, err := s.store.SelectExemplars(tenantID, request.StartTimestampMs, request.EndTimestampMs, toMatchers(matcherSet.Matchers))
		if err != nil {
			return nil, internal(err)
		}
		for _, series := range selected {
			if len(series.Exemplars) == 0 {
				continue
			}
			key := pairsKey(series.Labels)
			existing, ok := groups[key]
			if !ok {
				existing = &group{labels: series.Labels}
				groups[key] = existing
			}
			existing.exemplars = append(existing.exemplars, series.Exemplars...)
		}
	}
	sorted := make([]*group, 0, len(groups))
	for _, g := range groups {
		sorted = append(sorted, g)
	}
	slices.SortFunc(sorted, func(a, b *group) int { return comparePairs(a.labels, b.labels) })
	response := &client.ExemplarQueryResponse{Timeseries: make([]mimirpb.TimeSeries, 0, len(sorted))}
	total := 0
	for _, g := range sorted {
		slices.SortStableFunc(g.exemplars, func(a, b exemplars.Exemplar) int { return cmp.Compare(a.TimestampMs, b.TimestampMs) })
		deduplicated := slices.CompactFunc(g.exemplars, func(a, b exemplars.Exemplar) bool { return a.Equal(&b) })
		series := mimirpb.TimeSeries{Labels: labelAdapters(g.labels), Exemplars: make([]mimirpb.Exemplar, len(deduplicated))}
		for index, exemplar := range deduplicated {
			labels := make([]mimirpb.LabelAdapter, len(exemplar.Labels))
			for i, label := range exemplar.Labels {
				labels[i] = mimirpb.LabelAdapter{Name: label.Name, Value: label.Value}
			}
			series.Exemplars[index] = mimirpb.Exemplar{Labels: labels, Value: exemplar.Value, TimestampMs: exemplar.TimestampMs}
		}
		total += len(deduplicated)
		response.Timeseries = append(response.Timeseries, series)
	}
	if s.store.HasTenant(tenantID) {
		metrics.QueriedExemplars.Observe(float64(total))
	}
	return response, nil
}

func applyLimit[T any](values []T, limit int64) ([]T, error) {
	if limit < 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must be >= 0")
	}
	if limit > 0 && int64(len(values)) > limit {
		values = values[:limit]
	}
	return values, nil
}

func (s *Service) LabelValues(ctx context.Context, request *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	tenantID, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	values, err := s.store.LabelValues(tenantID, request.LabelName, request.StartTimestampMs, request.EndTimestampMs, labelMatchers(request.Matchers))
	if err != nil {
		return nil, internal(err)
	}
	if values, err = applyLimit(values, request.Limit); err != nil {
		return nil, err
	}
	return &client.LabelValuesResponse{LabelValues: values}, nil
}

func (s *Service) LabelNames(ctx context.Context, request *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	tenantID, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	names, err := s.store.LabelNames(tenantID, request.StartTimestampMs, request.EndTimestampMs, labelMatchers(request.Matchers))
	if err != nil {
		return nil, internal(err)
	}
	if names, err = applyLimit(names, request.Limit); err != nil {
		return nil, err
	}
	return &client.LabelNamesResponse{LabelNames: names}, nil
}

func countActive(method client.CountMethod) (bool, error) {
	switch method {
	case client.IN_MEMORY:
		return false, nil
	case client.ACTIVE:
		return true, nil
	default:
		return false, status.Error(codes.InvalidArgument, "invalid count method")
	}
}

func stats(view store.UserStatsView) *client.UserStatsResponse {
	return &client.UserStatsResponse{
		IngestionRate:     view.IngestionRate,
		NumSeries:         view.NumSeries,
		ApiIngestionRate:  view.APIIngestionRate,
		RuleIngestionRate: view.RuleIngestionRate,
	}
}

func (s *Service) UserStats(ctx context.Context, request *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	tenantID, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	active, err := countActive(request.CountMethod)
	if err != nil {
		return nil, err
	}
	return stats(s.store.UserStats(tenantID, active)), nil
}

func (s *Service) AllUserStats(_ context.Context, request *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	active, err := countActive(request.CountMethod)
	if err != nil {
		return nil, err
	}
	all := s.store.AllUserStats(active)
	response := &client.UsersStatsResponse{Stats: make([]*client.UserIDStatsResponse, len(all))}
	for index, tenantStats := range all {
		response.Stats[index] = &client.UserIDStatsResponse{UserId: tenantStats.Tenant, Data: stats(tenantStats.Stats)}
	}
	return response, nil
}

func (s *Service) MetricsForLabelMatchers(ctx context.Context, request *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	tenantID, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	if request.Limit < 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must be >= 0")
	}
	seen := map[string]struct{}{}
	var sets [][][2]string
	for _, matcherSet := range request.MatchersSet {
		selected, err := s.store.SelectLabels(tenantID, request.StartTimestampMs, request.EndTimestampMs, toMatchers(matcherSet.Matchers))
		if err != nil {
			return nil, internal(err)
		}
		for _, series := range selected {
			key := pairsKey(series)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			sets = append(sets, series)
		}
	}
	slices.SortFunc(sets, comparePairs)
	metric := make([]*mimirpb.Metric, len(sets))
	for index, series := range sets {
		metric[index] = &mimirpb.Metric{Labels: labelAdapters(series)}
	}
	if metric, err = applyLimit(metric, request.Limit); err != nil {
		return nil, err
	}
	return &client.MetricsForLabelMatchersResponse{Metric: metric}, nil
}

func (s *Service) MetricsMetadata(ctx context.Context, request *client.MetricsMetadataRequest) (*client.MetricsMetadataResponse, error) {
	tenantID, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	if request.Limit == 0 {
		return &client.MetricsMetadataResponse{}, nil
	}
	grouped := map[string][]*mimirpb.MetricMetadata{}
	for _, item := range s.store.Metadata(tenantID) {
		grouped[item.MetricFamilyName] = append(grouped[item.MetricFamilyName], &item)
	}
	var names []string
	switch {
	case len(request.MetricNames) > 0:
		names = request.MetricNames
	case request.Metric != "":
		names = []string{request.Metric}
	default:
		for name := range grouped {
			names = append(names, name)
		}
		slices.Sort(names)
	}
	response := &client.MetricsMetadataResponse{}
	metricCount := int32(0)
	for _, name := range names {
		items, ok := grouped[name]
		if !ok {
			continue
		}
		delete(grouped, name)
		if request.Limit > 0 && metricCount >= request.Limit {
			break
		}
		if request.LimitPerMetric > 0 && int32(len(items)) > request.LimitPerMetric {
			items = items[:request.LimitPerMetric]
		}
		response.Metadata = append(response.Metadata, items...)
		metricCount++
	}
	return response, nil
}

// batchMessages groups items into messages of about responseTargetBytes.
func batchMessages[T interface{ Size() int }](items []T, send func([]T) error) error {
	start, size := 0, 0
	for index, item := range items {
		itemSize := item.Size()
		if index > start && size+itemSize > responseTargetBytes {
			if err := send(items[start:index]); err != nil {
				return err
			}
			start, size = index, 0
		}
		size += itemSize
	}
	if start < len(items) {
		return send(items[start:])
	}
	return nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func (s *Service) LabelNamesAndValues(request *client.LabelNamesAndValuesRequest, stream client.Ingester_LabelNamesAndValuesServer) error {
	tenantID, err := s.begin(stream.Context())
	if err != nil {
		return err
	}
	active, err := countActive(request.CountMethod)
	if err != nil {
		return err
	}
	namesAndValues, err := s.store.LabelNamesAndValues(tenantID, toMatchers(request.Matchers), active)
	if err != nil {
		return internal(err)
	}
	items := make([]*client.LabelValues, 0, len(namesAndValues))
	for _, name := range sortedKeys(namesAndValues) {
		items = append(items, &client.LabelValues{LabelName: name, Values: sortedKeys(namesAndValues[name])})
	}
	return batchMessages(items, func(batch []*client.LabelValues) error {
		return stream.Send(&client.LabelNamesAndValuesResponse{Items: batch})
	})
}

func (s *Service) LabelValuesCardinality(request *client.LabelValuesCardinalityRequest, stream client.Ingester_LabelValuesCardinalityServer) error {
	tenantID, err := s.begin(stream.Context())
	if err != nil {
		return err
	}
	active, err := countActive(request.CountMethod)
	if err != nil {
		return err
	}
	cardinality, err := s.store.LabelValuesCardinality(tenantID, request.LabelNames, toMatchers(request.Matchers), active)
	if err != nil {
		return internal(err)
	}
	items := make([]*client.LabelValueSeriesCount, 0, len(cardinality))
	for _, name := range sortedKeys(cardinality) {
		items = append(items, &client.LabelValueSeriesCount{LabelName: name, LabelValueSeries: cardinality[name]})
	}
	return batchMessages(items, func(batch []*client.LabelValueSeriesCount) error {
		return stream.Send(&client.LabelValuesCardinalityResponse{Items: batch})
	})
}

func (s *Service) ActiveSeries(request *client.ActiveSeriesRequest, stream client.Ingester_ActiveSeriesServer) error {
	tenantID, err := s.begin(stream.Context())
	if err != nil {
		return err
	}
	if request.Type != client.SERIES && request.Type != client.NATIVE_HISTOGRAM_SERIES {
		return status.Error(codes.InvalidArgument, "invalid active series request type")
	}
	histogramsOnly := request.Type == client.NATIVE_HISTOGRAM_SERIES
	series, err := s.store.ActiveSeries(tenantID, toMatchers(request.Matchers), histogramsOnly)
	if err != nil {
		return internal(err)
	}
	// The response's encoded size, kept up to date rather than recomputed for every series.
	response := &client.ActiveSeriesResponse{}
	metricBytes, bucketBytes := 0, 0
	for _, item := range series {
		metric := &mimirpb.Metric{Labels: labelAdapters(item.Labels)}
		metricSize := metric.Size()
		metricBytes += 1 + protowire.SizeVarint(uint64(metricSize)) + metricSize
		response.Metric = append(response.Metric, metric)
		if histogramsOnly {
			response.BucketCount = append(response.BucketCount, item.BucketCount)
			bucketBytes += protowire.SizeVarint(item.BucketCount)
		}
		size := metricBytes
		if bucketBytes > 0 {
			size += 1 + protowire.SizeVarint(uint64(bucketBytes)) + bucketBytes
		}
		if size >= responseTargetBytes {
			if err := stream.Send(response); err != nil {
				return err
			}
			response = &client.ActiveSeriesResponse{}
			metricBytes, bucketBytes = 0, 0
		}
	}
	if len(response.Metric) > 0 {
		return stream.Send(response)
	}
	return nil
}

func (s *Service) SearchLabelNames(request *client.SearchLabelNamesRequest, stream client.Ingester_SearchLabelNamesServer) error {
	tenantID, err := s.begin(stream.Context())
	if err != nil {
		return err
	}
	values, err := s.store.LabelNames(tenantID, request.StartTimestampMs, request.EndTimestampMs, toMatchers(request.Matchers))
	if err != nil {
		return internal(err)
	}
	return search(values, request.Filter, request.Ordering, request.Limit, stream.Send)
}

func (s *Service) SearchLabelValues(request *client.SearchLabelValuesRequest, stream client.Ingester_SearchLabelValuesServer) error {
	tenantID, err := s.begin(stream.Context())
	if err != nil {
		return err
	}
	values, err := s.store.LabelValues(tenantID, request.Name, request.StartTimestampMs, request.EndTimestampMs, toMatchers(request.Matchers))
	if err != nil {
		return internal(err)
	}
	return search(values, request.Filter, request.Ordering, request.Limit, stream.Send)
}
