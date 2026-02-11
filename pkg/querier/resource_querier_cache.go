// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/seriesmetadata"
	"github.com/prometheus/prometheus/util/annotations"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// ResourceAttributesFetcher abstracts the RPC call to fetch resource attributes.
type ResourceAttributesFetcher interface {
	// FetchResourceAttributes fetches resource attributes for the given time range.
	// Returns nil, nil if no resource attributes are available.
	FetchResourceAttributes(ctx context.Context, minT, maxT int64) ([]*ResourceAttributesData, error)
}

// ResourceAttributesData represents resource attributes for a single series.
type ResourceAttributesData struct {
	LabelsHash uint64
	Versions   []*seriesmetadata.ResourceVersion
}

// resourceQuerierCache wraps a querier to provide cached ResourceQuerier functionality.
// It pre-fetches resource attributes on first access and serves GetResourceAt from cache.
type resourceQuerierCache struct {
	storage.Querier

	// Source for fetching resource attributes
	fetcher ResourceAttributesFetcher

	// Time range for this querier
	minT, maxT int64

	// Logger for debugging
	logger log.Logger

	// Stored context from Select() for use in GetResourceAt()
	// This is needed because the ResourceQuerier interface doesn't pass context
	storedCtx   context.Context
	storedCtxMu sync.Mutex

	// Cache state
	cache            map[uint64]*seriesmetadata.VersionedResource
	uniqueAttrNames  map[string]struct{}
	cacheInitMu      sync.Mutex
	cacheInitialized bool
	cacheInitErr     error
}

// NewResourceQuerierCache creates a new resourceQuerierCache wrapping the given querier.
func NewResourceQuerierCache(
	querier storage.Querier,
	fetcher ResourceAttributesFetcher,
	minT, maxT int64,
	logger log.Logger,
) storage.Querier {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &resourceQuerierCache{
		Querier: querier,
		fetcher: fetcher,
		minT:    minT,
		maxT:    maxT,
		logger:  logger,
	}
}

// GetResourceAt implements storage.ResourceQuerier.
// It returns the resource version active at the given timestamp for the series.
func (q *resourceQuerierCache) GetResourceAt(labelsHash uint64, timestamp int64) (*seriesmetadata.ResourceVersion, bool) {
	if q.fetcher == nil {
		return nil, false
	}

	// Use stored context from Select() call, fall back to Background if not available
	q.storedCtxMu.Lock()
	ctx := q.storedCtx
	q.storedCtxMu.Unlock()
	if ctx == nil {
		level.Debug(q.logger).Log("msg", "GetResourceAt: no stored context available")
		ctx = context.Background()
	}

	if err := q.ensureCacheInitialized(ctx); err != nil {
		// Log warning but don't fail the query - graceful degradation
		level.Warn(q.logger).Log("msg", "failed to initialize resource cache", "err", err)
		return nil, false
	}

	vr, ok := q.cache[labelsHash]
	if !ok {
		return nil, false
	}

	rv := vr.VersionAt(timestamp)
	return rv, rv != nil
}

// IterUniqueAttributeNames implements storage.ResourceQuerier.
// It iterates over all unique resource attribute names.
func (q *resourceQuerierCache) IterUniqueAttributeNames(fn func(name string)) error {
	if q.fetcher == nil {
		return nil
	}

	// Use stored context from Select() call, fall back to Background if not available
	q.storedCtxMu.Lock()
	ctx := q.storedCtx
	q.storedCtxMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}

	if err := q.ensureCacheInitialized(ctx); err != nil {
		return err
	}

	for name := range q.uniqueAttrNames {
		fn(name)
	}
	return nil
}

// ensureCacheInitialized pre-fetches resource attributes on first call (lazy loading).
func (q *resourceQuerierCache) ensureCacheInitialized(ctx context.Context) error {
	q.cacheInitMu.Lock()
	defer q.cacheInitMu.Unlock()

	if q.cacheInitialized {
		return q.cacheInitErr
	}

	q.cacheInitialized = true
	q.cacheInitErr = q.initializeCache(ctx)
	return q.cacheInitErr
}

// initializeCache performs the actual pre-fetch of resource attributes.
func (q *resourceQuerierCache) initializeCache(ctx context.Context) error {
	// Fetch all resource attributes for this time range
	data, err := q.fetcher.FetchResourceAttributes(ctx, q.minT, q.maxT)
	if err != nil {
		return err
	}

	// Populate cache
	q.cache = make(map[uint64]*seriesmetadata.VersionedResource, len(data))
	q.uniqueAttrNames = make(map[string]struct{})

	for _, item := range data {
		// Store versioned resource
		vr := &seriesmetadata.VersionedResource{
			Versions: item.Versions,
		}
		q.cache[item.LabelsHash] = vr

		// Collect unique attribute names
		for _, version := range item.Versions {
			for name := range version.Identifying {
				q.uniqueAttrNames[name] = struct{}{}
			}
			for name := range version.Descriptive {
				q.uniqueAttrNames[name] = struct{}{}
			}
		}
	}

	level.Debug(q.logger).Log(
		"msg", "initialized resource cache",
		"series_count", len(q.cache),
		"unique_attrs", len(q.uniqueAttrNames),
	)

	return nil
}

// Select implements storage.Querier and captures the context for later use in GetResourceAt.
func (q *resourceQuerierCache) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	// Store the context for later use in GetResourceAt
	q.storedCtxMu.Lock()
	if q.storedCtx == nil {
		q.storedCtx = ctx
	}
	q.storedCtxMu.Unlock()

	return q.Querier.Select(ctx, sortSeries, hints, matchers...)
}

// LabelValues implements storage.Querier and captures the context for later use.
func (q *resourceQuerierCache) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// Store the context for later use in GetResourceAt
	q.storedCtxMu.Lock()
	if q.storedCtx == nil {
		q.storedCtx = ctx
	}
	q.storedCtxMu.Unlock()

	return q.Querier.LabelValues(ctx, name, hints, matchers...)
}

// LabelNames implements storage.Querier and captures the context for later use.
func (q *resourceQuerierCache) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// Store the context for later use in GetResourceAt
	q.storedCtxMu.Lock()
	if q.storedCtx == nil {
		q.storedCtx = ctx
	}
	q.storedCtxMu.Unlock()

	return q.Querier.LabelNames(ctx, hints, matchers...)
}

// Close releases resources.
func (q *resourceQuerierCache) Close() error {
	return q.Querier.Close()
}

// distributorResourceFetcher implements ResourceAttributesFetcher for distributorQuerier.
type distributorResourceFetcher struct {
	distributor Distributor
}

// FetchResourceAttributes fetches resource attributes from ingesters via the distributor.
func (f *distributorResourceFetcher) FetchResourceAttributes(ctx context.Context, minT, maxT int64) ([]*ResourceAttributesData, error) {
	// Use a matcher that matches all series (required for getPostings to return results)
	allSeriesMatcher, _ := labels.NewMatcher(labels.MatchNotEqual, model.MetricNameLabel, "")
	results, err := f.distributor.ResourceAttributes(ctx, minT, maxT, []*labels.Matcher{allSeriesMatcher}, 0)
	if err != nil {
		return nil, err
	}

	data := make([]*ResourceAttributesData, 0, len(results))
	for _, item := range results {
		labelsHash := convertIngesterLabelsToHash(item.Labels)
		versions := convertIngesterVersions(item.Versions)
		data = append(data, &ResourceAttributesData{
			LabelsHash: labelsHash,
			Versions:   versions,
		})
	}
	return data, nil
}

// blocksResourceFetcher implements ResourceAttributesFetcher for blocksStoreQuerier.
type blocksResourceFetcher struct {
	blocksQueryable ResourceAttributesBlocksQueryable
}

// FetchResourceAttributes fetches resource attributes from store-gateways.
func (f *blocksResourceFetcher) FetchResourceAttributes(ctx context.Context, minT, maxT int64) ([]*ResourceAttributesData, error) {
	// Use a matcher that matches all series (required for getPostings to return results)
	allSeriesMatcher, _ := labels.NewMatcher(labels.MatchNotEqual, model.MetricNameLabel, "")
	results, err := f.blocksQueryable.ResourceAttributes(ctx, minT, maxT, []*labels.Matcher{allSeriesMatcher}, 0)
	if err != nil {
		return nil, err
	}

	data := make([]*ResourceAttributesData, 0, len(results))
	for _, item := range results {
		labelsHash := convertStoreLabelsToHash(item.Labels)
		versions := convertStoreVersions(item.Versions)
		data = append(data, &ResourceAttributesData{
			LabelsHash: labelsHash,
			Versions:   versions,
		})
	}
	return data, nil
}

// convertIngesterLabelsToHash converts ingester labels to a hash.
// Uses StableHash to match the hash used when storing resource attributes in the ingester.
func convertIngesterLabelsToHash(lbls []mimirpb.LabelAdapter) uint64 {
	return labels.StableHash(mimirpb.FromLabelAdaptersToLabels(lbls))
}

// convertIngesterVersions converts ingester version data to seriesmetadata.ResourceVersion.
func convertIngesterVersions(versions []*ingester_client.ResourceVersionData) []*seriesmetadata.ResourceVersion {
	result := make([]*seriesmetadata.ResourceVersion, 0, len(versions))
	for _, v := range versions {
		rv := &seriesmetadata.ResourceVersion{
			Identifying: v.Identifying,
			Descriptive: v.Descriptive,
			MinTime:     v.MinTimeMs,
			MaxTime:     v.MaxTimeMs,
		}

		// Convert entities
		if len(v.Entities) > 0 {
			rv.Entities = make([]*seriesmetadata.Entity, 0, len(v.Entities))
			for _, e := range v.Entities {
				rv.Entities = append(rv.Entities, &seriesmetadata.Entity{
					Type:        e.Type,
					ID:          e.Id,
					Description: e.Description,
				})
			}
		}

		result = append(result, rv)
	}
	return result
}

// convertStoreLabelsToHash converts store-gateway labels (map) to a hash.
// Uses StableHash to match the hash used when storing resource attributes.
func convertStoreLabelsToHash(lbls map[string]string) uint64 {
	return labels.StableHash(labels.FromMap(lbls))
}

// convertStoreVersions converts store-gateway version data to seriesmetadata.ResourceVersion.
func convertStoreVersions(versions []*storepb.ResourceVersionData) []*seriesmetadata.ResourceVersion {
	result := make([]*seriesmetadata.ResourceVersion, 0, len(versions))
	for _, v := range versions {
		rv := &seriesmetadata.ResourceVersion{
			Identifying: v.Identifying,
			Descriptive: v.Descriptive,
			MinTime:     v.MinTimeMs,
			MaxTime:     v.MaxTimeMs,
		}

		// Convert entities
		if len(v.Entities) > 0 {
			rv.Entities = make([]*seriesmetadata.Entity, 0, len(v.Entities))
			for _, e := range v.Entities {
				rv.Entities = append(rv.Entities, &seriesmetadata.Entity{
					Type:        e.Type,
					ID:          e.Id,
					Description: e.Description,
				})
			}
		}

		result = append(result, rv)
	}
	return result
}
