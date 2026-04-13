// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
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

	// Provider for per-tenant cache size limit (bytes). May be nil.
	maxCacheBytesProvider func(userID string) int

	// Tenant ID captured from Select()/LabelValues()/LabelNames() for use in
	// GetResourceAt(). We store only the tenant ID (not the full context) because
	// the span-derived context from Select() may be cancelled by the time the
	// PromQL engine calls GetResourceAt() during sample iteration.
	tenantID   string
	tenantIDMu sync.Mutex

	// Cache state
	cache            map[uint64]*seriesmetadata.VersionedResource
	uniqueAttrNames  map[string]struct{}
	cacheInitMu      sync.Mutex
	cacheInitialized bool
	cacheInitErr     error // memoized error from first failed init attempt
}

// NewResourceQuerierCache creates a new resourceQuerierCache wrapping the given querier.
// maxCacheBytesProvider, if non-nil, limits the total estimated memory of the cache.
func NewResourceQuerierCache(
	querier storage.Querier,
	fetcher ResourceAttributesFetcher,
	minT, maxT int64,
	maxCacheBytesProvider func(userID string) int,
	logger log.Logger,
) storage.Querier {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &resourceQuerierCache{
		Querier:               querier,
		fetcher:               fetcher,
		minT:                  minT,
		maxT:                  maxT,
		maxCacheBytesProvider: maxCacheBytesProvider,
		logger:                logger,
	}
}

// GetResourceAt implements storage.ResourceQuerier.
// It returns the resource version active at the given timestamp for the series.
func (q *resourceQuerierCache) GetResourceAt(labelsHash uint64, timestamp int64) (*seriesmetadata.ResourceVersion, bool) {
	if q.fetcher == nil {
		return nil, false
	}

	q.tenantIDMu.Lock()
	tid := q.tenantID
	q.tenantIDMu.Unlock()
	if tid == "" {
		// No tenant ID means Select/LabelValues/LabelNames was never called.
		level.Warn(q.logger).Log("msg", "GetResourceAt: no stored tenant ID available, skipping resource attributes")
		return nil, false
	}

	// Build a fresh context with just the tenant ID and a timeout. We cannot
	// use the context from Select() because its span may already be finished.
	// The timeout prevents indefinite blocking if the store-gateway RPC hangs.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = user.InjectOrgID(ctx, tid)

	if err := q.ensureCacheInitialized(ctx); err != nil {
		// Log warning but don't fail the query - graceful degradation
		level.Warn(q.logger).Log("msg", "failed to initialize resource cache", "err", err)
		return nil, false
	}

	vr, ok := q.cache[labelsHash]
	if !ok {
		return nil, false
	}

	rv, ok := vr.VersionAt(timestamp)
	return rv, ok
}

// IterUniqueAttributeNames implements storage.ResourceQuerier.
// It iterates over all unique resource attribute names.
func (q *resourceQuerierCache) IterUniqueAttributeNames(fn func(name string)) error {
	if q.fetcher == nil {
		return nil
	}

	q.tenantIDMu.Lock()
	tid := q.tenantID
	q.tenantIDMu.Unlock()
	if tid == "" {
		level.Warn(q.logger).Log("msg", "IterUniqueAttributeNames: no stored tenant ID available, skipping resource attributes")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = user.InjectOrgID(ctx, tid)

	if err := q.ensureCacheInitialized(ctx); err != nil {
		// Degrade gracefully, matching GetResourceAt's behavior.
		level.Warn(q.logger).Log("msg", "failed to initialize resource cache", "err", err)
		return nil
	}

	for name := range q.uniqueAttrNames {
		fn(name)
	}
	return nil
}

// ensureCacheInitialized pre-fetches resource attributes on first call (lazy loading).
// On failure, the error is memoized to avoid retrying the RPC on every sample point.
// This is safe because the error is scoped to this querier instance (one per query),
// not shared globally; a new query gets a fresh cache with no memoized error.
func (q *resourceQuerierCache) ensureCacheInitialized(ctx context.Context) error {
	q.cacheInitMu.Lock()
	defer q.cacheInitMu.Unlock()

	if q.cacheInitialized {
		return nil
	}
	if q.cacheInitErr != nil {
		return q.cacheInitErr
	}

	if err := q.initializeCache(ctx); err != nil {
		q.cacheInitErr = err
		return err
	}
	q.cacheInitialized = true
	return nil
}

// estimateResourceDataSize returns an approximate byte size for a ResourceAttributesData item.
func estimateResourceDataSize(item *ResourceAttributesData) int64 {
	size := int64(32) // LabelsHash + slice header
	for _, v := range item.Versions {
		size += 64 // struct overhead + time fields
		for k, val := range v.Identifying {
			size += int64(len(k) + len(val) + 16)
		}
		for k, val := range v.Descriptive {
			size += int64(len(k) + len(val) + 16)
		}
	}
	return size
}

// initializeCache performs the actual pre-fetch of resource attributes.
func (q *resourceQuerierCache) initializeCache(ctx context.Context) error {
	// Fetch all resource attributes for this time range.
	data, err := q.fetcher.FetchResourceAttributes(ctx, q.minT, q.maxT)
	if err != nil {
		return err
	}

	// Determine max cache size from the provider (if set).
	var maxBytes int64
	if q.maxCacheBytesProvider != nil {
		q.tenantIDMu.Lock()
		tid := q.tenantID
		q.tenantIDMu.Unlock()
		if tid != "" {
			maxBytes = int64(q.maxCacheBytesProvider(tid))
		}
	}

	// Populate cache
	q.cache = make(map[uint64]*seriesmetadata.VersionedResource, len(data))
	q.uniqueAttrNames = make(map[string]struct{})

	var totalBytes int64
	for _, item := range data {
		if maxBytes > 0 {
			totalBytes += estimateResourceDataSize(item)
			if totalBytes > maxBytes {
				level.Warn(q.logger).Log("msg", "resource cache size limit reached, truncating",
					"limit_bytes", maxBytes, "cached_series", len(q.cache), "total_series", len(data))
				break
			}
		}

		// Merge versions if the same series appears from multiple sources
		// (e.g., different store-gateways serving different blocks).
		if existing, ok := q.cache[item.LabelsHash]; ok {
			existing.Versions = mergeCacheVersions(existing.Versions, item.Versions)
		} else {
			q.cache[item.LabelsHash] = &seriesmetadata.VersionedResource{
				Versions: item.Versions,
			}
		}

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

// Select implements storage.Querier and captures the tenant ID for later use in GetResourceAt.
func (q *resourceQuerierCache) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	q.storeTenantID(ctx)
	return q.Querier.Select(ctx, sortSeries, hints, matchers...)
}

// LabelValues implements storage.Querier and captures the tenant ID for later use.
func (q *resourceQuerierCache) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	q.storeTenantID(ctx)
	return q.Querier.LabelValues(ctx, name, hints, matchers...)
}

// LabelNames implements storage.Querier and captures the tenant ID for later use.
func (q *resourceQuerierCache) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	q.storeTenantID(ctx)
	return q.Querier.LabelNames(ctx, hints, matchers...)
}

// storeTenantID extracts and stores the tenant ID from the given context.
func (q *resourceQuerierCache) storeTenantID(ctx context.Context) {
	if tid, err := user.ExtractOrgID(ctx); err == nil {
		q.tenantIDMu.Lock()
		q.tenantID = tid
		q.tenantIDMu.Unlock()
	}
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
	// Use a matcher that matches all series (required for getPostings to return results).
	allSeriesMatcher, _ := labels.NewMatcher(labels.MatchNotEqual, model.MetricNameLabel, "")
	results, err := f.distributor.ResourceAttributes(ctx, minT, maxT, []*labels.Matcher{allSeriesMatcher}, 0, nil)
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
	// Use a matcher that matches all series (required for getPostings to return results).
	allSeriesMatcher, _ := labels.NewMatcher(labels.MatchNotEqual, model.MetricNameLabel, "")
	results, err := f.blocksQueryable.ResourceAttributes(ctx, minT, maxT, []*labels.Matcher{allSeriesMatcher}, 0, nil)
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

// mergeCacheVersions merges two slices of resource versions, deduplicating by
// time range AND identifying attributes. Two versions with the same time range
// but different identifying attributes are distinct and must not be collapsed.
func mergeCacheVersions(a, b []*seriesmetadata.ResourceVersion) []*seriesmetadata.ResourceVersion {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	type versionKey struct {
		minTime     int64
		maxTime     int64
		identifying string
	}

	seen := make(map[versionKey]bool, len(a))
	result := make([]*seriesmetadata.ResourceVersion, 0, len(a)+len(b))

	for _, v := range a {
		key := versionKey{v.MinTime, v.MaxTime, labelsMapToKey(v.Identifying)}
		if !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}

	for _, v := range b {
		key := versionKey{v.MinTime, v.MaxTime, labelsMapToKey(v.Identifying)}
		if !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].MinTime < result[j].MinTime
	})

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
