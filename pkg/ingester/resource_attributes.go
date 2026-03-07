// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"

	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/seriesmetadata"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const resourceAttributesMaxSizeBytes = 1 * 1024 * 1024

// ResourceAttributes returns OTel resource attributes for series matching the matchers.
// This is a streaming RPC that returns batches of series with their resource attributes.
// When ResourceAttrFilters is present, it uses the inverted index for reverse lookup
// instead of PostingsForMatchers.
func (i *Ingester) ResourceAttributes(request *client.ResourceAttributesRequest, stream client.Ingester_ResourceAttributesServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	spanlog, ctx := spanlogger.New(stream.Context(), i.logger, tracer, "Ingester.ResourceAttributes")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return nil
	}

	// Get series metadata reader for resource attributes
	metaReader, err := db.Head().SeriesMetadata()
	if err != nil {
		return fmt.Errorf("error getting series metadata: %w", err)
	}
	defer metaReader.Close()

	if len(request.GetResourceAttrFilters()) > 0 {
		return i.resourceAttributesByFilter(ctx, request, stream, metaReader)
	}
	return i.resourceAttributesByMatchers(ctx, request, stream, db, metaReader)
}

// resourceAttributesByFilter performs a reverse lookup using the inverted index.
// It finds series that have specific resource attribute key:value pairs.
func (i *Ingester) resourceAttributesByFilter(
	ctx context.Context,
	request *client.ResourceAttributesRequest,
	stream client.Ingester_ResourceAttributesServer,
	metaReader seriesmetadata.Reader,
) error {
	filters := request.GetResourceAttrFilters()

	// Intersect results from all filters (AND semantics).
	var matchingHashes []uint64
	for idx, filter := range filters {
		hashes := metaReader.LookupResourceAttr(filter.GetKey(), filter.GetValue())
		if idx == 0 {
			// Clone so we don't alias the metaReader's internal slice.
			matchingHashes = append([]uint64(nil), hashes...)
		} else {
			matchingHashes = intersectSortedUint64(matchingHashes, hashes)
		}
		if len(matchingHashes) == 0 {
			return nil
		}
	}

	startMs := request.GetStartTimestampMs()
	endMs := request.GetEndTimestampMs()
	limit := request.GetLimit()

	resp := &client.ResourceAttributesResponse{}
	currentSize := 0
	count := int64(0)

	for _, labelsHash := range matchingHashes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if limit > 0 && count >= limit {
			break
		}

		// Get labels for this hash
		lbls, found := metaReader.LabelsForHash(labelsHash)
		if !found {
			continue
		}

		// Get versioned resource attributes
		versionedResource, found := metaReader.GetVersionedResource(labelsHash)
		if !found || versionedResource == nil || len(versionedResource.Versions) == 0 {
			continue
		}

		item := buildResourceAttributesItem(lbls, versionedResource, startMs, endMs)
		if item == nil {
			continue
		}

		itemSize := item.Size()
		if currentSize+itemSize > resourceAttributesMaxSizeBytes && len(resp.Items) > 0 {
			if err := sendResourceAttributesResponse(stream, resp); err != nil {
				return fmt.Errorf("error sending response: %w", err)
			}
			resp = &client.ResourceAttributesResponse{}
			currentSize = 0
		}

		resp.Items = append(resp.Items, item)
		currentSize += itemSize
		count++
	}

	// Send final batch
	if len(resp.Items) > 0 {
		if err := sendResourceAttributesResponse(stream, resp); err != nil {
			return fmt.Errorf("error sending final response: %w", err)
		}
	}

	return nil
}

// resourceAttributesByMatchers performs the forward lookup using PostingsForMatchers.
func (i *Ingester) resourceAttributesByMatchers(
	ctx context.Context,
	request *client.ResourceAttributesRequest,
	stream client.Ingester_ResourceAttributesServer,
	db *userTSDB,
	metaReader seriesmetadata.Reader,
) error {
	matchers, err := client.FromLabelMatchers(request.GetMatchers())
	if err != nil {
		return fmt.Errorf("error parsing label matchers: %w", err)
	}

	// Strip the __query_shard__ matcher injected by the query-frontend's
	// sharding middleware. PostingsForMatchers treats it as a real label
	// and returns empty postings since no series has that label.
	_, matchers, err = sharding.RemoveShardFromMatchers(matchers)
	if err != nil {
		return fmt.Errorf("error removing shard matcher: %w", err)
	}

	idx, err := db.Head().Index()
	if err != nil {
		return fmt.Errorf("error getting index: %w", err)
	}
	defer idx.Close()

	// Get postings for matching series
	postings, err := tsdb.PostingsForMatchers(ctx, idx, matchers...)
	if err != nil {
		return fmt.Errorf("error getting postings: %w", err)
	}

	startMs := request.GetStartTimestampMs()
	endMs := request.GetEndTimestampMs()
	limit := request.GetLimit()

	buf := labels.NewScratchBuilder(10)
	resp := &client.ResourceAttributesResponse{}
	currentSize := 0
	count := int64(0)

	for postings.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if limit > 0 && count >= limit {
			break
		}

		seriesRef := postings.At()
		err = idx.Series(seriesRef, &buf, nil)
		if err != nil {
			// Postings may be stale. Skip if no underlying series exists.
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return fmt.Errorf("error getting series: %w", err)
		}

		lbls := buf.Labels()
		labelsHash := labels.StableHash(lbls)

		// Get versioned resource attributes for this series
		versionedResource, found := metaReader.GetVersionedResource(labelsHash)
		if !found || versionedResource == nil || len(versionedResource.Versions) == 0 {
			continue
		}

		item := buildResourceAttributesItem(lbls, versionedResource, startMs, endMs)
		if item == nil {
			continue
		}

		itemSize := item.Size()
		if currentSize+itemSize > resourceAttributesMaxSizeBytes && len(resp.Items) > 0 {
			if err := sendResourceAttributesResponse(stream, resp); err != nil {
				return fmt.Errorf("error sending response: %w", err)
			}
			resp = &client.ResourceAttributesResponse{}
			currentSize = 0
		}

		resp.Items = append(resp.Items, item)
		currentSize += itemSize
		count++
	}

	if err := postings.Err(); err != nil {
		return fmt.Errorf("error iterating postings: %w", err)
	}

	// Send final batch
	if len(resp.Items) > 0 {
		if err := sendResourceAttributesResponse(stream, resp); err != nil {
			return fmt.Errorf("error sending final response: %w", err)
		}
	}

	return nil
}

// buildResourceAttributesItem converts versioned resource data to the response format,
// filtering versions by time range. Returns nil if no versions match.
func buildResourceAttributesItem(lbls labels.Labels, versionedResource *seriesmetadata.VersionedResource, startMs, endMs int64) *client.SeriesResourceAttributes {
	item := &client.SeriesResourceAttributes{
		Labels: mimirpb.FromLabelsToLabelAdapters(lbls),
	}

	for _, ver := range versionedResource.Versions {
		if endMs > 0 && ver.MinTime > endMs {
			continue
		}
		if startMs > 0 && ver.MaxTime < startMs {
			continue
		}

		version := &client.ResourceVersionData{
			Identifying: make(map[string]string),
			Descriptive: make(map[string]string),
			MinTimeMs:   ver.MinTime,
			MaxTimeMs:   ver.MaxTime,
		}

		for k, v := range ver.Identifying {
			version.Identifying[k] = v
		}
		for k, v := range ver.Descriptive {
			version.Descriptive[k] = v
		}

		for _, ent := range ver.Entities {
			entity := &client.EntityData{
				Type:        ent.Type,
				Id:          make(map[string]string),
				Description: make(map[string]string),
			}
			for k, v := range ent.ID {
				entity.Id[k] = v
			}
			for k, v := range ent.Description {
				entity.Description[k] = v
			}
			version.Entities = append(version.Entities, entity)
		}

		item.Versions = append(item.Versions, version)
	}

	if len(item.Versions) == 0 {
		return nil
	}
	return item
}

// intersectSortedUint64 returns the intersection of two sorted uint64 slices.
func intersectSortedUint64(a, b []uint64) []uint64 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}

	result := make([]uint64, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

func sendResourceAttributesResponse(stream client.Ingester_ResourceAttributesServer, resp *client.ResourceAttributesResponse) error {
	return stream.Send(resp)
}
