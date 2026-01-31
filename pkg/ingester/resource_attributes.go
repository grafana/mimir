// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/seriesmetadata"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const resourceAttributesMaxSizeBytes = 1 * 1024 * 1024

// ResourceAttributes returns OTel resource attributes for series matching the matchers.
// This is a streaming RPC that returns batches of series with their resource attributes.
func (i *Ingester) ResourceAttributes(request *client.ResourceAttributesRequest, stream client.Ingester_ResourceAttributesServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	spanlog, ctx := spanlogger.New(stream.Context(), i.logger, tracer, "Ingester.ResourceAttributes")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	matchers, err := client.FromLabelMatchers(request.GetMatchers())
	if err != nil {
		return fmt.Errorf("error parsing label matchers: %w", err)
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

	idx, err := db.Head().Index()
	if err != nil {
		return fmt.Errorf("error getting index: %w", err)
	}
	defer idx.Close()

	// Get series metadata reader for resource attributes
	metaReader, err := db.Head().SeriesMetadata()
	if err != nil {
		return fmt.Errorf("error getting series metadata: %w", err)
	}
	defer metaReader.Close()

	// Get postings for matching series
	postings, err := getPostings(ctx, db, idx, matchers, false)
	if err != nil {
		return fmt.Errorf("error getting postings: %w", err)
	}

	buf := labels.NewScratchBuilder(10)
	resp := &client.ResourceAttributesResponse{}
	currentSize := 0
	count := int64(0)
	limit := request.GetLimit()

	// Debug: count total resources in metadata reader
	totalResources := uint64(0)
	if err := metaReader.IterVersionedResources(func(_ uint64, _ *seriesmetadata.VersionedResource) error {
		totalResources++
		return nil
	}); err != nil {
		level.Error(i.logger).Log("msg", "error counting resources", "err", err)
	}
	level.Info(i.logger).Log("msg", "ResourceAttributes query", "user", userID, "totalResourcesInHead", totalResources)

	seriesCount := 0
	for postings.Next() {
		seriesCount++
		if limit > 0 && count >= limit {
			break
		}

		seriesRef, _ := postings.AtBucketCount()
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
			// No resource attributes for this series, skip it
			continue
		}

		// Convert to response format
		item := &client.SeriesResourceAttributes{
			Labels: mimirpb.FromLabelsToLabelAdapters(lbls),
		}

		// Filter versions by time range if specified
		startMs := request.GetStartTimestampMs()
		endMs := request.GetEndTimestampMs()

		for _, ver := range versionedResource.Versions {
			// Check if version overlaps with requested time range
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

			// Copy identifying attributes
			for k, v := range ver.Identifying {
				version.Identifying[k] = v
			}

			// Copy descriptive attributes
			for k, v := range ver.Descriptive {
				version.Descriptive[k] = v
			}

			// Convert entities
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

		// Skip series with no matching versions
		if len(item.Versions) == 0 {
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

func sendResourceAttributesResponse(stream client.Ingester_ResourceAttributesServer, resp *client.ResourceAttributesResponse) error {
	return stream.Send(resp)
}
