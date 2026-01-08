// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/mimir/pkg/querier/api"
)

// CachedMimirClient wraps MimirClient with file-based caching.
type CachedMimirClient struct {
	client    *MimirClient
	cache     *FileCache
	namespace string
	tenantID  string
}

// NewCachedMimirClient creates a new CachedMimirClient.
func NewCachedMimirClient(client *MimirClient, cache *FileCache, namespace, tenantID string) *CachedMimirClient {
	return &CachedMimirClient{client: client, cache: cache, namespace: namespace, tenantID: tenantID}
}

// GetLabelNamesCardinality returns label names sorted by cardinality, using cache if available.
func (c *CachedMimirClient) GetLabelNamesCardinality(ctx context.Context, limit int) (*api.LabelNamesCardinalityResponse, error) {
	key := buildKey("mimir-label-names", c.namespace, c.tenantID, strconv.Itoa(limit))

	var cached api.LabelNamesCardinalityResponse
	if found, err := c.cache.Get(key, &cached); err != nil {
		return nil, err
	} else if found {
		return &cached, nil
	}

	result, err := c.client.GetLabelNamesCardinality(ctx, limit)
	if err != nil {
		return nil, err
	}

	_ = c.cache.Set(key, result)
	return result, nil
}

// GetLabelValuesCardinality returns series counts for each value of the specified label names, using cache if available.
func (c *CachedMimirClient) GetLabelValuesCardinality(ctx context.Context, labelNames []string, limit int) (*api.LabelValuesCardinalityResponse, error) {
	// Sort label names for consistent cache key.
	sortedNames := make([]string, len(labelNames))
	copy(sortedNames, labelNames)
	sort.Strings(sortedNames)

	key := buildKey("mimir-label-values", c.namespace, c.tenantID, strings.Join(sortedNames, ","), strconv.Itoa(limit))

	var cached api.LabelValuesCardinalityResponse
	if found, err := c.cache.Get(key, &cached); err != nil {
		return nil, err
	} else if found {
		return &cached, nil
	}

	result, err := c.client.GetLabelValuesCardinality(ctx, labelNames, limit)
	if err != nil {
		return nil, err
	}

	_ = c.cache.Set(key, result)
	return result, nil
}

// CachedLokiClient wraps LokiClient with file-based caching.
type CachedLokiClient struct {
	client *LokiClient
	cache  *FileCache
}

// NewCachedLokiClient creates a new CachedLokiClient.
func NewCachedLokiClient(client *LokiClient, cache *FileCache) *CachedLokiClient {
	return &CachedLokiClient{client: client, cache: cache}
}

// QueryQueryStats queries Loki for query stats logs, using streaming cache.
// Entries are cached in JSONL format (one JSON object per line) to avoid memory issues with large responses.
func (c *CachedLokiClient) QueryQueryStats(ctx context.Context, namespace, tenantID, container string, start, end time.Time, handler QueryStatsHandler) error {
	key := buildKey("loki", namespace, tenantID, container, start.Format(time.RFC3339), end.Format(time.RFC3339))

	// Try to read from cache (streaming).
	if c.cache.StreamRead(key, func(line []byte) error {
		var entry QueryStatsEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return err
		}
		return handler(entry)
	}) {
		return nil
	}

	// Cache miss - query and stream-write to cache.
	writer, err := c.cache.StreamWrite(key)
	if err != nil {
		return err
	}
	if writer != nil {
		defer writer.Close()
	}

	return c.client.QueryQueryStats(ctx, namespace, tenantID, container, start, end, func(entry QueryStatsEntry) error {
		if writer != nil {
			_ = writer.Write(entry)
		}
		return handler(entry)
	})
}
