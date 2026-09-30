// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/limits"
)

// The partition ring is read through the ring sidecar, which already watches it over memberlist:
// the active partition count for global limits and each tenant's token ranges for owned series.

var ringClient = &http.Client{Timeout: 5 * time.Second}

func fetchActivePartitions(ctx context.Context, url string) (uint64, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	response, err := ringClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return 0, fmt.Errorf("%s returned %s", url, response.Status)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(body)), 10, 64)
}

// WaitActivePartitions waits, up to timeout, for an active partition, so records aren't consumed
// while global limits have no local share and are ignored. The Go ingester only consumes once its
// own partition is in the ring.
func WaitActivePartitions(ctx context.Context, url string, overrides *limits.Overrides, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		partitions, err := fetchActivePartitions(ctx, url)
		if err == nil {
			overrides.SetActivePartitions(partitions)
			if partitions > 0 {
				return
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			fmt.Fprintf(os.Stderr, "phase=active_partitions_wait_timeout last=%d error=%v\n", partitions, err)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// PollActivePartitions keeps overrides informed of the active partitions in the partition ring,
// which the ring sidecar serves as a plain number, so global limits convert to this partition's
// share.
func PollActivePartitions(ctx context.Context, url string, overrides *limits.Overrides, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	last, hasLast := uint64(0), false
	for {
		partitions, err := fetchActivePartitions(ctx, url)
		switch {
		case err != nil:
			fmt.Fprintf(os.Stderr, "phase=active_partitions_error error=%v\n", err)
		default:
			overrides.SetActivePartitions(partitions)
			if !hasLast || last != partitions {
				fmt.Fprintf(os.Stderr, "phase=active_partitions count=%d\n", partitions)
				last, hasLast = partitions, true
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// PollOwnedRanges asks the sidecar, every period, for this partition's token ranges in each stored
// tenant's shuffle shard, like Mimir's owned series service with the partition ring strategy.
func PollOwnedRanges(ctx context.Context, url string, s *Store, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		ranges, err := fetchOwnedRanges(ctx, url, s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "phase=owned_ranges_error error=%v\n", err)
		} else {
			s.SetOwnedRanges(ranges)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func fetchOwnedRanges(ctx context.Context, url string, s *Store) (map[string]TenantRanges, error) {
	tenants := map[string]int64{}
	for _, tenant := range s.TenantIDs() {
		tenants[tenant] = s.Overrides().Tenant(tenant).Limits.IngestionPartitionsTenantShardSize
	}
	body, err := json.Marshal(map[string]any{"tenants": tenants})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := ringClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%s returned %s", url, response.Status)
	}
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return ParseOwnedRanges(responseBody)
}

// ParseOwnedRanges parses `{"tenant": [start, end, ...] | null}`, null when the tenant's shard
// skips this partition.
func ParseOwnedRanges(body []byte) (map[string]TenantRanges, error) {
	var parsed map[string]*[]uint32
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}
	if parsed == nil {
		return nil, fmt.Errorf("owned ranges are not an object: %s", body)
	}
	ranges := make(map[string]TenantRanges, len(parsed))
	for tenant, owned := range parsed {
		if owned == nil {
			ranges[tenant] = TenantRanges{}
			continue
		}
		ranges[tenant] = TenantRanges{InShard: true, Ranges: *owned}
	}
	return ranges, nil
}
