// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"fmt"
	"sort"
	"strings"

	"github.com/grafana/dskit/ring"
)

// formatPartitionKeySummary renders a compact per-partition key count
// summary for routing audit logs.
func formatPartitionKeySummary(partitionKeys []ring.PartitionKeys) string {
	if len(partitionKeys) == 0 {
		return ""
	}
	type entry struct {
		pid   int32
		count int
	}
	entries := make([]entry, len(partitionKeys))
	for i, pk := range partitionKeys {
		entries[i] = entry{pid: pk.PartitionID, count: len(pk.Indexes)}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].pid < entries[j].pid })

	const maxShown = 8
	parts := make([]string, 0, len(entries))
	for i, e := range entries {
		if i >= maxShown {
			parts = append(parts, fmt.Sprintf("...+%d", len(entries)-maxShown))
			break
		}
		parts = append(parts, fmt.Sprintf("P%d:%d", e.pid, e.count))
	}
	return strings.Join(parts, ",")
}
