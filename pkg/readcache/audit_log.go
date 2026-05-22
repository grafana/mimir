// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"fmt"
	"strings"
)

// formatPartitionIDs renders partition IDs for structured logs. Long
// lists are truncated to keep log lines readable.
func formatPartitionIDs(ids []int32, maxShown int) string {
	if len(ids) == 0 {
		return ""
	}
	if maxShown <= 0 || len(ids) <= maxShown {
		return joinPartitionIDs(ids)
	}
	return fmt.Sprintf("%s,...+%d", joinPartitionIDs(ids[:maxShown]), len(ids)-maxShown)
}

func joinPartitionIDs(ids []int32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(parts, ",")
}
