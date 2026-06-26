package scheduler

import (
	"fmt"
	"strings"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
)

// jobIDForSpec builds a stable, unique ID for a job. It keys off each WC's start
// offset (not end), so the ID is unchanged if a job's end is later recomputed
// larger after a restart — letting recovery match it to the in-flight job.
func jobIDForSpec(spec *schedulerpb.JobSpec) string {
	if len(spec.OffsetRanges) == 0 {
		// Non compartment case (vc not included
		return fmt.Sprintf("%s/%d/%d", spec.Topic, spec.Partition, spec.StartOffset)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s/%d/", spec.Topic, spec.Partition)
	for i, wc := range spec.OffsetRanges {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%d:%d", wc.WcId, wc.StartOffset)
	}
	return sb.String()
}
