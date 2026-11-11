// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import "github.com/prometheus/prometheus/tsdb"

func (e prometheusEngine) Head() engineHead { return prometheusHead{e.db.Head()} }

// prometheusHead is the Prometheus TSDB's head as an engineHead.
type prometheusHead struct {
	*tsdb.Head
}

func (h prometheusHead) Sync() error {
	return h.FsyncWLSegments()
}

func (h prometheusHead) TimeBounds() timeBounds {
	return timeBounds{MinTime: h.MinTime(), MaxTime: h.MaxTime(), MinOOOTime: h.MinOOOTime(), MaxOOOTime: h.MaxOOOTime()}
}

func (h prometheusHead) OldestAppendableTime() (int64, bool) {
	return h.AppendableMinValidTime()
}
