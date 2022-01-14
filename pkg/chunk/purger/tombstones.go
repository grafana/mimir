// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/purger/tombstones.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package purger

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

type TombstonesSet struct{}

type NoopTombstonesLoader struct{}

func NewNoopTombstonesLoader() *NoopTombstonesLoader {
	return &NoopTombstonesLoader{}
}

func (tl *NoopTombstonesLoader) Stop() {}

func (tl *NoopTombstonesLoader) GetPendingTombstonesForInterval(userID string, from, to model.Time) (*TombstonesSet, error) {
	return &TombstonesSet{}, nil
}

func (tl *NoopTombstonesLoader) GetStoreCacheGenNumber(tenantIDs []string) string {
	return ""
}

func (tl *NoopTombstonesLoader) GetResultsCacheGenNumber(tenantIDs []string) string {
	return ""
}

func (ts TombstonesSet) GetDeletedIntervals(lbls labels.Labels, from, to model.Time) []model.Interval {
	return nil
}

func (ts TombstonesSet) Len() int {
	return 0
}

func (ts TombstonesSet) HasTombstonesForInterval(from, to model.Time) bool {
	return false
}
