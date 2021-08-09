// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/testexporter/correctness/case.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package correctness

import (
	"context"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

// Case is a metric that can be used for exporting a metric and querying it for tests.
type Case interface {
	prometheus.Collector

	Name() string
	Query(ctx context.Context, client v1.API, selectors string, start time.Time, duration time.Duration) ([]model.SamplePair, error)
	ExpectedValueAt(time.Time) float64

	MinQueryTime() time.Time
	Test(ctx context.Context, client v1.API, selectors string, start time.Time, duration time.Duration) (bool, error)
	Stop()
}
