// SPDX-License-Identifier: AGPL-3.0-only

package usagestats

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/util/version"
)

func TestBuildReport(t *testing.T) {
	var (
		clusterCreatedAt = time.Now().Add(-time.Hour)
		seed             = ClusterSeed{UID: "test", CreatedAt: clusterCreatedAt}
		reportAt         = time.Now()
		reportInterval   = time.Hour
	)

	SetTarget("all")
	version.Version = "dev-version"
	version.Branch = "dev-branch"
	version.Revision = "dev-revision"

	report := buildReport(seed, reportAt, reportInterval)
	assert.Equal(t, "test", report.ClusterID)
	assert.Equal(t, clusterCreatedAt, report.CreatedAt)
	assert.Equal(t, reportAt, report.Interval)
	assert.Equal(t, reportInterval.Seconds(), report.IntervalPeriod)
	assert.Equal(t, "all", report.Target)
	assert.Equal(t, runtime.GOOS, report.Os)
	assert.Equal(t, runtime.GOARCH, report.Arch)
	assert.Equal(t, "oss", report.Edition)
	assert.NotNil(t, report.Metrics["memstats"])
	assert.Equal(t, "dev-version", report.Version.Version)
	assert.Equal(t, "dev-branch", report.Version.Branch)
	assert.Equal(t, "dev-revision", report.Version.Revision)
	assert.Equal(t, runtime.Version(), report.Version.GoVersion)
}
