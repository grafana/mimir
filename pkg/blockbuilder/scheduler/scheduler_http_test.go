// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
)

func TestSchedulerHTTPHandlers(t *testing.T) {
	cfg := Config{
		ConsumerGroup:       "test-group",
		SchedulingInterval:  1,
		JobSize:             1,
		StartupObserveTime:  1,
		JobLeaseExpiry:      1,
		LookbackOnNoCommit:  1,
		MaxScanAge:          1,
		MaxJobsPerPartition: 1,
		EnqueueInterval:     1,
		JobFailuresAllowed:  1,
	}

	logger := log.NewNopLogger()
	scheduler, err := New(cfg, logger, nil)
	require.NoError(t, err)

	// Test StatusHandler when not running
	req := httptest.NewRequest("GET", "/status", nil)
	w := httptest.NewRecorder()
	scheduler.StatusHandler(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "not running yet")

	// Test JobsHandler when not running
	req = httptest.NewRequest("GET", "/jobs", nil)
	w = httptest.NewRecorder()
	scheduler.JobsHandler(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "not running yet")

	// Test PartitionsHandler when not running
	req = httptest.NewRequest("GET", "/partitions", nil)
	w = httptest.NewRecorder()
	scheduler.PartitionsHandler(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "not running yet")
}

func TestJobQueueGetAllJobs(t *testing.T) {
	logger := log.NewNopLogger()
	metrics := newSchedulerMetrics(nil)
	policy := noOpJobCreationPolicy[schedulerpb.JobSpec]{}

	queue := newJobQueue(1, policy, 1, metrics, logger)

	// Test with empty queue
	jobs := queue.getAllJobs()
	assert.Empty(t, jobs)
}
