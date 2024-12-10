// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"net/http"
	"time"

	"github.com/grafana/dskit/services"
)

type nopHaTracker struct {
	services.Service
	http.Handler
}

func newNopHaTracker() *nopHaTracker {
	tracker := &nopHaTracker{}
	tracker.Service = services.NewIdleService(nil, nil)
	tracker.Handler = http.NotFoundHandler()
	return tracker
}

func (n nopHaTracker) checkReplica(context.Context, string, string, string, time.Time) error {
	return nil
}

func (n nopHaTracker) cleanupHATrackerMetricsForUser(string) {
	// no-op
}
