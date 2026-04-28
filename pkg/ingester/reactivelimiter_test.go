// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"

	"github.com/failsafe-go/failsafe-go/priority"
	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/util/reactivelimiter"
)

func Test_ingesterReactiveLimiter(t *testing.T) {
	// Asserts that building a reactive limiter with a prioritizer works without panic
	t.Run("newReactiveLimiter with Prioritizer", func(t *testing.T) {
		cfg := reactivelimiter.Config{}
		cfg.Enabled = true
		cfg.MaxLimitFactor = 1
		cfg.MinLimitFactor = 1.2
		cfg.InitialRejectionFactor = 1
		cfg.MaxRejectionFactor = 2
		prioritizer := reactivelimiter.NewPrioritizer(log.NewNopLogger())
		newReactiveLimiter(&cfg, "read", log.NewNopLogger(), nil, priority.High, prioritizer)
	})
}
