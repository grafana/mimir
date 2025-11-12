package ingester

import (
	"testing"

	"github.com/failsafe-go/failsafe-go/priority"
	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/util/reactivelimiter"
)

func Test_ingesterReactiveLimiter(t *testing.T) {
	t.Run("newReactiveLimiter with Prioritizer", func(t *testing.T) {
		cfg := reactivelimiter.Config{}
		cfg.Enabled = true
		cfg.MaxLimitFactor = 1
		cfg.InitialRejectionFactor = 1
		cfg.MaxRejectionFactor = 2
		prioritizer := reactivelimiter.NewPrioritizer(log.NewNopLogger())
		newReactiveLimiter(&cfg, "read", log.NewNopLogger(), nil, priority.High, prioritizer)
	})
}
