// SPDX-License-Identifier: AGPL-3.0-only

package reactivelimiter

import (
	"flag"
	"time"

	"github.com/failsafe-go/failsafe-go/adaptivelimiter"
	"github.com/failsafe-go/failsafe-go/priority"
	"github.com/go-kit/log"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

type PrioritizerConfig struct {
	CalibrationInterval time.Duration `yaml:"calibration_interval" category:"experimental"`
}

func (cfg *PrioritizerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.CalibrationInterval, prefix+"calibration-interval", time.Second, "The interval at which the rejection threshold is calibrated")
}

func NewPrioritizer(logger log.Logger) *Prioritizer {
	return &Prioritizer{
		Prioritizer: adaptivelimiter.NewPrioritizerBuilder().
			WithLogger(util_log.SlogFromGoKit(logger)).
			Build(),
	}
}

type Prioritizer struct {
	priority.Prioritizer
}
