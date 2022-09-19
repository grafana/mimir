// SPDX-License-Identifier: AGPL-3.0-only

package schedulerdiscovery

import (
	"flag"
	"fmt"
	"strings"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/util"
)

const (
	ModeFlagName = "query-scheduler.service-discovery-mode"

	ModeDNS  = "dns"
	ModeRing = "ring"
)

var (
	modes = []string{ModeDNS, ModeRing}
)

type Config struct {
	Mode          string     `yaml:"service_discovery_mode" category:"experimental"`
	SchedulerRing RingConfig `yaml:"ring" doc:"description=The hash ring configuration. The query-schedulers hash ring is used for service discovery."`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.StringVar(&cfg.Mode, ModeFlagName, ModeDNS, fmt.Sprintf("Service discovery mode that query-frontends and queriers use to find query-scheduler instances.%s Supported values are: %s.", sharedOptionWithRingClient, strings.Join(modes, ", ")))
	cfg.SchedulerRing.RegisterFlags(f, logger)
}

func (cfg *Config) Validate() error {
	if !util.StringsContain(modes, cfg.Mode) {
		return fmt.Errorf("unsupported query-scheduler service discovery mode (supported values are: %s)", strings.Join(modes, ", "))
	}

	return nil
}
