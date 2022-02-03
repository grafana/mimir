// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"flag"
	"os"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"

	util_log "github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/log"
)

type RingConfig struct {
	KVStore          kv.Config     `yaml:"kvstore"`
	HeartbeatPeriod  time.Duration `yaml:"heartbeat_period"`
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout"`

	// Wait ring stability.
	WaitStabilityMinDuration time.Duration `yaml:"wait_stability_min_duration"`
	WaitStabilityMaxDuration time.Duration `yaml:"wait_stability_max_duration"`

	// Instance details
	InstanceID             string   `yaml:"instance_id" doc:"hidden"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names"`
	InstancePort           int      `yaml:"instance_port" doc:"hidden"`
	InstanceAddr           string   `yaml:"instance_addr" doc:"hidden"`

	// Injected internally

	WaitActiveInstanceTimeout time.Duration `yaml:"wait_active_instance_timeout"`
}

func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	// Ring flags
	cfg.KVStore.RegisterFlagsWithPrefix("compactor.ring.", "collectors/", f)
	f.DurationVar(&cfg.HeartbeatPeriod, "compactor.ring.heartbeat-period", 5*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&cfg.HeartbeatTimeout, "compactor.ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which compactors are considered unhealthy within the ring. 0 = never (timeout disabled).")

	// Wait stability flags.
	f.DurationVar(&cfg.WaitStabilityMinDuration, "compactor.ring.wait-stability-min-duration", time.Minute, "Minimum time to wait for ring stability at startup. 0 to disable.")
	f.DurationVar(&cfg.WaitStabilityMaxDuration, "compactor.ring.wait-stability-max-duration", 5*time.Minute, "Maximum time to wait for ring stability at startup. If the compactor ring keeps changing after this period of time, the compactor will start anyway.")

	// Instance flags
	cfg.InstanceInterfaceNames = []string{"eth0", "en0"}
	f.Var((*flagext.StringSlice)(&cfg.InstanceInterfaceNames), "compactor.ring.instance-interface-names", "Name of network interface to read address from.")
	f.StringVar(&cfg.InstanceAddr, "compactor.ring.instance-addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.InstancePort, "compactor.ring.instance-port", 0, "Port to advertise in the ring (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, "compactor.ring.instance-id", hostname, "Instance ID to register in the ring.")

	// Timeout durations
	f.DurationVar(&cfg.WaitActiveInstanceTimeout, "compactor.ring.wait-active-instance-timeout", 10*time.Minute, "Timeout for waiting on compactor to become ACTIVE in the ring.")
}
