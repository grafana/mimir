// SPDX-License-Identifier: AGPL-3.0-only

package distributor

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

	// Instance details
	InstanceID             string   `yaml:"instance_id" doc:"hidden"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names"`
	InstancePort           int      `yaml:"instance_port" doc:"hidden"`
	InstanceAddr           string   `yaml:"instance_addr" doc:"hidden"`

	// Injected internally
}

func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	// Ring flags
	cfg.KVStore.RegisterFlagsWithPrefix("distributor.ring.", "collectors/", f)
	f.DurationVar(&cfg.HeartbeatPeriod, "distributor.ring.heartbeat-period", 5*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&cfg.HeartbeatTimeout, "distributor.ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which distributors are considered unhealthy within the ring. 0 = never (timeout disabled).")

	// Instance flags
	cfg.InstanceInterfaceNames = []string{"eth0", "en0"}
	f.Var((*flagext.StringSlice)(&cfg.InstanceInterfaceNames), "distributor.ring.instance-interface-names", "Name of network interface to read address from.")
	f.StringVar(&cfg.InstanceAddr, "distributor.ring.instance-addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.InstancePort, "distributor.ring.instance-port", 0, "Port to advertise in the ring (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, "distributor.ring.instance-id", hostname, "Instance ID to register in the ring.")
}
