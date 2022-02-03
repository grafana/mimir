// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
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
	NumTokens              int      `yaml:"num_tokens"`

	// Injected internally

	// Used for testing
}

const (
	// If a ruler is unable to heartbeat the ring, its better to quickly remove it and resume
	// the evaluation of all rules since the worst case scenario is that some rulers will
	// receive duplicate/out-of-order sample errors.
	ringAutoForgetUnhealthyPeriods = 2
)

func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet) {
	hostname, err := os.Hostname()
	if err != nil {
		panic(fmt.Errorf("failed to get hostname, %w", err))
	}

	// Ring flags
	cfg.KVStore.RegisterFlagsWithPrefix("ruler.ring.", "rulers/", f)
	f.DurationVar(&cfg.HeartbeatPeriod, "ruler.ring.heartbeat-period", 5*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&cfg.HeartbeatTimeout, "ruler.ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which rulers are considered unhealthy within the ring. 0 = never (timeout disabled).")

	// Instance flags
	cfg.InstanceInterfaceNames = []string{"eth0", "en0"}
	f.Var((*flagext.StringSlice)(&cfg.InstanceInterfaceNames), "ruler.ring.instance-interface-names", "Name of network interface to read address from.")
	f.StringVar(&cfg.InstanceAddr, "ruler.ring.instance-addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.InstancePort, "ruler.ring.instance-port", 0, "Port to advertise in the ring (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, "ruler.ring.instance-id", hostname, "Instance ID to register in the ring.")
	f.IntVar(&cfg.NumTokens, "ruler.ring.num-tokens", 128, "Number of tokens for each ruler.")
}
