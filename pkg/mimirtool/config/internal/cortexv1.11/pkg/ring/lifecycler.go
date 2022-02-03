// SPDX-License-Identifier: AGPL-3.0-only

package ring

import (
	"flag"
	"os"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/log"
)

type LifecyclerConfig struct {
	RingConfig Config `yaml:"ring"`

	// Config for the ingester lifecycle control
	NumTokens            int           `yaml:"num_tokens"`
	HeartbeatPeriod      time.Duration `yaml:"heartbeat_period"`
	ObservePeriod        time.Duration `yaml:"observe_period"`
	JoinAfter            time.Duration `yaml:"join_after"`
	MinReadyDuration     time.Duration `yaml:"min_ready_duration"`
	InfNames             []string      `yaml:"interface_names"`
	FinalSleep           time.Duration `yaml:"final_sleep"`
	TokensFilePath       string        `yaml:"tokens_file_path"`
	Zone                 string        `yaml:"availability_zone"`
	UnregisterOnShutdown bool          `yaml:"unregister_on_shutdown"`

	// For testing, you can override the address and ID of this ingester
	Addr string `yaml:"address" doc:"hidden"`
	Port int    `doc:"hidden"`
	ID   string `doc:"hidden"`

	// Injected internally
}

func (cfg *LifecyclerConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}
func (cfg *LifecyclerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.RingConfig.RegisterFlagsWithPrefix(prefix, f)

	// In order to keep backwards compatibility all of these need to be prefixed
	// with "ingester."
	if prefix == "" {
		prefix = "ingester."
	}

	f.IntVar(&cfg.NumTokens, prefix+"num-tokens", 128, "Number of tokens for each ingester.")
	f.DurationVar(&cfg.HeartbeatPeriod, prefix+"heartbeat-period", 5*time.Second, "Period at which to heartbeat to consul. 0 = disabled.")
	f.DurationVar(&cfg.JoinAfter, prefix+"join-after", 0*time.Second, "Period to wait for a claim from another member; will join automatically after this.")
	f.DurationVar(&cfg.ObservePeriod, prefix+"observe-period", 0*time.Second, "Observe tokens after generating to resolve collisions. Useful when using gossiping ring.")
	f.DurationVar(&cfg.MinReadyDuration, prefix+"min-ready-duration", 1*time.Minute, "Minimum duration to wait before becoming ready. This is to work around race conditions with ingesters exiting and updating the ring.")
	f.DurationVar(&cfg.FinalSleep, prefix+"final-sleep", 30*time.Second, "Duration to sleep for before exiting, to ensure metrics are scraped.")
	f.StringVar(&cfg.TokensFilePath, prefix+"tokens-file-path", "", "File path where tokens are stored. If empty, tokens are not stored at shutdown and restored at startup.")

	hostname, err := os.Hostname()
	if err != nil {
		level.Error(log.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	cfg.InfNames = []string{"eth0", "en0"}
	f.Var((*flagext.StringSlice)(&cfg.InfNames), prefix+"lifecycler.interface", "Name of network interface to read address from.")
	f.StringVar(&cfg.Addr, prefix+"lifecycler.addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.Port, prefix+"lifecycler.port", 0, "port to advertise in consul (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.ID, prefix+"lifecycler.ID", hostname, "ID to register in the ring.")
	f.StringVar(&cfg.Zone, prefix+"availability-zone", "", "The availability zone where this instance is running.")
	f.BoolVar(&cfg.UnregisterOnShutdown, prefix+"unregister-on-shutdown", true, "Unregister from the ring upon clean shutdown. It can be useful to disable for rolling restarts with consistent naming in conjunction with -distributor.extend-writes=false.")
}
