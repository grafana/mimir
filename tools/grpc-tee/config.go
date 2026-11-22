// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"flag"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/server"
)

type TeeConfig struct {
	SecondaryDrainTimeout      time.Duration `yaml:"secondary_drain_timeout"`
	SecondaryRequestBufferSize int           `yaml:"secondary_request_buffer_size"`
}

func (c *TeeConfig) RegisterFlags(f *flag.FlagSet, _ log.Logger) {
	f.DurationVar(&c.SecondaryDrainTimeout, "tee.secondary-drain-timeout", 30*time.Second, "How long the tee waits for the secondary backend to finish after the primary backend finished. After this time, the tee cancels the secondary call.")
	f.IntVar(&c.SecondaryRequestBufferSize, "tee.secondary-request-buffer-size", 16, "Maximum number of request frames that wait to be sent to the secondary backend. If the queue is full, the tee cancels the secondary call. The primary call is not affected.")
}

type Config struct {
	Server       server.Config       `yaml:"server"`
	MemberlistKV memberlist.KVConfig `yaml:"memberlist"`
	Tee          TeeConfig           `yaml:"tee"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	c.Server.MetricsNamespace = "cortex"
	c.Server.ExcludeRequestInLog = true
	c.Server.DisableRequestSuccessLog = true

	c.registerServerFlagsWithChangedDefaultValues(f)
	c.MemberlistKV.RegisterFlags(f)
	c.Tee.RegisterFlags(f, logger)
}

func (c *Config) registerServerFlagsWithChangedDefaultValues(fs *flag.FlagSet) {
	throwaway := flag.NewFlagSet("throwaway", flag.PanicOnError)

	// Register to throwaway flags first.
	// Default values are remembered during registration and cannot be changed,
	// but we can take values from throwaway flag set
	// and re-register into supplied flag set with new default values.
	c.Server.RegisterFlags(throwaway)

	defaultsOverrides := map[string]string{
		"server.grpc-max-recv-msg-size-bytes":                       strconv.Itoa(100 * 1024 * 1024),
		"server.grpc-max-send-msg-size-bytes":                       strconv.Itoa(100 * 1024 * 1024),
		"server.grpc.keepalive.min-time-between-pings":              "10s",
		"server.grpc.keepalive.ping-without-stream-allowed":         "true",
		"server.grpc.num-workers":                                   "100",
		"server.http-listen-port":                                   "8080",
		"server.http-write-timeout":                                 "2m",
		"server.report-grpc-codes-in-instrumentation-label-enabled": "true",
	}

	throwaway.VisitAll(func(f *flag.Flag) {
		if defaultValue, overridden := defaultsOverrides[f.Name]; overridden {
			_ = f.Value.Set(defaultValue)
		}

		fs.Var(f.Value, f.Name, f.Usage)
	})
}
