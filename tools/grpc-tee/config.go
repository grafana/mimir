// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"strconv"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/server"
)

type Config struct {
	Server       server.Config       `yaml:"server"`
	MemberlistKV memberlist.KVConfig `yaml:"memberlist"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet, _ log.Logger) {
	c.Server.MetricsNamespace = "cortex"
	c.Server.ExcludeRequestInLog = true
	c.Server.DisableRequestSuccessLog = true

	c.registerServerFlagsWithChangedDefaultValues(f)
	c.MemberlistKV.RegisterFlags(f)
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
