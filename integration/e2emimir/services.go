// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/e2ecortex/services.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package e2emimir

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/grafana/e2e"
)

const (
	httpPort = 8080
	grpcPort = 9095
)

// GetDefaultImage returns the Docker image to use to run Mimir.
func GetDefaultImage() string {
	// Get the mimir image from the MIMIR_IMAGE env variable,
	// falling back to grafana/mimir:latest"
	if img := os.Getenv("MIMIR_IMAGE"); img != "" {
		return img
	}

	return "grafana/mimir:latest"
}

func GetDefaultEnvVariables() map[string]string {
	// Add jaeger configuration with a 50% sampling rate so that we can trigger
	// code paths that rely on a trace being sampled.
	envVars := map[string]string{
		"JAEGER_SAMPLER_TYPE":  "probabilistic",
		"JAEGER_SAMPLER_PARAM": "0.5",
	}

	str := os.Getenv("MIMIR_ENV_VARS_JSON")
	if str == "" {
		return envVars
	}
	if err := json.Unmarshal([]byte(str), &envVars); err != nil {
		panic(fmt.Errorf("can't unmarshal MIMIR_ENV_VARS_JSON as JSON, it should be a map of env var name to value: %s", err))
	}
	return envVars
}

func GetMimirtoolImage() string {
	if img := os.Getenv("MIMIRTOOL_IMAGE"); img != "" {
		return img
	}

	return "grafana/mimirtool:latest"
}

func getExtraFlags() map[string]string {
	str := os.Getenv("MIMIR_EXTRA_FLAGS")
	if str == "" {
		return nil
	}
	extraArgs := map[string]string{}
	if err := json.Unmarshal([]byte(str), &extraArgs); err != nil {
		panic(fmt.Errorf("can't unmarshal MIMIR_EXTRA_FLAGS as JSON, it should be a map of arg name to arg value: %s", err))
	}
	return extraArgs
}

func newMimirServiceFromOptions(name string, defaultFlags, flags map[string]string, options ...Option) *MimirService {
	o := newOptions(options)
	serviceFlags := o.MapFlags(e2e.MergeFlags(defaultFlags, flags, getExtraFlags()))
	binaryName := getBinaryNameForBackwardsCompatibility()

	return NewMimirService(
		name,
		o.Image,
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(serviceFlags)...),
		e2e.NewHTTPReadinessProbe(o.HTTPPort, "/ready", 200, 299),
		o.Environment,
		o.HTTPPort,
		o.GRPCPort,
		o.OtherPorts...,
	)
}

func NewDistributor(name string, consulAddress string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":                           "distributor",
			"-log.level":                        "warn",
			"-auth.multitenancy-enabled":        "true",
			"-ingester.ring.replication-factor": "1",
			"-distributor.remote-timeout":       "2s", // Fail fast in integration tests.
			// Configure the ingesters ring backend
			"-ingester.ring.store":                     "consul",
			"-ingester.ring.consul.hostname":           consulAddress,
			"-ingester.partition-ring.store":           "consul",
			"-ingester.partition-ring.consul.hostname": consulAddress,
			// Configure the distributor ring backend
			"-distributor.ring.store": "memberlist",
		},
		flags,
		options...,
	)
}

func NewQuerier(name string, consulAddress string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":                           "querier",
			"-log.level":                        "warn",
			"-ingester.ring.replication-factor": "1",
			// Ingesters ring backend.
			"-ingester.ring.store":                     "consul",
			"-ingester.ring.consul.hostname":           consulAddress,
			"-ingester.partition-ring.store":           "consul",
			"-ingester.partition-ring.consul.hostname": consulAddress,
			// Query-frontend worker.
			"-querier.frontend-client.backoff-min-period": "100ms",
			"-querier.frontend-client.backoff-max-period": "100ms",
			"-querier.frontend-client.backoff-retries":    "1",
			"-querier.max-concurrent":                     "4",
			// Quickly detect query-frontend and query-scheduler when running it.
			"-querier.dns-lookup-period": "1s",
			// Store-gateway ring backend.
			"-store-gateway.sharding-ring.store":              "consul",
			"-store-gateway.sharding-ring.consul.hostname":    consulAddress,
			"-store-gateway.sharding-ring.replication-factor": "1",
		},
		flags,
		options...,
	)
}

func NewStoreGateway(name string, consulAddress string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":    "store-gateway",
			"-log.level": "warn",
			// Store-gateway ring backend.
			"-store-gateway.sharding-ring.store":              "consul",
			"-store-gateway.sharding-ring.consul.hostname":    consulAddress,
			"-store-gateway.sharding-ring.replication-factor": "1",
		},
		flags,
		options...,
	)
}

func NewIngester(name string, consulAddress string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":                   "ingester",
			"-log.level":                "warn",
			"-ingester.ring.num-tokens": "512",
			// Configure the ingesters ring backend
			"-ingester.ring.store":                     "consul",
			"-ingester.ring.consul.hostname":           consulAddress,
			"-ingester.partition-ring.store":           "consul",
			"-ingester.partition-ring.consul.hostname": consulAddress,
			// Speed up the startup.
			"-ingester.ring.min-ready-duration": "0s",
			// Enable native histograms
			"-ingester.native-histograms-ingestion-enabled": "true",
		},
		flags,
		options...,
	)
}

func getBinaryNameForBackwardsCompatibility() string {
	return "mimir"
}

// NewQueryFrontend returns a new query-frontend instance. The consulAddress is required to be a valid
// consul address when Mimir is configured with ingest storage enabled, otherwise it's ignored.
func NewQueryFrontend(name string, consulAddress string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":    "query-frontend",
			"-log.level": "warn",
			// Quickly detect query-scheduler when running it.
			"-query-frontend.scheduler-dns-lookup-period": "1s",
			// Configure the partitions ring backend.
			"-ingester.partition-ring.store":           "consul",
			"-ingester.partition-ring.consul.hostname": consulAddress,
		},
		flags,
		options...,
	)
}

func NewQueryScheduler(name string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":    "query-scheduler",
			"-log.level": "warn",
		},
		flags,
		options...,
	)
}

func NewCompactor(name string, consulAddress string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":    "compactor",
			"-log.level": "warn",
			// Store-gateway ring backend.
			"-compactor.ring.store":           "consul",
			"-compactor.ring.consul.hostname": consulAddress,
			// Startup quickly.
			"-compactor.ring.wait-stability-min-duration":   "0",
			"-compactor.ring.wait-stability-max-duration":   "0",
			"-compactor.first-level-compaction-wait-period": "0s",
		},
		flags,
		options...,
	)
}

func NewSingleBinary(name string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			// Do not pass any extra default flags (except few used to speed up the test)
			// because the config could be driven by the config file.
			"-target":    "all",
			"-log.level": "warn",
			// Speed up the startup.
			"-ingester.ring.min-ready-duration": "0s",
			// Enable native histograms
			"-ingester.native-histograms-ingestion-enabled": "true",
		},
		flags,
		options...,
	)
}

func NewReadInstance(name string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":                           "read",
			"-log.level":                        "warn",
			"-ingester.ring.replication-factor": "1",
		},
		flags,
		options...,
	)
}

func NewWriteInstance(name string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":                           "write",
			"-log.level":                        "warn",
			"-ingester.ring.replication-factor": "1",
			// Speed up startup.
			"-ingester.ring.min-ready-duration": "0s",
			// Enable native histograms
			"-ingester.native-histograms-ingestion-enabled": "true",
		},
		flags,
		options...,
	)
}

func NewBackendInstance(name string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":                           "backend",
			"-log.level":                        "warn",
			"-ingester.ring.replication-factor": "1",
		},
		flags,
		options...,
	)
}

func NewAlertmanager(name string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":    "alertmanager",
			"-log.level": "warn",
		},
		flags,
		options...,
	)
}

func NewAlertmanagerWithTLS(name string, flags map[string]string, options ...Option) *MimirService {
	o := newOptions(options)
	serviceFlags := o.MapFlags(e2e.MergeFlags(map[string]string{
		"-target":    "alertmanager",
		"-log.level": "warn",
	}, flags, getExtraFlags()))
	binaryName := getBinaryNameForBackwardsCompatibility()

	return NewMimirService(
		name,
		o.Image,
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(serviceFlags)...),
		e2e.NewTCPReadinessProbe(o.HTTPPort),
		o.Environment,
		o.HTTPPort,
		o.GRPCPort,
		o.OtherPorts...,
	)
}

func NewRuler(name string, consulAddress string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":    "ruler",
			"-log.level": "warn",
			// Configure the ingesters ring backend
			"-ingester.ring.store":                     "consul",
			"-ingester.ring.consul.hostname":           consulAddress,
			"-ingester.partition-ring.store":           "consul",
			"-ingester.partition-ring.consul.hostname": consulAddress,
		},
		flags,
		options...,
	)
}

func NewOverridesExporter(name string, consulAddress string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":    "overrides-exporter",
			"-log.level": "warn",
			// overrides-exporter ring backend.
			"-overrides-exporter.ring.store":           "consul",
			"-overrides-exporter.ring.consul.hostname": consulAddress,
		}, flags, options...)
}

// Options holds a set of options for running services, they can be altered passing Option funcs.
type Options struct {
	Image       string
	MapFlags    FlagMapper
	OtherPorts  []int
	HTTPPort    int
	GRPCPort    int
	Environment map[string]string
}

// Option modifies options.
type Option func(*Options)

// newOptions creates an Options with default values and applies the options provided.
func newOptions(options []Option) *Options {
	o := &Options{
		Image:       GetDefaultImage(),
		MapFlags:    NoopFlagMapper,
		HTTPPort:    httpPort,
		GRPCPort:    grpcPort,
		Environment: GetDefaultEnvVariables(),
	}
	for _, opt := range options {
		opt(o)
	}
	return o
}

// WithFlagMapper creates an option that sets a FlagMapper.
// Multiple flag mappers can be set, each one maps the output of the previous one.
// Flag mappers added using this option act on a copy of the flags, so they don't alter the input.
func WithFlagMapper(mapFlags FlagMapper) Option {
	return func(options *Options) {
		options.MapFlags = ChainFlagMappers(options.MapFlags, mapFlags)
	}
}

// WithImage creates an option that overrides the image of the service.
func WithImage(image string) Option {
	return func(options *Options) {
		options.Image = image
	}
}

// WithPorts creats an option that overrides the HTTP and gRPC ports.
func WithPorts(http, grpc int) Option {
	return func(options *Options) {
		options.HTTPPort = http
		options.GRPCPort = grpc
	}
}

// WithOtherPorts creates an option that adds other ports to the service.
func WithOtherPorts(ports ...int) Option {
	return func(options *Options) {
		options.OtherPorts = ports
	}
}

// WithConfigFile returns an option that sets the config file flag if the provided string is not empty.
func WithConfigFile(configFile string) Option {
	if configFile == "" {
		return WithNoopOption()
	}
	return WithFlagMapper(SetFlagMapper(map[string]string{
		"-config.file": filepath.Join(e2e.ContainerSharedDir, configFile),
	}))
}

// WithNoopOption returns an option that doesn't change anything.
func WithNoopOption() Option { return func(*Options) {} }

// FlagMapper is the type of function that maps flags, just to reduce some verbosity.
type FlagMapper func(flags map[string]string) map[string]string

// UnmarshalJSON unmarshals a single json object into a single FlagMapper, or an array into a ChainMapper.
func (fm *FlagMapper) UnmarshalJSON(data []byte) error {
	var val struct {
		Rename map[string]string `json:"rename"`
		Remove []string          `json:"remove"`
		Set    map[string]string `json:"set"`
	}
	if err := json.Unmarshal(data, &val); err != nil {
		// It's not an object, try to unmarshal it as an array, and build a chain mapper.
		var chain []FlagMapper
		if chainErr := json.Unmarshal(data, &chain); chainErr == nil {
			*fm = ChainFlagMappers(chain...)
			return nil
		}
		return err
	}
	set := 0
	var m FlagMapper
	if len(val.Rename) > 0 {
		m = RenameFlagMapper(val.Rename)
		set++
	}
	if len(val.Remove) > 0 {
		m = RemoveFlagMapper(val.Remove)
		set++
	}
	if len(val.Set) > 0 {
		m = SetFlagMapper(val.Set)
		set++
	}
	if set != 1 {
		return fmt.Errorf("should set exactly one flag mapper, but %v set %d", val, set)
	}
	*fm = m
	return nil
}

// NoopFlagMapper is a flag mapper that does not alter the provided flags.
func NoopFlagMapper(flags map[string]string) map[string]string { return flags }

// ChainFlagMappers chains multiple flag mappers, each flag mapper gets a copy of the flags so it can safely modify the values.
// ChainFlagMappers(a, b)(flags) == b(copy(a(copy(flags)
func ChainFlagMappers(mappers ...FlagMapper) FlagMapper {
	return func(flags map[string]string) map[string]string {
		for _, mapFlags := range mappers {
			flags = mapFlags(copyFlags(flags))
		}
		return copyFlags(flags)
	}
}

// RenameFlagMapper builds a flag mapper that renames flags.
func RenameFlagMapper(fromTo map[string]string) FlagMapper {
	return func(flags map[string]string) map[string]string {
		for name, renamed := range fromTo {
			if v, ok := flags[name]; ok {
				flags[renamed] = v
				delete(flags, name)
			}
		}
		return flags
	}
}

// RemoveFlagMapper builds a flag mapper that remove flags.
func RemoveFlagMapper(toRemove []string) FlagMapper {
	return func(flags map[string]string) map[string]string {
		for _, name := range toRemove {
			delete(flags, name)
		}
		return flags
	}
}

// SetFlagMapper builds a flag mapper that sets the provided flags.
func SetFlagMapper(set map[string]string) FlagMapper {
	return func(flags map[string]string) map[string]string {
		for f, v := range set {
			flags[f] = v
		}
		return flags
	}
}

// copyFlags provides a copy of the flags map provided.
func copyFlags(flags map[string]string) map[string]string {
	cp := make(map[string]string)
	for f, v := range flags {
		cp[f] = v
	}
	return cp
}
