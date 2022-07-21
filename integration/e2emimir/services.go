// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/e2ecortex/services.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package e2emimir

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/grafana/e2e"
)

const (
	httpPort   = 8080
	grpcPort   = 9095
	GossipPort = 9094
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

func GetMimirtoolImage() string {
	if img := os.Getenv("MIMIRTOOL_IMAGE"); img != "" {
		return img
	}

	return "grafana/mimirtool:latest"
}

// GetExtraArgs returns the extra args to pass to the Docker command used to run Mimir.
func GetExtraArgs() []string {
	// Get extra args from the MIMIR_EXTRA_ARGS env variable
	// falling back to an empty list
	if os.Getenv("MIMIR_EXTRA_ARGS") != "" {
		return strings.Fields(os.Getenv("MIMIR_EXTRA_ARGS"))
	}

	return nil
}

func buildArgsWithExtra(args []string) []string {
	extraArgs := GetExtraArgs()
	if len(extraArgs) > 0 {
		return append(extraArgs, args...)
	}

	return args
}

func newMimirServiceFromOptions(name string, defaultFlags, flags map[string]string, options ...Option) *MimirService {
	o := newOptions(options)
	serviceFlags := o.MapFlags(e2e.MergeFlags(defaultFlags, flags))
	binaryName := getBinaryNameForBackwardsCompatibility(o.Image)

	return NewMimirService(
		name,
		o.Image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(serviceFlags))...),
		e2e.NewHTTPReadinessProbe(o.HTTPPort, "/ready", 200, 299),
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
			"-ingester.ring.store":           "consul",
			"-ingester.ring.consul.hostname": consulAddress,
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
			"-ingester.ring.store":           "consul",
			"-ingester.ring.consul.hostname": consulAddress,
			// Query-frontend worker.
			"-querier.frontend-client.backoff-min-period": "100ms",
			"-querier.frontend-client.backoff-max-period": "100ms",
			"-querier.frontend-client.backoff-retries":    "1",
			"-querier.max-concurrent":                     "1",
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
			"-ingester.ring.store":           "consul",
			"-ingester.ring.consul.hostname": consulAddress,
			// Speed up the startup.
			"-ingester.ring.min-ready-duration":          "0s",
			"-ingester.ring.readiness-check-ring-health": "false",
		},
		flags,
		options...,
	)
}

func getBinaryNameForBackwardsCompatibility(image string) string {
	return "mimir"
}

func NewQueryFrontend(name string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":    "query-frontend",
			"-log.level": "warn",
			// Quickly detect query-scheduler when running it.
			"-query-frontend.scheduler-dns-lookup-period": "1s",
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
			"-compactor.ring.wait-stability-min-duration": "0",
			"-compactor.ring.wait-stability-max-duration": "0",
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
			"-ingester.ring.min-ready-duration":          "0s",
			"-ingester.ring.readiness-check-ring-health": "false",
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
	}, flags))
	binaryName := getBinaryNameForBackwardsCompatibility(o.Image)

	return NewMimirService(
		name,
		o.Image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(serviceFlags))...),
		e2e.NewTCPReadinessProbe(o.HTTPPort),
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
			"-ingester.ring.store":           "consul",
			"-ingester.ring.consul.hostname": consulAddress,
		},
		flags,
		options...,
	)
}

func NewPurger(name string, flags map[string]string, options ...Option) *MimirService {
	return newMimirServiceFromOptions(
		name,
		map[string]string{
			"-target":                   "purger",
			"-log.level":                "warn",
			"-purger.object-store-type": "filesystem",
			"-local.chunk-directory":    e2e.ContainerSharedDir,
		},
		flags,
		options...,
	)
}

// Options holds a set of options for running services, they can be altered passing Option funcs.
type Options struct {
	Image      string
	MapFlags   FlagMapper
	OtherPorts []int
	HTTPPort   int
	GRPCPort   int
}

// Option modifies options.
type Option func(*Options)

// newOptions creates an Options with default values and applies the options provided.
func newOptions(options []Option) *Options {
	o := &Options{
		Image:    GetDefaultImage(),
		MapFlags: NoopFlagMapper,
		HTTPPort: httpPort,
		GRPCPort: grpcPort,
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
func WithNoopOption() Option { return func(options *Options) {} }

// FlagMapper is the type of function that maps flags, just to reduce some verbosity.
type FlagMapper func(flags map[string]string) map[string]string

// NoopFlagMapper is a flag mapper that does not alter the provided flags.
func NoopFlagMapper(flags map[string]string) map[string]string { return flags }

// ChainFlagMappers chains multiple flag mappers, each flag mapper gets a copy of the flags so it can safely modify the values.
// ChainFlagMappers(a, b)(flags) == b(copy(a(copy(flags)
func ChainFlagMappers(mappers ...FlagMapper) FlagMapper {
	return func(flags map[string]string) map[string]string {
		for _, mapFlags := range mappers {
			flags = mapFlags(copyFlags(flags))
		}
		return flags
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
