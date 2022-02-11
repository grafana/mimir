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
	// falling back to  "us.gcr.io/kubernetes-dev/mimir:latest"
	if os.Getenv("MIMIR_IMAGE") != "" {
		return os.Getenv("MIMIR_IMAGE")
	}

	return "us.gcr.io/kubernetes-dev/mimir:latest"
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

func NewDistributor(name string, consulAddress string, flags map[string]string, image string, options ...Option) *MimirService {
	return NewDistributorWithConfigFile(name, consulAddress, "", flags, image, options...)
}

func NewDistributorWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string, options ...Option) *MimirService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	defaultFlags := map[string]string{
		"-target":                         "distributor",
		"-log.level":                      "warn",
		"-auth.multitenancy-enabled":      "true",
		"-distributor.replication-factor": "1",
		"-distributor.remote-timeout":     "2s", // Fail fast in integration tests.
		// Configure the ingesters ring backend
		"-ring.store":      "consul",
		"-consul.hostname": consulAddress,
		// Configure the distributor ring backend
		"-distributor.ring.store": "memberlist",
	}

	o := newOptions(options)
	serviceFlags := o.MapFlags(e2e.MergeFlags(defaultFlags, flags))

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(serviceFlags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}
func NewQuerier(name string, consulAddress string, flags map[string]string, image string, options ...Option) *MimirService {
	return NewQuerierWithConfigFile(name, consulAddress, "", flags, image, options...)
}

func NewQuerierWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string, options ...Option) *MimirService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	defaultFlags := map[string]string{
		"-target":                         "querier",
		"-log.level":                      "warn",
		"-distributor.replication-factor": "1",
		// Ingesters ring backend.
		"-ring.store":      "consul",
		"-consul.hostname": consulAddress,
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
	}

	o := newOptions(options)
	serviceFlags := o.MapFlags(e2e.MergeFlags(defaultFlags, flags))

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(serviceFlags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewStoreGateway(name string, consulAddress string, flags map[string]string, image string) *MimirService {
	return NewStoreGatewayWithConfigFile(name, consulAddress, "", flags, image)
}

func NewStoreGatewayWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string) *MimirService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	defaultFlags := map[string]string{
		"-target":    "store-gateway",
		"-log.level": "warn",
		// Store-gateway ring backend.
		"-store-gateway.sharding-ring.store":              "consul",
		"-store-gateway.sharding-ring.consul.hostname":    consulAddress,
		"-store-gateway.sharding-ring.replication-factor": "1",
	}

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(e2e.MergeFlags(defaultFlags, flags)))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewIngester(name string, consulAddress string, flags map[string]string, image string, options ...Option) *MimirService {
	return NewIngesterWithConfigFile(name, consulAddress, "", flags, image, options...)
}

func NewIngesterWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string, options ...Option) *MimirService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}
	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	defaultFlags := map[string]string{
		"-target":                      "ingester",
		"-log.level":                   "warn",
		"-ingester.final-sleep":        "0s",
		"-ingester.join-after":         "0s",
		"-ingester.min-ready-duration": "0s",
		"-ingester.num-tokens":         "512",
		// Configure the ingesters ring backend
		"-ring.store":      "consul",
		"-consul.hostname": consulAddress,
	}

	o := newOptions(options)
	serviceFlags := o.MapFlags(e2e.MergeFlags(defaultFlags, flags))

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(serviceFlags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func getBinaryNameForBackwardsCompatibility(image string) string {
	if strings.Contains(image, "quay.io/cortexproject/cortex") {
		return "cortex"
	}
	return "mimir"
}

func NewQueryFrontend(name string, flags map[string]string, image string, options ...Option) *MimirService {
	return NewQueryFrontendWithConfigFile(name, "", flags, image, options...)
}

func NewQueryFrontendWithConfigFile(name, configFile string, flags map[string]string, image string, options ...Option) *MimirService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	defaultFlags := map[string]string{
		"-target":    "query-frontend",
		"-log.level": "warn",
		// Quickly detect query-scheduler when running it.
		"-frontend.scheduler-dns-lookup-period": "1s",
	}

	o := newOptions(options)
	serviceFlags := o.MapFlags(e2e.MergeFlags(defaultFlags, flags))

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(serviceFlags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewQueryScheduler(name string, flags map[string]string, image string) *MimirService {
	return NewQuerySchedulerWithConfigFile(name, "", flags, image)
}

func NewQuerySchedulerWithConfigFile(name, configFile string, flags map[string]string, image string) *MimirService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "query-scheduler",
			"-log.level": "warn",
		}, flags)))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewCompactor(name string, consulAddress string, flags map[string]string, image string) *MimirService {
	return NewCompactorWithConfigFile(name, consulAddress, "", flags, image)
}

func NewCompactorWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string) *MimirService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "compactor",
			"-log.level": "warn",
			// Store-gateway ring backend.
			"-compactor.ring.store":           "consul",
			"-compactor.ring.consul.hostname": consulAddress,
			// Startup quickly.
			"-compactor.ring.wait-stability-min-duration": "0",
			"-compactor.ring.wait-stability-max-duration": "0",
		}, flags)))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewSingleBinary(name string, flags map[string]string, image string, otherPorts ...int) *MimirService {
	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	defaultFlags := map[string]string{
		"-target":                    "all",
		"-log.level":                 "warn",
		"-auth.multitenancy-enabled": "true",
		// Query-frontend worker.
		"-querier.frontend-client.backoff-min-period": "100ms",
		"-querier.frontend-client.backoff-max-period": "100ms",
		"-querier.frontend-client.backoff-retries":    "1",
		"-querier.max-concurrent":                     "1",
		// Distributor.
		"-distributor.replication-factor": "1",
		"-distributor.ring.store":         "memberlist",
		// Ingester.
		"-ingester.final-sleep":        "0s",
		"-ingester.join-after":         "0s",
		"-ingester.min-ready-duration": "0s",
		"-ingester.num-tokens":         "512",
	}

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(e2e.MergeFlags(defaultFlags, flags)))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
		otherPorts...,
	)
}

func NewSingleBinaryWithConfigFile(name string, configFile string, flags map[string]string, image string, httpPort, grpcPort int, otherPorts ...int) *MimirService {
	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	defaultFlags := map[string]string{
		// Do not pass any extra default flags because the config should be drive by the config file.
		"-target":      "all",
		"-log.level":   "warn",
		"-config.file": filepath.Join(e2e.ContainerSharedDir, configFile),
	}

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(e2e.MergeFlags(defaultFlags, flags)))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
		otherPorts...,
	)
}

func NewAlertmanager(name string, flags map[string]string, image string) *MimirService {
	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "alertmanager",
			"-log.level": "warn",
		}, flags)))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewAlertmanagerWithTLS(name string, flags map[string]string, image string) *MimirService {
	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "alertmanager",
			"-log.level": "warn",
		}, flags)))...),
		e2e.NewTCPReadinessProbe(httpPort),
		httpPort,
		grpcPort,
	)
}

func NewRuler(name string, consulAddress string, flags map[string]string, image string) *MimirService {
	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	defaultFlags := map[string]string{
		"-target":    "ruler",
		"-log.level": "warn",
		// Configure the ingesters ring backend
		"-ring.store":      "consul",
		"-consul.hostname": consulAddress,
	}

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(e2e.MergeFlags(defaultFlags, flags)))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewPurger(name string, flags map[string]string, image string) *MimirService {
	return NewPurgerWithConfigFile(name, "", flags, image)
}

func NewPurgerWithConfigFile(name, configFile string, flags map[string]string, image string) *MimirService {
	if configFile != "" {
		flags["-config.file"] = filepath.Join(e2e.ContainerSharedDir, configFile)
	}

	if image == "" {
		image = GetDefaultImage()
	}
	binaryName := getBinaryNameForBackwardsCompatibility(image)

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, buildArgsWithExtra(e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                   "purger",
			"-log.level":                "warn",
			"-purger.object-store-type": "filesystem",
			"-local.chunk-directory":    e2e.ContainerSharedDir,
		}, flags)))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

// Options holds a set of options for running services, they can be altered passing Option funcs.
type Options struct {
	MapFlags FlagMapper
}

// Option modifies options.
type Option func(*Options)

// newOptions creates an Options with default values and applies the options provided.
func newOptions(options []Option) *Options {
	o := &Options{
		MapFlags: NoopFlagMapper,
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
