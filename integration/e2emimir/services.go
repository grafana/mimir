// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/e2ecortex/services.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package e2emimir

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/grafana/mimir/integration/e2e"
)

const (
	httpPort   = 80
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

func NewDistributor(name string, consulAddress string, flags map[string]string, image string) *MimirService {
	return NewDistributorWithConfigFile(name, consulAddress, "", flags, image)
}

func NewDistributorWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string) *MimirService {
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
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                         "distributor",
			"-log.level":                      "warn",
			"-auth.enabled":                   "true",
			"-distributor.replication-factor": "1",
			// Configure the ingesters ring backend
			"-ring.store":      "consul",
			"-consul.hostname": consulAddress,
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewQuerier(name string, consulAddress string, flags map[string]string, image string) *MimirService {
	return NewQuerierWithConfigFile(name, consulAddress, "", flags, image)
}

func NewQuerierWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string) *MimirService {
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
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
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
			"-querier.worker-parallelism":                 "1",
			// Quickly detect query-frontend and query-scheduler when running it.
			"-querier.dns-lookup-period": "1s",
			// Store-gateway ring backend.
			"-store-gateway.sharding-enabled":                 "true",
			"-store-gateway.sharding-ring.store":              "consul",
			"-store-gateway.sharding-ring.consul.hostname":    consulAddress,
			"-store-gateway.sharding-ring.replication-factor": "1",
		}, flags))...),
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

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "store-gateway",
			"-log.level": "warn",
			// Store-gateway ring backend.
			"-store-gateway.sharding-enabled":                 "true",
			"-store-gateway.sharding-ring.store":              "consul",
			"-store-gateway.sharding-ring.consul.hostname":    consulAddress,
			"-store-gateway.sharding-ring.replication-factor": "1",
			// Startup quickly.
			"-store-gateway.sharding-ring.wait-stability-min-duration": "0",
			"-store-gateway.sharding-ring.wait-stability-max-duration": "0",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewIngester(name string, consulAddress string, flags map[string]string, image string) *MimirService {
	return NewIngesterWithConfigFile(name, consulAddress, "", flags, image)
}

func NewIngesterWithConfigFile(name, consulAddress, configFile string, flags map[string]string, image string) *MimirService {
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
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                      "ingester",
			"-log.level":                   "warn",
			"-ingester.final-sleep":        "0s",
			"-ingester.join-after":         "0s",
			"-ingester.min-ready-duration": "0s",
			"-ingester.num-tokens":         "512",
			// Configure the ingesters ring backend
			"-ring.store":      "consul",
			"-consul.hostname": consulAddress,
		}, flags))...),
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

func NewTableManager(name string, flags map[string]string, image string) *MimirService {
	return NewTableManagerWithConfigFile(name, "", flags, image)
}

func NewTableManagerWithConfigFile(name, configFile string, flags map[string]string, image string) *MimirService {
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
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "table-manager",
			"-log.level": "warn",
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}

func NewQueryFrontend(name string, flags map[string]string, image string) *MimirService {
	return NewQueryFrontendWithConfigFile(name, "", flags, image)
}

func NewQueryFrontendWithConfigFile(name, configFile string, flags map[string]string, image string) *MimirService {
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
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "query-frontend",
			"-log.level": "warn",
			// Quickly detect query-scheduler when running it.
			"-frontend.scheduler-dns-lookup-period": "1s",
		}, flags))...),
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
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "query-scheduler",
			"-log.level": "warn",
		}, flags))...),
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
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "compactor",
			"-log.level": "warn",
			// Store-gateway ring backend.
			"-compactor.sharding-enabled":     "true",
			"-compactor.ring.store":           "consul",
			"-compactor.ring.consul.hostname": consulAddress,
			// Startup quickly.
			"-compactor.ring.wait-stability-min-duration": "0",
			"-compactor.ring.wait-stability-max-duration": "0",
		}, flags))...),
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

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":       "all",
			"-log.level":    "warn",
			"-auth.enabled": "true",
			// Query-frontend worker.
			"-querier.frontend-client.backoff-min-period": "100ms",
			"-querier.frontend-client.backoff-max-period": "100ms",
			"-querier.frontend-client.backoff-retries":    "1",
			"-querier.worker-parallelism":                 "1",
			// Distributor.
			"-distributor.replication-factor": "1",
			// Ingester.
			"-ingester.final-sleep":        "0s",
			"-ingester.join-after":         "0s",
			"-ingester.min-ready-duration": "0s",
			"-ingester.num-tokens":         "512",
			// Startup quickly.
			"-store-gateway.sharding-ring.wait-stability-min-duration": "0",
			"-store-gateway.sharding-ring.wait-stability-max-duration": "0",
		}, flags))...),
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

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			// Do not pass any extra default flags because the config should be drive by the config file.
			"-target":      "all",
			"-log.level":   "warn",
			"-config.file": filepath.Join(e2e.ContainerSharedDir, configFile),
		}, flags))...),
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
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                               "alertmanager",
			"-log.level":                            "warn",
			"-experimental.alertmanager.enable-api": "true",
		}, flags))...),
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
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                               "alertmanager",
			"-log.level":                            "warn",
			"-experimental.alertmanager.enable-api": "true",
		}, flags))...),
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

	return NewMimirService(
		name,
		image,
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":    "ruler",
			"-log.level": "warn",
			// Configure the ingesters ring backend
			"-ring.store":      "consul",
			"-consul.hostname": consulAddress,
		}, flags))...),
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
		e2e.NewCommandWithoutEntrypoint(binaryName, e2e.BuildArgs(e2e.MergeFlags(map[string]string{
			"-target":                   "purger",
			"-log.level":                "warn",
			"-purger.object-store-type": "filesystem",
			"-local.chunk-directory":    e2e.ContainerSharedDir,
		}, flags))...),
		e2e.NewHTTPReadinessProbe(httpPort, "/ready", 200, 299),
		httpPort,
		grpcPort,
	)
}
