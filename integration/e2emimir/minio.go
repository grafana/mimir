// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/e2e/blob/708d63bcc673/db/db.go
// Provenance-includes-license: Apache-2.0
package e2emimir

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
)

const (
	MinioImage     = "pgsty/silo:RELEASE.2026-10-26T00-00-00Z@sha256:7bb2aff6b89efe1cb1303c10fc21d579447434ed06649ac9ec05a5e2dbf30ad7"
	KESImage       = "quay.io/minio/kes:v0.17.1"
	MinioAccessKey = e2edb.MinioAccessKey
	MinioSecretKey = e2edb.MinioSecretKey
)

// NewMinio returns minio server, used as a local replacement for S3.
func NewMinio(port int, bktNames ...string) *e2e.HTTPService {
	return newMinio(port, map[string]string{}, bktNames...)
}

// NewMinioWithKES returns minio server, configured to talk to a KES service.
func NewMinioWithKES(port int, kesEndpoint, rootKeyFile, rootCertFile, caCertFile string, bktNames ...string) *e2e.HTTPService {
	kesEnvVars := map[string]string{
		"MINIO_KMS_KES_ENDPOINT":  kesEndpoint,
		"MINIO_KMS_KES_KEY_FILE":  filepath.Join(e2e.ContainerSharedDir, rootKeyFile),
		"MINIO_KMS_KES_CERT_FILE": filepath.Join(e2e.ContainerSharedDir, rootCertFile),
		"MINIO_KMS_KES_CAPATH":    filepath.Join(e2e.ContainerSharedDir, caCertFile),
		"MINIO_KMS_KES_KEY_NAME":  "my-minio-key",
	}
	return newMinio(port, kesEnvVars, bktNames...)
}

func newMinio(port int, envVars map[string]string, bktNames ...string) *e2e.HTTPService {
	commands := []string{}
	for _, bkt := range bktNames {
		commands = append(commands, fmt.Sprintf("mkdir -p /data/%s", bkt))
	}
	commands = append(commands, fmt.Sprintf("silo server --address :%v --quiet /data", port))

	m := e2e.NewHTTPService(
		fmt.Sprintf("minio-%v", port),
		MinioImage,
		// Create the buckets before starting minio
		e2e.NewCommandWithoutEntrypoint("sh", "-c", strings.Join(commands, " && ")),
		e2e.NewHTTPReadinessProbe(port, "/minio/health/ready", 200, 200),
		port,
	)
	envVars["MINIO_ACCESS_KEY"] = MinioAccessKey
	envVars["MINIO_SECRET_KEY"] = MinioSecretKey
	envVars["MINIO_BROWSER"] = "off"
	envVars["ENABLE_HTTPS"] = "0"
	m.SetEnvVars(envVars)
	return m
}

// NewKES returns KES server, used as a local key management store
func NewKES(port int, serverName, serverKeyFile, serverCertFile, clientKeyFile, clientCertFile, rootCertFile, hostSharedDir string) (*e2e.HTTPService, error) {
	// Run this as a shell command, so sub-shell can evaluate 'identity' of root user.
	command := fmt.Sprintf("/kes server --addr 0.0.0.0:%d --key=%s --cert=%s --root=$(/kes tool identity of %s) --auth=off",
		port, filepath.Join(e2e.ContainerSharedDir, serverKeyFile), filepath.Join(e2e.ContainerSharedDir, serverCertFile), filepath.Join(e2e.ContainerSharedDir, clientCertFile))

	readinessProbe, err := e2e.NewHTTPSReadinessProbe(port, "/v1/status", serverName, filepath.Join(hostSharedDir, clientKeyFile), filepath.Join(hostSharedDir, clientCertFile), filepath.Join(hostSharedDir, rootCertFile), 200, 200)
	if err != nil {
		return nil, err
	}

	return e2e.NewHTTPService(
		"kes",
		KESImage,
		e2e.NewCommandWithoutEntrypoint("sh", "-c", command),
		readinessProbe,
		port,
	), nil
}
