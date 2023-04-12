// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/getting_started_single_process_config_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"fmt"
	"testing"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestGettingStartedSingleProcessConfigWithBlocksStorage(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))

	// Start Mimir in single binary mode, reading the config from file and overwriting
	// the backend config to make it work with Minio.
	flags := map[string]string{
		"-blocks-storage.s3.access-key-id":     e2edb.MinioAccessKey,
		"-blocks-storage.s3.secret-access-key": e2edb.MinioSecretKey,
		"-blocks-storage.s3.bucket-name":       blocksBucketName,
		"-blocks-storage.s3.endpoint":          fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-blocks-storage.s3.insecure":          "true",

		// Enable protobuf format so that we can use native histograms.
		"-query-frontend.query-result-response-format": "protobuf",
	}

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithPorts(9009, 9095), e2emimir.WithConfigFile(mimirConfigFile))
	require.NoError(t, s.StartAndWaitReady(mimir))

	runTestPushSeriesAndQueryBack(t, mimir, "series_1", generateFloatSeries)
	runTestPushSeriesAndQueryBack(t, mimir, "hseries_1", generateHistogramSeries)
}
