// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/api_endpoints_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/grafana/e2e"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/grafana/mimir/integration/e2emimir"
)

func newMimirSingleBinaryWithLocalFilesytemBucket(t *testing.T, name string, flags map[string]string) (*e2e.Scenario, *e2emimir.MimirService) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)

	// Start Mimir in single binary mode, reading the config from file.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))

	if flags == nil {
		flags = map[string]string{}
	}

	setFlagIfNotExistingAlready := func(key, value string) {
		if _, ok := flags[key]; !ok {
			flags[key] = value
		}
	}

	setFlagIfNotExistingAlready("-blocks-storage.backend", "filesystem")
	setFlagIfNotExistingAlready("-blocks-storage.filesystem.dir", "./bucket")

	mimir := e2emimir.NewSingleBinaryWithConfigFile(name, mimirConfigFile, flags, "", 9009, 9095)

	return s, mimir
}

func TestIndexAPIEndpoint(t *testing.T) {
	// Start Mimir in single binary mode, reading the config from file
	s, mimir1 := newMimirSingleBinaryWithLocalFilesytemBucket(t, "mimir-1", nil)
	defer s.Close()
	require.NoError(t, s.StartAndWaitReady(mimir1))

	// GET / should succeed
	res, err := e2e.DoGet(fmt.Sprintf("http://%s", mimir1.Endpoint(9009)))
	require.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode)

	// POST / should fail
	res, err = e2e.DoPost(fmt.Sprintf("http://%s", mimir1.Endpoint(9009)))
	require.NoError(t, err)
	assert.Equal(t, 405, res.StatusCode)
}

func TestConfigAPIEndpoint(t *testing.T) {
	// Start Mimir in single binary mode, reading the config from file
	s, mimir1 := newMimirSingleBinaryWithLocalFilesytemBucket(t, "mimir-1", nil)
	defer s.Close()
	require.NoError(t, s.StartAndWaitReady(mimir1))

	// Get config from /config API endpoint.
	res, err := e2e.DoGet(fmt.Sprintf("http://%s/config", mimir1.Endpoint(9009)))
	require.NoError(t, err)

	defer runutil.ExhaustCloseWithErrCapture(&err, res.Body, "config API response")
	body, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Start again Mimir in single binary with the exported config
	// and ensure it starts (pass the readiness probe).
	require.NoError(t, writeFileToSharedDir(s, mimirConfigFile, body))
	mimir2 := e2emimir.NewSingleBinaryWithConfigFile("mimir-2", mimirConfigFile, nil, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(mimir2))
}
