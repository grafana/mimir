// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/pkg/mimirtool/config"
)

func TestConfigDescriptorIsUpToDate(t *testing.T) {
	const descriptorLocation = "../../cmd/mimir/config-descriptor.json"
	upToDateBytes, err := config.Describe(&mimir.Config{})
	require.NoError(t, err)

	committedBytes, err := os.ReadFile(descriptorLocation)
	require.NoError(t, err)

	assert.JSONEq(t, string(upToDateBytes), string(committedBytes), "config descriptor is not up to date; run `make reference-help`")
}
