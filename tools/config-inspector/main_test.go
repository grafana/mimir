// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"os"
	"strings"
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

func TestFlagNamesEqualsConfigNames(t *testing.T) {
	root, err := config.InspectConfig(&mimir.Config{})
	require.NoError(t, err)

	visit(t, root)
}

func visit(t *testing.T, entry *config.InspectedEntry) {
	flagNameEqualsConfigName(t, entry)

	for i := 0; i < len(entry.BlockEntries); i++ {
		visit(t, entry.BlockEntries[i])
	}

}

func flagNameEqualsConfigName(t *testing.T, entry *config.InspectedEntry) {
	if entry.FieldFlag == "" {
		return
	}
	flagConfigPart := entry.FieldFlag[strings.LastIndex(entry.FieldFlag, ".")+1:]

	assert.Equal(t, entry.Name, strings.ReplaceAll(flagConfigPart, "-", "_"), fmt.Sprintf("Flag value is: [%s], config value is: [%s]", entry.FieldFlag, entry.Name))
}
