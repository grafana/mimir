// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test runtime-config validate sub-command.
func TestRuntimeConfigValidate(t *testing.T) {
	t.Run("missing runtime config", func(t *testing.T) {
		c := RuntimeConfigCommand{}
		require.EqualError(t, c.validate(nil), "--config-file cannot be empty")
	})

	t.Run("valid runtime config", func(t *testing.T) {
		c := RuntimeConfigCommand{
			configFile: filepath.Join("testdata", "runtimeconfig", "valid.yaml"),
		}
		require.NoError(t, c.validate(nil))
	})

	t.Run("invalid runtime config", func(t *testing.T) {
		c := RuntimeConfigCommand{
			configFile: filepath.Join("testdata", "runtimeconfig", "invalid.yaml"),
		}
		err := c.validate(nil)
		require.ErrorContains(t, err, "failed to load runtime config")
		require.ErrorContains(t, err, "field unknown_field not found in type")
	})
}
