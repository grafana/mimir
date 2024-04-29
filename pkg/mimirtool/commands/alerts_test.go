// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadAlertmanagerConfigTemplates(t *testing.T) {
	cmd := AlertmanagerCommand{
		TemplateFiles: []string{
			"./testdata/alertmanager_template.tmpl",
		},
	}

	templates, err := cmd.readAlertManagerConfigTemplates()
	require.NoError(t, err)

	require.Len(t, templates, 1)

	var keys []string
	for k := range templates {
		keys = append(keys, k)
	}

	assert.Equal(t, "alertmanager_template.tmpl", keys[0])
	assert.NotEmpty(t, templates[keys[0]])
}

func TestReadAlertmanagerConfigTemplates_collidingNames(t *testing.T) {
	cmd := AlertmanagerCommand{
		TemplateFiles: []string{
			"./testdata/alertmanager_template.tmpl",
			"./testdata/otherdir/alertmanager_template.tmpl",
		},
	}

	_, err := cmd.readAlertManagerConfigTemplates()
	require.Error(t, err)
}

func TestReadAlertmanagerValidConfigFile(t *testing.T) {
	cmd := AlertmanagerCommand{
		AlertmanagerConfigFile: "./testdata/alertmanager_config.yaml",
	}

	_, _, err := cmd.readAlertManagerConfig()
	require.NoError(t, err)
}

func TestLoadAlertmanagerConfigAndTemplateFile(t *testing.T) {
	// creates a temp directory to persist the original config and templates to disk
	dir := t.TempDir()
	// reads the existing config and template to memory
	cmd := AlertmanagerCommand{
		AlertmanagerConfigFile: "./testdata/alertmanager_config.yaml",
		TemplateFiles: []string{
			"./testdata/alertmanager_template.tmpl",
		},
		OutputDir: dir,
	}
	originalCfg, originalTemplates, err := cmd.readAlertManagerConfig()
	require.NoError(t, err)
	// write the data to a temp directory and reads it again
	err = cmd.outputAlertManagerConfigTemplates(originalCfg, originalTemplates)
	require.NoError(t, err)
	// loads the new files that has been written out from memory, load the new files and compare
	cmdLoad := AlertmanagerCommand{
		AlertmanagerConfigFile: filepath.Join(dir, "config.yaml"),
		TemplateFiles: []string{
			filepath.Join(dir, "alertmanager_template.tmpl"),
		},
	}
	loadedCfg, loadedTemplates, err := cmdLoad.readAlertManagerConfig()
	require.NoError(t, err)

	assert.Equal(t, originalCfg, loadedCfg)
	assert.Equal(t, len(originalTemplates), len(loadedTemplates))
	for fileName, fileContent := range originalTemplates {
		if assert.Contains(t, loadedTemplates, fileName) {
			assert.Equal(t, loadedTemplates[fileName], fileContent)
		}
	}
}
