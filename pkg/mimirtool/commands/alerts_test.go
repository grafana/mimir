// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
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
