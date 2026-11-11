// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarkdownFiles(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{
		".gitignore":        "ignored/\n",
		".prettierignore":   "CHANGELOG.md\nvendor\n.*/\n",
		"README.md":         "",
		"CHANGELOG.md":      "",
		"docs/a.md":         "",
		"docs/a.txt":        "",
		"docs/CHANGELOG.md": "",
		"ignored/b.md":      "",
		"pkg/vendor/c.md":   "",
		".github/d.md":      "",
		"node_modules/e.md": "",
	} {
		path := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}
	t.Chdir(dir)

	files, err := markdownFiles()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"README.md", filepath.Join("docs", "a.md")}, files)
}
