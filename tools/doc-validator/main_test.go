// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertHeadingToAnchor(t *testing.T) {
	assert.Equal(t, "grafana-mimir-documentation", convertHeadingToAnchor("Grafana Mimir Documentation"))
	assert.Equal(t, "optional-grafana-mimir-alertmanager", convertHeadingToAnchor("(Optional) Grafana Mimir Alertmanager"))
	assert.Equal(t, "multi-tenancy", convertHeadingToAnchor("Multi-tenancy"))
	assert.Equal(t, "format-version-1", convertHeadingToAnchor("Format (version 1)"))
	assert.Equal(t, "gossip-based-memberlist-protocol-default", convertHeadingToAnchor("Gossip-based memberlist protocol (default)"))
	assert.Equal(t, "example-1-full-query-is-shardable", convertHeadingToAnchor("Example 1: Full query is shardable"))
	assert.Equal(t, "flush-chunks--blocks", convertHeadingToAnchor("Flush chunks / blocks"))
	assert.Equal(t, "how-to-estimate--querierquery-store-after", convertHeadingToAnchor("How to estimate -querier.query-store-after"))
}

func TestRemoveFrontmatter(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected string
	}{
		"should not modify an empty content": {
			input:    "",
			expected: "",
		},
		"should not modify a content without frontmatter": {
			input:    "Hello world",
			expected: "Hello world",
		},
		"should remove frontmatter": {
			input: `---
title: Test
---

Hello World
`,
			expected: `
Hello World
`,
		},
		"should not remove metadata block if it's not a frontmatter": {
			input: `
This is before

---
title: Test
---

This is after
`,
			expected: `
This is before

---
title: Test
---

This is after
`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, string(removeFrontmatter([]byte(testData.input))))
		})
	}
}

func TestValidateMarkdownImage(t *testing.T) {
	// Create some fixtures on filesystem.
	rootDir := t.TempDir()
	require.NoError(t, ioutil.WriteFile(filepath.Join(rootDir, "index.md"), []byte{}, os.ModePerm))
	require.NoError(t, ioutil.WriteFile(filepath.Join(rootDir, "_index.md"), []byte{}, os.ModePerm))
	require.NoError(t, ioutil.WriteFile(filepath.Join(rootDir, "other.md"), []byte{}, os.ModePerm))
	require.NoError(t, ioutil.WriteFile(filepath.Join(rootDir, "image.png"), []byte{}, os.ModePerm))
	require.NoError(t, os.Mkdir(filepath.Join(rootDir, "bundle"), os.ModePerm))
	require.NoError(t, ioutil.WriteFile(filepath.Join(rootDir, "bundle", "index.md"), []byte{}, os.ModePerm))

	tests := map[string]struct {
		sourceFile  string
		link        string
		expectedErr string
	}{
		"should pass if image exists and source file is index.md": {
			sourceFile: filepath.Join(rootDir, "index.md"),
			link:       "image.png",
		},
		"should pass if image exists and source file is _index.md": {
			sourceFile: filepath.Join(rootDir, "_index.md"),
			link:       "image.png",
		},
		"should pass if image exists, is in the same directory but located using a relative path": {
			sourceFile: filepath.Join(rootDir, "_index.md"),
			link:       "./image.png",
		},
		"should fail if image exists but source file is not an index": {
			sourceFile:  filepath.Join(rootDir, "other.md"),
			link:        "image.png",
			expectedErr: "can only be linked from an index.md or _index.md file",
		},
		"should fail if image does not exist": {
			sourceFile:  filepath.Join(rootDir, "_index.md"),
			link:        "unknown.png",
			expectedErr: "does not exist",
		},
		"should fail if image exists but is located in another directory": {
			sourceFile:  filepath.Join(rootDir, "bundle", "index.md"),
			link:        "../image.png",
			expectedErr: "must be located in the same directory of the markdown file",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			err := validateMarkdownImage(testData.sourceFile, testData.link)

			if testData.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), testData.expectedErr)
			}
		})
	}
}

func TestValidateMarkdownLink(t *testing.T) {
	// Create some fixtures on filesystem.
	rootDir := t.TempDir()
	require.NoError(t, ioutil.WriteFile(filepath.Join(rootDir, "index.md"), []byte{}, os.ModePerm))
	require.NoError(t, ioutil.WriteFile(filepath.Join(rootDir, "other.md"), []byte{}, os.ModePerm))
	require.NoError(t, os.Mkdir(filepath.Join(rootDir, "bundle"), os.ModePerm))
	require.NoError(t, ioutil.WriteFile(filepath.Join(rootDir, "bundle", "index.md"), []byte{}, os.ModePerm))

	tests := map[string]struct {
		sourceFile  string
		link        string
		anchors     markdownAnchors
		expectedErr string
	}{
		"should pass if link is external URL with HTTP protocol": {
			link: "http://google.com",
		},
		"should pass if link is external URL with HTTPS protocol": {
			link: "https://google.com",
		},
		"should fail if link is external URL with unsupported protocol": {
			link:        "ftp://google.com",
			expectedErr: "invalid",
		},
		"should fail if link is relative but specified without Hugo syntax": {
			link:        "../index.md",
			expectedErr: "invalid",
		},
		"should pass if link is local anchor and anchor exists": {
			sourceFile: "index.md",
			link:       "#content",
			anchors: markdownAnchors{
				absPath("index.md"): []string{"content"},
			},
		},
		"should fail if link is local anchor and anchor does not exist": {
			sourceFile: "index.md",
			link:       "#unknown",
			anchors: markdownAnchors{
				absPath("index.md"): []string{"content"},
			},
			expectedErr: `the anchor "unknown" does not exist`,
		},
		"should pass if link is Hugo relref referencing an existing document": {
			sourceFile: filepath.Join(rootDir, "other.md"),
			link:       `{{< relref "bundle/index.md" >}}`,
		},
		"should fail if link is Hugo relref referencing a non existing document": {
			sourceFile:  filepath.Join(rootDir, "other.md"),
			link:        `{{< relref "bundle/unknown.md" >}}`,
			expectedErr: "references a document which does not exist",
		},
		"should pass if link is Hugo relref referencing an existing anchor": {
			sourceFile: filepath.Join(rootDir, "other.md"),
			link:       `{{< relref "#content" >}}`,
			anchors: markdownAnchors{
				absPath(filepath.Join(rootDir, "other.md")): []string{"content"},
			},
		},
		"should fail if link is Hugo relref referencing a non existing anchor": {
			sourceFile: filepath.Join(rootDir, "other.md"),
			link:       `{{< relref "#unknown" >}}`,
			anchors: markdownAnchors{
				absPath(filepath.Join(rootDir, "other.md")): []string{"content"},
			},
			expectedErr: `the anchor "unknown" does not exist`,
		},
		"should pass if link is Hugo relref referencing an existing document and anchor": {
			sourceFile: filepath.Join(rootDir, "other.md"),
			link:       `{{< relref "bundle/index.md#content" >}}`,
			anchors: markdownAnchors{
				absPath(filepath.Join(rootDir, "bundle/index.md")): []string{"content"},
			},
		},
		"should fail if link is Hugo relref referencing an existing document but invalid anchor": {
			sourceFile: filepath.Join(rootDir, "other.md"),
			link:       `{{< relref "bundle/index.md#unknown" >}}`,
			anchors: markdownAnchors{
				absPath(filepath.Join(rootDir, "bundle/index.md")): []string{"content"},
			},
			expectedErr: `the anchor "unknown" does not exist`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			err := validateMarkdownLink(testData.sourceFile, testData.link, testData.anchors)

			if testData.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), testData.expectedErr)
			}
		})
	}
}

func TestValidateMarkdownFileContent(t *testing.T) {
	// Create some fixtures on filesystem.
	rootDir := t.TempDir()
	require.NoError(t, ioutil.WriteFile(filepath.Join(rootDir, "index.md"), []byte{}, os.ModePerm))
	require.NoError(t, ioutil.WriteFile(filepath.Join(rootDir, "other.md"), []byte{}, os.ModePerm))
	require.NoError(t, ioutil.WriteFile(filepath.Join(rootDir, "image.png"), []byte{}, os.ModePerm))

	content := `---
title: Page title in metadata
---

# Page title

This is a testing page.
This link is [invalid because not in Hugo syntax](other.md).
This link is [broken]({{< relref "unknown.md" >}}).
This link is [valid]({{< relref "other.md" >}}).
This link is [invalid because the anchor does not exist]({{< relref "other.md#unknown" >}}).
This link is [valid]({{< relref "other.md#content" >}}).

![missing image](unknown.png)
![valid image](image.png)
`

	sourceFile := filepath.Join(rootDir, "index.md")
	anchors := markdownAnchors{
		absPath(filepath.Join(rootDir, "other.md")): []string{"content"},
	}

	expected := validationErrors{
		newValidationError(sourceFile, errLinkInvalid, "other.md"),
		newValidationError(sourceFile, errLinkNotExist, `{{< relref "unknown.md" >}}`),
		newValidationError(sourceFile, errAnchorNotExist, "unknown", filepath.Join(rootDir, "other.md"), "content"),
		newValidationError(sourceFile, errImageNotExist, "unknown.png"),
	}

	assert.Equal(t, expected, validateMarkdownFileContent(sourceFile, []byte(content), anchors))
}

func TestParseMarkdownAnchorsFromFileContent(t *testing.T) {
	content := `---
title: Page title in metadata
---

# Page title

Some text.

## First headline

Some other text.
`

	anchors, err := parseMarkdownAnchorsFromFileContent([]byte(content))
	require.NoError(t, err)
	require.Equal(t, []string{"page-title", "first-headline"}, anchors)
}
