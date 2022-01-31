// SPDX-License-Identifier: AGPL-3.0-only

package integration

// DefaultPreviousVersionImages is used by `tools/pre-pull-images` so it needs
// to be in a non `_test.go` file.
var DefaultPreviousVersionImages = map[string]func(map[string]string) map[string]string{
	"quay.io/cortexproject/cortex:v1.11.0": func(flags map[string]string) map[string]string {
		flags["-store.engine"] = "blocks"
		flags["-server.http-listen-port"] = "8080"
		return flags
	},
}
