// SPDX-License-Identifier: AGPL-3.0-only

package validation

type BlockedRequest struct {
	Path        string                              `yaml:"path,omitempty"`
	Method      string                              `yaml:"method,omitempty"`
	QueryParams map[string]BlockedRequestQueryParam `yaml:"query_params,omitempty"`
}

type BlockedRequestQueryParam struct {
	Value    string `yaml:"value"`
	IsRegexp bool   `yaml:"is_regexp,omitempty"`
}
