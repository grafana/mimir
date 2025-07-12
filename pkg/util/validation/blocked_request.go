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

type BlockedRequestsConfig []BlockedRequest

func (lq *BlockedRequestsConfig) ExampleDoc() (comment string, yaml interface{}) {
	return `The following configuration blocks all GET requests to /foo when the "limit" parameter is set to 100.`,
		[]BlockedRequest{
			{
				Path:   "/foo",
				Method: "GET",
				QueryParams: map[string]BlockedRequestQueryParam{
					"limit": {
						Value: "100",
					},
				},
			},
		}
}
