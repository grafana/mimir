// SPDX-License-Identifier: AGPL-3.0-only

package validation

type BlockedRequest struct {
	Path        string                              `yaml:"path,omitempty" doc:"description=Path to match, including leading slash (/). Leave blank to match all paths."`
	Method      string                              `yaml:"method,omitempty" doc:"description=HTTP method to match. Leave blank to match all methods."`
	QueryParams map[string]BlockedRequestQueryParam `yaml:"query_params,omitempty" doc:"description=Query parameters to match. Requests must have all of the provided query parameters to be considered a match."`
}

type BlockedRequestQueryParam struct {
	Value    string `yaml:"value" doc:"description=Value to match."`
	IsRegexp bool   `yaml:"is_regexp,omitempty" doc:"description=If true, the value is treated as a regular expression. If false, the value is treated as a literal match."`
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
