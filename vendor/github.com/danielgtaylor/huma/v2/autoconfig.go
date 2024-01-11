package huma

// AutoConfigVar represents a variable given by the user when prompted during
// auto-configuration setup of an API.
type AutoConfigVar struct {
	Description string        `json:"description,omitempty"`
	Example     string        `json:"example,omitempty"`
	Default     interface{}   `json:"default,omitempty"`
	Enum        []interface{} `json:"enum,omitempty"`

	// Exclude the value from being sent to the server. This essentially makes
	// it a value which is only used in param templates.
	Exclude bool `json:"exclude,omitempty"`
}

// AutoConfig holds an API's automatic configuration settings for the CLI. These
// are advertised via OpenAPI extension and picked up by the CLI to make it
// easier to get started using an API. This struct should be put into the
// `OpenAPI.Extensions` map under the key `x-cli-config`. See also:
// https://rest.sh/#/openapi?id=autoconfiguration
type AutoConfig struct {
	Security string                   `json:"security"`
	Headers  map[string]string        `json:"headers,omitempty"`
	Prompt   map[string]AutoConfigVar `json:"prompt,omitempty"`
	Params   map[string]string        `json:"params"`
}
