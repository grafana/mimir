package definition

import "github.com/grafana/alerting/receivers"

// MarshalJSONWithSecrets marshals the given value to JSON with secrets in plain text.
//
// alertmanager's and prometheus' Secret and SecretURL types mask their values
// when marshaled with the standard JSON or YAML marshallers. This function
// preserves the values of these types by using a custom JSON encoder.
func MarshalJSONWithSecrets(v any) ([]byte, error) {
	return receivers.PlainSecretsJSON.Marshal(v)
}
