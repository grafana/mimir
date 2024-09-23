package mqtt

// FullValidConfigForTesting is a string representation of a JSON object that contains all fields supported by the notifier Config. It can be used without secrets.
const FullValidConfigForTesting = `{
	"brokerUrl": "tcp://localhost:1883",
	"topic": "grafana/alerts",
	"messageFormat": "json",
	"clientId": "grafana-test-client-id",
	"username": "test-username",
	"password": "test-password",
	"insecureSkipVerify": false
}`

// FullValidSecretsForTesting is a string representation of JSON object that contains all fields that can be overridden from secrets
const FullValidSecretsForTesting = `{
	"password": "test-password"
}`
