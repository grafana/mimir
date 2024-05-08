package pagerduty

// FullValidConfigForTesting is a string representation of a JSON object that contains all fields supported by the notifier Config. It can be used without secrets.
const FullValidConfigForTesting = `{
	"integrationKey": "test-api-key", 
	"severity" : "test-severity", 
	"class" : "test-class", 
	"component": "test-component", 
	"group": "test-group", 
	"summary": "test-summary", 
	"source": "test-source",
	"client" : "test-client",
	"client_url": "test-client-url"
}`

// FullValidSecretsForTesting is a string representation of JSON object that contains all fields that can be overridden from secrets
const FullValidSecretsForTesting = `{
	"integrationKey": "test-secret-api-key"
}`
