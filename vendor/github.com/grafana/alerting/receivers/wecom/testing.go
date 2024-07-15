package wecom

// FullValidConfigForTesting is a string representation of a JSON object that contains all fields supported by the notifier Config. It can be used without secrets.
const FullValidConfigForTesting = `{
	"endpointUrl" : "test-endpointUrl",
	"agent_id" : "test-agent_id",
	"corp_id" : "test-corp_id",
	"url" : "test-url",
	"secret" : "test-secret",
	"msgtype" : "markdown",
	"message" : "test-message",
	"title" : "test-title",
	"touser" : "test-touser"
}`

// FullValidSecretsForTesting is a string representation of JSON object that contains all fields that can be overridden from secrets
const FullValidSecretsForTesting = `{
	"url": "test-url-secret"
}`
