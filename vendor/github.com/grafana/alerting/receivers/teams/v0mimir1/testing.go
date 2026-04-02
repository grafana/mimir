package v0mimir1

import "github.com/grafana/alerting/receivers"

const FullValidConfigForTesting = `{
	"send_resolved": true,
	"webhook_url": "http://localhost",
	"http_config": {},
	"title": "test title",
	"summary": "test summary",
	"text": "test text"
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	cfg := DefaultConfig
	cfg.WebhookURL = receivers.MustParseSecretURL("http://teams.example.com/webhook")
	cfg.Title = "Custom Title"
	cfg.Summary = "Custom Summary"
	cfg.Text = "Custom Text"
	return cfg
}
