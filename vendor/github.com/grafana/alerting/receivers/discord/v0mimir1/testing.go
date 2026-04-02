package v0mimir1

import "github.com/grafana/alerting/receivers"

const FullValidConfigForTesting = `{
	"send_resolved": true,
	"webhook_url": "http://localhost",
	"http_config": {},
	"title": "test title",
	"message": "test message"
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	cfg := DefaultConfig
	cfg.WebhookURL = receivers.MustParseSecretURL("http://discord.example.com/webhook")
	cfg.Title = "Custom Title"
	cfg.Message = "Custom Message"
	return cfg
}
