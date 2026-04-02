package v0mimir1

import "github.com/grafana/alerting/receivers"

const FullValidConfigForTesting = `{
	"api_key": "api-secret-key",
	"api_url": "http://localhost",
	"http_config": {},
	"message": "test message",
	"description": "test description",
	"source": "Alertmanager",
	"details": {
		"firing": "test firing"
	},
	"entity": "test entity",
	"responders": [{ "type": "team", "name": "ops-team" }],
	"actions": "test actions",
	"tags": "test-tags",
	"note": "Triggered by Alertmanager",
	"priority": "P3",
	"update_alerts": true,
	"send_resolved": true
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	cfg := DefaultConfig
	cfg.APIKey = "secret-api-key"
	cfg.APIURL = receivers.MustParseURL("https://api.opsgenie.com")
	cfg.Message = "Custom Message"
	cfg.Description = "Custom Description"
	cfg.Source = "Custom Source"
	cfg.Details = map[string]string{"key": "value"}
	cfg.Entity = "entity"
	cfg.Responders = []Responder{
		{ID: "resp-id", Type: "team"},
		{Username: "user1", Type: "user"},
	}
	cfg.Actions = "action1,action2"
	cfg.Tags = "tag1,tag2"
	cfg.Note = "Custom Note"
	cfg.Priority = "P1"
	cfg.UpdateAlerts = true
	return cfg
}
