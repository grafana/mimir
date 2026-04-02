package v0mimir1

import "github.com/grafana/alerting/receivers"

const FullValidConfigForTesting = ` {
	"api_url": "http://localhost",
	"api_key": "secret-api-key",
	"http_config": {},
	"routing_key": "team1",
	"message_type": "CRITICAL",
	"entity_display_name": "test entity",
	"state_message": "test state message",
	"monitoring_tool": "Grafana",
	"custom_fields": {
		"test": "test"
	},
	"send_resolved": true
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	cfg := DefaultConfig
	cfg.APIKey = "secret-api-key"
	cfg.APIURL = receivers.MustParseURL("https://alert.victorops.com/integrations/generic/20131114/alert")
	cfg.RoutingKey = "test-routing-key"
	cfg.MessageType = "CRITICAL"
	cfg.StateMessage = "Custom State Message"
	cfg.EntityDisplayName = "Custom Entity"
	cfg.MonitoringTool = "Custom Tool"
	cfg.CustomFields = map[string]string{"custom_key": "custom_value"}
	return cfg
}
