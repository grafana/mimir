package v0mimir1

import "github.com/grafana/alerting/receivers"

const FullValidConfigForTesting = ` {
	"url": "http://localhost/",
	"http_config": {},
	"routing_key": "test-routing-secret-key",
	"service_key": "test-service-secret-key",
	"client": "Alertmanager",
	"client_url": "https://monitoring.example.com",
	"description": "test description",
	"severity": "test severity",
	"details": {
	  "firing": "test firing"
	},
	"images": [
		{
			"alt": "test alt",
			"src": "test src",
			"href": "http://localhost"
		}
	],
	"links": [
		{
			"href": "http://localhost",
			"text": "test text"
		}
	],
	"source": "test source",
	"class": "test class",
	"component": "test component",
	"group": "test group",
	"send_resolved": true
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	cfg := DefaultConfig
	cfg.ServiceKey = "secret-service-key"
	cfg.RoutingKey = "secret-routing-key"
	cfg.URL = receivers.MustParseURL("https://events.pagerduty.com")
	cfg.Client = "Custom Client"
	cfg.ClientURL = "http://client.example.com"
	cfg.Description = "Custom Description"
	cfg.Details = map[string]string{
		"firing":       DefaultPagerdutyDetails["firing"],
		"resolved":     DefaultPagerdutyDetails["resolved"],
		"num_firing":   DefaultPagerdutyDetails["num_firing"],
		"num_resolved": DefaultPagerdutyDetails["num_resolved"],
		"custom_key":   "custom_value",
	}
	cfg.Images = []PagerdutyImage{
		{Src: "http://img.example.com/img.png", Alt: "test image", Href: "http://example.com"},
	}
	cfg.Links = []PagerdutyLink{
		{Href: "http://example.com", Text: "test link"},
	}
	cfg.Source = "Custom Source"
	cfg.Severity = "critical"
	cfg.Class = "alert"
	cfg.Component = "api"
	cfg.Group = "production"
	return cfg
}
