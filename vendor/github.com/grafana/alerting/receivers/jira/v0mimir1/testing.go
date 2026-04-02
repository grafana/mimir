package v0mimir1

import (
	"time"

	"github.com/prometheus/common/model"

	"github.com/grafana/alerting/receivers"
)

const FullValidConfigForTesting = `{
	"api_url": "http://localhost",
	"project": "PROJ",
	"issue_type": "Bug",
	"summary": "test summary",
	"description": "test description",
	"priority": "High",
	"labels": ["alertmanager"],
	"custom_fields": {
		"customfield_10000": "test customfield_10000"
	},
	"send_resolved": true
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	cfg := DefaultConfig
	cfg.APIURL = receivers.MustParseURL("http://jira.example.com")
	cfg.Project = "TEST"
	cfg.IssueType = "Bug"
	cfg.Summary = "Custom Summary"
	cfg.Description = "Custom Description"
	cfg.Labels = []string{"alert", "critical"}
	cfg.Priority = "P1"
	cfg.ReopenTransition = "Reopen"
	cfg.ResolveTransition = "Resolve"
	cfg.WontFixResolution = "Won't Fix"
	cfg.ReopenDuration = model.Duration(time.Hour)
	cfg.Fields = map[string]any{"custom_field": "value"}
	return cfg
}
