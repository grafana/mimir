package v0mimir1

import (
	"time"

	"github.com/prometheus/common/model"

	"github.com/grafana/alerting/receivers"
)

const FullValidConfigForTesting = `{
	"send_resolved": true,
	"url": "http://localhost",
	"url_file": "",
	"http_config": {},
	"max_alerts": 10,
	"timeout": "30s"
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	cfg := DefaultConfig
	cfg.URL = receivers.MustParseSecretURL("http://webhook.example.com")
	cfg.MaxAlerts = 100
	cfg.Timeout = model.Duration(30 * time.Second)
	return cfg
}
