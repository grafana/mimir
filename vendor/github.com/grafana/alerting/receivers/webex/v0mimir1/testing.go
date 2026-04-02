package v0mimir1

import (
	"net/url"

	"github.com/prometheus/common/config"

	httpcfg "github.com/grafana/alerting/http/v0mimir"
	"github.com/grafana/alerting/receivers"
)

const FullValidConfigForTesting = `{
	"api_url": "https://localhost",
	"http_config": {
	  "authorization": { "type": "Bearer", "credentials": "bot_token" }
	},
	"room_id": "12345",
	"message": "test templated message",
	"send_resolved": true
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	cfg := DefaultConfig
	cfg.HTTPConfig = &httpcfg.HTTPClientConfig{
		Authorization: &httpcfg.Authorization{
			Type:        "Bearer",
			Credentials: "bot_token",
		},
		FollowRedirects: true,
		EnableHTTP2:     true,
		// this is to workaround the marshaling logic that always sets ProxyURL to empty string on unmarshal
		ProxyConfig: httpcfg.ProxyConfig{
			ProxyURL: config.URL{URL: &url.URL{}},
		},
	}
	cfg.APIURL = receivers.MustParseURL("https://webexapis.com/v1/messages")
	cfg.RoomID = "test-room-id"
	cfg.Message = "Custom Message"
	return cfg
}
