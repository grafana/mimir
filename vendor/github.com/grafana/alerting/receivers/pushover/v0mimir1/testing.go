package v0mimir1

import "time"

const FullValidConfigForTesting = `{
	"user_key": "secret-user-key",
	"token": "secret-token",
	"title": "test title",
	"message": "test message",
	"url": "https://monitoring.example.com",
	"http_config": {},
	"url_title": "test url title",
	"device": "test device",
	"sound": "bike",
	"priority": "urgent",
	"retry": "30s",
	"expire": "1h0m0s",
	"ttl": "1h0m0s",
	"html": true,
	"send_resolved": true
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	cfg := DefaultConfig
	cfg.UserKey = "secret-user-key"
	cfg.Token = "secret-token"
	cfg.Title = "Custom Title"
	cfg.Message = "Custom Message"
	cfg.URL = "http://example.com"
	cfg.URLTitle = "Example"
	cfg.Device = "device1"
	cfg.Sound = "pushover"
	cfg.Priority = "1"
	cfg.Retry = FractionalDuration(2 * time.Minute)
	cfg.Expire = FractionalDuration(2 * time.Hour)
	cfg.TTL = FractionalDuration(30 * time.Minute)
	cfg.HTML = true
	return cfg
}
