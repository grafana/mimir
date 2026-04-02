package v0mimir1

import "github.com/grafana/alerting/receivers"

const FullValidConfigForTesting = `{
	"api_url": "https://localhost",
	"http_config": {},
	"token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
	"chat": -1001234567890,
	"message": "TestMessage",
	"parse_mode": "MarkdownV2",
	"send_resolved": true
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	cfg := DefaultConfig
	cfg.APIUrl = receivers.MustParseURL("https://api.telegram.org")
	cfg.BotToken = "secret-bot-token"
	cfg.ChatID = 12345
	cfg.Message = "Custom Message"
	cfg.DisableNotifications = true
	cfg.ParseMode = "HTML"
	return cfg
}
