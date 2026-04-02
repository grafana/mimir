package v0mimir1

import "github.com/grafana/alerting/receivers"

const FullValidConfigForTesting = `{
	"api_url": "http://localhost",
	"http_config": {},
	"channel": "#alerts",
	"username": "Alerting Team",
	"color": "danger",
	"title": "test title",
	"title_link": "http://localhost",
	"pretext": "test pretext",
	"text": "test text",
	"fields": [
		{
			"title": "test title",
			"value": "test value",
			"short": true
		}
	],
	"short_fields": true,
	"footer": "test footer",
	"fallback": "test fallback",
	"callback_id": "test callback id",
	"icon_emoji": ":warning:",
	"icon_url": "https://example.com/icon.png",
	"image_url": "https://example.com/image.png",
	"thumb_url": "https://example.com/thumb.png",
	"link_names": true,
	"mrkdwn_in": ["fallback", "pretext", "text"],
	"actions": [
		{
			"type": "test-type",
			"text": "test-text",
			"url": "http://localhost",
			"style": "test-style",
			"name": "test-name",
			"value": "test-value",
			"confirm": {
				"title": "test-title",
				"text": "test-text",
				"ok_text": "test-ok-text",
				"dismiss_text": "test-dismiss-text"
			}
		}
	],
	"send_resolved": true
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	shortField := true
	cfg := DefaultConfig
	cfg.APIURL = receivers.MustParseSecretURL("http://slack.example.com/webhook")
	cfg.Channel = "#test-channel"
	cfg.Username = "Custom Username"
	cfg.Color = "good"
	cfg.Title = "Custom Title"
	cfg.TitleLink = "http://example.com"
	cfg.Pretext = "Custom Pretext"
	cfg.Text = "Custom Text"
	cfg.Fields = []*SlackField{
		{Title: "field1", Value: "value1", Short: &shortField},
	}
	cfg.ShortFields = true
	cfg.Footer = "Custom Footer"
	cfg.Fallback = "Custom Fallback"
	cfg.CallbackID = "callback-123"
	cfg.IconEmoji = ":rocket:"
	cfg.IconURL = "http://example.com/icon.png"
	cfg.ImageURL = "http://example.com/image.png"
	cfg.ThumbURL = "http://example.com/thumb.png"
	cfg.LinkNames = true
	cfg.MrkdwnIn = []string{"text", "pretext"}
	cfg.Actions = []*SlackAction{
		{
			Type:  "button",
			Text:  "Click me",
			Name:  "action1",
			Value: "val1",
			Style: "primary",
			ConfirmField: &SlackConfirmationField{
				Text:        "Are you sure?",
				Title:       "Confirm",
				OkText:      "Yes",
				DismissText: "No",
			},
		},
	}
	return cfg
}
