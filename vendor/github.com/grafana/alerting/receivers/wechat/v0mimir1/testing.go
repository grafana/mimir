package v0mimir1

import "github.com/grafana/alerting/receivers"

const FullValidConfigForTesting = `{
	"send_resolved": true,
	"api_url": "http://localhost",
	"http_config": {},
	"api_secret": "12345-secret",
	"corp_id": "12345",
	"to_user": "user1",
	"to_party": "party1",
	"to_tag": "tag1",
	"agent_id": "1000002",
	"message": "test message",
	"message_type": "text"
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	cfg := DefaultConfig
	cfg.APISecret = "secret-api-secret"
	cfg.CorpID = "test-corp-id"
	cfg.Message = "Custom Message"
	cfg.APIURL = receivers.MustParseURL("https://qyapi.weixin.qq.com/cgi-bin/")
	cfg.ToUser = "user1"
	cfg.ToParty = "party1"
	cfg.ToTag = "tag1"
	cfg.AgentID = "agent1"
	cfg.MessageType = "text"
	return cfg
}
