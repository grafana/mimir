package mqtt

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/types"

	"github.com/grafana/alerting/logging"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/templates"
)

type client interface {
	Connect(ctx context.Context, brokerURL, clientID, username, password string, tlsCfg *tls.Config) error
	Disconnect(ctx context.Context) error
	Publish(ctx context.Context, message message) error
}

type message struct {
	topic   string
	payload []byte
}

type Notifier struct {
	*receivers.Base
	log      logging.Logger
	tmpl     *templates.Template
	settings Config
	client   client
}

func New(cfg Config, meta receivers.Metadata, template *templates.Template, logger logging.Logger, cli client) *Notifier {
	if cli == nil {
		cli = &mqttClient{}
	}

	return &Notifier{
		Base:     receivers.NewBase(meta),
		log:      logger,
		tmpl:     template,
		settings: cfg,
		client:   cli,
	}
}

// mqttMessage defines the JSON object send to an MQTT broker.
type mqttMessage struct {
	*templates.ExtendedData

	// The protocol version.
	Version  string `json:"version"`
	GroupKey string `json:"groupKey"`
	Message  string `json:"message"`
}

func (n *Notifier) Notify(ctx context.Context, as ...*types.Alert) (bool, error) {
	n.log.Debug("Sending an MQTT message")

	msg, err := n.buildMessage(ctx, as...)
	if err != nil {
		n.log.Error("Failed to build MQTT message", "error", err.Error())
		return false, err
	}

	var tlsCfg *tls.Config
	if n.settings.InsecureSkipVerify {
		tlsCfg = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	err = n.client.Connect(ctx, n.settings.BrokerURL, n.settings.ClientID, n.settings.Username, n.settings.Password, tlsCfg)
	if err != nil {
		n.log.Error("Failed to connect to MQTT broker", "error", err.Error())
		return false, fmt.Errorf("Failed to connect to MQTT broker: %s", err.Error())
	}
	defer func() {
		err := n.client.Disconnect(ctx)
		if err != nil {
			n.log.Error("Failed to disconnect from MQTT broker", "error", err.Error())
		}
	}()

	err = n.client.Publish(
		ctx,
		message{
			topic:   n.settings.Topic,
			payload: []byte(msg),
		},
	)

	if err != nil {
		n.log.Error("Failed to publish MQTT message", "error", err.Error())
		return false, fmt.Errorf("Failed to publish MQTT message: %s", err.Error())
	}

	return true, nil
}

func (n *Notifier) buildMessage(ctx context.Context, as ...*types.Alert) (string, error) {
	groupKey, err := notify.ExtractGroupKey(ctx)
	if err != nil {
		return "", err
	}

	var tmplErr error
	tmpl, data := templates.TmplText(ctx, n.tmpl, as, n.log, &tmplErr)
	messageText := tmpl(n.settings.Message)
	if tmplErr != nil {
		n.log.Warn("Failed to template MQTT message", "error", tmplErr.Error())
	}

	switch n.settings.MessageFormat {
	case MessageFormatText:
		return messageText, nil
	case MessageFormatJSON:
		msg := &mqttMessage{
			Version:      "1",
			ExtendedData: data,
			GroupKey:     groupKey.String(),
			Message:      messageText,
		}

		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			return "", err
		}

		return string(jsonMsg), nil
	default:
		return "", errors.New("Invalid message format")
	}
}

func (n *Notifier) SendResolved() bool {
	return !n.GetDisableResolveMessage()
}
