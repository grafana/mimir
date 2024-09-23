package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	mqttLib "github.com/at-wat/mqtt-go"
)

const (
	// It's not expected that sending a message will take a long time,
	// so the keepalive is relatively short.
	keepAlive      = 30
	connectTimeout = 10
)

// mqttClient is a wrapper around the mqtt-go client,
// implements MQTT v3.1.1 protocol: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
type mqttClient struct {
	client mqttLib.Client
}

func (c *mqttClient) Connect(ctx context.Context, brokerURL, clientID, username, password string, tlsCfg *tls.Config) error {
	ctx, cancel := context.WithTimeout(ctx, connectTimeout*time.Second)
	defer cancel()

	dialer := &mqttLib.URLDialer{
		URL: brokerURL,
		Options: []mqttLib.DialOption{
			mqttLib.WithTLSConfig(tlsCfg),
		},
	}
	cli, err := dialer.DialContext(ctx)
	if err != nil {
		return err
	}
	c.client = cli

	connectOpts := []mqttLib.ConnectOption{
		// The client only publishes messages and doesn't require the server to keep a session.
		mqttLib.WithCleanSession(true),
		mqttLib.WithKeepAlive(keepAlive),
		mqttLib.WithUserNamePassword(username, password),
	}

	_, err = c.client.Connect(ctx, clientID, connectOpts...)

	return err
}

func (c *mqttClient) Disconnect(ctx context.Context) error {
	if c.client == nil {
		return nil
	}

	return c.client.Disconnect(ctx)
}

func (c *mqttClient) Publish(ctx context.Context, message message) error {
	if c.client == nil {
		return errors.New("failed to publish: client is not connected to the broker")
	}

	return c.client.Publish(ctx, &mqttLib.Message{
		Topic:   message.topic,
		QoS:     mqttLib.QoS0,
		Retain:  false,
		Payload: message.payload,
	})
}
