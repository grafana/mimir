// SPDX-License-Identifier: AGPL-3.0-only

package remotewrite

import (
	"errors"
	"fmt"
	"time"

	"github.com/grafana/dskit/flagext"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	promconfig "github.com/prometheus/prometheus/config"
)

// Config holds the configuration for a single remote write endpoint.
type Config struct {
	// Name identifies this config. Used as the WAL subdirectory name and the
	// "remote" label on metrics. Must be unique within a tenant's config list.
	Name string `yaml:"name" json:"name"`

	// URL of the Prometheus-compatible remote write endpoint.
	URL flagext.URLValue `yaml:"url" json:"url"`

	// RemoteTimeout is the timeout for each remote write request.
	RemoteTimeout model.Duration `yaml:"remote_timeout,omitempty" json:"remote_timeout,omitempty"`

	// Headers are additional HTTP headers to send with every request.
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`

	// BasicAuth is the HTTP basic authentication credentials.
	BasicAuth *BasicAuth `yaml:"basic_auth,omitempty" json:"basic_auth,omitempty"`

	// BearerToken is the HTTP bearer token value.
	BearerToken flagext.Secret `yaml:"bearer_token,omitempty" json:"bearer_token,omitempty"`

	// TLSConfig is the TLS client configuration.
	TLSConfig TLSConfig `yaml:"tls_config,omitempty" json:"tls_config,omitempty"`

	// QueueConfig controls the queue of pending samples.
	QueueConfig QueueConfig `yaml:"queue_config,omitempty" json:"queue_config,omitempty"`
}

// Validate checks that the Config is valid.
func (c *Config) Validate() error {
	if c.Name == "" {
		return errors.New("remote write config name must not be empty")
	}
	if c.URL.URL == nil {
		return fmt.Errorf("remote write config %q: url must not be empty", c.Name)
	}
	return nil
}

// BasicAuth holds HTTP basic authentication credentials.
type BasicAuth struct {
	Username string         `yaml:"username" json:"username"`
	Password flagext.Secret `yaml:"password" json:"password"`
}

// TLSConfig holds TLS client configuration.
type TLSConfig struct {
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty" json:"insecure_skip_verify,omitempty"`
	CAFile             string `yaml:"ca_file,omitempty" json:"ca_file,omitempty"`
	CertFile           string `yaml:"cert_file,omitempty" json:"cert_file,omitempty"`
	KeyFile            string `yaml:"key_file,omitempty" json:"key_file,omitempty"`
	ServerName         string `yaml:"server_name,omitempty" json:"server_name,omitempty"`
}

// QueueConfig controls the queue of pending samples waiting to be sent.
// All fields default to Prometheus' DefaultQueueConfig values.
type QueueConfig struct {
	// Capacity is the number of samples to buffer per shard before blocking.
	Capacity int `yaml:"capacity,omitempty" json:"capacity,omitempty"`

	// MaxShards is the maximum number of concurrent shards (goroutines sending to remote).
	MaxShards int `yaml:"max_shards,omitempty" json:"max_shards,omitempty"`

	// MinShards is the minimum number of shards.
	MinShards int `yaml:"min_shards,omitempty" json:"min_shards,omitempty"`

	// MaxSamplesPerSend is the maximum number of samples per HTTP request.
	MaxSamplesPerSend int `yaml:"max_samples_per_send,omitempty" json:"max_samples_per_send,omitempty"`

	// BatchSendDeadline is the maximum time a sample sits in a shard before being sent.
	BatchSendDeadline time.Duration `yaml:"batch_send_deadline,omitempty" json:"batch_send_deadline,omitempty"`

	// MinBackoff is the initial retry backoff duration.
	MinBackoff model.Duration `yaml:"min_backoff,omitempty" json:"min_backoff,omitempty"`

	// MaxBackoff is the maximum retry backoff duration.
	MaxBackoff model.Duration `yaml:"max_backoff,omitempty" json:"max_backoff,omitempty"`
}

// toPrometheusRemoteWriteConfig translates a Config into a Prometheus RemoteWriteConfig.
func (c *Config) toPrometheusRemoteWriteConfig() (*promconfig.RemoteWriteConfig, error) {
	rwCfg := promconfig.DefaultRemoteWriteConfig
	rwCfg.Name = c.Name

	if c.URL.URL == nil {
		return nil, fmt.Errorf("remote write config %q: url is nil", c.Name)
	}
	rwCfg.URL = &config_util.URL{URL: c.URL.URL}

	if c.RemoteTimeout > 0 {
		rwCfg.RemoteTimeout = c.RemoteTimeout
	}
	if len(c.Headers) > 0 {
		rwCfg.Headers = c.Headers
	}

	// Auth
	if c.BasicAuth != nil {
		rwCfg.HTTPClientConfig.BasicAuth = &config_util.BasicAuth{
			Username: c.BasicAuth.Username,
			Password: config_util.Secret(c.BasicAuth.Password.String()),
		}
	}
	if token := c.BearerToken.String(); token != "" {
		rwCfg.HTTPClientConfig.Authorization = &config_util.Authorization{
			Type:        "Bearer",
			Credentials: config_util.Secret(token),
		}
	}

	// TLS
	rwCfg.HTTPClientConfig.TLSConfig = config_util.TLSConfig{
		InsecureSkipVerify: c.TLSConfig.InsecureSkipVerify,
		CAFile:             c.TLSConfig.CAFile,
		CertFile:           c.TLSConfig.CertFile,
		KeyFile:            c.TLSConfig.KeyFile,
		ServerName:         c.TLSConfig.ServerName,
	}

	// Queue config — apply overrides over defaults.
	def := promconfig.DefaultQueueConfig
	if c.QueueConfig.Capacity > 0 {
		def.Capacity = c.QueueConfig.Capacity
	}
	if c.QueueConfig.MaxShards > 0 {
		def.MaxShards = c.QueueConfig.MaxShards
	}
	if c.QueueConfig.MinShards > 0 {
		def.MinShards = c.QueueConfig.MinShards
	}
	if c.QueueConfig.MaxSamplesPerSend > 0 {
		def.MaxSamplesPerSend = c.QueueConfig.MaxSamplesPerSend
	}
	if c.QueueConfig.BatchSendDeadline > 0 {
		def.BatchSendDeadline = model.Duration(c.QueueConfig.BatchSendDeadline)
	}
	if c.QueueConfig.MinBackoff > 0 {
		def.MinBackoff = c.QueueConfig.MinBackoff
	}
	if c.QueueConfig.MaxBackoff > 0 {
		def.MaxBackoff = c.QueueConfig.MaxBackoff
	}
	rwCfg.QueueConfig = def

	return &rwCfg, nil
}
