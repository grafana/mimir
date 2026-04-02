// Copyright 2016 The Prometheus Authors
// Modifications Copyright Grafana Labs
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v0mimir

import (
	"errors"

	commoncfg "github.com/prometheus/common/config"

	"github.com/grafana/alerting/receivers/schema"
)

// TLSConfig configures the options for TLS connections.
type TLSConfig struct {
	// Text of the CA cert to use for the targets.
	CA string `yaml:"ca,omitempty" json:"ca,omitempty"`
	// Text of the client cert file for the targets.
	Cert string `yaml:"cert,omitempty" json:"cert,omitempty"`
	// Text of the client key file for the targets.
	Key commoncfg.Secret `yaml:"key,omitempty" json:"key,omitempty"`
	// The CA cert to use for the targets.
	CAFile string `yaml:"ca_file,omitempty" json:"ca_file,omitempty"`
	// The client cert file for the targets.
	CertFile string `yaml:"cert_file,omitempty" json:"cert_file,omitempty"`
	// The client key file for the targets.
	KeyFile string `yaml:"key_file,omitempty" json:"key_file,omitempty"`
	// CARef is the name of the secret within the secret manager to use as the CA cert for the
	// targets.
	CARef string `yaml:"ca_ref,omitempty" json:"ca_ref,omitempty"`
	// CertRef is the name of the secret within the secret manager to use as the client cert for
	// the targets.
	CertRef string `yaml:"cert_ref,omitempty" json:"cert_ref,omitempty"`
	// KeyRef is the name of the secret within the secret manager to use as the client key for
	// the targets.
	KeyRef string `yaml:"key_ref,omitempty" json:"key_ref,omitempty"`
	// Used to verify the hostname for the targets.
	ServerName string `yaml:"server_name,omitempty" json:"server_name,omitempty"`
	// Disable target certificate validation.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify" json:"insecure_skip_verify"`
	// Minimum TLS version.
	MinVersion commoncfg.TLSVersion `yaml:"min_version,omitempty" json:"min_version,omitempty"`
	// Maximum TLS version.
	MaxVersion commoncfg.TLSVersion `yaml:"max_version,omitempty" json:"max_version,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *TLSConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain TLSConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	return c.validate()
}

func (c *TLSConfig) Validate() error {
	return c.validate()
}

// validate validates the TLSConfig to check that only one of the inlined or
// file-based fields for the TLS CA, client certificate, and client key are
// used.
func (c *TLSConfig) validate() error {
	if nonZeroCount(len(c.CA) > 0, len(c.CAFile) > 0, len(c.CARef) > 0) > 1 {
		return errors.New("at most one of ca, ca_file & ca_ref must be configured")
	}
	if nonZeroCount(len(c.Cert) > 0, len(c.CertFile) > 0, len(c.CertRef) > 0) > 1 {
		return errors.New("at most one of cert, cert_file & cert_ref must be configured")
	}
	if nonZeroCount(len(c.Key) > 0, len(c.KeyFile) > 0, len(c.KeyRef) > 0) > 1 {
		return errors.New("at most one of key and key_file must be configured")
	}

	if c.usingClientCert() && !c.usingClientKey() {
		return errors.New("exactly one of key or key_file must be configured when a client certificate is configured")
	} else if c.usingClientKey() && !c.usingClientCert() {
		return errors.New("exactly one of cert or cert_file must be configured when a client key is configured")
	}

	return nil
}

func (c *TLSConfig) usingClientCert() bool {
	return len(c.Cert) > 0 || len(c.CertFile) > 0 || len(c.CertRef) > 0
}

func (c *TLSConfig) usingClientKey() bool {
	return len(c.Key) > 0 || len(c.KeyFile) > 0 || len(c.KeyRef) > 0
}

func V0TLSConfigOption(propertyName string) schema.Field {
	// Fields CA, Cert and Key are not allowed in V0 integrations. The restriction comes from Mimir
	return schema.Field{
		Label:        "TLS config",
		Description:  "Configures the TLS settings.",
		PropertyName: propertyName,
		Element:      schema.ElementTypeSubform,
		SubformOptions: []schema.Field{
			{
				Label:        "Server name",
				Description:  "ServerName extension to indicate the name of the server.",
				Element:      schema.ElementTypeInput,
				InputType:    schema.InputTypeText,
				PropertyName: "server_name",
			},
			{
				Label:        "Skip verify",
				Description:  "Disable validation of the server certificate.",
				Element:      schema.ElementTypeCheckbox,
				PropertyName: "insecure_skip_verify",
			},
			{
				Label:        "Min TLS Version",
				Element:      schema.ElementTypeInput,
				InputType:    schema.InputTypeText,
				PropertyName: "min_version",
			},
			{
				Label:        "Max TLS Version",
				Element:      schema.ElementTypeInput,
				InputType:    schema.InputTypeText,
				PropertyName: "max_version",
			},
		},
	}
}
