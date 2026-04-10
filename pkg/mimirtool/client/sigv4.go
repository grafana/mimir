// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"fmt"
	"net/http"

	promconfig "github.com/prometheus/common/config"
	promsigv4 "github.com/prometheus/sigv4"
)

// SigV4Config configures AWS Signature Version 4 request signing for Mimir API requests.
type SigV4Config struct {
	Region             string `yaml:"region"`
	AccessKey          string `yaml:"access_key"`
	SecretKey          string `yaml:"secret_key"`
	Profile            string `yaml:"profile"`
	RoleARN            string `yaml:"role_arn"`
	ExternalID         string `yaml:"external_id"`
	UseFIPSSTSEndpoint bool   `yaml:"use_fips_sts_endpoint"`
	ServiceName        string `yaml:"service_name"`
}

// IsConfigured reports whether any SigV4 configuration option has been set.
func (c SigV4Config) IsConfigured() bool {
	return c.Region != "" ||
		c.AccessKey != "" ||
		c.SecretKey != "" ||
		c.Profile != "" ||
		c.RoleARN != "" ||
		c.ExternalID != "" ||
		c.UseFIPSSTSEndpoint ||
		c.ServiceName != ""
}

// Validate reports whether the configured SigV4 settings are valid.
// It provides a useful public validation hook for callers outside this package.
func (c SigV4Config) Validate() error {
	if !c.IsConfigured() {
		return nil
	}

	return c.validateConfigured()
}

func (c SigV4Config) validateConfigured() error {
	if c.Region == "" {
		return fmt.Errorf("sigv4 region must be configured")
	}

	return c.toPrometheusSigV4Config().Validate()
}

func (c SigV4Config) toPrometheusSigV4Config() *promsigv4.SigV4Config {
	return &promsigv4.SigV4Config{
		Region:             c.Region,
		AccessKey:          c.AccessKey,
		SecretKey:          promconfig.Secret(c.SecretKey),
		Profile:            c.Profile,
		RoleARN:            c.RoleARN,
		ExternalID:         c.ExternalID,
		UseFIPSSTSEndpoint: c.UseFIPSSTSEndpoint,
		ServiceName:        c.ServiceName,
	}
}

func wrapSigV4RoundTripper(cfg SigV4Config, next http.RoundTripper) (http.RoundTripper, error) {
	if !cfg.IsConfigured() {
		return next, nil
	}
	if err := cfg.validateConfigured(); err != nil {
		return nil, err
	}

	return promsigv4.NewSigV4RoundTripper(cfg.toPrometheusSigV4Config(), next)
}
