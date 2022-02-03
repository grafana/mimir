// SPDX-License-Identifier: AGPL-3.0-only

package tls

import (
	"flag"

	"github.com/pkg/errors"
)

type ClientConfig struct {
	CertPath           string `yaml:"tls_cert_path"`
	KeyPath            string `yaml:"tls_key_path"`
	CAPath             string `yaml:"tls_ca_path"`
	ServerName         string `yaml:"tls_server_name"`
	InsecureSkipVerify bool   `yaml:"tls_insecure_skip_verify"`
}

var (
	errKeyMissing  = errors.New("certificate given but no key configured")
	errCertMissing = errors.New("key given but no certificate configured")
)

func (cfg *ClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.CertPath, prefix+".tls-cert-path", "", "Path to the client certificate file, which will be used for authenticating with the server. Also requires the key path to be configured.")
	f.StringVar(&cfg.KeyPath, prefix+".tls-key-path", "", "Path to the key file for the client certificate. Also requires the client certificate to be configured.")
	f.StringVar(&cfg.CAPath, prefix+".tls-ca-path", "", "Path to the CA certificates file to validate server certificate against. If not set, the host's root CA certificates are used.")
	f.StringVar(&cfg.ServerName, prefix+".tls-server-name", "", "Override the expected name on the server certificate.")
	f.BoolVar(&cfg.InsecureSkipVerify, prefix+".tls-insecure-skip-verify", false, "Skip validating server certificate.")
}
