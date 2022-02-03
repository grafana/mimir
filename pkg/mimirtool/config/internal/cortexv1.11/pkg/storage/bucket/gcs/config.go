// SPDX-License-Identifier: AGPL-3.0-only

package gcs

import (
	"flag"

	"github.com/grafana/dskit/flagext"
)

type Config struct {
	BucketName     string         `yaml:"bucket_name"`
	ServiceAccount flagext.Secret `yaml:"service_account"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, prefix+"gcs.bucket-name", "", "GCS bucket name")
	f.Var(&cfg.ServiceAccount, prefix+"gcs.service-account", "JSON representing either a Google Developers Console client_credentials.json file or a Google Developers service account key file. If empty, fallback to Google default logic.")
}
