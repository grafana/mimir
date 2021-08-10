// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/bucket/gcs/config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package gcs

import (
	"flag"

	"github.com/grafana/mimir/pkg/util/flagext"
)

// Config holds the config options for GCS backend
type Config struct {
	BucketName     string         `yaml:"bucket_name"`
	ServiceAccount flagext.Secret `yaml:"service_account"`
}

// RegisterFlags registers the flags for GCS storage
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix registers the flags for GCS storage with the provided prefix
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, prefix+"gcs.bucket-name", "", "GCS bucket name")
	f.Var(&cfg.ServiceAccount, prefix+"gcs.service-account", "JSON representing either a Google Developers Console client_credentials.json file or a Google Developers service account key file. If empty, fallback to Google default logic.")
}
