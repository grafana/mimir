// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/bucket/gcs/config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package gcs

import (
	"flag"

	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/storage/bucket/common"
)

// Config holds the config options for GCS backend
type Config struct {
	BucketName     string         `yaml:"bucket_name"`
	ServiceAccount flagext.Secret `yaml:"service_account" doc:"description_method=GCSServiceAccountLongDescription"`

	// EnableUploadRetries enables automatic retries for all GCS upload operations
	// by using the RetryAlways policy. When enabled, uploads will be retried on
	// transient errors. Note that this does NOT guarantee idempotency - concurrent
	// writes or retries may overwrite existing objects.
	EnableUploadRetries bool `yaml:"enable_upload_retries" category:"advanced"`

	// MaxRetries controls the maximum number of retry attempts for GCS operations.
	// Default is 20 attempts. Set to 0 for unlimited retries (continues until context timeout).
	// Set to 1 to disable retries.
	MaxRetries int `yaml:"max_retries" category:"advanced"`

	HTTP common.HTTPConfig `yaml:"http"`
}

// RegisterFlags registers the flags for GCS storage
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
	cfg.HTTP.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix registers the flags for GCS storage with the provided prefix
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, prefix+"gcs.bucket-name", "", "GCS bucket name")
	f.Var(&cfg.ServiceAccount, prefix+"gcs.service-account", cfg.GCSServiceAccountShortDescription())
	f.BoolVar(&cfg.EnableUploadRetries, prefix+"gcs.enable-upload-retries", false, "Enable automatic retries for GCS uploads using the RetryAlways policy. Uploads will be retried on transient errors. Note: this does not guarantee idempotency.")
	f.IntVar(&cfg.MaxRetries, prefix+"gcs.max-retries", 20, "Maximum number of attempts for GCS operations (0 = unlimited, 1 = no retries). Applies to both regular and upload retry modes.")
	cfg.HTTP.RegisterFlagsWithPrefix(prefix+"gcs.", f)
}

func (cfg *Config) GCSServiceAccountShortDescription() string {
	return "JSON either from a Google Developers Console client_credentials.json file, or a Google Developers service account key. Needs to be valid JSON, not a filesystem path."
}

func (cfg *Config) GCSServiceAccountLongDescription() string {
	return cfg.GCSServiceAccountShortDescription() +
		" If empty, fallback to Google default logic:" +
		"\n1. A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable. For workload identity federation, refer to https://cloud.google.com/iam/docs/how-to#using-workload-identity-federation on how to generate the JSON configuration file for on-prem/non-Google cloud platforms." +
		"\n2. A JSON file in a location known to the gcloud command-line tool: $HOME/.config/gcloud/application_default_credentials.json." +
		"\n3. On Google Compute Engine it fetches credentials from the metadata server."
}
