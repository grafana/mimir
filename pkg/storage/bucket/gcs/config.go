// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/bucket/gcs/config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package gcs

import (
	"errors"
	"flag"
	"time"

	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/storage/bucket/common"
)

var (
	errInvalidUploadInitialQPS = errors.New("gcs.upload-initial-qps must be greater than 0 when rate limiting is enabled")
	errInvalidUploadMaxQPS     = errors.New("gcs.upload-max-qps must be greater than 0 when rate limiting is enabled")
	errInvalidUploadRampPeriod = errors.New("gcs.upload-ramp-period must be greater than 0 when rate limiting is enabled")
	errInvalidReadInitialQPS   = errors.New("gcs.read-initial-qps must be greater than 0 when rate limiting is enabled")
	errInvalidReadMaxQPS       = errors.New("gcs.read-max-qps must be greater than 0 when rate limiting is enabled")
	errInvalidReadRampPeriod   = errors.New("gcs.read-ramp-period must be greater than 0 when rate limiting is enabled")
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

	// UploadRateLimitEnabled enables rate limiting for GCS uploads.
	// When enabled, uploads will gradually ramp up to UploadMaxQPS following
	// Google Cloud Storage best practices for request rate ramping.
	UploadRateLimitEnabled bool `yaml:"upload_rate_limit_enabled" category:"advanced"`

	// UploadInitialQPS is the initial queries per second limit for GCS uploads
	// when rate limiting is enabled. The rate will double every UploadRampPeriod
	// until it reaches UploadMaxQPS.
	UploadInitialQPS int `yaml:"upload_initial_qps" category:"advanced"`

	// UploadMaxQPS is the maximum queries per second limit for GCS uploads.
	UploadMaxQPS int `yaml:"upload_max_qps" category:"advanced"`

	// UploadRampPeriod is the time period over which the upload rate doubles.
	// Following Google's recommendation, this defaults to 20 minutes.
	UploadRampPeriod time.Duration `yaml:"upload_ramp_period" category:"advanced"`

	// ReadRateLimitEnabled enables rate limiting for GCS reads.
	// When enabled, reads will gradually ramp up to ReadMaxQPS following
	// Google Cloud Storage best practices for request rate ramping.
	ReadRateLimitEnabled bool `yaml:"read_rate_limit_enabled" category:"advanced"`

	// ReadInitialQPS is the initial queries per second limit for GCS reads
	// when rate limiting is enabled. The rate will double every ReadRampPeriod
	// until it reaches ReadMaxQPS.
	ReadInitialQPS int `yaml:"read_initial_qps" category:"advanced"`

	// ReadMaxQPS is the maximum queries per second limit for GCS reads.
	ReadMaxQPS int `yaml:"read_max_qps" category:"advanced"`

	// ReadRampPeriod is the time period over which the read rate doubles.
	// Following Google's recommendation, this defaults to 20 minutes.
	ReadRampPeriod time.Duration `yaml:"read_ramp_period" category:"advanced"`

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
	f.BoolVar(&cfg.UploadRateLimitEnabled, prefix+"gcs.upload-rate-limit-enabled", false, "Enable rate limiting for GCS uploads. When enabled, uploads will gradually ramp up following Google Cloud Storage best practices.")
	f.IntVar(&cfg.UploadInitialQPS, prefix+"gcs.upload-initial-qps", 1000, "Initial queries per second limit for GCS uploads. The rate doubles every ramp period until it reaches the maximum.")
	f.IntVar(&cfg.UploadMaxQPS, prefix+"gcs.upload-max-qps", 3200, "Maximum queries per second limit for GCS uploads.")
	f.DurationVar(&cfg.UploadRampPeriod, prefix+"gcs.upload-ramp-period", 20*time.Minute, "Time period over which the upload rate doubles, following Google's recommendation.")
	f.BoolVar(&cfg.ReadRateLimitEnabled, prefix+"gcs.read-rate-limit-enabled", false, "Enable rate limiting for GCS reads. When enabled, reads will gradually ramp up following Google Cloud Storage best practices.")
	f.IntVar(&cfg.ReadInitialQPS, prefix+"gcs.read-initial-qps", 5000, "Initial queries per second limit for GCS reads. The rate doubles every ramp period until it reaches the maximum.")
	f.IntVar(&cfg.ReadMaxQPS, prefix+"gcs.read-max-qps", 16000, "Maximum queries per second limit for GCS reads.")
	f.DurationVar(&cfg.ReadRampPeriod, prefix+"gcs.read-ramp-period", 20*time.Minute, "Time period over which the read rate doubles, following Google's recommendation.")
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

// Validate validates the GCS config and returns an error on failure.
func (cfg *Config) Validate() error {
	if cfg.UploadRateLimitEnabled {
		if cfg.UploadInitialQPS <= 0 {
			return errInvalidUploadInitialQPS
		}
		if cfg.UploadMaxQPS <= 0 {
			return errInvalidUploadMaxQPS
		}
		if cfg.UploadRampPeriod <= 0 {
			return errInvalidUploadRampPeriod
		}
	}
	if cfg.ReadRateLimitEnabled {
		if cfg.ReadInitialQPS <= 0 {
			return errInvalidReadInitialQPS
		}
		if cfg.ReadMaxQPS <= 0 {
			return errInvalidReadMaxQPS
		}
		if cfg.ReadRampPeriod <= 0 {
			return errInvalidReadRampPeriod
		}
	}
	return nil
}
