// SPDX-License-Identifier: AGPL-3.0-only

package aws

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"

	cortex_s3 "github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storage/bucket/s3"
)

type S3Config struct {
	S3               flagext.URLValue
	S3ForcePathStyle bool

	BucketNames      string
	Endpoint         string              `yaml:"endpoint"`
	Region           string              `yaml:"region"`
	AccessKeyID      string              `yaml:"access_key_id"`
	SecretAccessKey  string              `yaml:"secret_access_key"`
	Insecure         bool                `yaml:"insecure"`
	SSEEncryption    bool                `yaml:"sse_encryption"`
	HTTPConfig       HTTPConfig          `yaml:"http_config"`
	SignatureVersion string              `yaml:"signature_version"`
	SSEConfig        cortex_s3.SSEConfig `yaml:"sse"`
}

type HTTPConfig struct {
	IdleConnTimeout       time.Duration `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout time.Duration `yaml:"response_header_timeout"`
	InsecureSkipVerify    bool          `yaml:"insecure_skip_verify"`
}

const (
	SignatureVersionV4 = "v4"
	SignatureVersionV2 = "v2"
)

var (
	supportedSignatureVersions     = []string{SignatureVersionV4, SignatureVersionV2}
	errUnsupportedSignatureVersion = errors.New("unsupported signature version")
)

var (
	s3RequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "s3_request_duration_seconds",
		Help:      "Time spent doing S3 requests.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
	}, []string{"operation", "status_code"}))
)

func (cfg *S3Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}
func (cfg *S3Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Var(&cfg.S3, prefix+"s3.url", "S3 endpoint URL with escaped Key and Secret encoded. "+
		"If only region is specified as a host, proper endpoint will be deduced. Use inmemory:///<bucket-name> to use a mock in-memory implementation.")
	f.BoolVar(&cfg.S3ForcePathStyle, prefix+"s3.force-path-style", false, "Set this to `true` to force the request to use path-style addressing.")
	f.StringVar(&cfg.BucketNames, prefix+"s3.buckets", "", "Comma separated list of bucket names to evenly distribute chunks over. Overrides any buckets specified in s3.url flag")

	f.StringVar(&cfg.Endpoint, prefix+"s3.endpoint", "", "S3 Endpoint to connect to.")
	f.StringVar(&cfg.Region, prefix+"s3.region", "", "AWS region to use.")
	f.StringVar(&cfg.AccessKeyID, prefix+"s3.access-key-id", "", "AWS Access Key ID")
	f.StringVar(&cfg.SecretAccessKey, prefix+"s3.secret-access-key", "", "AWS Secret Access Key")
	f.BoolVar(&cfg.Insecure, prefix+"s3.insecure", false, "Disable https on s3 connection.")

	// TODO Remove in Cortex 1.10.0
	f.BoolVar(&cfg.SSEEncryption, prefix+"s3.sse-encryption", false, "Enable AWS Server Side Encryption [Deprecated: Use .sse instead. if s3.sse-encryption is enabled, it assumes .sse.type SSE-S3]")

	cfg.SSEConfig.RegisterFlagsWithPrefix(prefix+"s3.sse.", f)

	f.DurationVar(&cfg.HTTPConfig.IdleConnTimeout, prefix+"s3.http.idle-conn-timeout", 90*time.Second, "The maximum amount of time an idle connection will be held open.")
	f.DurationVar(&cfg.HTTPConfig.ResponseHeaderTimeout, prefix+"s3.http.response-header-timeout", 0, "If non-zero, specifies the amount of time to wait for a server's response headers after fully writing the request.")
	f.BoolVar(&cfg.HTTPConfig.InsecureSkipVerify, prefix+"s3.http.insecure-skip-verify", false, "Set to false to skip verifying the certificate chain and hostname.")
	f.StringVar(&cfg.SignatureVersion, prefix+"s3.signature-version", SignatureVersionV4, fmt.Sprintf("The signature version to use for authenticating against S3. Supported values are: %s.", strings.Join(supportedSignatureVersions, ", ")))
}
