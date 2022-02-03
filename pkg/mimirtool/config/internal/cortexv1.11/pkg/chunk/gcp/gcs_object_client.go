// SPDX-License-Identifier: AGPL-3.0-only

package gcp

import (
	"flag"
	"time"
)

type GCSConfig struct {
	BucketName       string        `yaml:"bucket_name"`
	ChunkBufferSize  int           `yaml:"chunk_buffer_size"`
	RequestTimeout   time.Duration `yaml:"request_timeout"`
	EnableOpenCensus bool          `yaml:"enable_opencensus"`
}

func (cfg *GCSConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}
func (cfg *GCSConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, prefix+"gcs.bucketname", "", "Name of GCS bucket. Please refer to https://cloud.google.com/docs/authentication/production for more information about how to configure authentication.")
	f.IntVar(&cfg.ChunkBufferSize, prefix+"gcs.chunk-buffer-size", 0, "The size of the buffer that GCS client for each PUT request. 0 to disable buffering.")
	f.DurationVar(&cfg.RequestTimeout, prefix+"gcs.request-timeout", 0, "The duration after which the requests to GCS should be timed out.")
	f.BoolVar(&cfg.EnableOpenCensus, prefix+"gcs.enable-opencensus", true, "Enabled OpenCensus (OC) instrumentation for all requests.")
}
