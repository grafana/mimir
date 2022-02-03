// SPDX-License-Identifier: AGPL-3.0-only

package bucket

import (
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storage/bucket/azure"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storage/bucket/gcs"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storage/bucket/swift"
)

type Config struct {
	Backend string `yaml:"backend"`
	// Backends
	S3         s3.Config         `yaml:"s3"`
	GCS        gcs.Config        `yaml:"gcs"`
	Azure      azure.Config      `yaml:"azure"`
	Swift      swift.Config      `yaml:"swift"`
	Filesystem filesystem.Config `yaml:"filesystem"`

	// Not used internally, meant to allow callers to wrap Buckets
	// created using this config

	// Used to inject additional backends into the config. Allows for this config to
	// be embedded in multiple contexts and support non-object storage based backends.
	ExtraBackends []string `yaml:"-"`
}

const (
	// S3 is the value for the S3 storage backend.
	S3 = "s3"

	// GCS is the value for the GCS storage backend.
	GCS = "gcs"

	// Azure is the value for the Azure storage backend.
	Azure = "azure"

	// Swift is the value for the Openstack Swift storage backend.
	Swift = "swift"

	// Filesystem is the value for the filesystem storage backend.
	Filesystem = "filesystem"
)

var (
	SupportedBackends = []string{S3, GCS, Azure, Swift, Filesystem}

	ErrUnsupportedStorageBackend = errors.New("unsupported storage backend")
)

func (cfg *Config) supportedBackends() []string {
	return append(SupportedBackends, cfg.ExtraBackends...)
}
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.S3.RegisterFlagsWithPrefix(prefix, f)
	cfg.GCS.RegisterFlagsWithPrefix(prefix, f)
	cfg.Azure.RegisterFlagsWithPrefix(prefix, f)
	cfg.Swift.RegisterFlagsWithPrefix(prefix, f)
	cfg.Filesystem.RegisterFlagsWithPrefix(prefix, f)

	f.StringVar(&cfg.Backend, prefix+"backend", "s3", fmt.Sprintf("Backend storage to use. Supported backends are: %s.", strings.Join(cfg.supportedBackends(), ", ")))
}
