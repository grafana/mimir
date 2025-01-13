// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/master/pkg/block/metadata/meta.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package block

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"gopkg.in/yaml.v3"
)

type SourceType string

const (
	ReceiveSource         SourceType = "receive"
	CompactorSource       SourceType = "compactor"
	CompactorRepairSource SourceType = "compactor.repair"
	BucketRepairSource    SourceType = "bucket.repair"
	BlockBuilderSource    SourceType = "block-builder"
	SplitBlocksSource     SourceType = "split-blocks"
	TestSource            SourceType = "test"
)

const (
	// TSDBVersion1 is a enumeration of TSDB meta versions supported by Thanos.
	TSDBVersion1 = 1
	// ThanosVersion1 is a enumeration of Thanos section of TSDB meta supported by Thanos.
	ThanosVersion1 = 1
)

// Meta describes the a block's meta. It wraps the known TSDB meta structure and
// extends it by Thanos-specific fields.
type Meta struct {
	tsdb.BlockMeta

	Thanos ThanosMeta `json:"thanos"`
}

func (m Meta) GetMinTime() time.Time {
	return time.UnixMilli(m.MinTime)
}

func (m Meta) GetMaxTime() time.Time {
	return time.UnixMilli(m.MaxTime)
}

func (m Meta) String() string {
	return fmt.Sprintf("%s (min time: %d, max time: %d)", m.ULID, m.MinTime, m.MaxTime)
}

// ThanosMeta holds block meta information specific to Thanos.
type ThanosMeta struct {
	// Version of Thanos meta file. If none specified, 1 is assumed (since first version did not have explicit version specified).
	Version int `json:"version,omitempty"`

	// Labels are the external labels identifying the producer as well as tenant.
	// See https://thanos.io/tip/thanos/storage.md#external-labels for details.
	Labels     map[string]string `json:"labels"`
	Downsample ThanosDownsample  `json:"downsample"`

	// Source is a real upload source of the block.
	Source SourceType `json:"source"`

	// List of segment files (in chunks directory), in sorted order. Optional.
	// Deprecated. Use Files instead.
	SegmentFiles []string `json:"segment_files,omitempty"`

	// File is a sorted (by rel path) list of all files in block directory of this block known to TSDB.
	// Sorted by relative path.
	// Useful to avoid API call to get size of each file, as well as for debugging purposes.
	// Optional, added in v0.17.0.
	Files []File `json:"files,omitempty"`
}

type Matchers []*labels.Matcher

func (m *Matchers) UnmarshalYAML(value *yaml.Node) (err error) {
	*m, err = parser.ParseMetricSelector(value.Value)
	if err != nil {
		return errors.Wrapf(err, "parse metric selector %v", value.Value)
	}
	return nil
}

type File struct {
	RelPath string `json:"rel_path"`
	// SizeBytes is optional (e.g meta.json does not show size).
	SizeBytes int64 `json:"size_bytes,omitempty"`

	// The json field "hash" is reserved because it is used by Thanos for the file hash.
}

type ThanosDownsample struct {
	Resolution int64 `json:"resolution"`
}

// InjectThanosMeta sets Thanos meta to the block meta JSON and saves it to the disk.
// NOTE: It should be used after writing any block by any Thanos component, otherwise we will miss crucial metadata.
func InjectThanosMeta(logger log.Logger, bdir string, meta ThanosMeta, downsampledMeta *tsdb.BlockMeta) (*Meta, error) {
	newMeta, err := ReadMetaFromDir(bdir)
	if err != nil {
		return nil, errors.Wrap(err, "read new meta")
	}
	newMeta.Thanos = meta

	// While downsampling we need to copy original compaction.
	if downsampledMeta != nil {
		newMeta.Compaction = downsampledMeta.Compaction
	}

	if err := newMeta.WriteToDir(logger, bdir); err != nil {
		return nil, errors.Wrap(err, "write new meta")
	}

	return newMeta, nil
}

// WriteToDir writes the encoded meta into <dir>/meta.json.
func (m Meta) WriteToDir(logger log.Logger, dir string) error {
	// Make any changes to the file appear atomic.
	path := filepath.Join(dir, MetaFilename)
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if err := m.Write(f); err != nil {
		runutil.CloseWithLogOnErr(logger, f, "close meta")
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return renameFile(logger, tmp, path)
}

// Write writes the given encoded meta to writer.
func (m Meta) Write(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	return enc.Encode(&m)
}

// BlockBytes calculates the size of all files in the block.
func (m Meta) BlockBytes() int64 {
	var b int64
	for _, f := range m.Thanos.Files {
		if f.SizeBytes > 0 {
			b += f.SizeBytes
		}
	}
	return b
}

func renameFile(logger log.Logger, from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = fileutil.Fdatasync(pdir); err != nil {
		runutil.CloseWithLogOnErr(logger, pdir, "close dir")
		return err
	}
	return pdir.Close()
}

// ReadMetaFromDir reads the given meta from <dir>/meta.json.
func ReadMetaFromDir(dir string) (*Meta, error) {
	f, err := os.Open(filepath.Join(dir, filepath.Clean(MetaFilename)))
	if err != nil {
		return nil, err
	}
	return ReadMeta(f)
}

// ReadMeta reads the block meta from the given reader.
func ReadMeta(rc io.ReadCloser) (_ *Meta, err error) {
	defer runutil.ExhaustCloseWithErrCapture(&err, rc, "close meta JSON")

	var m Meta
	if err = json.NewDecoder(rc).Decode(&m); err != nil {
		return nil, err
	}

	if m.Version != TSDBVersion1 {
		return nil, errors.Errorf("unexpected meta file version %d", m.Version)
	}

	version := m.Thanos.Version
	if version == 0 {
		// For compatibility.
		version = ThanosVersion1
	}

	if version != ThanosVersion1 {
		return nil, errors.Errorf("unexpected meta file Thanos section version %d", m.Version)
	}

	if m.Thanos.Labels == nil {
		// To avoid extra nil checks, allocate map here if empty.
		m.Thanos.Labels = make(map[string]string)
	}
	return &m, nil
}
