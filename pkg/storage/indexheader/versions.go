// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// indexHeaderFilenamePattern matches the on-disk index-header filename shape, for a versioned name index-header-vN,
// or for format v1, which lacks the -vN suffix.
var indexHeaderFilenamePattern = regexp.MustCompile(`^` + regexp.QuoteMeta(block.IndexHeaderFilename) + `(?:-v([0-9]+))?$`)

func indexHeaderFilename(version int) string {
	if version == BinaryFormatV1 {
		return block.IndexHeaderFilename
	}
	return fmt.Sprintf("%s-v%d", block.IndexHeaderFilename, version)
}

// indexHeaderPath returns the path of the index-header file for the given format version,
// within blockDir.
func indexHeaderPath(blockDir string, version int) string {
	return filepath.Join(blockDir, indexHeaderFilename(version))
}

// OnDiskIndexHeader describes a single index-header file found on local disk by
// IndexHeadersOnDisk.
type OnDiskIndexHeader struct {
	// Version is parsed from the filename. It's a hint: the version byte in the file itself
	// remains authoritative, and callers that care must still verify it.
	Version int
	Path    string
	Info    os.FileInfo
}

// IndexHeadersOnDisk lists every index-header file present in blockDir, regardless of version, based on filename.
func IndexHeadersOnDisk(blockDir string) ([]OnDiskIndexHeader, error) {
	entries, err := os.ReadDir(blockDir)
	if err != nil {
		return nil, err
	}

	var headers []OnDiskIndexHeader
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		match := indexHeaderFilenamePattern.FindStringSubmatch(entry.Name())
		if match == nil {
			continue
		}

		version := BinaryFormatV1
		if match[1] != "" {
			version, err = strconv.Atoi(match[1])
			if err != nil {
				return nil, fmt.Errorf("parse index-header version from filename %q: %w", entry.Name(), err)
			}
		}

		info, err := entry.Info()
		if err != nil {
			return nil, err
		}

		headers = append(headers, OnDiskIndexHeader{
			Version: version,
			Path:    filepath.Join(blockDir, entry.Name()),
			Info:    info,
		})
	}

	return headers, nil
}

// removeOtherIndexHeaderVersions removes every index-header file in blockDir whose version is not keepVersion.
func removeOtherIndexHeaderVersions(blockDir string, keepVersion int) error {
	headers, err := IndexHeadersOnDisk(blockDir)
	if err != nil {
		return err
	}

	keepPath := indexHeaderPath(blockDir, keepVersion)
	for _, h := range headers {
		if h.Path == keepPath {
			continue
		}
		if err := os.Remove(h.Path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove leftover index-header %s: %w", h.Path, err)
		}
	}
	return nil
}
