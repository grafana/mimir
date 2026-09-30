// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/prometheus/prometheus/tsdb/index"
)

// NameTable is the size of one block's metric-name entries in the postings
// offset table. The index-header copies that table from the index, so
// these are the bytes a store-gateway reads to list every name's postings
// span.
type NameTable struct {
	Block string
	Names int
	Bytes int64
	// TableBytes is the whole postings offset table, every label name.
	TableBytes int64
}

// BytesPerName is Bytes divided by Names, or 0 for an empty table.
func (t NameTable) BytesPerName() float64 {
	if t.Names == 0 {
		return 0
	}
	return float64(t.Bytes) / float64(t.Names)
}

// MeasureNameTables returns the NameTable of every block under
// bucketDir/anonymous.
func MeasureNameTables(bucketDir string) ([]NameTable, error) {
	metas, err := readMetas(bucketDir)
	if err != nil {
		return nil, err
	}
	var out []NameTable
	for _, m := range metas {
		t, err := nameTable(filepath.Join(bucketDir, "anonymous", m.ULID.String(), "index"))
		if err != nil {
			return nil, fmt.Errorf("block %s: %w", m.ULID, err)
		}
		t.Block = m.ULID.String()
		out = append(out, t)
	}
	return out, nil
}

// nameTable walks the postings offset table of one index file. Each
// entry's size is the distance from its own offset to the next entry's.
func nameTable(indexPath string) (NameTable, error) {
	b, err := os.ReadFile(indexPath)
	if err != nil {
		return NameTable{}, err
	}
	bs := byteSlice(b)
	toc, err := index.NewTOCFromByteSlice(bs)
	if err != nil {
		return NameTable{}, err
	}

	var t NameTable
	prevPos, prevIsName := -1, false
	err = index.ReadPostingsOffsetTable(bs, toc.PostingsTable, func(name, _ []byte, _ uint64, pos int) error {
		if prevPos >= 0 && prevIsName {
			t.Bytes += int64(pos - prevPos)
		}
		prevPos, prevIsName = pos, string(name) == "__name__"
		if prevIsName {
			t.Names++
		}
		return nil
	})
	if err != nil {
		return NameTable{}, err
	}

	// The table is a 4-byte length, the content, and a 4-byte CRC; entry
	// positions count from the start of the content.
	contentLen := int(uint32(b[toc.PostingsTable])<<24 | uint32(b[toc.PostingsTable+1])<<16 | uint32(b[toc.PostingsTable+2])<<8 | uint32(b[toc.PostingsTable+3]))
	if prevPos >= 0 && prevIsName {
		t.Bytes += int64(contentLen - prevPos)
	}
	t.TableBytes = int64(contentLen)
	return t, nil
}

type byteSlice []byte

func (b byteSlice) Len() int                    { return len(b) }
func (b byteSlice) Range(start, end int) []byte { return b[start:end] }
