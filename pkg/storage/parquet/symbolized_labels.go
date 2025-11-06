// SPDX-License-Identifier: AGPL-3.0-only

package parquet

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/search"
	"github.com/prometheus/prometheus/model/labels"
)

type StringMapSymbolsTable struct {
	strings    []string
	symbolsMap map[string]uint32
}

// SyncMapSymbolsTable is a SymbolsTable implementation which can be used concurrently.
// Re-using the SyncMapSymbolsTable across goroutines for concurrent label materialization may reduce in-use memory.
// The sync.Map is optimized to offer write-once-read-many usage with minimal lock contention.
type SyncMapSymbolsTable struct {
	strings    []string
	stringsMu  *sync.RWMutex
	symbolsMap *sync.Map
}

// NewSyncMapSymbolsTable returns a symbol table.
func NewSyncMapSymbolsTable() *SyncMapSymbolsTable {
	m := &sync.Map{}
	// Empty string is required as a first element.
	m.Store("", 0)

	return &SyncMapSymbolsTable{
		strings:    []string{""},
		stringsMu:  &sync.RWMutex{},
		symbolsMap: m,
	}
}

func (t *SyncMapSymbolsTable) Symbolize(str string) uint32 {
	if ref, ok := t.symbolsMap.Load(str); ok {
		return ref.(uint32)
	}
	t.stringsMu.RLock()
	ref := uint32(len(t.strings))
	t.stringsMu.RUnlock()
	t.stringsMu.Lock()
	t.strings = append(t.strings, str)
	t.stringsMu.Unlock()
	t.symbolsMap.Store(str, ref)
	return ref
}

func (t *SyncMapSymbolsTable) SymbolizeLabels(lbls []labels.Label, buf []search.SymbolizedLabel) []search.SymbolizedLabel {
	result := buf[:0]
	for _, l := range lbls {
		result = append(result, search.SymbolizedLabel{
			Name:  t.Symbolize(l.Name),
			Value: t.Symbolize(l.Value),
		})
	}

	return result
}

func (t *SyncMapSymbolsTable) Desymbolize(ref uint32) (string, error) {
	idx := int(ref)
	t.stringsMu.RLock()
	defer t.stringsMu.RUnlock()
	if idx >= len(t.strings) {
		return "", errors.New("symbols ref out of range")
	}
	return t.strings[idx], nil
}

func (t *SyncMapSymbolsTable) DesymbolizeLabels(
	symbolizedLabels []search.SymbolizedLabel, b *labels.ScratchBuilder, sort bool,
) (labels.Labels, error) {
	b.Reset()
	for i := 0; i < len(symbolizedLabels); i++ {
		nameRef := symbolizedLabels[i].Name
		valueRef := symbolizedLabels[i].Value
		t.stringsMu.RLock()
		if int(nameRef) >= len(t.strings) || int(valueRef) >= len(t.strings) {
			t.stringsMu.RUnlock()
			return labels.EmptyLabels(), errors.New("symbols ref out of range")
		}
		b.Add(t.strings[nameRef], t.strings[valueRef])
		t.stringsMu.RUnlock()
	}
	if sort {
		b.Sort()
	}
	return b.Labels(), nil
}

// Reset clears symbols table.
func (t *SyncMapSymbolsTable) Reset() {
	t.symbolsMap.Clear()
	// NOTE: Make sure to keep empty symbol.
	t.symbolsMap.Store("", 0)
	t.stringsMu.Lock()
	t.strings = t.strings[:1]
	t.stringsMu.Unlock()
}
