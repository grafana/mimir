// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
)

type SymbolizedLabel struct {
	Name, Value uint32
}

type SymbolsTable interface {
	Symbolize(str string) uint32
	SymbolizeLabels(lbls []labels.Label, buf []SymbolizedLabel) []SymbolizedLabel
	DesymbolizeLabels(symbolizedLabels []SymbolizedLabel, b *labels.ScratchBuilder, sort bool) (labels.Labels, error)
	Reset()
}

type StringMapSymbolsTable struct {
	strings    []string
	symbolsMap map[string]uint32
}

// NewStringMapSymbolsTable returns a symbol table.
func NewStringMapSymbolsTable() *StringMapSymbolsTable {
	return &StringMapSymbolsTable{
		// Empty string is required as a first element.
		symbolsMap: map[string]uint32{"": 0},
		strings:    []string{""},
	}
}

func (t *StringMapSymbolsTable) Symbolize(str string) uint32 {
	if ref, ok := t.symbolsMap[str]; ok {
		return ref
	}
	ref := uint32(len(t.strings))
	t.strings = append(t.strings, str)
	t.symbolsMap[str] = ref
	return ref
}

func (t *StringMapSymbolsTable) SymbolizeLabels(lbls []labels.Label, buf []SymbolizedLabel) []SymbolizedLabel {
	result := buf[:0]
	for _, l := range lbls {
		result = append(result, SymbolizedLabel{
			Name:  t.Symbolize(l.Name),
			Value: t.Symbolize(l.Value),
		})
	}

	return result
}

func (t *StringMapSymbolsTable) Desymbolize(ref uint32) (string, error) {
	idx := int(ref)
	if idx >= len(t.strings) {
		return "", errors.New("symbols ref out of range")
	}
	return t.strings[idx], nil
}

func (t *StringMapSymbolsTable) DesymbolizeLabels(
	symbolizedLabels []SymbolizedLabel, b *labels.ScratchBuilder, sort bool,
) (labels.Labels, error) {
	b.Reset()
	for i := 0; i < len(symbolizedLabels); i++ {
		nameRef := symbolizedLabels[i].Name
		valueRef := symbolizedLabels[i].Value
		if int(nameRef) >= len(t.strings) || int(valueRef) >= len(t.strings) {
			return labels.EmptyLabels(), errors.New("symbols ref out of range")
		}
		b.Add(t.strings[nameRef], t.strings[valueRef])
	}
	if sort {
		b.Sort()
	}
	return b.Labels(), nil
}

// Reset clears symbols table.
func (t *StringMapSymbolsTable) Reset() {
	// NOTE: Make sure to keep empty symbol.
	t.strings = t.strings[:1]
	for k := range t.symbolsMap {
		if k == "" {
			continue
		}
		delete(t.symbolsMap, k)
	}
}

// SyncMapSymbolsTable is a SymbolsTable implementation which can be used concurrently.
// Re-using the SyncMapSymbolsTable across goroutines for concurrent label materialization may reduce in-use memory.
// The sync.Map is optimized to offer write-once-read-many usage with minimal lock contention.
type SyncMapSymbolsTable struct {
	strings    []string
	symbolsMap *sync.Map
}

// NewSyncMapSymbolsTable returns a symbol table.
func NewSyncMapSymbolsTable() *SyncMapSymbolsTable {
	m := &sync.Map{}
	// Empty string is required as a first element.
	m.Store("", 0)

	return &SyncMapSymbolsTable{
		symbolsMap: m,
		strings:    []string{""},
	}
}

func (t *SyncMapSymbolsTable) Symbolize(str string) uint32 {
	if ref, ok := t.symbolsMap.Load(str); ok {
		return ref.(uint32)
	}
	ref := uint32(len(t.strings))
	t.strings = append(t.strings, str)
	t.symbolsMap.Store(str, ref)
	return ref
}

func (t *SyncMapSymbolsTable) SymbolizeLabels(lbls []labels.Label, buf []SymbolizedLabel) []SymbolizedLabel {
	result := buf[:0]
	for _, l := range lbls {
		result = append(result, SymbolizedLabel{
			Name:  t.Symbolize(l.Name),
			Value: t.Symbolize(l.Value),
		})
	}

	return result
}

func (t *SyncMapSymbolsTable) Desymbolize(ref uint32) (string, error) {
	idx := int(ref)
	if idx >= len(t.strings) {
		return "", errors.New("symbols ref out of range")
	}
	return t.strings[idx], nil
}

func (t *SyncMapSymbolsTable) DesymbolizeLabels(
	symbolizedLabels []SymbolizedLabel, b *labels.ScratchBuilder, sort bool,
) (labels.Labels, error) {
	b.Reset()
	for i := 0; i < len(symbolizedLabels); i++ {
		nameRef := symbolizedLabels[i].Name
		valueRef := symbolizedLabels[i].Value
		if int(nameRef) >= len(t.strings) || int(valueRef) >= len(t.strings) {
			return labels.EmptyLabels(), errors.New("symbols ref out of range")
		}
		b.Add(t.strings[nameRef], t.strings[valueRef])
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
	t.strings = t.strings[:1]
}
