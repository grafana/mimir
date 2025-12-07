// Copyright 2017 The Prometheus Authors
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

//go:build uniquelabels

package labels

import (
	"bytes"
	"slices"
	"strings"
	"unique"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

// Labels is a sorted set of labels. Order has to be guaranteed upon
// instantiation.
type Labels []SymbolisedLabel

type SymbolisedLabel struct {
	Name  Symbol
	Value string
}

func (s SymbolisedLabel) ToLabel() Label {
	return Label{Name: s.Name.String(), Value: s.Value}
}

type Symbol unique.Handle[string]

func NewSymbol(s string) Symbol {
	return Symbol(unique.Make(s))
}

func (s Symbol) String() string {
	return unique.Handle[string](s).Value()
}

// TODO: move this somewhere else
var MetricNameSymbol = NewSymbol(MetricName)
var EmptySymbol = NewSymbol("")

func (ls Labels) Len() int           { return len(ls) }
func (ls Labels) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }
func (ls Labels) Less(i, j int) bool { return ls[i].Name.String() < ls[j].Name.String() }

// Bytes returns an opaque, not-human-readable, encoding of ls, usable as a map key.
// Encoding may change over time or between runs of Prometheus.
func (ls Labels) Bytes(buf []byte) []byte {
	b := bytes.NewBuffer(buf[:0])
	b.WriteByte(labelSep)
	for i, l := range ls {
		if i > 0 {
			b.WriteByte(sep)
		}
		b.WriteString(l.Name.String())
		b.WriteByte(sep)
		b.WriteString(l.Value)
	}
	return b.Bytes()
}

// MatchLabels returns a subset of Labels that matches/does not match with the provided label names based on the 'on' boolean.
// If on is set to true, it returns the subset of labels that match with the provided label names and its inverse when 'on' is set to false.
func (ls Labels) MatchLabels(on bool, names ...string) Labels {
	matchedLabels := Labels{}

	nameSet := make(map[string]struct{}, len(names))
	for _, n := range names {
		// TODO: convert to symbol here?
		nameSet[n] = struct{}{}
	}

	for _, v := range ls {
		if _, ok := nameSet[v.Name.String()]; on == ok && (on || v.Name != MetricNameSymbol) {
			matchedLabels = append(matchedLabels, v)
		}
	}

	return matchedLabels
}

// Hash returns a hash value for the label set.
// Note: the result is not guaranteed to be consistent across different runs of Prometheus.
func (ls Labels) Hash() uint64 {
	// Use xxhash.Sum64(b) for fast path as it's faster.
	b := make([]byte, 0, 1024)
	for i, v := range ls {
		if len(b)+len(v.Name.String())+len(v.Value)+2 >= cap(b) {
			// If labels entry is 1KB+ do not allocate whole entry.
			h := xxhash.New()
			_, _ = h.Write(b)
			for _, v := range ls[i:] {
				_, _ = h.WriteString(v.Name.String())
				_, _ = h.Write(seps)
				_, _ = h.WriteString(v.Value)
				_, _ = h.Write(seps)
			}
			return h.Sum64()
		}

		b = append(b, v.Name.String()...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b)
}

// HashForLabels returns a hash value for the labels matching the provided names.
// 'names' have to be sorted in ascending order.
func (ls Labels) HashForLabels(b []byte, names ...string) (uint64, []byte) {
	b = b[:0]
	i, j := 0, 0
	for i < len(ls) && j < len(names) {
		switch {
		case names[j] < ls[i].Name.String():
			j++
		case ls[i].Name.String() < names[j]:
			i++
		default:
			b = append(b, ls[i].Name.String()...)
			b = append(b, sep)
			b = append(b, ls[i].Value...)
			b = append(b, sep)
			i++
			j++
		}
	}
	return xxhash.Sum64(b), b
}

// HashWithoutLabels returns a hash value for all labels except those matching
// the provided names.
// 'names' have to be sorted in ascending order.
func (ls Labels) HashWithoutLabels(b []byte, names ...string) (uint64, []byte) {
	b = b[:0]
	j := 0
	for i := range ls {
		for j < len(names) && names[j] < ls[i].Name.String() {
			j++
		}
		if ls[i].Name == MetricNameSymbol || (j < len(names) && ls[i].Name.String() == names[j]) {
			continue
		}
		b = append(b, ls[i].Name.String()...)
		b = append(b, sep)
		b = append(b, ls[i].Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b), b
}

// BytesWithLabels is just as Bytes(), but only for labels matching names.
// 'names' have to be sorted in ascending order.
func (ls Labels) BytesWithLabels(buf []byte, names ...string) []byte {
	b := bytes.NewBuffer(buf[:0])
	b.WriteByte(labelSep)
	i, j := 0, 0
	for i < len(ls) && j < len(names) {
		switch {
		case names[j] < ls[i].Name.String():
			j++
		case ls[i].Name.String() < names[j]:
			i++
		default:
			if b.Len() > 1 {
				b.WriteByte(sep)
			}
			b.WriteString(ls[i].Name.String())
			b.WriteByte(sep)
			b.WriteString(ls[i].Value)
			i++
			j++
		}
	}
	return b.Bytes()
}

// BytesWithoutLabels is just as Bytes(), but only for labels not matching names.
// 'names' have to be sorted in ascending order.
func (ls Labels) BytesWithoutLabels(buf []byte, names ...string) []byte {
	b := bytes.NewBuffer(buf[:0])
	b.WriteByte(labelSep)
	j := 0
	for i := range ls {
		for j < len(names) && names[j] < ls[i].Name.String() {
			j++
		}
		if j < len(names) && ls[i].Name.String() == names[j] {
			continue
		}
		if b.Len() > 1 {
			b.WriteByte(sep)
		}
		b.WriteString(ls[i].Name.String())
		b.WriteByte(sep)
		b.WriteString(ls[i].Value)
	}
	return b.Bytes()
}

// Copy returns a copy of the labels.
func (ls Labels) Copy() Labels {
	res := make(Labels, len(ls))
	copy(res, ls)
	return res
}

// Get returns the value for the label with the given name.
// Returns an empty string if the label doesn't exist.
func (ls Labels) Get(name string) string {
	for _, l := range ls {
		if l.Name.String() == name {
			return l.Value
		}
	}
	return ""
}

// getSymbol returns the value for the label with the given name.
// Returns an empty symbol if the label doesn't exist.
func (ls Labels) getSymbol(name Symbol) string {
	for _, l := range ls {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// Has returns true if the label with the given name is present.
func (ls Labels) Has(name string) bool {
	for _, l := range ls {
		if l.Name.String() == name {
			return true
		}
	}
	return false
}

// HasDuplicateLabelNames returns whether ls has duplicate label names.
// It assumes that the labelset is sorted.
func (ls Labels) HasDuplicateLabelNames() (string, bool) {
	for i, l := range ls {
		if i == 0 {
			continue
		}
		if l.Name == ls[i-1].Name {
			return l.Name.String(), true
		}
	}
	return "", false
}

// WithoutEmpty returns the labelset without empty labels.
// May return the same labelset.
func (ls Labels) WithoutEmpty() Labels {
	for _, v := range ls {
		if v.Value != "" {
			continue
		}
		// Do not copy the slice until it's necessary.
		els := make(Labels, 0, len(ls)-1)
		for _, v := range ls {
			if v.Value != "" {
				els = append(els, v)
			}
		}
		return els
	}
	return ls
}

// ByteSize returns the approximate size of the labels in bytes including
// the two string headers size for name and value.
// Slice header size is ignored because it should be amortized to zero.
func (ls Labels) ByteSize() uint64 {
	var size uint64
	for _, l := range ls {
		size += uint64(len(l.Name.String())+len(l.Value)) + 2*uint64(unsafe.Sizeof(""))
	}
	return size
}

// Equal returns whether the two label sets are equal.
func Equal(ls, o Labels) bool {
	return slices.Equal(ls, o)
}

// EmptyLabels returns n empty Labels value, for convenience.
func EmptyLabels() Labels {
	return Labels{}
}

// New returns a sorted Labels from the given labels.
// The caller has to guarantee that all label names are unique.
func New(ls ...Label) Labels {
	res := make(Labels, 0, len(ls))

	for _, l := range ls {
		res = append(res, SymbolisedLabel{
			Name:  NewSymbol(l.Name),
			Value: l.Value,
		})
	}

	res.sort()
	return res
}

// FromStrings creates new labels from pairs of strings.
func FromStrings(ss ...string) Labels {
	if len(ss)%2 != 0 {
		panic("invalid number of strings")
	}
	res := make(Labels, 0, len(ss)/2)
	for i := 0; i < len(ss); i += 2 {
		res = append(res, SymbolisedLabel{
			Name:  NewSymbol(ss[i]),
			Value: ss[i+1],
		})
	}

	res.sort()
	return res
}

// FromSymbols creates new labels from pairs of symbols and strings.
func FromSymbols(names []Symbol, values []string) Labels {
	if len(names) != len(values) {
		panic("invalid number of names or values")
	}
	res := make(Labels, 0, len(names))
	for i := range len(names) {
		res = append(res, SymbolisedLabel{
			Name:  names[i],
			Value: values[i],
		})
	}

	res.sort()
	return res
}

// sort sorts the labels in this label set by name.
func (ls Labels) sort() {
	slices.SortFunc(ls, func(a, b SymbolisedLabel) int { return strings.Compare(a.Name.String(), b.Name.String()) })
}

// Compare compares the two label sets.
// The result will be 0 if a==b, <0 if a < b, and >0 if a > b.
func Compare(a, b Labels) int {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}

	for i := 0; i < l; i++ {
		if a[i].Name != b[i].Name {
			if a[i].Name.String() < b[i].Name.String() {
				return -1
			}
			return 1
		}
		if a[i].Value != b[i].Value {
			if a[i].Value < b[i].Value {
				return -1
			}
			return 1
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a) - len(b)
}

// CopyFrom copies labels from b on top of whatever was in ls previously,
// reusing memory or expanding if needed.
func (ls *Labels) CopyFrom(b Labels) {
	(*ls) = append((*ls)[:0], b...)
}

// IsEmpty returns true if ls represents an empty set of labels.
func (ls Labels) IsEmpty() bool {
	return len(ls) == 0
}

// Range calls f on each label.
func (ls Labels) Range(f func(l Label)) {
	for _, l := range ls {
		f(l.ToLabel())
	}
}

// rangeSymbols calls f on each label.
func (ls Labels) rangeSymbols(f func(name Symbol, value string)) {
	for _, l := range ls {
		f(l.Name, l.Value)
	}
}

// Validate calls f on each label. If f returns a non-nil error, then it returns that error cancelling the iteration.
func (ls Labels) Validate(f func(l Label) error) error {
	for _, l := range ls {
		if err := f(l.ToLabel()); err != nil {
			return err
		}
	}
	return nil
}

// DropMetricName returns Labels with the "__name__" removed.
//
// Deprecated: Use DropReserved instead.
func (ls Labels) DropMetricName() Labels {
	return ls.dropReservedSymbols(func(n Symbol) bool { return n == MetricNameSymbol })
}

// DropReserved returns Labels without the chosen (via shouldDropFn) reserved (starting with underscore) labels.
func (ls Labels) DropReserved(shouldDropFn func(name string) bool) Labels {
	return ls.dropReservedSymbols(func(name Symbol) bool {
		return shouldDropFn(name.String())
	})
}

func (ls Labels) dropReservedSymbols(shouldDropFn func(name Symbol) bool) Labels {
	rm := 0
	for i, l := range ls {
		if l.Name.String()[0] > '_' { // Stop looking if we've gone past special labels.
			break
		}
		if shouldDropFn(l.Name) {
			i := i - rm // Offsetting after removals.
			if i == 0 { // Make common case fast with no allocations.
				ls = ls[1:]
			} else {
				// Avoid modifying original Labels - use [:i:i] so that left slice would not
				// have any spare capacity and append would have to allocate a new slice for the result.
				ls = append(ls[:i:i], ls[i+1:]...)
			}
			rm++
		}
	}
	return ls
}

// InternStrings calls intern on every string value inside ls, replacing them with what it returns.
func (ls *Labels) InternStrings(intern func(string) string) {
	for i, l := range *ls {
		(*ls)[i].Name = NewSymbol(intern(l.Name.String()))
		(*ls)[i].Value = intern(l.Value)
	}
}

// ReleaseStrings calls release on every string value inside ls.
func (ls Labels) ReleaseStrings(release func(string)) {
	for _, l := range ls {
		release(l.Name.String())
		release(l.Value)
	}
}

// Builder allows modifying Labels.
type Builder struct {
	base Labels
	del  []Symbol
	add  []SymbolisedLabel
}

// NewBuilder returns a new LabelsBuilder.
func NewBuilder(base Labels) *Builder {
	b := &Builder{
		del: make([]Symbol, 0, 5),
		add: make([]SymbolisedLabel, 0, 5),
	}
	b.Reset(base)
	return b
}

// Del deletes the label of the given name.
func (b *Builder) Del(ns ...string) *Builder {
	for _, n := range ns {
		n := NewSymbol(n)

		for i, a := range b.add {
			if a.Name == n {
				b.add = append(b.add[:i], b.add[i+1:]...)
			}
		}
		b.del = append(b.del, n)
	}
	return b
}

// Keep removes all labels from the base except those with the given names.
func (b *Builder) Keep(ns ...string) *Builder {
	b.base.rangeSymbols(func(n Symbol, v string) {
		if slices.Contains(ns, n.String()) {
			return
		}

		b.del = append(b.del, n)
	})

	return b
}

// Set the name/value pair as a label. A value of "" means delete that label.
func (b *Builder) Set(n, v string) *Builder {
	if v == "" {
		// Empty labels are the same as missing labels.
		return b.Del(n)
	}

	for i, a := range b.add {
		if a.Name.String() == n {
			b.add[i].Value = v
			return b
		}
	}

	nSymbol := NewSymbol(n)
	b.add = append(b.add, SymbolisedLabel{Name: nSymbol, Value: v})

	return b
}

func (b *Builder) Get(n string) string {
	nSymbol := NewSymbol(n)

	// Del() removes entries from .add but Set() does not remove from .del, so check .add first.
	for _, a := range b.add {
		if a.Name == nSymbol {
			return a.Value
		}
	}
	if slices.Contains(b.del, nSymbol) {
		return ""
	}

	return b.base.getSymbol(nSymbol)
}

// Range calls f on each label in the Builder.
func (b *Builder) Range(f func(l Label)) {
	// Stack-based arrays to avoid heap allocation in most cases.
	var addStack [128]SymbolisedLabel
	var delStack [128]Symbol
	// Take a copy of add and del, so they are unaffected by calls to Set() or Del().
	origAdd, origDel := append(addStack[:0], b.add...), append(delStack[:0], b.del...)
	b.base.rangeSymbols(func(n Symbol, v string) {
		if !slices.Contains(origDel, n) && !contains(origAdd, n) {
			f(Label{Name: n.String(), Value: v})
		}
	})
	for _, a := range origAdd {
		f(a.ToLabel())
	}
}

func contains(s []SymbolisedLabel, n Symbol) bool {
	for _, a := range s {
		if a.Name == n {
			return true
		}
	}
	return false
}

// Reset clears all current state for the builder.
func (b *Builder) Reset(base Labels) {
	b.base = base
	b.del = b.del[:0]
	b.add = b.add[:0]

	b.base.rangeSymbols(func(n Symbol, v string) {
		if v == "" {
			b.del = append(b.del, n)
		}
	})
}

// Labels returns the labels from the builder.
// If no modifications were made, the original labels are returned.
func (b *Builder) Labels() Labels {
	if len(b.del) == 0 && len(b.add) == 0 {
		return b.base
	}

	expectedSize := len(b.base) + len(b.add) - len(b.del)
	if expectedSize < 1 {
		expectedSize = 1
	}
	res := make(Labels, 0, expectedSize)
	for _, l := range b.base {
		if slices.Contains(b.del, l.Name) || contains(b.add, l.Name) {
			continue
		}
		res = append(res, l)
	}
	if len(b.add) > 0 { // Base is already in order, so we only need to sort if we add to it.
		res = append(res, b.add...)
		res.sort()
	}
	return res
}

// ScratchBuilder allows efficient construction of a Labels from scratch.
type ScratchBuilder struct {
	add       Labels
	unsafeAdd bool
}

// SymbolTable is no-op, just for api parity with dedupelabels.
type SymbolTable struct{}

func NewSymbolTable() *SymbolTable { return nil }

func (*SymbolTable) Len() int { return 0 }

// NewScratchBuilder creates a ScratchBuilder initialized for Labels with n entries.
func NewScratchBuilder(n int) ScratchBuilder {
	return ScratchBuilder{add: make([]SymbolisedLabel, 0, n)}
}

// NewBuilderWithSymbolTable creates a Builder, for api parity with dedupelabels.
func NewBuilderWithSymbolTable(*SymbolTable) *Builder {
	return NewBuilder(EmptyLabels())
}

// NewScratchBuilderWithSymbolTable creates a ScratchBuilder, for api parity with dedupelabels.
func NewScratchBuilderWithSymbolTable(_ *SymbolTable, n int) ScratchBuilder {
	return NewScratchBuilder(n)
}

func (*ScratchBuilder) SetSymbolTable(*SymbolTable) {
	// no-op
}

// SetUnsafeAdd allows turning on/off the assumptions that added strings are unsafe
// for reuse. ScratchBuilder implementations that do reuse strings, must clone
// the strings.
//
// SliceLabels will clone all added strings when this option is true.
func (b *ScratchBuilder) SetUnsafeAdd(unsafeAdd bool) {
	b.unsafeAdd = unsafeAdd
}

func (b *ScratchBuilder) Reset() {
	b.add = b.add[:0]
}

// Add a name/value pair.
// Note if you Add the same name twice you will get a duplicate label, which is invalid.
// If SetUnsafeAdd was set to false, the values must remain live until Labels() is called.
func (b *ScratchBuilder) Add(name, value string) {
	if b.unsafeAdd {
		// Underlying data structure for uniquelabels assumes that values are not reused.
		value = strings.Clone(value)
	}

	b.add = append(b.add, SymbolisedLabel{Name: NewSymbol(name), Value: value})
}

// Sort the labels added so far by name.
func (b *ScratchBuilder) Sort() {
	b.add.sort()
}

// Assign is for when you already have a Labels which you want this ScratchBuilder to return.
func (b *ScratchBuilder) Assign(ls Labels) {
	b.add = append(b.add[:0], ls...) // Copy on top of our slice, so we don't retain the input slice.
}

// Labels returns the name/value pairs added so far as a Labels object.
// Note: if you want them sorted, call Sort() first.
func (b *ScratchBuilder) Labels() Labels {
	// Copy the slice, so the next use of ScratchBuilder doesn't overwrite.
	return append(Labels{}, b.add...)
}

// Overwrite the newly-built Labels out to ls.
// Callers must ensure that there are no other references to ls, or any strings fetched from it.
func (b *ScratchBuilder) Overwrite(ls *Labels) {
	*ls = append((*ls)[:0], b.add...)
}

// SizeOfLabels returns the approximate space required for n copies of a label.
func SizeOfLabels(name, value string, n uint64) uint64 {
	return (uint64(len(name)) + uint64(unsafe.Sizeof(name)) + uint64(len(value)) + uint64(unsafe.Sizeof(value))) * n
}
