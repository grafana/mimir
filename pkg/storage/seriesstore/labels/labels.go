// SPDX-License-Identifier: AGPL-3.0-only

// Package labels keeps series labels in one shared buffer: the store keeps millions of series,
// most with the same few dozen label names, so names are ids into a process-wide table and each
// value is stored once per series, without the per-label headers of a slice of strings.
package labels

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

// table is an immutable snapshot of the interned names. Names are only ever added, so readers
// load it once and never lock: queries resolve every label they read, and trackers and cost
// attribution look up names no series has on every sample.
type table struct {
	ids   map[string]uint32
	names []string
}

var (
	current atomic.Pointer[table]
	// Serializes additions, which copy the table: tenants use a bounded set of names, so this
	// happens a few thousand times per process.
	internLock sync.Mutex
	// Counts how often a lookup had to take internLock, for tests.
	indexLocks atomic.Uint64
)

func init() {
	current.Store(&table{ids: map[string]uint32{}})
}

// Intern returns the id of name, adding it to the table when new.
func Intern(name string) uint32 {
	if id, ok := current.Load().ids[name]; ok {
		return id
	}
	indexLocks.Add(1)
	internLock.Lock()
	defer internLock.Unlock()
	old := current.Load()
	if id, ok := old.ids[name]; ok {
		return id
	}
	// Copied, so the table never aliases a caller's buffer.
	owned := strings.Clone(name)
	id := uint32(len(old.names))
	ids := make(map[string]uint32, len(old.ids)+1)
	for name, id := range old.ids {
		ids[name] = id
	}
	ids[owned] = id
	names := make([]string, len(old.names), len(old.names)+1)
	copy(names, old.names)
	current.Store(&table{ids: ids, names: append(names, owned)})
	return id
}

// Lookup returns the id of name when some series has it. It never locks.
func Lookup(name string) (uint32, bool) {
	id, ok := current.Load().ids[name]
	return id, ok
}

// Name returns the name of id.
func Name(id uint32) string {
	return TakeSnapshot().Name(id)
}

// Snapshot is the table as of now. Comparing a series' labels resolves every one of its names,
// which then synchronizes once rather than for each.
type Snapshot struct {
	t *table
}

func TakeSnapshot() Snapshot {
	return Snapshot{t: current.Load()}
}

func (s Snapshot) Name(id uint32) string {
	if int(id) >= len(s.t.names) {
		panic(fmt.Sprintf("unknown label name id %d", id))
	}
	return s.t.names[id]
}

// Labels are a series' labels, sorted by name: varint name ids and length-prefixed values in one
// immutable string, so copies share it and it compares, hashes and keys maps as bytes.
type Labels string

// FromSorted encodes pairs, which must be sorted by name without duplicate names.
func FromSorted(pairs [][2]string) Labels {
	size := 0
	for _, pair := range pairs {
		size += 2*binary.MaxVarintLen32 + len(pair[1])
	}
	bytes := make([]byte, 0, size)
	for _, pair := range pairs {
		bytes = PutVarint(bytes, uint64(Intern(pair[0])))
		bytes = PutVarint(bytes, uint64(len(pair[1])))
		bytes = append(bytes, pair[1]...)
	}
	return Labels(unsafe.String(unsafe.SliceData(bytes), len(bytes)))
}

// FromStrings encodes alternating names and values, which must be sorted by name.
func FromStrings(pairs ...string) Labels {
	sorted := make([][2]string, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		sorted = append(sorted, [2]string{pairs[i], pairs[i+1]})
	}
	return FromSorted(sorted)
}

// Iter walks labels in order.
type Iter struct {
	rest  string
	names Snapshot
}

func (l Labels) Iter() Iter {
	return Iter{rest: string(l), names: TakeSnapshot()}
}

// Next returns the next label, or ok false after the last.
func (it *Iter) Next() (name, value string, ok bool) {
	if len(it.rest) == 0 {
		return "", "", false
	}
	id := takeVarintString(&it.rest)
	size := takeVarintString(&it.rest)
	value = it.rest[:size]
	it.rest = it.rest[size:]
	return it.names.Name(uint32(id)), value, true
}

// Range calls visit for each label in order.
func (l Labels) Range(visit func(name, value string)) {
	it := l.Iter()
	for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
		visit(name, value)
	}
}

// Pairs returns the labels as pairs.
func (l Labels) Pairs() [][2]string {
	var pairs [][2]string
	l.Range(func(name, value string) { pairs = append(pairs, [2]string{name, value}) })
	return pairs
}

// EqPairs reports whether these are pairs, in order. The store checks this for every series it
// appends to, on short names and values, where a full comparison per string cost more.
func (l Labels) EqPairs(pairs [][2]string) bool {
	it := l.Iter()
	for _, pair := range pairs {
		name, value, ok := it.Next()
		if !ok || !ShortEq(name, pair[0]) || !ShortEq(value, pair[1]) {
			return false
		}
	}
	_, _, more := it.Next()
	return !more
}

func (l Labels) Len() int {
	n := 0
	it := l.Iter()
	for _, _, ok := it.Next(); ok; _, _, ok = it.Next() {
		n++
	}
	return n
}

func (l Labels) IsEmpty() bool { return len(l) == 0 }

// Get returns the value of name, empty when absent like Prometheus.
func (l Labels) Get(name string) string {
	id, ok := Lookup(name)
	if !ok {
		return ""
	}
	return l.ValueOf(id)
}

// ValueOf returns the value of the label with name id, empty when absent.
func (l Labels) ValueOf(id uint32) string {
	// Walks by index rather than through takeVarintString: this runs for every candidate series of
	// every query, and almost every name id and value length fits in one varint byte.
	s := string(l)
	for i := 0; i < len(s); {
		var name uint64
		if c := s[i]; c < 0x80 {
			name, i = uint64(c), i+1
		} else {
			rest := s[i:]
			name = takeVarintString(&rest)
			i = len(s) - len(rest)
		}
		var size uint64
		if c := s[i]; c < 0x80 {
			size, i = uint64(c), i+1
		} else {
			rest := s[i:]
			size = takeVarintString(&rest)
			i = len(s) - len(rest)
		}
		if uint32(name) == id {
			return s[i : i+int(size)]
		}
		i += int(size)
	}
	return ""
}

// HeapSize returns the heap bytes of the buffer, with the string header that references it.
func (l Labels) HeapSize() int {
	return 16 + len(l)
}

// Compare orders labels by name then value, pair by pair, like Prometheus.
func Compare(a, b Labels) int {
	ai, bi := a.Iter(), b.Iter()
	for {
		an, av, aok := ai.Next()
		bn, bv, bok := bi.Next()
		switch {
		case !aok && !bok:
			return 0
		case !aok:
			return -1
		case !bok:
			return 1
		}
		if c := strings.Compare(an, bn); c != 0 {
			return c
		}
		if c := strings.Compare(av, bv); c != 0 {
			return c
		}
	}
}

func (l Labels) String() string {
	var b strings.Builder
	b.WriteByte('[')
	first := true
	l.Range(func(name, value string) {
		if !first {
			b.WriteString(", ")
		}
		first = false
		fmt.Fprintf(&b, "(%q, %q)", name, value)
	})
	b.WriteByte(']')
	return b.String()
}

// ShortEq compares strings of up to 16 bytes with two overlapping loads, which covers every byte
// for less than a byte-by-byte comparison of short names and values.
func ShortEq(a, b string) bool {
	n := len(a)
	if n != len(b) {
		return false
	}
	switch {
	case n == 0:
		return true
	case n <= 3:
		return a[0] == b[0] && a[n/2] == b[n/2] && a[n-1] == b[n-1]
	case n <= 7:
		return load32(a, 0) == load32(b, 0) && load32(a, n-4) == load32(b, n-4)
	case n <= 16:
		return load64(a, 0) == load64(b, 0) && load64(a, n-8) == load64(b, n-8)
	default:
		return a == b
	}
}

func load32(s string, at int) uint32 {
	return binary.LittleEndian.Uint32(unsafe.Slice(unsafe.StringData(s[at:]), 4))
}

func load64(s string, at int) uint64 {
	return binary.LittleEndian.Uint64(unsafe.Slice(unsafe.StringData(s[at:]), 8))
}

// PutVarint appends value as an unsigned LEB128 varint.
func PutVarint(bytes []byte, value uint64) []byte {
	for value >= 0x80 {
		bytes = append(bytes, byte(value)|0x80)
		value >>= 7
	}
	return append(bytes, byte(value))
}

// TakeVarint decodes an unsigned varint from the start of bytes and advances it. Like the Rust
// version it panics on truncated input, which only corrupt data produces.
func TakeVarint(bytes *[]byte) uint64 {
	b := *bytes
	var value uint64
	var shift uint
	for i := 0; ; i++ {
		c := b[i]
		value |= uint64(c&0x7f) << shift
		if c < 0x80 {
			*bytes = b[i+1:]
			return value
		}
		shift += 7
	}
}

func takeVarintString(s *string) uint64 {
	b := *s
	var value uint64
	var shift uint
	for i := 0; ; i++ {
		c := b[i]
		value |= uint64(c&0x7f) << shift
		if c < 0x80 {
			*s = b[i+1:]
			return value
		}
		shift += 7
	}
}

// ValueHash is the hash postings key label values by.
func ValueHash(value string) uint64 {
	return xxhash.Sum64String(value)
}

// HashPairs is Go's `labels.Hash` of stringlabels, which keys series in the store: xxhash64 of the
// size-prefixed names and values. The buffer is reused because ingest hashes every series.
func HashPairs(pairs [][2]string) uint64 {
	buf := hashBuffers.Get().(*[]byte)
	b := (*buf)[:0]
	for _, pair := range pairs {
		b = appendLabelSize(b, len(pair[0]))
		b = append(b, pair[0]...)
		b = appendLabelSize(b, len(pair[1]))
		b = append(b, pair[1]...)
	}
	h := xxhash.Sum64(b)
	*buf = b
	hashBuffers.Put(buf)
	return h
}

// Hash is HashPairs of the labels.
func (l Labels) Hash() uint64 {
	buf := hashBuffers.Get().(*[]byte)
	b := (*buf)[:0]
	l.Range(func(name, value string) {
		b = appendLabelSize(b, len(name))
		b = append(b, name...)
		b = appendLabelSize(b, len(value))
		b = append(b, value...)
	})
	h := xxhash.Sum64(b)
	*buf = b
	hashBuffers.Put(buf)
	return h
}

func appendLabelSize(b []byte, size int) []byte {
	if size < 255 {
		return append(b, byte(size))
	}
	return append(b, 255, byte(size), byte(size>>8), byte(size>>16))
}

// LabelSet is a series' labels as trackers, cost attribution and query sharding read them.
type LabelSet interface {
	// Get returns the value of name, empty when absent.
	Get(name string) string
	Range(visit func(name, value string))
}

// StableHash is Go's `labels.StableHash`, by which the head shards queries and Mimir's ingest
// pusher routes series.
func StableHash(labels LabelSet) uint64 {
	buf := hashBuffers.Get().(*[]byte)
	b := (*buf)[:0]
	labels.Range(func(name, value string) {
		b = append(b, name...)
		b = append(b, 0xff)
		b = append(b, value...)
		b = append(b, 0xff)
	})
	h := xxhash.Sum64(b)
	*buf = b
	hashBuffers.Put(buf)
	return h
}

// StableHashPairs is StableHash of pairs sorted by name.
func StableHashPairs(pairs [][2]string) uint64 {
	return StableHash(Pairs(pairs))
}

var hashBuffers = sync.Pool{New: func() any { b := make([]byte, 0, 1024); return &b }}

// Pairs are labels as pairs sorted by name.
type Pairs [][2]string

func (p Pairs) Get(name string) string {
	lo, hi := 0, len(p)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if p[mid][0] < name {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(p) && p[lo][0] == name {
		return p[lo][1]
	}
	return ""
}

func (p Pairs) Range(visit func(name, value string)) {
	for _, pair := range p {
		visit(pair[0], pair[1])
	}
}
