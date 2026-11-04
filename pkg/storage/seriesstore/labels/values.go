// SPDX-License-Identifier: AGPL-3.0-only
package labels

import (
	"strings"
	"sync"
	"sync/atomic"
)

const (
	maxValueNames         = 512
	maxValueNameID        = 4096
	maxValuesPerName      = 256
	maxInternedValueBytes = 8 << 20
	maxInternedValues     = 65536
)

type valueTable struct {
	ids    map[string]uint32
	values []string
}
type valueRegistry struct {
	tables              []*valueTable
	names, count, bytes int
}

var valueTables atomic.Pointer[valueRegistry]
var valueLock sync.Mutex

func init() { valueTables.Store(&valueRegistry{}) }

func knownValue(name uint32, value string) (uint32, bool) {
	r := valueTables.Load()
	if int(name) >= len(r.tables) || r.tables[name] == nil {
		return 0, false
	}
	id, ok := r.tables[name].ids[value]
	return id, ok
}
func internValue(name uint32, value string) (uint32, bool) {
	if len(value) < 8 || len(value) > 256 || name >= maxValueNameID {
		return 0, false
	}
	snapshot := valueTables.Load()
	if int(name) < len(snapshot.tables) && snapshot.tables[name] != nil {
		table := snapshot.tables[name]
		if id, ok := table.ids[value]; ok {
			return id, true
		}
		if len(table.values) >= maxValuesPerName {
			return 0, false
		}
	} else if snapshot.names >= maxValueNames {
		return 0, false
	}
	// Exhausted dictionaries are immutable; high-cardinality labels must stay lock-free.
	if snapshot.count >= maxInternedValues || snapshot.bytes+len(value) > maxInternedValueBytes {
		return 0, false
	}
	valueLock.Lock()
	defer valueLock.Unlock()
	old := valueTables.Load()
	var table *valueTable
	if int(name) < len(old.tables) {
		table = old.tables[name]
	}
	if table != nil {
		if id, ok := table.ids[value]; ok {
			return id, true
		}
		if len(table.values) >= maxValuesPerName {
			return 0, false
		}
	} else if old.names >= maxValueNames {
		return 0, false
	}
	if old.count >= maxInternedValues || old.bytes+len(value) > maxInternedValueBytes {
		return 0, false
	}
	next := &valueRegistry{tables: make([]*valueTable, max(len(old.tables), int(name)+1)), names: old.names, count: old.count + 1, bytes: old.bytes + len(value)}
	copy(next.tables, old.tables)
	fresh := &valueTable{ids: map[string]uint32{}}
	if table != nil {
		fresh.values = make([]string, len(table.values), len(table.values)+1)
		copy(fresh.values, table.values)
		for value, id := range table.ids {
			fresh.ids[value] = id
		}
	} else {
		next.names++
	}
	owned := strings.Clone(value)
	id := uint32(len(fresh.values))
	fresh.values = append(fresh.values, owned)
	fresh.ids[owned] = id
	next.tables[name] = fresh
	// IDs never change or disappear: encoded labels remain valid across retention and snapshots.
	valueTables.Store(next)
	return id, true
}
func sharedValue(name uint32, id uint64) string { return valueTables.Load().tables[name].values[id] }

// AppendKnownPair matches FromSorted without admitting values or retaining the input buffer.
func AppendKnownPair(dst []byte, name uint32, value string) []byte {
	dst = PutVarint(dst, uint64(name))
	tag := KnownValueTag(name, value)
	dst = PutVarint(dst, tag)
	if tag&1 == 0 {
		dst = append(dst, value...)
	}
	return dst
}

// KnownValueTag can be cached while preparing a record's repeated symbol pairs.
func KnownValueTag(name uint32, value string) uint64 {
	if id, ok := knownValue(name, value); ok {
		return uint64(id)<<1 | 1
	}
	return uint64(len(value)) << 1
}

// AppendCanonical writes process-independent values for persistence; name IDs are supplied by the snapshot table.
func (l Labels) AppendCanonical(dst []byte) []byte {
	l.RangeIDs(func(id uint32, value string) bool {
		dst = PutVarint(dst, uint64(id))
		dst = PutVarint(dst, uint64(len(value)))
		dst = append(dst, value...)
		return true
	})
	return dst
}

// TakeValue consumes the value following a name ID in the in-memory encoding.
func TakeValue(name uint32, rest *string) string {
	tag := takeVarintString(rest)
	if tag&1 != 0 {
		return sharedValue(name, tag>>1)
	}
	size := tag >> 1
	value := (*rest)[:size]
	*rest = (*rest)[size:]
	return value
}
