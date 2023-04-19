// Package maglev implements maglev consistent hashing
/*
   http://research.google.com/pubs/pub44824.html
*/
package maglev

import (
	"github.com/dchest/siphash"
)

const (
	SmallM = 65537
	BigM   = 655373
)

type Table struct {
	n      int
	lookup []int
	m      uint64

	// currentOffsets saves the current offset of i index
	currentOffsets []uint64
	// skips saves the skips of i index
	skips []uint64

	// originOffsets saves the first currentOffsets
	originOffsets []uint64
}

func New(names []string, m uint64) *Table {
	offsets, skips := generateOffsetAndSkips(names, m)
	t := &Table{
		n:              len(names),
		skips:          skips,
		currentOffsets: offsets,
		originOffsets:  make([]uint64, len(names)),
		m:              m,
	}

	// save first currentOffsets to originOffsets, for reset
	copy(t.originOffsets, t.currentOffsets)
	t.lookup = t.populate(m, nil)

	return t
}

func (t *Table) Lookup(key uint64) int {
	return t.lookup[key%uint64(len(t.lookup))]
}

func (t *Table) Rebuild(dead []int) {
	t.lookup = t.populate(t.m, dead)
}

// generateOffsetAndSkips generates the first offset and skip, which is used to generate hash sequence
func generateOffsetAndSkips(names []string, M uint64) ([]uint64, []uint64) {
	offsets := make([]uint64, len(names))
	skips := make([]uint64, len(names))

	for i, name := range names {
		b := []byte(name)
		h := siphash.Hash(0xdeadbeefcafebabe, 0, b)
		offsets[i], skips[i] = (h>>32)%M, ((h&0xffffffff)%(M-1) + 1)
	}

	return offsets, skips
}

func (t *Table) populate(M uint64, dead []int) []int {
	t.resetOffsets()
	N := len(t.currentOffsets)

	entry := make([]int, M)
	for j := range entry {
		entry[j] = -1
	}

	var n uint64
	for {
		d := dead
		for i := 0; i < N; i++ {
			if len(d) > 0 && d[0] == i {
				d = d[1:]
				continue
			}

			var c uint64
			t.nextOffset(i, &c)

			for entry[c] >= 0 {
				t.nextOffset(i, &c)
			}
			entry[c] = i
			n++
			if n == M {
				return entry
			}
		}
	}
}

// nextOffset generate next offset of i index, by adding skips[i]
func (t *Table) nextOffset(i int, c *uint64) {
	*c = t.currentOffsets[i]

	t.currentOffsets[i] += t.skips[i]
	if t.currentOffsets[i] >= t.m {
		t.currentOffsets[i] -= t.m
	}
}

// resetOffsets reset originOffsets to current offset
func (t *Table) resetOffsets() {
	copy(t.currentOffsets, t.originOffsets)
}
