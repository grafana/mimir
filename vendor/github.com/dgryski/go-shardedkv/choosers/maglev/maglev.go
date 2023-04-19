// Package maglev is a chooser using Google's Maglev Consistent Hash.
package maglev

import (
	"errors"

	"github.com/dchest/siphash"
	"github.com/dgryski/go-maglev"
)

type Maglev struct {
	t     *maglev.Table
	nodes []string
}

func New() *Maglev {
	return &Maglev{}
}

func (m *Maglev) SetBuckets(buckets []string) error {
	if len(m.nodes) > 0 {
		m.nodes = m.nodes[:0]
	}
	m.nodes = append(m.nodes, buckets...)

	var n uint64
	switch {
	case len(buckets)*100 < maglev.SmallM:
		n = maglev.SmallM
	case len(buckets)*100 < maglev.BigM:
		n = maglev.BigM
	default:
		return errors.New("maglev: too many buckets")
	}

	m.t = maglev.New(buckets, n)

	return nil
}

func (m *Maglev) Choose(key string) string {
	k := siphash.Hash(0, 0, []byte(key))
	node := m.t.Lookup(k)
	return m.nodes[node]
}

func (m *Maglev) Buckets() []string { return m.nodes }
