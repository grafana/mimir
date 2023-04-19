// Package rendezvous is a chooser using rendezvous hashing
package rendezvous

import (
	"github.com/dgryski/go-metro"
	"github.com/dgryski/go-rendezvous"
)

type Rendezvous struct {
	r     *rendezvous.Rendezvous
	nodes []string
}

func New() *Rendezvous {
	return &Rendezvous{}
}

func metrohasher(s string) uint64 {
	return metro.Hash64Str(s, 0)
}

func (r *Rendezvous) SetBuckets(buckets []string) error {
	if len(r.nodes) > 0 {
		r.nodes = r.nodes[:0]
	}

	r.nodes = append(r.nodes, buckets...)
	r.r = rendezvous.New(buckets, metrohasher)

	return nil
}

func (r *Rendezvous) Choose(k string) string {
	node := r.r.Lookup(k)
	return node
}

func (r *Rendezvous) Buckets() []string { return r.nodes }
