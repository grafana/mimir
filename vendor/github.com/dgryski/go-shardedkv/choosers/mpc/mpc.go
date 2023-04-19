// Package mpc is a chooser wrapping github.com/dgryski/go-mpchash
package mpc

import (
	"github.com/dgryski/go-mpchash"
)

type Multi struct {
	mpc *mpchash.Multi
	s   []string

	seeds [2]uint64
	hashf func(b []byte, seed uint64) uint64
	k     int
}

func New(hashf func(b []byte, seed uint64) uint64, seeds [2]uint64, k int) *Multi {
	return &Multi{seeds: seeds, hashf: hashf, k: k}
}

func (m *Multi) SetBuckets(buckets []string) error {
	m.mpc = mpchash.New(buckets, m.hashf, m.seeds, m.k)
	m.s = buckets
	return nil
}

func (m *Multi) Choose(key string) string { return m.mpc.Hash(key) }

func (m *Multi) Buckets() []string { return m.s }
