// Package ketama is a chooser wrapping github.com/dgryski/go-ketama's consistent hash implementation
package ketama

import (
	"github.com/dgryski/go-ketama"
)

type Ketama struct {
	k *ketama.Continuum
	s []string
}

func New() *Ketama {
	return &Ketama{k: nil, s: nil}
}

func (k *Ketama) SetBuckets(buckets []string) error {
	b := make([]ketama.Bucket, len(buckets))

	for i, s := range buckets {
		b[i].Label = s
		b[i].Weight = 1
	}

	ket, err := ketama.New(b)
	if err != nil {
		return err
	}
	k.k = ket
	k.s = buckets
	return nil
}

func (k *Ketama) Choose(key string) string { return k.k.Hash(key) }

func (k *Ketama) ChooseReplicas(key string, n int) []string { return k.k.HashMultiple(key, n) }

func (k *Ketama) Buckets() []string { return k.s }
