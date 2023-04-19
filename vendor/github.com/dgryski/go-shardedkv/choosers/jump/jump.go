// Package jump is a chooser using Google's Jump Consistent Hash.  It requires a 64-bit hash function for the string-to-uint64 mapping.  Good candidates are siphash, spooky, cityhash and farmhash.
package jump

import "github.com/dgryski/go-jump"

type Jump struct {
	hasher func([]byte) uint64
	nodes  []string
}

func New(h func([]byte) uint64) *Jump {
	return &Jump{hasher: h}
}

func (j *Jump) SetBuckets(buckets []string) error {
	if len(j.nodes) != 0 {
		j.nodes = j.nodes[:0]
	}
	j.nodes = append(j.nodes, buckets...)
	return nil
}

func (j *Jump) Choose(key string) string {
	return j.nodes[jump.Hash(j.hasher([]byte(key)), len(j.nodes))]
}

func (j *Jump) ChooseReplicas(key string, n int) []string {

	// error instead of panic?
	if n > len(j.nodes) {
		panic("jump: too many replicas requested")
	}

	hkey := j.hasher([]byte(key))

	buckets := make([]int, len(j.nodes))
	for i := range buckets {
		buckets[i] = i
	}

	replicas := make([]string, n)

	for i := 0; i < n; i++ {
		b := jump.Hash(hkey, len(buckets))

		replicas[i] = j.nodes[buckets[b]]

		buckets[b], buckets = buckets[len(buckets)-1], buckets[:len(buckets)-1]

		hkey = xorshiftMult64(hkey)
	}

	return replicas
}

func (j *Jump) Buckets() []string { return j.nodes }

// 64-bit xorshift multiply rng from http://vigna.di.unimi.it/ftp/papers/xorshift.pdf
func xorshiftMult64(x uint64) uint64 {
	x ^= x >> 12 // a
	x ^= x << 25 // b
	x ^= x >> 27 // c
	return x * 2685821657736338717
}
