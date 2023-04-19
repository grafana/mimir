// Package ketama implements consistent hashing compatible with Algorithm::ConsistentHash::Ketama
/*
This implementation draws from the Daisuke Maki's Perl module, which itself is
based on the original libketama code.  That code was licensed under the GPLv2,
and thus so is this.

The major API change from libketama is that Algorithm::ConsistentHash::Ketama allows hashing
arbitrary strings, instead of just memcached server IP addresses.
*/
package ketama

import (
	"crypto/md5"
	"errors"
	"fmt"
	"sort"
)

type Bucket struct {
	Label  string
	Weight int
}

type continuumPoint struct {
	bucket Bucket
	point  uint
}

type HashFunc int

const (
	// Old broken hashing function
	HashFunc1 HashFunc = iota
	// `Correct` binary search
	HashFunc2
)

var (
	ErrNegativeWeight = errors.New("ketama: negative weight is not allowed")
)

type Continuum struct {
	ring points
	hash HashFunc
}

type points []continuumPoint

func (c points) Less(i, j int) bool { return c[i].point < c[j].point }
func (c points) Len() int           { return len(c) }
func (c points) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func md5Digest(in string) []byte {
	h := md5.New()
	h.Write([]byte(in))
	return h.Sum(nil)
}

func hashString(in string) uint {
	digest := md5Digest(in)
	return uint(digest[3])<<24 | uint(digest[2])<<16 | uint(digest[1])<<8 | uint(digest[0])
}

func fixWeight(weight int) int {
	// libmemcached treats 0 weight to be the same as 1 so we should behave
	// the same way
	if weight == 0 {
		return 1
	}

	return weight
}

func NewWithHash(buckets []Bucket, hash HashFunc) (*Continuum, error) {

	numbuckets := len(buckets)

	if numbuckets == 0 {
		// let them error when they try to use it
		return nil, nil
	}

	ring := make(points, 0, numbuckets*160)

	totalweight := 0
	for _, b := range buckets {
		if b.Weight < 0 {
			return nil, ErrNegativeWeight
		}

		totalweight += fixWeight(b.Weight)
	}

	for i, b := range buckets {
		pct := float32(fixWeight(b.Weight)) / float32(totalweight)

		// this is the equivalent of C's promotion rules, but in Go, to maintain exact compatibility with the C library
		limit := int(float32(float64(pct) * 40.0 * float64(numbuckets)))

		for k := 0; k < limit; k++ {
			/* 40 hashes, 4 numbers per hash = 160 points per bucket */
			ss := fmt.Sprintf("%s-%d", b.Label, k)
			digest := md5Digest(ss)

			for h := 0; h < 4; h++ {
				point := continuumPoint{
					point:  uint(digest[3+h*4])<<24 | uint(digest[2+h*4])<<16 | uint(digest[1+h*4])<<8 | uint(digest[h*4]),
					bucket: buckets[i],
				}
				ring = append(ring, point)
			}
		}
	}

	sort.Sort(ring)

	return &Continuum{
		ring: ring,
		hash: hash,
	}, nil
}

func New(buckets []Bucket) (*Continuum, error) {
	return NewWithHash(buckets, HashFunc1)
}

func (c Continuum) Hash(thing string) string {

	if len(c.ring) == 0 {
		return ""
	}

	h := hashString(thing)

	// the above md5 is way more expensive than this branch
	var i uint
	switch c.hash {
	case HashFunc1:
		i = search(c.ring, h)
	case HashFunc2:
		i = uint(sort.Search(len(c.ring), func(i int) bool { return c.ring[i].point >= h }))
		if i >= uint(len(c.ring)) {
			i = 0
		}
	}

	return c.ring[i].bucket.Label
}

func (c Continuum) HashMultiple(thing string, count int) []string {

	if len(c.ring) == 0 {
		return nil
	}

	h := hashString(thing)

	// the above md5 is way more expensive than this branch
	var i uint
	switch c.hash {
	case HashFunc1:
		i = search(c.ring, h)
	case HashFunc2:
		i = uint(sort.Search(len(c.ring), func(i int) bool { return c.ring[i].point >= h }))
		if i >= uint(len(c.ring)) {
			i = 0
		}
	}

	seen := make(map[string]struct{})
	result := make([]string, 0, count)

	for len(result) < count {
		label := c.ring[i].bucket.Label
		if _, ok := seen[label]; !ok {
			result = append(result, label)
			seen[label] = struct{}{}
		}
		i++
		if i >= uint(len(c.ring)) {
			i = 0
		}
	}

	return result
}

// This function taken from
// https://github.com/lestrrat/Algorithm-ConsistentHash-Ketama/blob/master/xs/Ketama.xs
// In order to maintain compatibility, we must reproduce the same integer
// underflow bug introduced in
// https://github.com/lestrrat/Algorithm-ConsistentHash-Ketama/commit/1efbcc0ead13114f8e4e454a8064b842b14da6f3

func search(ring points, h uint) uint {
	var maxp = uint(len(ring))
	var lowp = uint(0)
	var highp = maxp

	for {
		midp := (lowp + highp) / 2
		if midp >= maxp {
			if midp == maxp {
				midp = 1
			} else {
				midp = maxp
			}

			return midp - 1
		}
		midval := ring[midp].point
		var midval1 uint
		if midp == 0 {
			midval1 = 0
		} else {
			midval1 = ring[midp-1].point
		}

		if h <= midval && h > midval1 {
			return midp
		}

		if midval < h {
			lowp = midp + 1
		} else {
			// NOTE(dgryski): Maintaining compatibility with Algorithm::ConsistentHash::Ketama depends on integer underflow here
			highp = midp - 1
		}

		if lowp > highp {
			return 0
		}
	}
}
