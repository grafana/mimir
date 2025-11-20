// Derived from Go's hash/fnv/fnv.go
//
// Copyright 2009 The Go Authors.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//    * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//    * Neither the name of Google LLC nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.

package boom

import (
	"hash"
)

const (
	prime32 = 16777619
	prime64 = 1099511628211
)

func hash32DefaultFnv(data []byte, h hash.Hash32) uint32 {
	if h != nil {
		h.Write(data)
		sum := h.Sum32()
		h.Reset()
		return sum
	}

	var sum uint32 = 2166136261
	for _, c := range data {
		sum *= prime32
		sum ^= uint32(c)
	}
	return sum
}

func hash64DefaultFnv(data []byte, h hash.Hash64) uint64 {
	if h != nil {
		h.Write(data)
		sum := h.Sum64()
		h.Reset()
		return sum
	}

	var sum uint64 = 14695981039346656037
	for _, c := range data {
		sum *= prime64
		sum ^= uint64(c)
	}
	return sum
}

func hash32BytesDefaultFnv(data []byte, h hash.Hash32) []byte {
	if h != nil {
		h.Write(data)
		sum := h.Sum(nil)
		h.Reset()
		return sum
	}

	sum := hash32DefaultFnv(data, nil)
	return append([]byte(nil),
		byte(sum>>24),
		byte(sum>>16),
		byte(sum>>8),
		byte(sum),
	)
}
