// SPDX-License-Identifier: AGPL-3.0-only

//go:build goexperiment.arenas

package arena

import (
	stdarena "arena"
)

type arena struct {
	a *stdarena.Arena
}

func newArena() *arena {
	return &arena{a: stdarena.NewArena()}
}

func (a *arena) Free() {
	a.a.Free()
}

func allocate[T any](a *arena) *T {
	return stdarena.New[T](a.a)
}

func makeSlice[T any](a *arena, len, cap int) []T {
	return stdarena.MakeSlice[T](a.a, len, cap)
}

func clone[T any](s T) T {
	return stdarena.Clone(s)
}
