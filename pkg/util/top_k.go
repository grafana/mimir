// SPDX-License-Identifier: AGPL-3.0-only

package util

import "slices"

// TopK keeps the best items of all the items that it gets.
type TopK[T any] struct {
	limit   int
	compare func(a, b T) int
	// items is a heap. The worst kept item is at index 0.
	// Thus TopK compares a new item only with the worst kept item.
	items []T
}

// NewTopK returns a TopK that keeps the limit best items. A limit of 0 or less keeps all items.
// compare returns a negative number when a is better than b, a positive number when b is better than a, and 0 otherwise.
func NewTopK[T any](limit int, compare func(a, b T) int) *TopK[T] {
	return &TopK[T]{limit: limit, compare: compare}
}

// Accepts returns true when Push keeps item.
// Use Accepts to skip expensive work for an item that Push does not keep.
func (t *TopK[T]) Accepts(item T) bool {
	return t.limit <= 0 || len(t.items) < t.limit || t.compare(item, t.items[0]) < 0
}

// Push adds item. When TopK already keeps limit items, Push removes the worst item.
// Push returns the item that it removed or did not keep, and true.
// When Push removes no item, it returns false.
func (t *TopK[T]) Push(item T) (dropped T, isDropped bool) {
	if t.limit <= 0 {
		t.items = append(t.items, item)
		return dropped, false
	}
	if len(t.items) < t.limit {
		t.items = append(t.items, item)
		t.siftUp(len(t.items) - 1)
		return dropped, false
	}
	if t.compare(item, t.items[0]) >= 0 {
		return item, true
	}
	dropped = t.items[0]
	t.items[0] = item
	t.siftDown(0)
	return dropped, true
}

// Sorted returns the kept items, the best item first. It returns an empty slice and not nil when TopK keeps no items.
// Do not use TopK after Sorted.
func (t *TopK[T]) Sorted() []T {
	if t.items == nil {
		return []T{}
	}
	slices.SortFunc(t.items, t.compare)
	return t.items
}

// siftUp moves the item at index up the heap while it is worse than its parent.
func (t *TopK[T]) siftUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if t.compare(t.items[index], t.items[parent]) <= 0 {
			return
		}
		t.items[index], t.items[parent] = t.items[parent], t.items[index]
		index = parent
	}
}

// siftDown moves the item at index down the heap while one of its children is worse.
func (t *TopK[T]) siftDown(index int) {
	for {
		worst := index
		if left := 2*index + 1; left < len(t.items) && t.compare(t.items[left], t.items[worst]) > 0 {
			worst = left
		}
		if right := 2*index + 2; right < len(t.items) && t.compare(t.items[right], t.items[worst]) > 0 {
			worst = right
		}
		if worst == index {
			return
		}
		t.items[index], t.items[worst] = t.items[worst], t.items[index]
		index = worst
	}
}
