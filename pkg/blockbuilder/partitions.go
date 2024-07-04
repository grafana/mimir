// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import "slices"

// partitions represent a queue of partitions currently assigned to the consumer.
type partitions struct {
	list []int32
	cur  int
}

// next returns the next item in the queue of assigned partitions.
func (p *partitions) next() int32 {
	if len(p.list) == 0 {
		return -1
	}

	if p.cur >= len(p.list) {
		return -1
	}

	i := p.cur
	p.cur++

	return p.list[i]
}

// update replaces the queue with a new list of assigned partitions.
// It doesn't reset the queue's cursor. That is, the later call to next will return the last active item
// if the item exists in the new list.
func (p *partitions) update(list []int32) {
	if len(p.list) == 0 || p.cur == 0 || p.cur == len(p.list) {
		p.list = list
		p.cur = 0
		return
	}

	list = slices.Clone(list)
	slices.Sort(list)

	head := p.list[:p.cur]
	cur := len(head) - 1

	for i := 0; i < len(head); i++ {
		j, ok := slices.BinarySearch(list, head[i])
		if ok {
			// Remove from the list item that stays in the head.
			list = append(list[:j], list[j+1:]...)
		} else {
			// Remove from the head item that isn't present the list.
			head = append(head[:i], head[i+1:]...)
			if cur != i {
				// Shift the cursor beyond the head if the last item in the head was removed. Otherwise,
				// the cursor stays at the end of the head.
				cur--
			}
			i = i - 1
		}
	}

	p.cur = cur
	p.list = slices.Concat(head, list)
}

func (p *partitions) len() int {
	return len(p.list)
}

func (p *partitions) reset() {
	p.cur = 0
}

func (p *partitions) collect() []int32 {
	list := make([]int32, len(p.list))
	copy(list, p.list)
	return list
}
