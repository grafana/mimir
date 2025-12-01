// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !uniquelabels

package labels

import "slices"

// NewBuilder returns a new LabelsBuilder.
func NewBuilder(base Labels) *Builder {
	b := &Builder{
		del: make([]string, 0, 5),
		add: make([]Label, 0, 5),
	}
	b.Reset(base)
	return b
}

// Del deletes the label of the given name.
func (b *Builder) Del(ns ...string) *Builder {
	for _, n := range ns {
		for i, a := range b.add {
			if a.Name == n {
				b.add = append(b.add[:i], b.add[i+1:]...)
			}
		}
		b.del = append(b.del, n)
	}
	return b
}

// Keep removes all labels from the base except those with the given names.
func (b *Builder) Keep(ns ...string) *Builder {
	b.base.Range(func(l Label) {
		if slices.Contains(ns, l.Name) {
			return
		}
		b.del = append(b.del, l.Name)
	})
	return b
}

// Set the name/value pair as a label. A value of "" means delete that label.
func (b *Builder) Set(n, v string) *Builder {
	if v == "" {
		// Empty labels are the same as missing labels.
		return b.Del(n)
	}
	for i, a := range b.add {
		if a.Name == n {
			b.add[i].Value = v
			return b
		}
	}
	b.add = append(b.add, Label{Name: n, Value: v})

	return b
}

func (b *Builder) Get(n string) string {
	// Del() removes entries from .add but Set() does not remove from .del, so check .add first.
	for _, a := range b.add {
		if a.Name == n {
			return a.Value
		}
	}
	if slices.Contains(b.del, n) {
		return ""
	}
	return b.base.Get(n)
}

// Range calls f on each label in the Builder.
func (b *Builder) Range(f func(l Label)) {
	// Stack-based arrays to avoid heap allocation in most cases.
	var addStack [128]Label
	var delStack [128]string
	// Take a copy of add and del, so they are unaffected by calls to Set() or Del().
	origAdd, origDel := append(addStack[:0], b.add...), append(delStack[:0], b.del...)
	b.base.Range(func(l Label) {
		if !slices.Contains(origDel, l.Name) && !contains(origAdd, l.Name) {
			f(l)
		}
	})
	for _, a := range origAdd {
		f(a)
	}
}

func contains(s []Label, n string) bool {
	for _, a := range s {
		if a.Name == n {
			return true
		}
	}
	return false
}
