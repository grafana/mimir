// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/annotations/annotations.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package types

import (
	"sync"

	"github.com/prometheus/prometheus/util/annotations"
)

// Annotations is a thread-safe wrapper around github.com/prometheus/prometheus/util/annotations.Annotations.
type Annotations struct {
	mtx   *sync.RWMutex
	annos *annotations.Annotations
}

func NewAnnotations() *Annotations {
	return &Annotations{
		mtx:   &sync.RWMutex{},
		annos: annotations.New(),
	}
}

func (a *Annotations) Add(err error) {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	a.annos.Add(err)
}

func (a *Annotations) Count() int {
	a.mtx.RLock()
	defer a.mtx.RUnlock()
	return len(*a.annos)
}

// Get returns the underlying annotations.Annotations instance.
//
// This method is not thread-safe and should only be used once all possible Add calls have been made.
//
// Calling Add after this method has been called is not safe and may cause undefined behavior.
func (a *Annotations) Get() annotations.Annotations {
	return *a.annos
}
