// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import "sync"

// MockTenantLimits exposes per-tenant limits based on a provided map
type MockTenantLimits struct {
	limits map[string]*Limits

	listenersMx     sync.Mutex
	listeners       map[int]ChangeListener
	listenersNextID int
}

// NewMockTenantLimits creates a new MockTenantLimits that returns per-tenant limits based on
// the given map
func NewMockTenantLimits(limits map[string]*Limits) *MockTenantLimits {
	return &MockTenantLimits{
		limits:    limits,
		listeners: make(map[int]ChangeListener),
	}
}

func (l *MockTenantLimits) AddChangeListener(listener ChangeListener) ChangeListenerRef {
	l.listenersMx.Lock()
	id := l.listenersNextID
	l.listeners[id] = listener
	l.listenersNextID++
	l.listenersMx.Unlock()

	return id
}

func (l *MockTenantLimits) RemoveChangeListener(ref ChangeListenerRef) {
	id, ok := ref.(int)
	if !ok {
		return
	}

	l.listenersMx.Lock()
	delete(l.listeners, id)
	l.listenersMx.Unlock()
}

func (l *MockTenantLimits) NotifyChangeListeners() {
	// Get a copy of all listeners.
	l.listenersMx.Lock()
	listeners := make([]ChangeListener, 0, len(l.listeners))
	for _, entry := range l.listeners {
		listeners = append(listeners, entry)
	}
	l.listenersMx.Unlock()

	// Notify all listeners.
	for _, listener := range listeners {
		listener()
	}
}

func (l *MockTenantLimits) ByUserID(userID string) *Limits {
	return l.limits[userID]
}

func (l *MockTenantLimits) AllByUserID() map[string]*Limits {
	return l.limits
}
