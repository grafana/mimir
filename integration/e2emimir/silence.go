// SPDX-License-Identifier: AGPL-3.0-only

package e2emimir

import (
	"time"

	"github.com/prometheus/alertmanager/pkg/labels"
)

// Silence is a local representation of an alertmanager silence for use in integration tests.
// This was previously types.Silence, which was removed in alertmanager v0.32.0.
type Silence struct {
	ID        string          `json:"id"`
	Matchers  labels.Matchers `json:"matchers"`
	StartsAt  time.Time       `json:"startsAt"`
	EndsAt    time.Time       `json:"endsAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
	CreatedBy string          `json:"createdBy"`
	Comment   string          `json:"comment,omitempty"`
	Status    SilenceStatus   `json:"status"`
}

// SilenceStatus stores the state of a silence.
type SilenceStatus struct {
	State SilenceState `json:"state"`
}

// SilenceState is used as part of SilenceStatus.
type SilenceState string

// Possible values for SilenceState.
const (
	SilenceStateExpired SilenceState = "expired"
	SilenceStateActive  SilenceState = "active"
	SilenceStatePending SilenceState = "pending"
)
