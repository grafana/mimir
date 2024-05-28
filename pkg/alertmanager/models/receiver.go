package models

import (
	"github.com/go-openapi/strfmt"
)

// Receiver receiver
//
// swagger:model receiver
type Receiver struct {

	// active
	// Required: true
	Active *bool `json:"active"`

	// integrations
	// Required: true
	Integrations []*Integration `json:"integrations"`

	// name
	// Required: true
	Name *string `json:"name"`
}

// Integration integration
//
// swagger:model integration
type Integration struct {

	// A timestamp indicating the last attempt to deliver a notification regardless of the outcome.
	// Format: date-time
	LastNotifyAttempt strfmt.DateTime `json:"lastNotifyAttempt,omitempty"`

	// Duration of the last attempt to deliver a notification in humanized format (`1s` or `15ms`, etc).
	LastNotifyAttemptDuration string `json:"lastNotifyAttemptDuration,omitempty"`

	// Error string for the last attempt to deliver a notification. Empty if the last attempt was successful.
	LastNotifyAttemptError string `json:"lastNotifyAttemptError,omitempty"`

	// name
	// Required: true
	Name *string `json:"name"`

	// send resolved
	// Required: true
	SendResolved *bool `json:"sendResolved"`
}
