// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
)

var (
	ErrEpochFirstHasStart                   = errors.New("the first comp epoch must not have a start time")
	ErrEpochFirstHasCutover                 = errors.New("the first comp epoch must not have a cutover duration")
	ErrEpochMissingStart                    = errors.New("each comp epoch after the first must have a start time")
	ErrEpochStartNotFullHour                = errors.New("the comp epoch start time must be a full hour")
	ErrEpochCutoverNotFullHours             = errors.New("the comp epoch cutover duration must be a number of full hours that is not negative")
	ErrEpochInvalidReadCompartments         = errors.New("the comp epoch read compartments must be greater than 0")
	ErrEpochTooManyReadCompartments         = errors.New("the comp epoch read compartments must not be greater than compartments read.num-compartments")
	ErrEpochStartNotAfterPreviousStart      = errors.New("the comp epoch must start after the previous comp epoch starts")
	ErrEpochStartBeforePreviousCutoverEnded = errors.New("the comp epoch must not start before the cutover window of the previous comp epoch ends")
)

// Epoch is a period of time during which a fixed number of read compartments exists. An epoch
// ends when the next epoch starts.
type Epoch struct {
	// Start is zero for the first epoch.
	Start time.Time `yaml:"start,omitempty"`

	ReadCompartments int `yaml:"read_compartments"`

	// CutoverDuration is the length of the cutover window at the start of the epoch. In the cutover
	// window, each series changes from the read compartment count of the previous epoch to the read
	// compartment count of this epoch at its own switch time.
	CutoverDuration model.Duration `yaml:"cutover_duration,omitempty"`
}

// Epochs holds the comp epochs, ordered by start time.
type Epochs []Epoch

// Validate returns an error if the epochs are invalid. numReadCompartments is the maximum read
// compartment count of an epoch.
//
// The cutover window of an epoch is the half-open interval [Start, Start+CutoverDuration). It must
// end at or before the start of the next epoch, so at most two read compartment counts apply at
// any time.
func (epochs Epochs) Validate(numReadCompartments int) error {
	for i := range epochs {
		if err := epochs.validateEpoch(i, numReadCompartments); err != nil {
			return fmt.Errorf("comp epoch %d: %w", i, err)
		}
	}
	return nil
}

func (epochs Epochs) validateEpoch(i, numReadCompartments int) error {
	epoch := epochs[i]

	if epoch.ReadCompartments <= 0 {
		return ErrEpochInvalidReadCompartments
	}
	if epoch.ReadCompartments > numReadCompartments {
		return ErrEpochTooManyReadCompartments
	}

	cutoverDuration := time.Duration(epoch.CutoverDuration)
	if cutoverDuration < 0 || cutoverDuration%time.Hour != 0 {
		return ErrEpochCutoverNotFullHours
	}

	if i == 0 {
		if !epoch.Start.IsZero() {
			return ErrEpochFirstHasStart
		}
		if cutoverDuration != 0 {
			return ErrEpochFirstHasCutover
		}
		return nil
	}

	if epoch.Start.IsZero() {
		return ErrEpochMissingStart
	}
	if !epoch.Start.Truncate(time.Hour).Equal(epoch.Start) {
		return ErrEpochStartNotFullHour
	}

	previous := epochs[i-1]
	if !epoch.Start.After(previous.Start) {
		return ErrEpochStartNotAfterPreviousStart
	}
	if epoch.Start.Before(previous.Start.Add(time.Duration(previous.CutoverDuration))) {
		return ErrEpochStartBeforePreviousCutoverEnded
	}
	return nil
}
