// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestEpochs_UnmarshalYAML(t *testing.T) {
	const input = `
- read_compartments: 1
- start: 2026-12-01T10:00:00Z
  read_compartments: 10
  cutover_duration: 6h
`

	var epochs Epochs
	require.NoError(t, yaml.Unmarshal([]byte(input), &epochs))

	expected := Epochs{
		{ReadCompartments: 1},
		{
			Start:            time.Date(2026, 12, 1, 10, 0, 0, 0, time.UTC),
			ReadCompartments: 10,
			CutoverDuration:  model.Duration(6 * time.Hour),
		},
	}
	require.Len(t, epochs, len(expected))
	for i := range expected {
		assert.True(t, expected[i].Start.Equal(epochs[i].Start), "comp epoch %d start: expected %s, got %s", i, expected[i].Start, epochs[i].Start)
		assert.Equal(t, expected[i].ReadCompartments, epochs[i].ReadCompartments)
		assert.Equal(t, expected[i].CutoverDuration, epochs[i].CutoverDuration)
	}
	require.NoError(t, epochs.Validate(10))
}

func TestEpochs_Validate(t *testing.T) {
	start := time.Date(2026, 12, 1, 10, 0, 0, 0, time.UTC)
	hours := func(n int) model.Duration { return model.Duration(time.Duration(n) * time.Hour) }

	tests := map[string]struct {
		epochs              Epochs
		numReadCompartments int
		expectedErr         error
	}{
		"no epochs pass": {
			epochs:              nil,
			numReadCompartments: 1,
		},
		"single first epoch passes": {
			epochs:              Epochs{{ReadCompartments: 1}},
			numReadCompartments: 1,
		},
		"increase with a cutover window passes": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start, ReadCompartments: 10, CutoverDuration: hours(6)},
			},
			numReadCompartments: 10,
		},
		"epoch without a cutover window passes": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start, ReadCompartments: 2},
			},
			numReadCompartments: 2,
		},
		"start in a time zone that is offset by full hours passes": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start.In(time.FixedZone("UTC+2", 2*60*60)), ReadCompartments: 2},
			},
			numReadCompartments: 2,
		},
		"epoch that starts when the previous cutover window ends passes": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start, ReadCompartments: 2, CutoverDuration: hours(6)},
				{Start: start.Add(6 * time.Hour), ReadCompartments: 4, CutoverDuration: hours(3)},
			},
			numReadCompartments: 4,
		},
		"first epoch with zero read compartments is rejected": {
			epochs:              Epochs{{ReadCompartments: 0}},
			numReadCompartments: 1,
			expectedErr:         ErrEpochInvalidReadCompartments,
		},
		"epoch with negative read compartments is rejected": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start, ReadCompartments: -1},
			},
			numReadCompartments: 2,
			expectedErr:         ErrEpochInvalidReadCompartments,
		},
		"epoch with more read compartments than configured is rejected": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start, ReadCompartments: 11, CutoverDuration: hours(6)},
			},
			numReadCompartments: 10,
			expectedErr:         ErrEpochTooManyReadCompartments,
		},
		"first epoch with a start time is rejected": {
			epochs:              Epochs{{Start: start, ReadCompartments: 1}},
			numReadCompartments: 1,
			expectedErr:         ErrEpochFirstHasStart,
		},
		"first epoch with a cutover duration is rejected": {
			epochs:              Epochs{{ReadCompartments: 1, CutoverDuration: hours(6)}},
			numReadCompartments: 1,
			expectedErr:         ErrEpochFirstHasCutover,
		},
		"epoch after the first without a start time is rejected": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{ReadCompartments: 2, CutoverDuration: hours(6)},
			},
			numReadCompartments: 2,
			expectedErr:         ErrEpochMissingStart,
		},
		"start time that is not a full hour is rejected": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start.Add(30 * time.Minute), ReadCompartments: 2},
			},
			numReadCompartments: 2,
			expectedErr:         ErrEpochStartNotFullHour,
		},
		"start in a time zone that is not offset by full hours is rejected": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: time.Date(2026, 12, 1, 10, 0, 0, 0, time.FixedZone("UTC+5:30", 5*60*60+30*60)), ReadCompartments: 2},
			},
			numReadCompartments: 2,
			expectedErr:         ErrEpochStartNotFullHour,
		},
		"cutover duration that is not full hours is rejected": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start, ReadCompartments: 2, CutoverDuration: model.Duration(90 * time.Minute)},
			},
			numReadCompartments: 2,
			expectedErr:         ErrEpochCutoverNotFullHours,
		},
		"negative cutover duration is rejected": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start, ReadCompartments: 2, CutoverDuration: hours(-1)},
			},
			numReadCompartments: 2,
			expectedErr:         ErrEpochCutoverNotFullHours,
		},
		"epoch with the same start as the previous epoch is rejected": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start, ReadCompartments: 2},
				{Start: start, ReadCompartments: 4},
			},
			numReadCompartments: 4,
			expectedErr:         ErrEpochStartNotAfterPreviousStart,
		},
		"epochs out of order are rejected": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start, ReadCompartments: 2},
				{Start: start.Add(-time.Hour), ReadCompartments: 4},
			},
			numReadCompartments: 4,
			expectedErr:         ErrEpochStartNotAfterPreviousStart,
		},
		"epoch that starts in the cutover window of the previous epoch is rejected": {
			epochs: Epochs{
				{ReadCompartments: 1},
				{Start: start, ReadCompartments: 2, CutoverDuration: hours(6)},
				{Start: start.Add(5 * time.Hour), ReadCompartments: 4},
			},
			numReadCompartments: 4,
			expectedErr:         ErrEpochStartBeforePreviousCutoverEnded,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := tc.epochs.Validate(tc.numReadCompartments)
			if tc.expectedErr != nil {
				assert.ErrorIs(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
