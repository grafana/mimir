// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/merger/v2_receivers_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package merger

import (
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	v2_models "github.com/prometheus/alertmanager/api/v2/models"
	"github.com/stretchr/testify/require"
)

func TestV2Receivers(t *testing.T) {
	// This test is to check the parsing round-trip is working as expected, the merging logic is
	// tested in TestMergeV2Receivers. The test data is based on captures from an actual Alertmanager.

	in := [][]byte{
		[]byte(`[{"active":false,"integrations":[{"lastNotifyAttempt":"0001-01-01T00:00:00.000Z",
"lastNotifyAttemptDuration":"0s","name":"email[0]","sendResolved":false}],"name":"email"},{"active":true,
"integrations":[{"lastNotifyAttempt":"0001-01-01T00:00:00.000Z","lastNotifyAttemptDuration":"0s","name":"webhook[0]",
"sendResolved":true}],"name":"webhook"}]`),
		[]byte(`[{"active":false,"integrations":[{"lastNotifyAttempt":"0001-01-01T00:00:00.000Z",
"lastNotifyAttemptDuration":"0s","name":"email[0]","sendResolved":false}],"name":"email"},{"active":true,
"integrations":[{"lastNotifyAttempt":"2023-04-21T20:58:46.417Z","lastNotifyAttemptDuration":"715ms","name":"webhook[0]",
"sendResolved":true}],"name":"webhook"}]`),
		[]byte(`[{"active":false,"integrations":[{"lastNotifyAttempt":"0001-01-01T00:00:00.000Z",
"lastNotifyAttemptDuration":"0s","name":"email[0]","sendResolved":false}],"name":"email"},{"active":true,
"integrations":[{"lastNotifyAttempt":"0001-01-01T00:00:00.000Z","lastNotifyAttemptDuration":"715ms","name":"webhook[0]",
"sendResolved":true}],"name":"webhook"}]`),
	}

	expected := `[
    {
        "active": false,
        "integrations": [
            {
                "lastNotifyAttempt": "0001-01-01T00:00:00.000Z",
                "lastNotifyAttemptDuration": "0s",
                "name": "email[0]",
                "sendResolved": false
            }
        ],
        "name": "email"
    },
    {
        "active": true,
        "integrations": [
            {
                "lastNotifyAttempt": "2023-04-21T20:58:46.417Z",
                "lastNotifyAttemptDuration": "715ms",
                "name": "webhook[0]",
                "sendResolved": true
            }
        ],
        "name": "webhook"
    }
]`

	out, err := V2Receivers{}.MergeResponses(in)
	require.NoError(t, err)
	require.JSONEq(t, expected, string(out))
}

func v2receiver(name string, active bool, integrations ...*v2_models.Integration) v2_models.Receiver {
	return v2_models.Receiver{
		Integrations: integrations,
		Name:         &name,
		Active:       &active,
	}
}

func v2Integration(name, lastNotify string) *v2_models.Integration {
	var last strfmt.DateTime
	if lastNotify == "" {
		last = strfmt.DateTime(time.Time{})
	} else {
		l := v2ParseTime(lastNotify)
		last = *l
	}
	return &v2_models.Integration{
		LastNotifyAttempt:         last,
		LastNotifyAttemptDuration: "1s",
		LastNotifyAttemptError:    "",
		Name:                      &name,
		SendResolved:              nil,
	}
}

func TestMergeV2Receivers(t *testing.T) {
	var receiver1 = v2receiver("dummy1", true, v2Integration("dummy-int1", "2020-01-01T12:00:00.000Z"))
	var receiver1Newer = v2receiver("dummy1", true, v2Integration("dummy-int1", "2020-01-01T12:00:00.001Z"))
	var receiver2 = v2receiver("dummy2", true, v2Integration("dummy-int2", ""))
	var receiver2Newer = v2receiver("dummy2", true, v2Integration("dummy-int2", "2020-01-01T12:00:00.001Z"))
	var nilNameReceiver = v2receiver("dummy3", true, v2Integration("dummy-int2", "2020-01-01T12:00:00.001Z"))
	nilNameReceiver.Name = nil

	cases := []struct {
		name string
		in   []v2_models.Receiver
		err  error
		out  []v2_models.Receiver
	}{
		{
			name: "recent timestamp wins",
			in:   []v2_models.Receiver{receiver1, receiver2, receiver1Newer},
			out:  []v2_models.Receiver{receiver1Newer, receiver2},
		},
		{
			name: "non-zero timestamp wins",
			in:   []v2_models.Receiver{receiver1, receiver2, receiver2Newer},
			out:  []v2_models.Receiver{receiver1, receiver2Newer},
		},
		{
			name: "sorts resulst by name",
			in:   []v2_models.Receiver{receiver2, receiver1},
			out:  []v2_models.Receiver{receiver1, receiver2},
		},
		{
			name: "nil name should cause error",
			in:   []v2_models.Receiver{receiver2, receiver1, nilNameReceiver},
			err:  errors.New("unexpected nil name"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := mergeV2Receivers(c.in)
			require.Equal(t, c.err, err)
			require.Equal(t, c.out, out)
		})
	}
}
