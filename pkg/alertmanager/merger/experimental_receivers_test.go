// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/merger/v2_alerts_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package merger

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	am_models "github.com/grafana/mimir/pkg/alertmanager/models"
)

func TestExperimentalReceivers(t *testing.T) {

	// This test is to check the parsing round-trip is working as expected, the merging logic is
	// tested in TestMergeReceivers and TestMergeIntegrations.

	in := [][]byte{
		[]byte(`[` +
			`{"active":true,"integrations":[{"lastNotifyAttempt":"0001-01-01T00:00:00.000Z","lastNotifyAttemptDuration":"0s","name":"email","sendResolved":false}],"name":"dummy"}` +
			`]`),
		[]byte(`[` +
			`{"active":true,"integrations":[{"lastNotifyAttempt":"0001-01-01T00:00:00.000Z","lastNotifyAttemptDuration":"0s","name":"email","sendResolved":false}],"name":"dummy"}` +
			`]`),
		[]byte(`[]`),
	}

	expected := []byte(`[` +
		`{"active":true,"integrations":[{"lastNotifyAttempt":"0001-01-01T00:00:00.000Z","lastNotifyAttemptDuration":"0s","name":"email","sendResolved":false}],"name":"dummy"}` +
		`]`)

	out, err := ExperimentalReceivers{}.MergeResponses(in)
	require.NoError(t, err)
	require.Equal(t, expected, out)
}

func strPtr(s string) *string {
	return &s
}

func boolPtr(b bool) *bool {
	return &b
}

func TestMergeReceivers(t *testing.T) {
	cases := []struct {
		name string
		in   []am_models.Receiver
		err  error
		out  []am_models.Receiver
	}{
		{
			name: "no receivers, should return an empty list",
			in:   []am_models.Receiver{},
			out:  []am_models.Receiver{},
		},
		{
			name: "one receiver, should return the receiver",
			in: []am_models.Receiver{
				{
					Name:   strPtr("recv-a"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-a"),
						},
					},
				},
			},
			out: []am_models.Receiver{
				{
					Name:   strPtr("recv-a"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-a"),
						},
					},
				},
			},
		},
		{
			name: "two receivers with different names, should return two receivers",
			in: []am_models.Receiver{
				{
					Name:   strPtr("recv-a"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-a"),
						},
					},
				},
				{
					Name:   strPtr("recv-b"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-a"),
						},
					},
				},
			},
			out: []am_models.Receiver{
				{
					Name:   strPtr("recv-a"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-a"),
						},
					},
				},
				{
					Name:   strPtr("recv-b"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-a"),
						},
					},
				},
			},
		},
		// Two replicas might briefly have different configurations, causing this case.
		{
			name: "two receivers with same names, with integrations with different names, should return one receiver with two integrations",
			in: []am_models.Receiver{
				{
					Name:   strPtr("recv-a"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-a"),
						},
					},
				},
				{
					Name:   strPtr("recv-a"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-b"),
						},
					},
				},
			},
			out: []am_models.Receiver{
				{
					Name:   strPtr("recv-a"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-a"),
						},
						{
							Name: strPtr("inte-b"),
						},
					},
				},
			},
		},
		{
			name: "two receivers with same names, with integrations with same names, should return one receiver with one integrations",
			in: []am_models.Receiver{
				{
					Name:   strPtr("recv-a"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-a"),
						},
					},
				},
				{
					Name:   strPtr("recv-a"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-a"),
						},
					},
				},
			},
			out: []am_models.Receiver{
				{
					Name:   strPtr("recv-a"),
					Active: boolPtr(true),
					Integrations: []*am_models.Integration{
						{
							Name: strPtr("inte-a"),
						},
					},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := mergeReceivers(c.in)
			require.Equal(t, c.err, err)
			require.Equal(t, c.out, out)
		})
	}
}

func TestMergeIntegrations(t *testing.T) {
	cases := []struct {
		name string
		in   []*am_models.Integration
		err  error
		out  []*am_models.Integration
	}{
		{
			name: "no integrations, should return an empty list",
			in:   []*am_models.Integration{},
			out:  []*am_models.Integration{},
		},
		{
			name: "one integration, should return the integration",
			in: []*am_models.Integration{
				{
					Name:                      strPtr("int-a"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "",
				},
			},
			out: []*am_models.Integration{
				{
					Name:                      strPtr("int-a"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "",
				},
			},
		},
		{
			name: "two integrations with different names, should return two integrations in name order",
			in: []*am_models.Integration{
				{
					Name:                      strPtr("int-b"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:01.000Z"),
					LastNotifyAttemptDuration: "2s",
					LastNotifyAttemptError:    "err-b",
				},
				{
					Name:                      strPtr("int-a"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "err-a",
				},
			},
			out: []*am_models.Integration{
				{
					Name:                      strPtr("int-a"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "err-a",
				},
				{
					Name:                      strPtr("int-b"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:01.000Z"),
					LastNotifyAttemptDuration: "2s",
					LastNotifyAttemptError:    "err-b",
				},
			},
		},
		{
			name: "two integrations with same name, should return integration with newest notify attempt",
			in: []*am_models.Integration{
				{
					Name:                      strPtr("int-a"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:01.000Z"),
					LastNotifyAttemptDuration: "2s",
					LastNotifyAttemptError:    "err-2",
				},
				{
					Name:                      strPtr("int-a"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "err-1",
				},
			},
			out: []*am_models.Integration{
				{
					Name:                      strPtr("int-a"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:01.000Z"),
					LastNotifyAttemptDuration: "2s",
					LastNotifyAttemptError:    "err-2",
				},
			},
		},
		{
			name: "two integrations with same name, one without a notify attempt, should return integration with notify attempt",
			in: []*am_models.Integration{
				{
					Name:                      strPtr("int-a"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         strfmt.DateTime(time.Time{}),
					LastNotifyAttemptDuration: "",
					LastNotifyAttemptError:    "",
				},
				{
					Name:                      strPtr("int-a"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "",
				},
			},
			out: []*am_models.Integration{
				{
					Name:                      strPtr("int-a"),
					SendResolved:              boolPtr(true),
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "",
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := mergeIntegrations(c.in)
			require.Equal(t, c.err, err)
			require.Equal(t, c.out, out)
		})
	}
}
