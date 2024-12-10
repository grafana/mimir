// SPDX-License-Identifier: AGPL-3.0-only

package merger

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	alertingmodels "github.com/grafana/alerting/models"
	"github.com/stretchr/testify/require"
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

func TestMergeReceivers(t *testing.T) {
	cases := []struct {
		name string
		in   []alertingmodels.Receiver
		err  error
		out  []alertingmodels.Receiver
	}{
		{
			name: "no receivers, should return an empty list",
			in:   []alertingmodels.Receiver{},
			out:  []alertingmodels.Receiver{},
		},
		{
			name: "one receiver, should return the receiver",
			in: []alertingmodels.Receiver{
				{
					Name:   "recv-a",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-a",
						},
					},
				},
			},
			out: []alertingmodels.Receiver{
				{
					Name:   "recv-a",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-a",
						},
					},
				},
			},
		},
		{
			name: "two receivers with different names, should return two receivers",
			in: []alertingmodels.Receiver{
				{
					Name:   "recv-a",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-a",
						},
					},
				},
				{
					Name:   "recv-b",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-a",
						},
					},
				},
			},
			out: []alertingmodels.Receiver{
				{
					Name:   "recv-a",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-a",
						},
					},
				},
				{
					Name:   "recv-b",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-a",
						},
					},
				},
			},
		},
		// Two replicas might briefly have different configurations, causing this case.
		{
			name: "two receivers with same names, with integrations with different names, should return one receiver with two integrations",
			in: []alertingmodels.Receiver{
				{
					Name:   "recv-a",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-a",
						},
					},
				},
				{
					Name:   "recv-a",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-b",
						},
					},
				},
			},
			out: []alertingmodels.Receiver{
				{
					Name:   "recv-a",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-a",
						},
						{
							Name: "inte-b",
						},
					},
				},
			},
		},
		{
			name: "two receivers with same names, with integrations with same names, should return one receiver with one integrations",
			in: []alertingmodels.Receiver{
				{
					Name:   "recv-a",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-a",
						},
					},
				},
				{
					Name:   "recv-a",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-a",
						},
					},
				},
			},
			out: []alertingmodels.Receiver{
				{
					Name:   "recv-a",
					Active: true,
					Integrations: []alertingmodels.Integration{
						{
							Name: "inte-a",
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
		in   []alertingmodels.Integration
		err  error
		out  []alertingmodels.Integration
	}{
		{
			name: "no integrations, should return an empty list",
			in:   []alertingmodels.Integration{},
			out:  []alertingmodels.Integration{},
		},
		{
			name: "one integration, should return the integration",
			in: []alertingmodels.Integration{
				{
					Name:                      "int-a",
					SendResolved:              true,
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "",
				},
			},
			out: []alertingmodels.Integration{
				{
					Name:                      "int-a",
					SendResolved:              true,
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "",
				},
			},
		},
		{
			name: "two integrations with different names, should return two integrations in name order",
			in: []alertingmodels.Integration{
				{
					Name:                      "int-b",
					SendResolved:              true,
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:01.000Z"),
					LastNotifyAttemptDuration: "2s",
					LastNotifyAttemptError:    "err-b",
				},
				{
					Name:                      "int-a",
					SendResolved:              true,
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "err-a",
				},
			},
			out: []alertingmodels.Integration{
				{
					Name:                      "int-a",
					SendResolved:              true,
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "err-a",
				},
				{
					Name:                      "int-b",
					SendResolved:              true,
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:01.000Z"),
					LastNotifyAttemptDuration: "2s",
					LastNotifyAttemptError:    "err-b",
				},
			},
		},
		{
			name: "two integrations with same name, should return integration with newest notify attempt",
			in: []alertingmodels.Integration{
				{
					Name:                      "int-a",
					SendResolved:              true,
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:01.000Z"),
					LastNotifyAttemptDuration: "2s",
					LastNotifyAttemptError:    "err-2",
				},
				{
					Name:                      "int-a",
					SendResolved:              true,
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "err-1",
				},
			},
			out: []alertingmodels.Integration{
				{
					Name:                      "int-a",
					SendResolved:              true,
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:01.000Z"),
					LastNotifyAttemptDuration: "2s",
					LastNotifyAttemptError:    "err-2",
				},
			},
		},
		{
			name: "two integrations with same name, one without a notify attempt, should return integration with notify attempt",
			in: []alertingmodels.Integration{
				{
					Name:                      "int-a",
					SendResolved:              true,
					LastNotifyAttempt:         strfmt.DateTime(time.Time{}),
					LastNotifyAttemptDuration: "",
					LastNotifyAttemptError:    "",
				},
				{
					Name:                      "int-a",
					SendResolved:              true,
					LastNotifyAttempt:         *v2ParseTime("2020-01-01T12:00:00.000Z"),
					LastNotifyAttemptDuration: "1s",
					LastNotifyAttemptError:    "",
				},
			},
			out: []alertingmodels.Integration{
				{
					Name:                      "int-a",
					SendResolved:              true,
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
