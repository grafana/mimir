// SPDX-License-Identifier: AGPL-3.0-only

package vault

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	authTotal             prometheus.Counter
	authLeaseRenewalTotal prometheus.Counter
}

func newMetrics(r prometheus.Registerer) *metrics {
	var m metrics

	m.authTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vault_auth_total",
		Help: "Total number of times authentication to Vault happened during token lifecycle management",
	})

	m.authLeaseRenewalTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vault_token_lease_renewal_total",
		Help: "Total number of times the auth token was renewed",
	})

	if r != nil {
		r.MustRegister(
			m.authTotal,
			m.authLeaseRenewalTotal,
		)
	}

	return &m
}
