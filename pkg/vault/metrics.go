// SPDX-License-Identifier: AGPL-3.0-only

package vault

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	authTotal             prometheus.Counter
	authLeaseRenewalTotal prometheus.Counter
}

func newMetrics(r prometheus.Registerer) *metrics {
	var m metrics

	m.authTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "vault_auth_total",
		Help: "Total number of times authentication to Vault happened during token lifecycle management",
	})

	m.authLeaseRenewalTotal = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "vault_token_lease_renewal_total",
		Help: "Total number of times the auth token was renewed",
	})

	return &m
}
