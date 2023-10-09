// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	discardReasonLabel = "reason"
)

// DiscardedRequestsCounter creates per-user counter vector for requests discarded for a given reason.
func DiscardedRequestsCounter(reg prometheus.Registerer, reason string) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(
		prometheus.CounterOpts{
			Name: "cortex_discarded_requests_total",
			Help: "The total number of requests that were discarded due to rate limiting.",
			ConstLabels: map[string]string{
				discardReasonLabel: reason,
			},
		}, []string{"user"})
}

// DiscardedSamplesCounter creates per-user counter vector for samples discarded for a given reason.
func DiscardedSamplesCounter(reg prometheus.Registerer, reason string) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_discarded_samples_total",
		Help: "The total number of samples that were discarded.",
		ConstLabels: map[string]string{
			discardReasonLabel: reason,
		},
	}, []string{"user", "group"})
}

// DiscardedExemplarsCounter creates per-user counter vector for exemplars discarded for a given reason.
func DiscardedExemplarsCounter(reg prometheus.Registerer, reason string) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_discarded_exemplars_total",
		Help: "The total number of exemplars that were discarded.",
		ConstLabels: map[string]string{
			discardReasonLabel: reason,
		},
	}, []string{"user"})
}

// DiscardedMetadataCounter creates per-user counter vector for metadata discarded for a given reason.
func DiscardedMetadataCounter(reg prometheus.Registerer, reason string) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_discarded_metadata_total",
		Help: "The total number of metadata that were discarded.",
		ConstLabels: map[string]string{
			discardReasonLabel: reason,
		},
	}, []string{"user"})
}
