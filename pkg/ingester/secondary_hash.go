// SPDX-License-Identifier: AGPL-3.0-only

//go:build !stringlabels

package ingester

import (
	"github.com/prometheus/prometheus/model/labels"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
)

// this is taken from distributor.go

func SecondaryTSDBHashFunctionForUser(userID string) func(labels.Labels) uint32 {
	return func(ls labels.Labels) uint32 {
		return shardByAllLabels(userID, ls)
	}
}

func shardByUser(userID string) uint32 {
	h := ingester_client.HashNew32()
	h = ingester_client.HashAdd32(h, userID)
	return h
}

// This function generates different values for different order of same labels.
func shardByAllLabels(userID string, ls labels.Labels) uint32 {
	h := shardByUser(userID)
	for _, l := range ls {
		h = ingester_client.HashAdd32(h, l.Name)
		h = ingester_client.HashAdd32(h, l.Value)
	}
	return h
}
