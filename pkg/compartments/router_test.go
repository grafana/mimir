// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRouter(numCompartments int) *Router {
	return NewRouter(ReadConfig{NumCompartments: numCompartments, KafkaTopicFormat: "mimir-rc-<compartment-id>"})
}

func TestRouter_TopicForCompartment(t *testing.T) {
	r := newTestRouter(3)
	require.Equal(t, 3, r.NumCompartments())
	assert.Equal(t, "mimir-rc-0", r.TopicForCompartment(0))
	assert.Equal(t, "mimir-rc-1", r.TopicForCompartment(1))
	assert.Equal(t, "mimir-rc-2", r.TopicForCompartment(2))
}

func TestRouter_CompartmentForMetric(t *testing.T) {
	r := newTestRouter(4)

	t.Run("deterministic", func(t *testing.T) {
		first := r.CompartmentForMetric("user-1", "metric_name")
		for i := 0; i < 100; i++ {
			assert.Equal(t, first, r.CompartmentForMetric("user-1", "metric_name"))
		}
	})

	t.Run("always within bounds", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			id := r.CompartmentForMetric("user-1", "metric_"+strconv.Itoa(i))
			assert.GreaterOrEqual(t, id, 0)
			assert.Less(t, id, r.NumCompartments())
		}
	})

	t.Run("consistent with TopicForMetric", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			name := "metric_" + strconv.Itoa(i)
			assert.Equal(t, r.TopicForCompartment(r.CompartmentForMetric("user-1", name)), r.TopicForMetric("user-1", name))
		}
	})
}

func TestRouter_TopicForMetric(t *testing.T) {
	t.Run("the same metric may map to different compartments for different tenants", func(t *testing.T) {
		r := newTestRouter(16)
		differ := false
		for i := 0; i < 100; i++ {
			a := r.CompartmentForMetric("user-a-"+strconv.Itoa(i), "the_metric")
			b := r.CompartmentForMetric("user-b-"+strconv.Itoa(i), "the_metric")
			if a != b {
				differ = true
				break
			}
		}
		assert.True(t, differ, "expected the tenant to affect the compartment assignment")
	})

	t.Run("all compartments are used given enough distinct metrics", func(t *testing.T) {
		const numCompartments = 8
		r := newTestRouter(numCompartments)
		seen := make(map[int]struct{})
		for i := 0; i < 10000 && len(seen) < numCompartments; i++ {
			seen[r.CompartmentForMetric("user-1", fmt.Sprintf("metric_%d", i))] = struct{}{}
		}
		assert.Len(t, seen, numCompartments)
	})
}
