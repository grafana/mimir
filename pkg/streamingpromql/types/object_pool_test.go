// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestObjectPool_PoisonedQueryAbandonsObjects(t *testing.T) {
	type object struct{ value int }

	p := NewObjectPool(func() *object { return &object{} })
	tracker := limiter.NewUnlimitedMemoryConsumptionTracker(context.Background())

	first := p.Get()
	first.value = 42

	// Once the query has panicked and been recovered, its objects may still be referenced by its
	// inconsistent operators, so they must never be recycled into another query.
	tracker.Poison()
	p.Put(first, tracker)

	second := p.Get()
	require.NotSame(t, first, second, "an object returned by a poisoned query must not be handed out again")
	require.Equal(t, 0, second.value, "a fresh object is returned instead")
}
