package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestVersioned(t *testing.T) {
	t.Run("happy case: can store and retrieve", func(t *testing.T) {
		cache := NewMockCache()
		v1 := NewVersioned(cache, 1)
		data := map[string][]byte{"hit": []byte(`data`)}
		v1.Store(context.Background(), data, time.Minute)
		res := v1.Fetch(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, data, res)
	})

	t.Run("different versions use different datasets", func(t *testing.T) {
		cache := NewMockCache()
		v1 := NewVersioned(cache, 1)
		v1Data := map[string][]byte{"hit": []byte(`first version`)}
		v1.Store(context.Background(), v1Data, time.Minute)
		v2 := NewVersioned(cache, 2)
		v2Data := map[string][]byte{"hit": []byte(`second version`)}
		v2.Store(context.Background(), v2Data, time.Minute)

		resV1 := v1.Fetch(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, v1Data, resV1)
		resV2 := v2.Fetch(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, v2Data, resV2)
	})
}
