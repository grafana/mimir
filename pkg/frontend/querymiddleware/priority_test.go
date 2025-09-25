package querymiddleware

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
)

func TestPriorityAssigner(t *testing.T) {
	cfg := PriorityConfig{Enabled: true}
	assigner := NewPriorityAssigner(cfg, log.NewNopLogger())


	t.Run("ruler queries via User-Agent get highest priority", func(t *testing.T) {
		headers := map[string]string{"User-Agent": "mimir-ruler/2.11.0"}
		ctx := assigner.AssignPriorityLevel(context.Background(), "GET", "/api/v1/query", headers)
		level := GetPriorityLevel(ctx)
		assert.GreaterOrEqual(t, level, 400) // VeryHigh priority
		assert.Less(t, level, 500)
	})

	t.Run("dashboard queries get high priority", func(t *testing.T) {
		headers := map[string]string{"User-Agent": "Grafana/8.0.0"}
		ctx := assigner.AssignPriorityLevel(context.Background(), "GET", "/api/v1/query", headers)
		level := GetPriorityLevel(ctx)
		assert.GreaterOrEqual(t, level, 300) // High priority
		assert.Less(t, level, 400)
	})

	t.Run("disabled config returns original context", func(t *testing.T) {
		cfg := PriorityConfig{Enabled: false}
		assigner := NewPriorityAssigner(cfg, log.NewNopLogger())

		originalCtx := context.Background()
		ctx := assigner.AssignPriorityLevel(originalCtx, "GET", "/api/v1/query", nil)
		assert.Equal(t, originalCtx, ctx)
	})
}

func TestQuerySourceDetection(t *testing.T) {
	cfg := PriorityConfig{Enabled: true}
	assigner := NewPriorityAssigner(cfg, log.NewNopLogger())

	tests := []struct {
		name     string
		headers  map[string]string
		expected string
	}{
		{"ruler user agent", map[string]string{"User-Agent": "mimir-ruler/2.11.0"}, QuerySourceRuler},
		{"grafana user agent", map[string]string{"User-Agent": "Grafana/8.0.0"}, QuerySourceDashboard},
		{"prometheus user agent", map[string]string{"User-Agent": "prometheus/2.0"}, QuerySourceBackground},
		{"api user agent", map[string]string{"User-Agent": "curl/7.0"}, QuerySourceAPI},
		{"no user agent", map[string]string{}, QuerySourceUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := assigner.detectQuerySource("GET", "/api/v1/query", tt.headers)
			assert.Equal(t, tt.expected, source)
		})
	}
}
