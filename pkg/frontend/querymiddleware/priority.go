package querymiddleware

import (
	"context"
	"crypto/md5"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/util/reactivelimiter"
)

const (
	// gRPC metadata key for priority level propagation
	PriorityLevelKey = "x-mimir-priority-level"

	// Query source detection
	QuerySourceRuler        = "ruler"
	QuerySourceDashboard    = "dashboard"
	QuerySourceAPI          = "api"
	QuerySourceBackground   = "background"
	QuerySourceUnknown      = "unknown"

	// Priority range constants
	PriorityRangeSize = 100

	// Default priority assignments (using all 5 levels)
	DefaultRulerPriority      = reactivelimiter.PriorityVeryHigh // 400-499 (most critical)
	DefaultDashboardPriority  = reactivelimiter.PriorityHigh    // 300-399 (interactive)
	DefaultAPIPriority        = reactivelimiter.PriorityMedium   // 200-299 (programmatic)  
	DefaultBackgroundPriority = reactivelimiter.PriorityLow     // 100-199 (batch jobs)
	DefaultUnknownPriority    = reactivelimiter.PriorityVeryLow // 0-99   (lowest priority)
)

// PriorityConfig holds configuration for priority assignment
type PriorityConfig struct {
	Enabled bool `yaml:"enabled"`
}

// PriorityAssigner handles priority level assignment
type PriorityAssigner struct {
	cfg    PriorityConfig
	logger log.Logger
	rand   *rand.Rand
}

// NewPriorityAssigner creates a new priority assigner
func NewPriorityAssigner(cfg PriorityConfig, logger log.Logger) *PriorityAssigner {
	return &PriorityAssigner{
		cfg:    cfg,
		logger: logger,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// AssignPriorityLevel assigns priority level to a request context
func (p *PriorityAssigner) AssignPriorityLevel(ctx context.Context, method string, url string, headers map[string]string) context.Context {
	if !p.cfg.Enabled {
		return ctx
	}

	// Detect query source and get base priority
	source := p.detectQuerySource(method, url, headers)
	basePriority := p.getPriorityForSource(source)

	// Get request ID for level calculation (trace ID or generate one)
	requestID := p.getRequestID(ctx)

	// Calculate final level: priority_base + hash(request_id) % 100
	priorityLevel := p.calculateLevel(basePriority, requestID)

	level.Debug(p.logger).Log(
		"msg", "assigned priority level",
		"source", source,
		"base_priority", basePriority,
		"final_level", priorityLevel,
		"url", url,
	)

	return WithPriorityLevel(ctx, priorityLevel)
}

// detectQuerySource determines the source of the query
func (p *PriorityAssigner) detectQuerySource(method string, url string, headers map[string]string) string {
	userAgent := headers["User-Agent"]

	// Detect ruler queries by User-Agent pattern
	if strings.Contains(userAgent, "mimir-ruler") {
		return QuerySourceRuler
	}

	// Detect Grafana dashboard queries
	if strings.Contains(userAgent, "Grafana/") {
		return QuerySourceDashboard
	}

	// Detect background/batch processing
	if p.isBackgroundQuery(userAgent) {
		return QuerySourceBackground
	}

	// Everything else is API/external
	if userAgent != "" {
		return QuerySourceAPI
	}

	return QuerySourceUnknown
}

// isBackgroundQuery checks if the query appears to be background/batch processing
func (p *PriorityAssigner) isBackgroundQuery(userAgent string) bool {
	lowerUA := strings.ToLower(userAgent)
	return strings.Contains(lowerUA, "batch") ||
		strings.Contains(lowerUA, "background") ||
		strings.Contains(lowerUA, "cron") ||
		strings.Contains(lowerUA, "scheduler") ||
		strings.Contains(lowerUA, "prometheus") // Prometheus federation/scraping
}

// getPriorityForSource maps query source to priority level
func (p *PriorityAssigner) getPriorityForSource(source string) reactivelimiter.Priority {
	switch source {
	case QuerySourceRuler:
		return DefaultRulerPriority      // VeryHigh (400-499) - Critical for alerting
	case QuerySourceDashboard:
		return DefaultDashboardPriority  // High (300-399) - Interactive users
	case QuerySourceAPI:
		return DefaultAPIPriority        // Medium (200-299) - Programmatic access
	case QuerySourceBackground:
		return DefaultBackgroundPriority // Low (100-199) - Batch processing
	default: // QuerySourceUnknown
		return DefaultUnknownPriority    // VeryLow (0-99) - Lowest priority
	}
}

// getRequestID extracts trace ID or generates request ID for level calculation
func (p *PriorityAssigner) getRequestID(ctx context.Context) string {
	// First try to extract trace ID from context if request is sampled for tracing
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		return span.SpanContext().TraceID().String()
	}

	// Generate a new request ID for this request
	return p.generateRequestID()
}

// generateRequestID creates a unique request ID
func (p *PriorityAssigner) generateRequestID() string {
	// Simple approach: timestamp + random component
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), p.rand.Int63())
}

// calculateLevel determines the final level (0-499) based on priority and request ID
func (p *PriorityAssigner) calculateLevel(priority reactivelimiter.Priority, requestID string) int {
	// Get the base level for this priority (all 5 levels)
	baseLevelMap := map[reactivelimiter.Priority]int{
		reactivelimiter.PriorityVeryLow:  0,   // 0-99   (unknown/lowest)
		reactivelimiter.PriorityLow:      100, // 100-199 (background/batch)
		reactivelimiter.PriorityMedium:   200, // 200-299 (API/programmatic)
		reactivelimiter.PriorityHigh:     300, // 300-399 (dashboard/interactive)
		reactivelimiter.PriorityVeryHigh: 400, // 400-499 (ruler/critical)
	}

	baseLevel, exists := baseLevelMap[priority]
	if !exists {
		baseLevel = baseLevelMap[reactivelimiter.PriorityVeryLow] // Default to lowest
	}

	// Hash the request ID to get a value between 0 and 99
	hashValue := p.hashRequestID(requestID)

	// Final level = base + hash % 100
	return baseLevel + (hashValue % PriorityRangeSize)
}

// hashRequestID hashes the request ID to get a deterministic value for level calculation
func (p *PriorityAssigner) hashRequestID(requestID string) int {
	// Use MD5 hash for simplicity and convert to int
	hasher := md5.New()
	hasher.Write([]byte(requestID))
	hashBytes := hasher.Sum(nil)

	// Convert first 4 bytes to int
	hashInt := int(hashBytes[0]) | int(hashBytes[1])<<8 | int(hashBytes[2])<<16 | int(hashBytes[3])<<24
	if hashInt < 0 {
		hashInt = -hashInt // Ensure positive
	}

	return hashInt
}

// GetPriorityLevel extracts priority level (0-499) from context
func GetPriorityLevel(ctx context.Context) int {
	if priorityLevel := ctx.Value(PriorityLevelKey); priorityLevel != nil {
		if levelInt, ok := priorityLevel.(int); ok {
			return levelInt
		}
	}
	return 200 // Default to PriorityMedium base (200-299 range)
}

// WithPriorityLevel adds priority level to context
func WithPriorityLevel(ctx context.Context, priorityLevel int) context.Context {
	return context.WithValue(ctx, PriorityLevelKey, priorityLevel)
}