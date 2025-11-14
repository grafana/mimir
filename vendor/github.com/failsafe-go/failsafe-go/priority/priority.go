package priority

import (
	"context"
	"math"
	"math/rand"
	"sync"
)

// Priority is an execution priority.
type Priority int

const (
	VeryLow Priority = iota
	Low
	Medium
	High
	VeryHigh
)

const totalLevels = 500

// RandomLevel returns a random level for the Priority.
func (p Priority) RandomLevel() int {
	r := priorityLevelRanges[p]
	return rand.Intn(r.upper-r.lower+1) + r.lower
}

// AddTo returns the ctx with the priority added to it as a value with the PriorityKey.
func (p Priority) AddTo(ctx context.Context) context.Context {
	return ContextWithPriority(ctx, p)
}

// MaxLevel returns the max level for the priority.
func (p Priority) MaxLevel() int {
	return p.levelRange().upper
}

// MinLevel returns the min level for the priority.
func (p Priority) MinLevel() int {
	return p.levelRange().lower
}

func (p Priority) levelRange() levelRange {
	return priorityLevelRanges[p]
}

// levelRange provides a wider range of levels that allow for rejecting a subset of executions within a Priority.
type levelRange struct {
	lower, upper int
}

var priorityLevelRanges = map[Priority]levelRange{
	VeryLow:  {0, 99},
	Low:      {100, 199},
	Medium:   {200, 299},
	High:     {300, 399},
	VeryHigh: {400, 499},
}

type key int

// PriorityKey is a key to use with a Context that stores the priority value.
const PriorityKey key = 0

// LevelKey is a key to use with a Context that stores the level value.
const LevelKey key = 1

// ContextWithPriority returns a context with the priority value stored with the PriorityKey.
func ContextWithPriority(ctx context.Context, priority Priority) context.Context {
	return context.WithValue(ctx, PriorityKey, priority)
}

// ContextWithLevel returns a context with the level value stored with the LevelKey.
func ContextWithLevel(ctx context.Context, level int) context.Context {
	return context.WithValue(ctx, LevelKey, level)
}

// FromContext returns the priority from the context, else -1.
func FromContext(ctx context.Context) Priority {
	if untypedPriority := ctx.Value(PriorityKey); untypedPriority != nil {
		if priority, ok := untypedPriority.(Priority); ok {
			return priority
		}
	}
	return -1
}

// LevelFromContext returns a level for the level contained within the given context, else if a priority is contained
// within the context, a random level is generated within that priority, else -1 is returned.
func LevelFromContext(ctx context.Context) int {
	if untypedLevel := ctx.Value(LevelKey); untypedLevel != nil {
		if level, ok := untypedLevel.(int); ok {
			return level
		}
	}
	if untypedPriority := ctx.Value(PriorityKey); untypedPriority != nil {
		if priority, ok := untypedPriority.(Priority); ok {
			return priority.RandomLevel()
		}
	}
	return -1
}

// LevelTracker tracks priority levels for executions, which can be used to prioritize rejections.
type LevelTracker interface {
	// RecordLevel records an execution having been accepted for the level.
	RecordLevel(level int)

	// GetLevel returns the level that falls at the quantile among all recorded levels in the tracker, else returns 0 if no
	// levels have been recorded.
	GetLevel(quantile float64) int
}

type windowedLevelTracker struct {
	mu          sync.Mutex
	window      []int // records recent levels
	levelCounts []int // current counts of each level
	head        int
	filled      bool
}

// NewLevelTracker creates a LevelTracker that stores the last windowSize recorded levels.
func NewLevelTracker(windowSize int) LevelTracker {
	return &windowedLevelTracker{
		window:      make([]int, windowSize),
		levelCounts: make([]int, totalLevels),
	}
}

func (lt *windowedLevelTracker) RecordLevel(level int) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	// Remove old value from counts
	if lt.filled {
		oldLevel := lt.window[lt.head]
		lt.levelCounts[oldLevel]--
	}

	// Add new value to counts
	lt.window[lt.head] = level
	lt.levelCounts[level]++

	// Advance head
	lt.head++
	if lt.head >= len(lt.window) {
		lt.head = 0
		lt.filled = true
	}
}

func (lt *windowedLevelTracker) GetLevel(quantile float64) int {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	currentSize := len(lt.window)
	if !lt.filled {
		currentSize = lt.head
	}

	if currentSize > 0 {
		// Determine how many recorded levels we need to find to match the quantile
		targetLevels := int(math.Ceil(float64(currentSize) * quantile))
		if targetLevels < 1 {
			targetLevels = 1
		}

		// Count the levels until we hit the desired quantile
		countedLevels := 0
		for level := 0; level < totalLevels; level++ {
			countedLevels += lt.levelCounts[level]
			if countedLevels >= targetLevels {
				return level
			}
		}
	}

	return 0
}
