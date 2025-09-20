package priority

import (
	"container/list"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/failsafe-go/failsafe-go/internal/util"
)

// UserKey is a key to use with a Context that stores a user ID.
const UserKey key = 2

// ContextWithUser returns a context with the userID stored with the UserKey.
func ContextWithUser(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, UserKey, userID)
}

// UserFromContext returns the userID from the context, else "".
func UserFromContext(ctx context.Context) string {
	if untypedUser := ctx.Value(UserKey); untypedUser != nil {
		if userID, ok := untypedUser.(string); ok {
			return userID
		}
	}
	return ""
}

// UsageTracker tracks resource usage per user as execution usages for fair execution prioritization.
type UsageTracker interface {
	// RecordUsage calculates and records usage for the user.
	RecordUsage(userID string, usage int64)

	// GetUsage returns the total recorded usage for a user, returning the usage and true if the user exists in the tracker,
	// else 0 and false.
	GetUsage(userID string) (int64, bool)

	// GetLevel returns the priority level for a user based on their recent usage.
	GetLevel(userID string, priority Priority) int

	// Calibrate calibrates levels based on the distribution of usage across all users.
	Calibrate()
}

type usageTracker struct {
	clock              util.Clock
	newWindowFn        func() *util.UsageWindow
	maxUsers           int
	expirationDuration time.Duration

	mu sync.RWMutex
	// Guarded by mu
	users map[string]*userEntry
	lru   *list.List
}

type userEntry struct {
	window     *util.UsageWindow
	quantile   float64 // Negative value represents being uncalibrated
	lastActive time.Time
	lruElement *list.Element
}

// NewUsageTracker creates a new UsageTracker with the specified configuration. The UsageTracker will track up to the
// maxUsers, and track any recent usage within the usageWindow. If a user hasn't had activity in 2x the usageWindow,
// they're removed from the tracker.
func NewUsageTracker(usageWindow time.Duration, maxUsers int) UsageTracker {
	return &usageTracker{
		clock: util.WallClock,
		newWindowFn: func() *util.UsageWindow {
			return util.NewUsageWindow(30, usageWindow, util.WallClock)
		},
		maxUsers:           maxUsers,
		expirationDuration: 2 * usageWindow,
		users:              make(map[string]*userEntry),
		lru:                list.New(),
	}
}

func (tt *usageTracker) RecordUsage(userID string, usage int64) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	entry := tt.users[userID]
	if entry == nil {
		if len(tt.users) >= tt.maxUsers {
			tt.evictOldest()
		}
		entry = &userEntry{
			window:   tt.newWindowFn(),
			quantile: -1,
		}
		tt.users[userID] = entry
		entry.lruElement = tt.lru.PushFront(userID)
	} else {
		tt.lru.MoveToFront(entry.lruElement)
	}

	entry.lastActive = tt.clock.Now()
	entry.window.RecordUsage(usage)
}

func (tt *usageTracker) GetLevel(userID string, priority Priority) int {
	tt.mu.RLock()
	entry := tt.users[userID]
	tt.mu.RUnlock()

	lRange := priority.levelRange()
	// Handle users that have no recorded usages
	if entry == nil {
		return lRange.upper
	}

	// Handle new entries with no recently recorded usages
	totalUsage := entry.window.TotalUsage()
	if totalUsage == 0 {
		return lRange.upper
	}

	// Handle uncalibrated user
	if entry.quantile < 0 {
		return lRange.upper
	}

	fairnessScore := 1.0 - entry.quantile
	return lRange.lower + int(99.0*fairnessScore)
}

func (tt *usageTracker) GetUsage(userID string) (int64, bool) {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	entry := tt.users[userID]
	if entry == nil {
		return 0, false
	}
	return entry.window.TotalUsage(), true
}

// Calibrate has an O(n log n) time complexity, where n is the number of userse.
func (tt *usageTracker) Calibrate() {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	now := tt.clock.Now()
	cleanupThreshold := now.Add(-tt.expirationDuration)
	usages := make([]int64, 0, len(tt.users))

	for userID, entry := range tt.users {
		entry.window.ExpireBuckets()
		if usage := entry.window.TotalUsage(); usage > 0 {
			usages = append(usages, usage)
		} else if entry.lastActive.Before(cleanupThreshold) {
			delete(tt.users, userID)
			tt.lru.Remove(entry.lruElement)
		}
	}

	sort.Slice(usages, func(i, j int) bool {
		return usages[i] < usages[j]
	})

	// Update percentiles for all active users
	for _, entry := range tt.users {
		if usage := entry.window.TotalUsage(); usage > 0 {
			entry.quantile = tt.computeQuantile(usage, usages)
		} else {
			entry.quantile = -1
		}
	}
}

// computeQuantile returns the quantile for a usage, among the sortedUsages.
func (tt *usageTracker) computeQuantile(usage int64, sortedUsages []int64) float64 {
	if len(sortedUsages) == 0 {
		return 0
	}

	index := sort.Search(len(sortedUsages), func(i int) bool {
		return sortedUsages[i] >= usage
	})

	return util.Round(float64(index) / float64(len(sortedUsages)))
}

func (tt *usageTracker) evictOldest() {
	if oldest := tt.lru.Back(); oldest != nil {
		userID := oldest.Value.(string)
		delete(tt.users, userID)
		tt.lru.Remove(oldest)
	}
}
