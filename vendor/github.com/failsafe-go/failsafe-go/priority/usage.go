package priority

import (
	"container/list"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/failsafe-go/failsafe-go/internal/util"
)

const (
	// UserKey is a key to use with a Context that stores a user ID.
	UserKey key = 2

	// The max level value for a priority class
	maxLevel = 99.0
)

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
	newWindowFn        func() *usageWindow
	maxUsers           int
	expirationDuration time.Duration

	mu sync.RWMutex
	// Guarded by mu
	users map[string]*userEntry
	lru   *list.List
}

type userEntry struct {
	window     *usageWindow
	quantile   float64 // Negative value represents being uncalibrated
	lastActive time.Time
	lruElement *list.Element
}

// NewUsageTracker creates a new UsageTracker with the specified configuration. The UsageTracker will track up to the
// maxUsers, and track any recent usage within the usageWindow. If a user hasn't had activity in 2x the usageWindow,
// they're removed from the tracker.
func NewUsageTracker(windowDuration time.Duration, maxUsers int) UsageTracker {
	return &usageTracker{
		clock: util.WallClock,
		newWindowFn: func() *usageWindow {
			return newUsageWindow(30, windowDuration, util.WallClock)
		},
		maxUsers:           maxUsers,
		expirationDuration: 2 * windowDuration,
		users:              make(map[string]*userEntry),
		lru:                list.New(),
	}
}

func (ut *usageTracker) RecordUsage(userID string, usage int64) {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	entry := ut.users[userID]
	if entry == nil {
		if len(ut.users) >= ut.maxUsers {
			ut.evictOldest()
		}
		entry = &userEntry{
			window:   ut.newWindowFn(),
			quantile: -1,
		}
		ut.users[userID] = entry
		entry.lruElement = ut.lru.PushFront(userID)
	} else {
		ut.lru.MoveToFront(entry.lruElement)
	}

	entry.lastActive = ut.clock.Now()
	entry.window.RecordUsage(usage)
}

func (ut *usageTracker) GetLevel(userID string, priority Priority) int {
	ut.mu.RLock()
	entry := ut.users[userID]
	ut.mu.RUnlock()

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
	return lRange.lower + int(maxLevel*fairnessScore)
}

func (ut *usageTracker) GetUsage(userID string) (int64, bool) {
	ut.mu.RLock()
	defer ut.mu.RUnlock()

	entry := ut.users[userID]
	if entry == nil {
		return 0, false
	}
	return entry.window.TotalUsage(), true
}

// Calibrate has an O(n log n) time complexity, where n is the number of users.
func (ut *usageTracker) Calibrate() {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	now := ut.clock.Now()
	cleanupThreshold := now.Add(-ut.expirationDuration)
	usages := make([]int64, 0, len(ut.users))

	for userID, entry := range ut.users {
		entry.window.ExpireBuckets()
		if usage := entry.window.TotalUsage(); usage > 0 {
			usages = append(usages, usage)
		} else if entry.lastActive.Before(cleanupThreshold) {
			delete(ut.users, userID)
			ut.lru.Remove(entry.lruElement)
		}
	}

	sort.Slice(usages, func(i, j int) bool {
		return usages[i] < usages[j]
	})

	// Update percentiles for all active users
	for _, entry := range ut.users {
		if usage := entry.window.TotalUsage(); usage > 0 {
			entry.quantile = ut.computeQuantile(usage, usages)
		} else {
			entry.quantile = -1
		}
	}
}

// computeQuantile returns the quantile for a usage, among the sortedUsages.
func (ut *usageTracker) computeQuantile(usage int64, sortedUsages []int64) float64 {
	if len(sortedUsages) == 0 {
		return 0
	}

	index := sort.Search(len(sortedUsages), func(i int) bool {
		return sortedUsages[i] >= usage
	})

	return util.Round(float64(index) / float64(len(sortedUsages)))
}

func (ut *usageTracker) evictOldest() {
	if oldest := ut.lru.Back(); oldest != nil {
		userID := oldest.Value.(string)
		delete(ut.users, userID)
		ut.lru.Remove(oldest)
	}
}

type usageStat struct {
	totalUsage int64
	samples    uint32
}

type usageWindow struct {
	util.BucketedWindow[usageStat]
}

func newUsageWindow(bucketCount int, thresholdingPeriod time.Duration, clock util.Clock) *usageWindow {
	buckets := make([]usageStat, bucketCount)

	return &usageWindow{
		util.BucketedWindow[usageStat]{
			Clock:       clock,
			BucketCount: int64(bucketCount),
			BucketNanos: (thresholdingPeriod / time.Duration(bucketCount)).Nanoseconds(),
			Buckets:     buckets,
			Summary:     usageStat{},
			AddFn: func(summary *usageStat, bucket *usageStat) {
				summary.totalUsage += bucket.totalUsage
				summary.samples += bucket.samples
			},
			RemoveFn: func(summary *usageStat, bucket *usageStat) {
				summary.totalUsage -= bucket.totalUsage
				summary.samples -= bucket.samples
			},
			ResetFn: func(s *usageStat) {
				s.totalUsage = 0
				s.samples = 0
			},
		},
	}
}

func (w *usageWindow) RecordUsage(usage int64) {
	bucket := w.ExpireBuckets()
	bucket.totalUsage += usage
	bucket.samples++
	w.Summary.totalUsage += usage
	w.Summary.samples++
}

func (w *usageWindow) TotalUsage() int64 {
	return w.Summary.totalUsage
}

func (w *usageWindow) Samples() uint32 {
	return w.Summary.samples
}
