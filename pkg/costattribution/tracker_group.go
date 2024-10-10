package costattribution

import (
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/validation"
)

type attributionTrackerGroup struct {
	mu               sync.RWMutex
	trackersByUserID map[string]*tracker
	limits           *validation.Overrides
	cooldownTimeout  time.Duration
}

func newAttributionTrackerGroup(limits *validation.Overrides, cooldownTimeout time.Duration) *attributionTrackerGroup {
	return &attributionTrackerGroup{
		trackersByUserID: make(map[string]*tracker),
		limits:           limits,
		mu:               sync.RWMutex{},
		cooldownTimeout:  cooldownTimeout,
	}
}

// getUserAttributionLabelFromCache is read user attribution label through cache, if not found, get from config
func (atg *attributionTrackerGroup) getUserAttributionLabelFromCache(userID string) string {
	atg.mu.RLock()
	defer atg.mu.RUnlock()
	// if the user is not enabled for cost attribution, we don't need to track the attribution
	if atg.limits.CostAttributionLabel(userID) == "" {
		return ""
	}
	if _, exists := atg.trackersByUserID[userID]; !exists {
		atg.trackersByUserID[userID], _ = newTracker(atg.limits.CostAttributionLabel(userID), atg.limits.MaxCostAttributionPerUser(userID))
	}
	return atg.trackersByUserID[userID].trackedLabel
}

// getUserAttributionLimitFromCache is read per user attribution limit through cache, if not found, get from config
// always call only when the user is enabled for cost attribution
func (atg *attributionTrackerGroup) getUserAttributionLimitFromCache(userID string) int {
	atg.mu.Lock()
	defer atg.mu.Unlock()
	if _, exists := atg.trackersByUserID[userID]; !exists {
		atg.trackersByUserID[userID], _ = newTracker(atg.limits.CostAttributionLabel(userID), atg.limits.MaxCostAttributionPerUser(userID))
	}
	return atg.trackersByUserID[userID].attributionLimit
}

// deleteUserTracerFromCache is delete user from cache since the user is disabled for cost attribution
func (atg *attributionTrackerGroup) deleteUserTracerFromCache(userID string) {
	atg.mu.Lock()
	defer atg.mu.Unlock()
	if _, exists := atg.trackersByUserID[userID]; !exists {
		return
	}
	// clean up tracker metrics and delete the tracker
	atg.trackersByUserID[userID].cleanupTracker(userID)
	delete(atg.trackersByUserID, userID)
}

// updateAttributionCacheForUser function is guaranteed to update label and limit for the user in the cache
// if the label has changed, we will create a new tracker, and won't update the timestamp
// if the label has not changed, we will update the attribution timestamp
// if the limit is set to 0 or label is empty, we skip the update
func (atg *attributionTrackerGroup) updateAttributionCacheForUser(userID, label, attribution string, now time.Time) {
	// If the limit is set to 0, we don't need to track the attribution, clean the cache if exists
	if atg.limits.CostAttributionLabel(userID) == "" || atg.limits.MaxCostAttributionPerUser(userID) <= 0 {
		atg.deleteUserTracerFromCache(userID)
		return
	}
	ts := now.Unix()

	// if not in the cache, we create a new tracker
	if atg.trackersByUserID[userID] == nil {
		atg.trackersByUserID[userID], _ = newTracker(label, atg.limits.MaxCostAttributionPerUser(userID))
	}

	// if the label is not the one in the cache, we do nothing, that means the label input is outdated
	if label != atg.getUserAttributionLabelFromCache(userID) {
		return
	}

	/// update attribution timestamp
	if groupTs := atg.trackersByUserID[userID].attributionTimestamps[attribution]; groupTs != nil {
		groupTs.Store(ts)
		return
	}

	// if the user attribution is not exist, we add an attribution timestamp
	atg.mu.Lock()
	defer atg.mu.Unlock()
	atg.trackersByUserID[userID].attributionTimestamps[attribution] = atomic.NewInt64(ts)
}

func (atg *attributionTrackerGroup) purgeInactiveAttributionsForUser(userID string, deadline int64) []string {
	atg.mu.RLock()
	var inactiveAttributions []string
	if atg.trackersByUserID[userID] == nil {
		return nil
	}
	atg.mu.RUnlock()

	atg.mu.Lock()
	if atg.trackersByUserID[userID].trackedLabel != atg.limits.CostAttributionLabel(userID) {
		// reset everything if the label has changed
		atg.trackersByUserID[userID], _ = newTracker(atg.limits.CostAttributionLabel(userID), atg.limits.MaxCostAttributionPerUser(userID))
	}
	atg.mu.Unlock()

	atg.mu.RLock()
	attributionTimestamps := atg.trackersByUserID[userID].attributionTimestamps
	if attributionTimestamps == nil {
		return nil
	}
	for attr, ts := range attributionTimestamps {
		if ts.Load() <= deadline {
			inactiveAttributions = append(inactiveAttributions, attr)
		}
	}
	atg.mu.RUnlock()
	if len(inactiveAttributions) == 0 {
		return nil
	}

	// Cleanup inactive groups
	atg.mu.Lock()
	defer atg.mu.Unlock()

	for i := 0; i < len(inactiveAttributions); {
		inactiveAttribution := inactiveAttributions[i]
		groupTs := atg.trackersByUserID[userID].attributionTimestamps[inactiveAttribution]
		if groupTs != nil && groupTs.Load() <= deadline {
			delete(atg.trackersByUserID[userID].attributionTimestamps, inactiveAttribution)
			i++
		} else {
			inactiveAttributions[i] = inactiveAttributions[len(inactiveAttributions)-1]
			inactiveAttributions = inactiveAttributions[:len(inactiveAttributions)-1]
		}
	}

	return inactiveAttributions
}

func (atg *attributionTrackerGroup) purgeInactiveAttributions(inactiveTimeout time.Duration) {
	atg.mu.RLock()
	userIDs := make([]string, 0, len(atg.trackersByUserID))
	for userID := range atg.trackersByUserID {
		userIDs = append(userIDs, userID)
	}
	atg.mu.RUnlock()

	currentTime := time.Now()
	for _, userID := range userIDs {
		if atg.limits.CostAttributionLabel(userID) == "" || atg.limits.MaxCostAttributionPerUser(userID) <= 0 {
			atg.deleteUserTracerFromCache(userID)
			continue
		}
		// purge inactive attributions
		inactiveAttributions := atg.purgeInactiveAttributionsForUser(userID, currentTime.Add(-inactiveTimeout).UnixNano())
		for _, attribution := range inactiveAttributions {
			atg.trackersByUserID[userID].cleanupTrackerAttribution(userID, attribution)
		}
	}
}

func (atg *attributionTrackerGroup) attributionLimitExceeded(userID, attribution string, now time.Time) bool {
	// if we are still at the cooldown period, we will consider the limit reached
	atg.mu.RLock()
	defer atg.mu.RUnlock()
	// if the user is not exist, we don't need to check the limit
	if atg.trackersByUserID[userID] == nil {
		return false
	}

	if v := atg.trackersByUserID[userID].coolDownDeadline; v != nil && v.Load() > now.UnixNano() {
		return true
	}

	// if the user attribution is already exist and we are not in the cooldown period, we don't need to check the limit
	_, exists := atg.trackersByUserID[userID].attributionTimestamps[attribution]
	if exists {
		return false
	}

	// if the user has reached the limit, we will set the cooldown period which is 20 minutes
	maxReached := len(atg.trackersByUserID[userID].attributionTimestamps) >= atg.limits.MaxCostAttributionPerUser(userID)
	if maxReached {
		// if cooldownTimeout is set, we will set the cooldown period
		if atg.cooldownTimeout != 0 {
			atg.trackersByUserID[userID].coolDownDeadline.Store(now.Add(atg.cooldownTimeout).UnixNano())
		}
		return true
	}

	return maxReached
}
