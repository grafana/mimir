// SPDX-License-Identifier: AGPL-3.0-only

package parquetconverter

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestConversionTask(t *testing.T) {
	now := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(now.UnixNano())), 0)
	blockULID := ulid.MustNew(ulid.Timestamp(now), entropy)

	meta := &block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID: blockULID,
		},
	}

	userID := "user-123"
	var bucket objstore.InstrumentedBucket

	t.Run("older-first criteria sets priority correctly", func(t *testing.T) {
		task := newConversionTask(userID, meta, bucket, OlderFirst)
		
		assert.Equal(t, userID, task.UserID)
		assert.Equal(t, meta, task.Meta)
		assert.Equal(t, bucket, task.Bucket)
		assert.Equal(t, int64(blockULID.Time()), task.Priority)
		assert.WithinDuration(t, time.Now(), task.EnqueuedAt, time.Second)
	})

	t.Run("newer-first criteria sets priority correctly", func(t *testing.T) {
		task := newConversionTask(userID, meta, bucket, NewerFirst)
		
		expectedPriority := -int64(blockULID.Time())
		assert.Equal(t, expectedPriority, task.Priority)
	})

	t.Run("fifo criteria sets priority correctly", func(t *testing.T) {
		task := newConversionTask(userID, meta, bucket, FIFO)
		
		// Priority should be based on enqueue time
		assert.True(t, task.Priority > 0)
		// Should be close to current nanosecond timestamp
		assert.True(t, task.Priority <= time.Now().UnixNano())
	})

	t.Run("random criteria sets priority correctly", func(t *testing.T) {
		task1 := newConversionTask(userID, meta, bucket, Random)
		task2 := newConversionTask(userID, meta, bucket, Random)
		
		// Random priorities should be positive and different (with high probability)
		assert.True(t, task1.Priority > 0)
		assert.True(t, task2.Priority > 0)
		// Note: There's a small chance they could be equal, but it's extremely unlikely
	})

	t.Run("invalid criteria returns error during flag parsing", func(t *testing.T) {
		var pc PriorityCriteria
		err := pc.Set("invalid-criteria")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid priority criteria")
		assert.Contains(t, err.Error(), "older-first, newer-first, random, fifo")
	})

	t.Run("valid criteria are parsed correctly", func(t *testing.T) {
		testCases := []struct {
			input    string
			expected PriorityCriteria
		}{
			{"older-first", OlderFirst},
			{"OLDER-FIRST", OlderFirst}, // Test case-insensitive
			{"newer-first", NewerFirst},
			{"NEWER-FIRST", NewerFirst},
			{"random", Random},
			{"RANDOM", Random},
			{"fifo", FIFO},
			{"FIFO", FIFO},
		}

		for _, tc := range testCases {
			var pc PriorityCriteria
			err := pc.Set(tc.input)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, pc)
		}
	})
}

func TestPriorityQueue_BasicOperations(t *testing.T) {
	pq := newPriorityQueue()

	t.Run("new queue is empty", func(t *testing.T) {
		assert.Equal(t, 0, pq.Size())

		task, ok := pq.Pop()
		assert.False(t, ok)
		assert.Nil(t, task)
	})

	t.Run("single item push and pop", func(t *testing.T) {
		task := createTestTask(t, "user-1", time.Now())

		success := pq.Push(task)
		assert.True(t, success)
		assert.Equal(t, 1, pq.Size())

		poppedTask, ok := pq.Pop()
		assert.True(t, ok)
		assert.NotNil(t, poppedTask)
		assert.Equal(t, task.UserID, poppedTask.UserID)
		assert.Equal(t, 0, pq.Size())
	})
}

func TestPriorityQueue_PriorityOrdering(t *testing.T) {
	pq := newPriorityQueue()

	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	// Create blocks with different ages
	task1 := createTestTask(t, "user-1", baseTime.Add(-2*time.Hour)) // Oldest
	task2 := createTestTask(t, "user-2", baseTime.Add(-1*time.Hour)) // Middle
	task3 := createTestTask(t, "user-3", baseTime)                   // Newest

	t.Run("priority queue orders by ULID timestamp (oldest first)", func(t *testing.T) {
		// Add in random order
		require.True(t, pq.Push(task2)) // Middle
		require.True(t, pq.Push(task1)) // Oldest
		require.True(t, pq.Push(task3)) // Newest

		assert.Equal(t, 3, pq.Size())

		// Should pop oldest first (task1), then task2, then task3
		poppedTask1, ok := pq.Pop()
		require.True(t, ok)
		assert.Equal(t, "user-1", poppedTask1.UserID)
		assert.Equal(t, task1.Priority, poppedTask1.Priority)

		poppedTask2, ok := pq.Pop()
		require.True(t, ok)
		assert.Equal(t, "user-2", poppedTask2.UserID)
		assert.Equal(t, task2.Priority, poppedTask2.Priority)

		poppedTask3, ok := pq.Pop()
		require.True(t, ok)
		assert.Equal(t, "user-3", poppedTask3.UserID)
		assert.Equal(t, task3.Priority, poppedTask3.Priority)

		assert.Equal(t, 0, pq.Size())
	})
}

func TestPriorityQueue_RealisticScenario(t *testing.T) {
	pq := newPriorityQueue()

	// Simulate discovery scenario: first find older blocks, then newer blocks
	now := time.Now()

	t.Run("older blocks are processed before newer blocks", func(t *testing.T) {
		// First discovery run: add blocks from last 2 hours
		oldTasks := make([]*conversionTask, 5)
		for i := 0; i < 5; i++ {
			taskTime := now.Add(-time.Duration(120-i*10) * time.Minute) // 120min, 110min, 100min, 90min, 80min ago
			oldTasks[i] = createTestTask(t, fmt.Sprintf("user-old-%d", i), taskTime)
			require.True(t, pq.Push(oldTasks[i]))
		}

		assert.Equal(t, 5, pq.Size())

		// Second discovery run: add very recent blocks (last 30 minutes)
		newTasks := make([]*conversionTask, 3)
		for i := 0; i < 3; i++ {
			taskTime := now.Add(-time.Duration(30-i*5) * time.Minute) // 30min, 25min, 20min ago
			newTasks[i] = createTestTask(t, fmt.Sprintf("user-new-%d", i), taskTime)
			require.True(t, pq.Push(newTasks[i]))
		}

		assert.Equal(t, 8, pq.Size())

		// Pop all tasks and verify oldest come first
		var poppedTasks []*conversionTask
		for pq.Size() > 0 {
			task, ok := pq.Pop()
			require.True(t, ok)
			poppedTasks = append(poppedTasks, task)
		}

		// Verify order: oldest blocks should come first
		assert.Equal(t, 8, len(poppedTasks))

		// First should be the oldest task
		assert.Equal(t, "user-old-0", poppedTasks[0].UserID) // 120min ago (oldest)
		assert.Equal(t, "user-old-1", poppedTasks[1].UserID) // 110min ago
		assert.Equal(t, "user-old-2", poppedTasks[2].UserID) // 100min ago
		assert.Equal(t, "user-old-3", poppedTasks[3].UserID) // 90min ago
		assert.Equal(t, "user-old-4", poppedTasks[4].UserID) // 80min ago

		// Then the newer tasks should follow
		assert.Equal(t, "user-new-0", poppedTasks[5].UserID) // 30min ago
		assert.Equal(t, "user-new-1", poppedTasks[6].UserID) // 25min ago
		assert.Equal(t, "user-new-2", poppedTasks[7].UserID) // 20min ago (newest)
	})
}

func TestPriorityQueue_MultipleUsers(t *testing.T) {
	pq := newPriorityQueue()

	users := []string{"tenant-a", "tenant-b", "tenant-c"}
	baseTime := time.Now()

	t.Run("prioritizes by block age not user", func(t *testing.T) {

		// Each user has blocks at different times
		for i, user := range users {
			for j := 0; j < 3; j++ {
				// Create tasks with interleaved timestamps
				taskTime := baseTime.Add(-time.Duration(i*30+j*10) * time.Minute)
				task := createTestTask(t, user, taskTime)
				require.True(t, pq.Push(task))
			}
		}

		assert.Equal(t, 9, pq.Size())

		// Pop all and verify they're ordered by priority, not user
		var poppedTasks []*conversionTask
		for pq.Size() > 0 {
			task, ok := pq.Pop()
			require.True(t, ok)
			poppedTasks = append(poppedTasks, task)
		}

		// Verify strict priority ordering (lowest priority first)
		for i := 1; i < len(poppedTasks); i++ {
			assert.LessOrEqual(t, poppedTasks[i-1].Priority, poppedTasks[i].Priority,
				"Task %d should have lower or equal priority than task %d", i-1, i)
		}

		// First task should be the oldest block
		assert.Equal(t, "tenant-c", poppedTasks[0].UserID)

		// Last task should be the newest block
		assert.Equal(t, "tenant-a", poppedTasks[8].UserID)
	})
}

func TestPriorityQueue_ClosedQueue(t *testing.T) {
	pq := newPriorityQueue()

	t.Run("closed queue rejects new items, but allows to pop", func(t *testing.T) {
		task1 := createTestTask(t, "user-1", time.Now())
		success := pq.Push(task1)
		assert.True(t, success)
		assert.Equal(t, 1, pq.Size())

		pq.Close()

		task2 := createTestTask(t, "user-2", time.Now())
		success = pq.Push(task2)
		assert.False(t, success)
		assert.Equal(t, 1, pq.Size()) // Size unchanged

		poppedTask, ok := pq.Pop()
		assert.True(t, ok)
		assert.Equal(t, "user-1", poppedTask.UserID)
		assert.Equal(t, 0, pq.Size())
	})
}

func TestPriorityQueue_PriorityCriteria(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("newer-first criteria processes newest blocks first", func(t *testing.T) {
		pq := newPriorityQueue()

		// Create tasks with different ages
		task1 := createTestTaskWithCriteria(t, "user-1", baseTime.Add(-2*time.Hour), NewerFirst) // Oldest
		task2 := createTestTaskWithCriteria(t, "user-2", baseTime.Add(-1*time.Hour), NewerFirst) // Middle
		task3 := createTestTaskWithCriteria(t, "user-3", baseTime, NewerFirst)                   // Newest

		// Add in random order
		require.True(t, pq.Push(task2))
		require.True(t, pq.Push(task1))
		require.True(t, pq.Push(task3))

		// Should pop newest first (task3), then task2, then task1
		poppedTask1, ok := pq.Pop()
		require.True(t, ok)
		assert.Equal(t, "user-3", poppedTask1.UserID) // Newest first

		poppedTask2, ok := pq.Pop()
		require.True(t, ok)
		assert.Equal(t, "user-2", poppedTask2.UserID)

		poppedTask3, ok := pq.Pop()
		require.True(t, ok)
		assert.Equal(t, "user-1", poppedTask3.UserID) // Oldest last
	})

	t.Run("fifo criteria processes blocks in enqueue order", func(t *testing.T) {
		pq := newPriorityQueue()

		// Create tasks at the same timestamp but with slight delays between enqueues
		tasks := make([]*conversionTask, 3)
		for i := 0; i < 3; i++ {
			tasks[i] = createTestTaskWithCriteria(t, fmt.Sprintf("user-%d", i), baseTime, FIFO)
			require.True(t, pq.Push(tasks[i]))
			time.Sleep(1 * time.Millisecond) // Small delay to ensure different enqueue times
		}

		// Should pop in FIFO order
		for i := 0; i < 3; i++ {
			poppedTask, ok := pq.Pop()
			require.True(t, ok)
			assert.Equal(t, fmt.Sprintf("user-%d", i), poppedTask.UserID)
		}
	})

	t.Run("random criteria produces different orderings", func(t *testing.T) {
		// Run multiple times to verify randomness (though this test could be flaky)
		orderings := make([][]string, 3)
		
		for run := 0; run < 3; run++ {
			pq := newPriorityQueue()
			
			// Create multiple tasks with random criteria
			userIDs := []string{"user-a", "user-b", "user-c", "user-d"}
			for _, userID := range userIDs {
				task := createTestTaskWithCriteria(t, userID, baseTime, Random)
				require.True(t, pq.Push(task))
			}
			
			// Record the order they come out
			var order []string
			for pq.Size() > 0 {
				task, ok := pq.Pop()
				require.True(t, ok)
				order = append(order, task.UserID)
			}
			
			orderings[run] = order
		}
		
		// At least one ordering should be different (high probability, not guaranteed)
		// This is a probabilistic test that could theoretically fail
		differentOrderingFound := false
		for i := 1; i < len(orderings); i++ {
			if !slicesEqual(orderings[0], orderings[i]) {
				differentOrderingFound = true
				break
			}
		}
		
		// Note: This assertion could fail with very low probability due to randomness
		// In practice, with 4! = 24 possible orderings, the chance of getting the same
		// ordering 3 times is 1/24^2 = 1/576, which is very low
		assert.True(t, differentOrderingFound, "Expected at least one different ordering from random criteria")
	})
}

// Helper function to compare slices
func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestPriorityQueue_ConcurrentAccess(t *testing.T) {
	pq := newPriorityQueue()

	t.Run("thread safety under concurrent access", func(t *testing.T) {
		const numGoroutines = 10
		const tasksPerGoroutine = 50

		done := make(chan bool, numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(routineID int) {
				defer func() { done <- true }()
				for j := 0; j < tasksPerGoroutine; j++ {
					task := createTestTask(t, fmt.Sprintf("user-%d-%d", routineID, j), time.Now())
					pq.Push(task)
				}
			}(i)
		}

		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		expectedSize := numGoroutines * tasksPerGoroutine
		assert.Equal(t, expectedSize, pq.Size())

		popCountChan := make(chan int, numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer func() { done <- true }()
				localCount := 0
				for {
					_, ok := pq.Pop()
					if !ok {
						break
					}
					localCount++
				}
				popCountChan <- localCount
			}()
		}

		// Wait for all pops to complete
		totalPopCount := 0
		for i := 0; i < numGoroutines; i++ {
			<-done
			select {
			case count := <-popCountChan:
				totalPopCount += count
			default:
				// No count to add
			}
		}

		assert.Equal(t, 0, pq.Size())
		assert.Equal(t, expectedSize, totalPopCount)
	})
}

func createTestTask(t *testing.T, userID string, timestamp time.Time) *conversionTask {
	return createTestTaskWithCriteria(t, userID, timestamp, OlderFirst)
}

func createTestTaskWithCriteria(t *testing.T, userID string, timestamp time.Time, criteria PriorityCriteria) *conversionTask {
	entropy := ulid.Monotonic(rand.New(rand.NewSource(timestamp.UnixNano())), 0)
	blockULID := ulid.MustNew(ulid.Timestamp(timestamp), entropy)

	meta := &block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID: blockULID,
		},
	}

	var bucket objstore.InstrumentedBucket
	task := newConversionTask(userID, meta, bucket, criteria)

	return task
}
