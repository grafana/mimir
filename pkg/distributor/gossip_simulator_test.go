package distributor

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

type distrib struct {
	cluster *haClusterInfo
	kvEntry *ReplicaDesc
}

type simulation struct {
	propagationDelay int
	updateTimeout    int
	distributors     []distrib

	accepted map[string]int
	dropped  map[string]int
}

type futureChange struct {
	time   int
	change func(time int, s *simulation) []futureChange
}

func (s *simulation) propagateKvUpdate(time int, skipDistrib int, desc ReplicaDesc) []futureChange {
	var changes []futureChange
	for ix := range s.distributors {
		if ix == skipDistrib {
			continue
		}

		if s.distributors[ix].kvEntry == nil || s.distributors[ix].kvEntry.ReceivedAt < desc.ReceivedAt {
			s.distributors[ix].kvEntry = &desc
			if s.distributors[ix].cluster == nil {
				s.distributors[ix].cluster = &haClusterInfo{}
			}
			s.distributors[ix].cluster.elected = desc

			fmt.Println("time", time, "distributor", ix, "got KV change", desc.String())

			changes = append(changes, futureChange{
				time: time + s.propagationDelay,
				change: func(time int, s *simulation) []futureChange {
					return s.propagateKvUpdate(time, ix, desc)
				},
			})
		}
	}
	return changes
}

func (s *simulation) requestReceived(time int, distrib int, fromReplica string) []futureChange {
	//fmt.Println("time", time, "request received on distributor", distrib, "from replica", fromReplica)
	if s.distributors[distrib].cluster == nil && s.distributors[distrib].kvEntry != nil {
		s.distributors[distrib].cluster = &haClusterInfo{
			elected: *s.distributors[distrib].kvEntry,
		}
	}

	if s.distributors[distrib].cluster == nil {
		rd := ReplicaDesc{
			Replica:    fromReplica,
			ReceivedAt: int64(time),
		}

		s.distributors[distrib].cluster = &haClusterInfo{
			elected:                  rd,
			electedLastSeenTimestamp: int64(time),
		}
		s.distributors[distrib].kvEntry = &rd

		fmt.Println("time", time, "distributor", distrib, "accepted request from replica", fromReplica)
		s.accepted[fromReplica]++

		return []futureChange{{
			time: time + s.propagationDelay,
			change: func(time int, s *simulation) []futureChange {
				return s.propagateKvUpdate(time, distrib, rd)
			},
		}}
	}

	if s.distributors[distrib].cluster.elected.Replica == fromReplica {
		s.distributors[distrib].cluster.electedLastSeenTimestamp = int64(time)

		fmt.Println("time", time, "distributor", distrib, "accepted request from replica", fromReplica)
		s.accepted[fromReplica]++
	} else {
		s.distributors[distrib].cluster.nonElectedLastSeenTimestamp = int64(time)
		s.distributors[distrib].cluster.nonElectedLastSeenReplica = fromReplica

		fmt.Println("time", time, "distributor", distrib, "dropped request from replica", fromReplica)
		s.dropped[fromReplica]++
	}

	return nil
}

// Simulate update of distributor's KV Store from cached values
func (s *simulation) kvStoreUpdate(time int, distrib int) []futureChange {
	if s.distributors[distrib].cluster == nil {
		return nil
	}

	// nothing to do
	if s.withinUpdateTimeout(time, s.distributors[distrib].cluster.elected.ReceivedAt) {
		return nil
	}

	replica := ""
	if s.withinUpdateTimeout(time, s.distributors[distrib].cluster.electedLastSeenTimestamp) {
		replica = s.distributors[distrib].cluster.elected.Replica
	} else if s.withinUpdateTimeout(time, s.distributors[distrib].cluster.nonElectedLastSeenTimestamp) {
		replica = s.distributors[distrib].cluster.nonElectedLastSeenReplica
	} else {
		return nil
	}

	rd := ReplicaDesc{Replica: replica, ReceivedAt: int64(time)}
	if s.distributors[distrib].kvEntry == nil || s.distributors[distrib].kvEntry.ReceivedAt < int64(time) {
		if s.distributors[distrib].kvEntry.Replica != replica {
			fmt.Println("time", time, "distributor", distrib, "propagating replica (kvStoreUpdate)", replica)
		}

		s.distributors[distrib].kvEntry = &rd
		s.distributors[distrib].cluster.elected = rd

		return []futureChange{{
			time: time + s.propagationDelay,
			change: func(time int, s *simulation) []futureChange {
				return s.propagateKvUpdate(time, distrib, rd)
			},
		}}
	}

	return nil
}

func (s *simulation) withinUpdateTimeout(now int, ts int64) bool {
	return (int64(now) - ts) < int64(s.updateTimeout)
}

func TestRunSimulation(t *testing.T) {
	now := time.Now().UnixNano()
	fmt.Println("seed:", now)
	r := rand.New(rand.NewSource(now))

	const (
		requests         = 20
		distributors     = 5
		requestDelay     = 10
		updateTimeout    = 15
		propagationDelay = 3
	)

	s := simulation{
		propagationDelay: propagationDelay,
		updateTimeout:    updateTimeout,
		distributors:     make([]distrib, distributors),

		accepted: map[string]int{},
		dropped:  map[string]int{},
	}

	var futureChanges []futureChange

	// Gegenrate requests from prom1 every
	futureChanges = append(futureChanges, generateRequestsForReplica(r, requests, r.Intn(requestDelay), requestDelay, "prom1")...)
	futureChanges = append(futureChanges, generateRequestsForReplica(r, requests, r.Intn(requestDelay), requestDelay, "prom2")...)

	for d := range s.distributors {
		futureChanges = append(futureChanges, generateKVStoreUpdatesForDistributor(d, s.updateTimeout, 500)...)
	}

	sort.Sort(sortedFutureChanges(futureChanges))

	for len(futureChanges) > 0 {
		fc := futureChanges[0]
		futureChanges = futureChanges[1:]

		moreChanges := fc.change(fc.time, &s)
		futureChanges = append(futureChanges, moreChanges...)
		sort.Stable(sortedFutureChanges(futureChanges))
	}

	fmt.Println("accepted:", s.accepted)
	fmt.Println("dropped:", s.dropped)
}

func generateKVStoreUpdatesForDistributor(distrib int, interval int, maxTime int) []futureChange {
	var result []futureChange

	for t := distrib; t < maxTime; t += interval {
		result = append(result,
			futureChange{
				time: t,
				change: func(time int, s *simulation) []futureChange {
					return s.kvStoreUpdate(time, distrib)
				},
			})
	}
	return result
}

func generateRequestsForReplica(r *rand.Rand, requests int, initialDelay, interval int, replica string) []futureChange {
	var result []futureChange

	for i := 0; i < requests; i++ {
		//if r.Float64() < 0.25 {
		//	continue
		//}

		result = append(result,
			futureChange{
				time: initialDelay + (i * interval),
				change: func(time int, s *simulation) []futureChange {
					d := r.Intn(len(s.distributors))
					return s.requestReceived(time, d, replica)
				},
			})
	}
	return result
}

type sortedFutureChanges []futureChange

func (s sortedFutureChanges) Len() int           { return len(s) }
func (s sortedFutureChanges) Less(i, j int) bool { return s[i].time < s[j].time }
func (s sortedFutureChanges) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
