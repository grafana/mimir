package main

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"golang.org/x/exp/slices"
)

type tokenGenerator func(ingesterID, zone string, ringDesc *ring.Desc) []uint32

func generateRingWithEvenlyDistributedTokensPerZone(numIngesters, numZones, numTokensPerIngester int, maxTokenValue uint32, now time.Time) (*ring.Desc, map[string]*tokenInfo) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokenInfoByZone := make(map[string]*tokenInfo, numZones)
	tokenInfoPriorityQueueByZone := make(map[string]*tokenInfoPriorityQueue, numZones)
	instanceIdByToken := make(map[uint32]string)

	tokens := generateRingWithEvenlyDistributedTokensGenerator(numIngesters, numZones, now, func(ingesterID, zone string, ringDesc *ring.Desc) []uint32 {
		// If the ring is empty, generate random bucketed tokens.
		if len(ringDesc.Ingesters) < numZones {
			return evenlyBucketedTokens(ingesterID, zone, numTokensPerIngester, maxTokenValue, r, tokenInfoByZone, tokenInfoPriorityQueueByZone, instanceIdByToken)
		}

		priorityQueue := tokenInfoPriorityQueueByZone[zone]
		tokenInfoHead := tokenInfoByZone[zone]
		myTokens := make([]uint32, 0, numTokensPerIngester)
		for i := 0; i < numTokensPerIngester; i++ {
			largestOwnershipTokenInfo := heap.Pop(priorityQueue).(*tokenInfoPriorityQueueItem).tokenInfo
			prevTokenInfo := largestOwnershipTokenInfo.prev
			candidateToken := calculateRegisteredOwnershipWithOffset(prevTokenInfo.token, uint32(math.Ceil(float64(largestOwnershipTokenInfo.registeredOwnership)/2)), maxTokenValue)
			for {
				_, taken := instanceIdByToken[candidateToken]
				if !taken {
					break
				}
				candidateToken = calculateRegisteredOwnership(candidateToken, 1, maxTokenValue)
			}
			instanceIdByToken[candidateToken] = ingesterID
			//fmt.Printf("Currently the largest difference for zone %s is for token %v, and it is %v\n", zone, largestOwnershipTokenInfo.token, largestOwnershipTokenInfo.registeredOwnership)
			//fmt.Printf("Its previous token is %v, so the candidate will be %v\n", prevTokenInfo.token, candidateToken)
			newTokenInfo := &tokenInfo{
				instanceID: ingesterID,
				zone:       zone,
				token:      candidateToken,
			}
			tokenInfoHead = newTokenInfo.insertBefore(tokenInfoHead, largestOwnershipTokenInfo, maxTokenValue)
			//fmt.Printf("List after: %s\n", tokenInfoHead.toString())
			heap.Push(priorityQueue, largestOwnershipTokenInfo)
			heap.Push(priorityQueue, newTokenInfo)
			//fmt.Printf("Heap after: %s\n", priorityQueue)
			//fmt.Println(strings.Repeat("-", 50))
			myTokens = append(myTokens, candidateToken)
		}
		slices.SortFunc(myTokens, func(a, b uint32) bool {
			return a < b
		})
		return myTokens

	})
	/*for zone, heap := range tokenInfoPriorityQueueByZone {
		fmt.Printf("zone %s, heap: %v\n", zone, *heap)
	}
	for zone, tokenInfo := range tokenInfoByZone {
		fmt.Printf("zone %s, tokenInfo: %v\n", zone, tokenInfo.toString())
	}*/
	return tokens, tokenInfoByZone
}

func generateRingWithEvenlyDistributedTokensGenerator(numIngesters, numZones int, now time.Time, generateTokens tokenGenerator) *ring.Desc {
	desc := &ring.Desc{Ingesters: map[string]ring.InstanceDesc{}}

	for i := 0; i < numIngesters; i++ {
		// Get the zone letter starting from "a".
		zoneID := "zone-" + string(rune('a'+(i%numZones)))
		ingesterID := fmt.Sprintf("ingester-%s-%d", zoneID, i/numZones)

		desc.Ingesters[ingesterID] = ring.InstanceDesc{
			Addr:                ingesterID,
			Timestamp:           now.Unix(),
			State:               ring.ACTIVE,
			Tokens:              generateTokens(ingesterID, zoneID, desc),
			Zone:                zoneID,
			RegisteredTimestamp: now.Unix(),
		}
	}

	return desc
}

func evenlyBucketedTokens(instanceID, zone string, numTokensPerIngester int, maxTokenValue uint32, r *rand.Rand, tokenInfoByZone map[string]*tokenInfo, tokenInfoPriorityQueueByZone map[string]*tokenInfoPriorityQueue, instanceIDByToken map[uint32]string) []uint32 {
	tokens := make([]uint32, 0, numTokensPerIngester)
	offset := maxTokenValue / uint32(numTokensPerIngester)
	curr := uint32(r.Intn(int(maxTokenValue)))

	for i := uint32(0); i < uint32(numTokensPerIngester); i++ {
		tokens = append(tokens, curr)
		instanceIDByToken[curr] = instanceID
		curr += offset
		if curr > maxTokenValue {
			curr -= maxTokenValue
		}
	}

	slices.SortFunc(tokens, func(a, b uint32) bool {
		return a < b
	})

	var head, prev *tokenInfo
	priorityQueue := make(tokenInfoPriorityQueue, 0, len(tokens))
	for _, token := range tokens {
		token := &tokenInfo{
			token:      token,
			instanceID: instanceID,
			zone:       zone,
		}
		priorityQueueItem := &tokenInfoPriorityQueueItem{
			tokenInfo: token,
			index:     len(priorityQueue),
		}
		head = token.insertAfter(head, prev, maxTokenValue)
		prev = token
		priorityQueue = append(priorityQueue, priorityQueueItem)
	}
	tokenInfoByZone[zone] = head

	heap.Init(&priorityQueue)
	tokenInfoPriorityQueueByZone[zone] = &priorityQueue
	return tokens
}

type tokenInfo struct {
	prev, next          *tokenInfo
	token               uint32
	instanceID          string
	zone                string
	registeredOwnership uint32
}

func (t *tokenInfo) insertBefore(head, token *tokenInfo, maxTokenValue uint32) *tokenInfo {
	if head == nil {
		t.prev = t
		t.next = t
		t.registeredOwnership = maxTokenValue
		return t
	}
	t.prev = token.prev
	t.next = token
	t.prev.next = t
	t.next.prev = t
	t.next.registeredOwnership = calculateRegisteredOwnership(t.token, t.next.token, maxTokenValue)
	t.registeredOwnership = calculateRegisteredOwnership(t.prev.token, t.token, maxTokenValue)
	// if token was head, it might be that this object became a new head, although  its token is greater
	// in that case we move the head forward
	if head == token {
		if t.token < head.token {
			return t
		}
	}
	return head
}

func (t *tokenInfo) insertAfter(head, token *tokenInfo, maxTokenValue uint32) *tokenInfo {
	if head == nil {
		t.prev = t
		t.next = t
		t.registeredOwnership = maxTokenValue
		return t
	}
	t.prev = token
	t.next = token.next
	t.prev.next = t
	t.next.prev = t
	t.registeredOwnership = calculateRegisteredOwnership(t.prev.token, t.token, maxTokenValue)
	t.next.registeredOwnership = calculateRegisteredOwnership(t.token, t.next.token, maxTokenValue)
	// if token was head, it might be that this object became a tail, although its token is greater
	// in that case we move the head backward
	if head == token {
		if t.token < head.token {
			return t
		}
	}

	return head
}

func (t *tokenInfo) toString() string {
	if t.prev == nil || t.next == nil {
		return "[]"
	}
	last := t.prev
	result := fmt.Sprintf("[head<->")
	for curr := t; curr != last; curr = curr.next {
		result = result + fmt.Sprintf("%v<->", curr)
	}
	return result + fmt.Sprintf("%v<->head", last)

}

func (t *tokenInfo) String() string {
	return fmt.Sprintf("<%d, %d>", t.token, t.registeredOwnership)
}

func calculateRegisteredOwnership(prevToken, nextToken, maxTokenValue uint32) uint32 {
	if nextToken <= prevToken {
		return maxTokenValue - prevToken + nextToken
	}
	return nextToken - prevToken
}

func calculateRegisteredOwnershipWithOffset(token, offset, maxValueToken uint32) uint32 {
	if maxValueToken-token <= offset {
		return offset - (maxValueToken - token)
	}
	return token + offset
}

type tokenInfoPriorityQueueItem struct {
	tokenInfo *tokenInfo
	index     int
}

func (i *tokenInfoPriorityQueueItem) String() string {
	return fmt.Sprintf("<%v, %v>", i.tokenInfo.token, i.tokenInfo.registeredOwnership)
}

type tokenInfoPriorityQueue []*tokenInfoPriorityQueueItem

// Len is the number of elements in the collection.
func (h tokenInfoPriorityQueue) Len() int {
	return len(h)
}

// Less reports whether the element with index i
// must sort before the element with index j.
// This will determine whether the heap is a min heap or a max heap
func (h tokenInfoPriorityQueue) Less(i int, j int) bool {
	return h[i].tokenInfo.registeredOwnership > h[j].tokenInfo.registeredOwnership
}

// Swap swaps the elements with indexes i and j.
func (h tokenInfoPriorityQueue) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = j
	h[j].index = i
}

// Push and Pop are used to append and remove the last element of the slice
func (h *tokenInfoPriorityQueue) Push(x any) {
	n := len(*h)
	item := &tokenInfoPriorityQueueItem{
		tokenInfo: x.(*tokenInfo),
		index:     n,
	}
	*h = append(*h, item)
}

func (h *tokenInfoPriorityQueue) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type tokenInfoStatistics struct {
	min, max uint32
	spread   float64
}

func getTokenInfoByZoneStatistics(tokenInfoByZone map[string]*tokenInfo) map[string]tokenInfoStatistics {
	statistics := make(map[string]tokenInfoStatistics, len(tokenInfoByZone))
	for zone, tokenInfo := range tokenInfoByZone {
		// Find min and max ownership
		first := tokenInfo
		if first != nil {
			min := tokenInfo.registeredOwnership
			max := tokenInfo.registeredOwnership

			for curr := first.next; curr != first; curr = curr.next {
				if curr.registeredOwnership < min {
					min = curr.registeredOwnership
				}
				if curr.registeredOwnership > max {
					max = curr.registeredOwnership
				}
			}
			statistics[zone] = tokenInfoStatistics{
				min:    min,
				max:    max,
				spread: (float64(max) - float64(min)) / float64(max),
			}
		}
	}
	return statistics
}

func analyzeRingOwnershipSpreadOnDifferentTokensPerZone(numIngesters, numZones int, numTokensPerIngesterScenarios []int, logger log.Logger) (*ring.Desc, error) {
	const (
		numIterations = 10
	)

	var (
		allIterationsPerZoneResults     [][][]float64
		allIterationsPerIngesterResults [][]float64
		ringDesc                        *ring.Desc
	)

	level.Info(logger).Log("msg", "Analyzing ring tokens ownership spread on different tokens per zone")

	for i := 0; i < numIterations; i++ {
		iterationPerZoneResults := make([][]float64, 0, len(numTokensPerIngesterScenarios))
		iterationPerIngesterResults := make([]float64, 0, len(numTokensPerIngesterScenarios))

		for _, numTokensPerIngester := range numTokensPerIngesterScenarios {
			level.Info(logger).Log("msg", "Analysis iteration", "iteration", i, "num tokens per ingester", numTokensPerIngester)

			ringDesc, tokenInfoByZone := generateRingWithEvenlyDistributedTokensPerZone(numIngesters, numZones, numTokensPerIngester, math.MaxUint32, time.Now())

			tokenInfoStatisticsPerZone := getTokenInfoByZoneStatistics(tokenInfoByZone)
			iterationPerZoneResult := make([]float64, numZones)
			for _, tokenInfoStatistics := range tokenInfoStatisticsPerZone {
				iterationPerZoneResult = append(iterationPerZoneResult, tokenInfoStatistics.spread)
			}
			iterationPerZoneResults = append(iterationPerZoneResults, iterationPerZoneResult)

			ringTokens := ringDesc.GetTokens()
			ringInstanceByToken := getRingInstanceByToken(ringDesc)

			_, _, spread := getRegisteredTokensOwnershipStatistics(ringTokens, ringInstanceByToken)
			iterationPerIngesterResults = append(iterationPerIngesterResults, spread)
		}

		allIterationsPerZoneResults = append(allIterationsPerZoneResults, iterationPerZoneResults)
		allIterationsPerIngesterResults = append(allIterationsPerIngesterResults, iterationPerIngesterResults)
	}

	// Generate CSV header.
	csvHeader := make([]string, 0, numZones*len(numTokensPerIngesterScenarios))
	for _, numTokensPerIngester := range numTokensPerIngesterScenarios {
		for i := 1; i <= numZones; i++ {
			zoneID := "zone-" + string(rune('a'+(i%numZones)))
			csvHeader = append(csvHeader, fmt.Sprintf("%d tokens per ingester in %s", numTokensPerIngester, zoneID))
		}
	}

	// Write result to CSV.
	writerPerZone := newCSVWriter[[][]float64]()
	writerPerZone.setHeader(csvHeader)
	writerPerZone.setData(allIterationsPerZoneResults, func(entry [][]float64) []string {
		formatted := make([]string, 0, numZones*len(entry))
		for _, value := range entry {
			for _, spread := range value {
				formatted = append(formatted, fmt.Sprintf("%.3f", spread))
			}
		}

		return formatted
	})
	if err := writerPerZone.writeCSV(fmt.Sprintf("per-zone-simulated-ingesters-ring-ownership-spread-on-different-tokens-per-zone-%d-%d.csv", numIngesters, numZones)); err != nil {
		return nil, err
	}

	csvHeaderPerIngestesr := make([]string, 0, len(numTokensPerIngesterScenarios))
	for _, numTokensPerIngester := range numTokensPerIngesterScenarios {
		csvHeaderPerIngestesr = append(csvHeaderPerIngestesr, fmt.Sprintf("%d tokens per ingester", numTokensPerIngester))
	}

	// Write result to CSV.
	writerPerIngester := newCSVWriter[[]float64]()
	writerPerIngester.setHeader(csvHeaderPerIngestesr)
	writerPerIngester.setData(allIterationsPerIngesterResults, func(entry []float64) []string {
		formatted := make([]string, 0, len(entry))
		for _, value := range entry {
			formatted = append(formatted, fmt.Sprintf("%.3f", value))
		}

		return formatted
	})
	if err := writerPerIngester.writeCSV(fmt.Sprintf("per-zone-simulated-ingesters-ring-ownership-spread-on-different-tokens-per-ingester-%d-%d.csv", numIngesters, numZones)); err != nil {
		return nil, err
	}

	return ringDesc, nil
}
