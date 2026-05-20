// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func TestFormatHashRangeKey_RoundTrip(t *testing.T) {
	cases := []assignment.HashRange{
		{Lo: 0, Hi: 0},
		{Lo: 0, Hi: 100},
		{Lo: 1234567890, Hi: 4294967295},
		{Lo: 4294967295, Hi: 4294967295},
	}
	for _, hr := range cases {
		k := FormatHashRangeKey(hr)
		got, err := ParseHashRangeKey(k)
		require.NoErrorf(t, err, "key=%q", k)
		assert.Equal(t, hr, got, "round-trip via key %q", k)
	}
}

func TestParseHashRangeKey_RejectsMalformed(t *testing.T) {
	for _, bad := range []string{"", "abc", "100", "100:abc", "abc:100", "100:200:300"} {
		_, err := ParseHashRangeKey(bad)
		assert.Errorf(t, err, "expected error for %q", bad)
	}
}

// TestTrace_QueryLoadFields_JSONRoundTrip checks that the Phase 1
// query-load observability fields (PartitionQuerySamples,
// UnnamedQuerySamples) survive JSON serialization. External replay
// tools persist traces to disk, so any new field must round-trip.
func TestTrace_QueryLoadFields_JSONRoundTrip(t *testing.T) {
	tr := Trace{
		SlicerVersion:    SlicerVersion,
		Now:              time.Unix(1_000_000, 0),
		Start:            []assignment.Entry{{Range: assignment.HashRange{Lo: 0, Hi: 100}, PartitionID: 0}},
		End:              []assignment.Entry{{Range: assignment.HashRange{Lo: 0, Hi: 100}, PartitionID: 0}},
		ActivePartitions: []int32{0, 1, 2},
		PartitionL:       map[int32]int64{0: 100, 1: 200, 2: 50},
		// Partition 0 is a real partition; explicitly include it
		// to assert the field is not collapsed into the unnamed
		// bucket.
		PartitionQuerySamples: map[int32]float64{0: 11.5, 1: 222.0, 2: 33.333},
		UnnamedQuerySamples:   map[string]float64{"ingester-zone-a-0": 100.0, "ingester-zone-a-1": 0.0},
		Config:                ConfigSnapshot{MovementBudget: 0.5, MoveCooldown: 90 * time.Second},
	}

	buf, err := json.Marshal(tr)
	require.NoError(t, err)

	var decoded Trace
	require.NoError(t, json.Unmarshal(buf, &decoded))

	assert.Equal(t, tr.PartitionQuerySamples, decoded.PartitionQuerySamples,
		"PartitionQuerySamples must round-trip including partition 0 (a real partition, not the unnamed bucket)")
	assert.Equal(t, tr.UnnamedQuerySamples, decoded.UnnamedQuerySamples,
		"UnnamedQuerySamples must round-trip including instances reporting 0 (idle ingesters)")
}

// captureTrace builds a Trace as if rebalance() had just run with
// the given inputs. Mirrors the production capture path so tests
// exercise the same conversions (ratesToWire, cooldownsToWire) used
// in production.
func captureTrace(
	t *testing.T,
	r *Rebalancer,
	current *assignment.Assignment,
	rates []rangeRate,
	partitionLByPID map[int32]int64,
	activePartitions []int32,
	now time.Time,
) Trace {
	t.Helper()
	startEntries := append([]assignment.Entry(nil), current.Entries...)
	cooldownsSnapshot := cooldownsToWire(r.moveCooldowns)
	partitionRateByPID := partitionLoadFromRates(rates, activePartitions)
	end, actions := r.runSlicer(current, rates, partitionRateByPID, activePartitions, now)
	require.NoError(t, end.Validate())
	return Trace{
		SlicerVersion: SlicerVersion,
		Round: RoundLog{
			Time:    now,
			Actions: actions,
		},
		Now:              now,
		Start:            startEntries,
		Rates:            ratesToWire(rates),
		PartitionL:       partitionLByPID,
		ActivePartitions: append([]int32(nil), activePartitions...),
		Cooldowns:        cooldownsSnapshot,
		Config: ConfigSnapshot{
			MovementBudget: r.cfg.MovementBudget,
			MoveCooldown:   r.cfg.MoveCooldown,
		},
		End: append([]assignment.Entry(nil), end.Entries...),
	}
}

// nonTrivialTrace constructs a captured trace from a scenario that
// exercises every Phase of runSlicer: skewed per-range load, an
// above-average partition that the slicer must drain, multiple
// partitions, and pre-seeded cooldowns for serialization coverage.
func nonTrivialTrace(t *testing.T) Trace {
	t.Helper()
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.FineEvenSplit(partitions, 8)

	var rates []rangeRate
	for i, e := range initial.Entries {
		switch e.PartitionID {
		case 0:
			rates = append(rates, rangeRate{hr: e.Range, series: int64(1000 + i*10), partitionID: e.PartitionID})
		case 1:
			rates = append(rates, rangeRate{hr: e.Range, series: int64(2000 + i*5), partitionID: e.PartitionID})
		case 2:
			rates = append(rates, rangeRate{hr: e.Range, series: 50, partitionID: e.PartitionID})
		case 3:
			rates = append(rates, rangeRate{hr: e.Range, series: 10, partitionID: e.PartitionID})
		}
	}
	rates = withSampleRateFromSeries(rates)

	// PartitionL skewed to match: P1 hottest, P0 elevated, P2/P3 cold.
	partL := map[int32]int64{
		0: 9000,
		1: 17000,
		2: 400,
		3: 80,
	}

	cfg := Config{
		MovementBudget: 0.5,
		MoveCooldown:   90 * time.Second,
	}
	r := &Rebalancer{
		cfg:           cfg,
		moveCooldowns: make(map[assignment.HashRange]time.Time),
	}
	// Pre-seed a cooldown so cooldown serialization is also exercised.
	r.moveCooldowns[initial.Entries[0].Range] = time.Unix(2_000_000, 0)

	now := time.Unix(1_000_000, 0)
	return captureTrace(t, r, initial, rates, partL, partitions, now)
}

// TestReplayTrace_Deterministic confirms that the slicer is a pure
// function of its captured inputs: replaying a Trace produces the
// exact same Actions and End assignment as the original run. This is
// the contract that lets external tools (e.g. AI verification
// agents) trust traces fetched from /nautilus/rebalancer/rounds/{i}.json.
func TestReplayTrace_Deterministic(t *testing.T) {
	tr := nonTrivialTrace(t)
	require.NotEmpty(t, tr.Round.Actions, "scenario must produce actions to be a meaningful test")

	endAsg, actions := ReplayTrace(tr)
	require.NoError(t, endAsg.Validate())

	assert.Equal(t, tr.Round.Actions, actions,
		"replay must produce byte-equal Actions slice")
	assert.Equal(t, tr.End, endAsg.Entries,
		"replay must produce the same End assignment")
}

// TestReplayTrace_JSONRoundTrip confirms the Trace JSON shape is
// lossless: marshal → unmarshal → replay still matches the original
// run. This is the contract that lets external tools persist traces
// to disk and replay them later.
func TestReplayTrace_JSONRoundTrip(t *testing.T) {
	tr := nonTrivialTrace(t)

	buf, err := json.Marshal(tr)
	require.NoError(t, err)

	var decoded Trace
	require.NoError(t, json.Unmarshal(buf, &decoded))

	endAsg, actions := ReplayTrace(decoded)
	require.NoError(t, endAsg.Validate())

	assert.Equal(t, tr.Round.Actions, actions,
		"actions must survive JSON round-trip")
	assert.Equal(t, tr.End, endAsg.Entries,
		"End assignment must survive JSON round-trip")
}

// TestTrace_Invariants checks the structural invariants an external
// verification tool would assert on a captured Trace. Codifies what
// "the slicer did the right thing" means so an AI agent has a
// concrete checklist.
func TestTrace_Invariants(t *testing.T) {
	tr := nonTrivialTrace(t)

	t.Run("end assignment is valid", func(t *testing.T) {
		end := &assignment.Assignment{Entries: tr.End}
		assert.NoError(t, end.Validate(),
			"End must cover [0, MaxUint32] with no gaps or overlaps")
	})

	t.Run("movement budget respected", func(t *testing.T) {
		hashSpace := float64(uint64(1<<32 - 1))
		var moved float64
		for _, a := range tr.Round.Actions {
			if a.Kind == ActionMove {
				moved += float64(a.Range.Size())
			}
		}
		assert.LessOrEqualf(t, moved/hashSpace, tr.Config.MovementBudget+1e-9,
			"sum of move sizes must not exceed configured movement budget; moved=%v budget=%v",
			moved/hashSpace, tr.Config.MovementBudget)
	})

	t.Run("no move violates active cooldown", func(t *testing.T) {
		for _, a := range tr.Round.Actions {
			if a.Kind != ActionMove {
				continue
			}
			for k, deadline := range tr.Cooldowns {
				if !tr.Now.Before(deadline) {
					continue
				}
				cooled, err := ParseHashRangeKey(k)
				require.NoError(t, err)
				assert.Falsef(t, hashRangesOverlap(a.Range, cooled),
					"move %v overlaps cooled range %v (deadline=%v)", a.Range, cooled, deadline)
			}
		}
	})

	t.Run("phase ordering: reassign, merge, move, split", func(t *testing.T) {
		order := map[ActionKind]int{
			ActionReassign: 0,
			ActionMerge:    1,
			ActionMove:     2,
			ActionSplit:    3,
		}
		prev := -1
		for _, a := range tr.Round.Actions {
			cur, ok := order[a.Kind]
			require.Truef(t, ok, "unknown action kind %q", a.Kind)
			assert.GreaterOrEqualf(t, cur, prev,
				"actions must appear in reassign→merge→move→split order; got %v after stage %v", a.Kind, prev)
			prev = cur
		}
	})

	t.Run("active partitions only", func(t *testing.T) {
		active := make(map[int32]bool, len(tr.ActivePartitions))
		for _, pid := range tr.ActivePartitions {
			active[pid] = true
		}
		for _, e := range tr.End {
			assert.Truef(t, active[e.PartitionID],
				"End assigns range %v to inactive partition %d", e.Range, e.PartitionID)
		}
		for _, a := range tr.Round.Actions {
			if a.ToPart != 0 || a.Kind == ActionMove || a.Kind == ActionReassign {
				assert.Truef(t, active[a.ToPart],
					"action %v targets inactive partition %d", a, a.ToPart)
			}
		}
	})
}

// TestServeHTTP_RoundsJSON_ListAndDetail exercises the path
// dispatcher and confirms that the JSON endpoints serve the data
// shape the replay contract requires. Bypasses gorilla/mux by
// invoking ServeHTTP directly with full URL paths, mirroring how a
// production request reaches the handler after RegisterRoutesWithPrefix.
func TestServeHTTP_RoundsJSON_ListAndDetail(t *testing.T) {
	r := &Rebalancer{}
	tr1 := nonTrivialTrace(t)
	tr2 := nonTrivialTrace(t)
	tr2.Now = tr1.Now.Add(time.Minute)
	tr2.Round.Time = tr2.Now
	r.admin.addTrace(tr1)
	r.admin.addTrace(tr2)

	t.Run("list newest first", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nautilus/rebalancer/rounds.json", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Header().Get("Content-Type"), "application/json")

		var got struct {
			SlicerVersion string     `json:"slicer_version"`
			Rounds        []RoundLog `json:"rounds"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, SlicerVersion, got.SlicerVersion)
		require.Len(t, got.Rounds, 2)
		assert.True(t, got.Rounds[0].Time.After(got.Rounds[1].Time),
			"index 0 must be newest; got %v before %v",
			got.Rounds[0].Time, got.Rounds[1].Time)
	})

	t.Run("detail at idx 0 is replayable", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nautilus/rebalancer/rounds/0.json", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var fetched Trace
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &fetched))
		assert.Equal(t, tr2.Round.Time.UTC(), fetched.Round.Time.UTC(),
			"idx 0 must serve the newest trace")

		end, actions := ReplayTrace(fetched)
		require.NoError(t, end.Validate())
		assert.Equal(t, fetched.Round.Actions, actions,
			"a fetched trace must replay deterministically")
	})

	t.Run("detail with bad index", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nautilus/rebalancer/rounds/abc.json", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("detail with out-of-range index", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nautilus/rebalancer/rounds/99.json", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("unknown sub-path 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nautilus/rebalancer/whatever", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}

// TestAdminState_TraceAt confirms the display-index → trace mapping
// (0 = newest) used by the /rounds/{idx}.json endpoint.
func TestAdminState_TraceAt(t *testing.T) {
	var s adminState
	for i := 0; i < 5; i++ {
		s.addTrace(Trace{Now: time.Unix(int64(i), 0)})
	}
	got, ok := s.traceAt(0)
	require.True(t, ok)
	assert.Equal(t, int64(4), got.Now.Unix(), "idx 0 must be the newest")

	got, ok = s.traceAt(4)
	require.True(t, ok)
	assert.Equal(t, int64(0), got.Now.Unix(), "idx N-1 must be the oldest")

	_, ok = s.traceAt(5)
	assert.False(t, ok, "out-of-range index returns ok=false")
	_, ok = s.traceAt(-1)
	assert.False(t, ok, "negative index returns ok=false")
}

// TestSlicerVersion_IsBumpedForNewModel is a smoke test that the
// version marker has been advanced past "1" — older traces captured
// under the orphan-series model must not silently replay against
// this binary.
//
// Version "3" added partition_id to RangeRate; traces from v2 and
// earlier are missing that field and cannot be replayed
// deterministically.
func TestSlicerVersion_IsBumpedForNewModel(t *testing.T) {
	assert.NotEqual(t, "1", SlicerVersion,
		"SlicerVersion must be bumped when the load model changes incompatibly")
	assert.NotEqual(t, "2", SlicerVersion,
		"SlicerVersion must be bumped when RangeRate wire shape changes")
}
