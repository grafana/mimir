// Copyright 2023 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"container/list"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/prometheus/prometheus/tsdb/chunks"
)

type oooIsolation struct {
	mtx       sync.RWMutex
	openReads *list.List
	logger    log.Logger
}

type oooIsolationState struct {
	i *oooIsolation
	e *list.Element

	minRef chunks.ChunkDiskMapperRef
}

func newOOOIsolation(logger log.Logger) *oooIsolation {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &oooIsolation{
		openReads: list.New(),
		logger:    logger,
	}
}

// HasOpenReadsAtOrBefore returns true if this oooIsolation is aware of any reads that use
// chunks with reference at or before ref.
func (i *oooIsolation) HasOpenReadsAtOrBefore(ref chunks.ChunkDiskMapperRef) bool {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	for e := i.openReads.Front(); e != nil; e = e.Next() {
		s := e.Value.(*oooIsolationState)

		if ref.GreaterThan(s.minRef) {
			level.Info(i.logger).Log("tag", "missing_chunks", "msg", "found open ooo read before ooo head truncation", "truncation_ref", ref, "read_min_ref", s.minRef)
			return true
		}
	}

	level.Info(i.logger).Log("tag", "missing_chunks", "msg", "no open ooo reads before ooo head truncation", "truncation_ref", ref)
	return false
}

// TrackReadAfter records a read that uses chunks with reference after minRef.
//
// The caller must ensure that the returned oooIsolationState is eventually closed when
// the read is complete.
func (i *oooIsolation) TrackReadAfter(minRef chunks.ChunkDiskMapperRef) *oooIsolationState {
	s := &oooIsolationState{
		i:      i,
		minRef: minRef,
	}

	i.mtx.Lock()
	s.e = i.openReads.PushBack(s)
	level.Info(s.i.logger).Log("tag", "missing_chunks", "msg", "appended to ooo isolation state", "minRef", minRef)
	i.mtx.Unlock()

	return s
}

func (s oooIsolationState) Close() {
	s.i.mtx.Lock()
	s.i.openReads.Remove(s.e)
	level.Info(s.i.logger).Log("tag", "missing_chunks", "msg", "closed ooo iso state", "minRef", s.minRef)
	s.i.mtx.Unlock()
}
