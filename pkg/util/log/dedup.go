// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"bytes"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
)

const (
	maxEntries          = 1024
	maxDedupCount       = 1000
	expireEntriesAfter  = 3 * time.Second
	garbageCollectEvery = 5 * time.Second

	dedupLogKey = "dedup"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(nil)
	},
}

type dedupEntry struct {
	mu        sync.Mutex
	keyValues []interface{}
	dedupKey  string
	count     int
	seenAt    time.Time
	isActive  bool
}

func (e *dedupEntry) updateOrForward(
	keyvals []interface{},
	dedupKey string,
	maxDedupCount int,
	next log.Logger,
) (matched bool) {
	e.mu.Lock()
	if !e.isActive || e.dedupKey != dedupKey {
		e.mu.Unlock()
		return false
	}
	e.keyValues = keyvals // keep most recent entry
	e.seenAt = time.Now()

	e.count++
	if e.count >= maxDedupCount {
		keyValues := append(e.keyValues, dedupLogKey, e.count)
		e.isActive = false
		e.mu.Unlock()

		// log entry after reaching max dedup count
		_ = next.Log(keyValues...)
		return true
	}
	e.mu.Unlock()
	return true
}

func (e *dedupEntry) activate(keyvals []interface{}, dedupKey string) (matched bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.isActive {
		return false
	}
	e.keyValues = keyvals
	e.dedupKey = dedupKey
	e.seenAt = time.Now()
	e.count = 1
	e.isActive = true
	return true
}

func (e *dedupEntry) deactivateExpired(expireAfter time.Duration, next log.Logger) {
	e.mu.Lock()
	if !e.isActive || (e.isActive && time.Since(e.seenAt) < expireAfter) {
		e.mu.Unlock()
		return
	}
	keyValues := e.keyValues
	if e.count > 1 {
		keyValues = append(keyValues, dedupLogKey, e.count)
	}
	e.isActive = false
	e.mu.Unlock()

	// log entry after expiring
	_ = next.Log(keyValues...)
}

func (e *dedupEntry) deactivate(next log.Logger) {
	e.mu.Lock()
	if !e.isActive {
		e.mu.Unlock()
		return
	}
	keyValues := e.keyValues
	if e.count > 1 {
		keyValues = append(keyValues, dedupLogKey, e.count)
	}
	e.isActive = false
	e.mu.Unlock()

	_ = next.Log(keyValues...)
}

// Deduper implement log.Logger, dedupes log lines.
type Deduper struct {
	callerPrefix       string
	targetKeys         []string
	maxDedupCount      int
	expireEntriesAfter time.Duration
	next               log.Logger
	stopCh             chan chan struct{}
	entries            []dedupEntry
}

// NewDedupLogger creates and initializes a new Deduper instance.
func NewDedupLogger(callerPrefix string, targetKeys []string, next log.Logger) *Deduper {
	return newDedupLogger(
		callerPrefix,
		targetKeys,
		next,
		maxEntries,
		maxDedupCount,
		expireEntriesAfter,
		garbageCollectEvery,
	)
}

func newDedupLogger(
	callerPrefix string,
	targetKeys []string,
	next log.Logger,
	maxEntries int,
	maxDedupCount int,
	expireEntriesAfter time.Duration,
	garbageCollectEvery time.Duration,
) *Deduper {
	d := &Deduper{
		callerPrefix:       callerPrefix,
		targetKeys:         targetKeys,
		maxDedupCount:      maxDedupCount,
		expireEntriesAfter: expireEntriesAfter,
		next:               next,
		stopCh:             make(chan chan struct{}, 1),
		entries:            make([]dedupEntry, maxEntries),
	}
	go d.loop(garbageCollectEvery)
	return d
}

// Log implements log.Logger.
func (d *Deduper) Log(keyvals ...interface{}) error {
	if len(keyvals) == 0 {
		return nil
	}
	if len(keyvals)%2 == 1 {
		keyvals = append(keyvals, nil)
	}
	// check whether log entry matches caller prefix
	if !d.hasCallerPrefix(keyvals) {
		return d.next.Log(keyvals...)
	}
	// compute dedup key if target keys are present
	dedupKey := computeDedupKey(keyvals, d.targetKeys)
	if len(dedupKey) == 0 {
		return d.next.Log(keyvals...)
	}

	// look for an active entry
	for i := 0; i < len(d.entries); i++ {
		if matched := d.entries[i].updateOrForward(keyvals, dedupKey, d.maxDedupCount, d.next); matched {
			return nil
		}
	}
	// if no active entry was found, look for an empty slot and register the new entry
	for i := 0; i < len(d.entries); i++ {
		if matched := d.entries[i].activate(keyvals, dedupKey); matched {
			return nil
		}
	}
	// no free slot available... forward entry
	return d.next.Log(keyvals...)
}

// Stop stops deduper loop.
// Invoking this method guarantees all pending entries to be forwarded after returning.
func (d *Deduper) Stop() {
	ch := make(chan struct{})
	d.stopCh <- ch
	<-ch
}

func (d *Deduper) loop(collectInterval time.Duration) {
	tc := time.NewTicker(collectInterval)
	defer tc.Stop()

	for {
		select {
		case <-tc.C:
			// deactivate expired entries
			for i := 0; i < len(d.entries); i++ {
				d.entries[i].deactivateExpired(d.expireEntriesAfter, d.next)
			}

		case ch := <-d.stopCh:
			// forward all active entries
			for i := 0; i < len(d.entries); i++ {
				d.entries[i].deactivate(d.next)
			}
			close(ch) // signal completion
			return
		}
	}
}

func (d *Deduper) hasCallerPrefix(keyvals []interface{}) bool {
	valStr, ok := getValue("caller", keyvals).(string)
	if !ok {
		return false
	}
	return strings.HasPrefix(valStr, d.callerPrefix)
}

func getValue(key string, keyvals []interface{}) interface{} {
	kvsLen := len(keyvals)
	for i := 0; i < kvsLen; i += 2 {
		kStr, ok := keyvals[i].(string)
		if !ok {
			continue
		}
		if kStr != key {
			continue
		}
		return keyvals[i+1]
	}
	return nil
}

func computeDedupKey(keyvalues []interface{}, keys []string) string {
	dedupKvs := make([]interface{}, len(keys)*2)

	// build dedup kvs
	for i, key := range keys {
		val := getValue(key, keyvalues)
		if val == nil {
			return ""
		}
		j := i * 2
		dedupKvs[j] = key
		dedupKvs[j+1] = val
	}
	// format dedup key
	b := bufPool.Get().(*bytes.Buffer)
	defer func() {
		b.Reset()
		bufPool.Put(b)
	}()

	l := log.NewLogfmtLogger(b)
	_ = l.Log(dedupKvs...)

	return b.String()
}
