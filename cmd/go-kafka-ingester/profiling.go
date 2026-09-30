// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	runtimepprof "runtime/pprof"
	"strconv"
	"sync"
	"time"
)

// startProfiling serves CPU and heap profiles on the paths the Rust ingester serves them, which
// the profile scraper follows.
func startProfiling(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("bind profiling listener %s: %w", address, err)
	}
	mux := http.NewServeMux()
	var running sync.Mutex
	mux.HandleFunc("/debug/pprof/profile", func(w http.ResponseWriter, r *http.Request) {
		cpuProfile(w, r, &running)
	})
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	go func() {
		if err := http.Serve(listener, mux); err != nil {
			log.Printf("CPU profiling server stopped: %v", err)
		}
	}()
	log.Printf("phase=profiling_start address=%s", address)
	return nil
}

// cpuProfile profiles for `seconds`, 10 by default and at most 30, one profile at a time.
func cpuProfile(w http.ResponseWriter, r *http.Request, running *sync.Mutex) {
	if !running.TryLock() {
		http.Error(w, "CPU profile already running", http.StatusTooManyRequests)
		return
	}
	defer running.Unlock()
	seconds := int64(10)
	if value := r.URL.Query().Get("seconds"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			http.Error(w, "invalid seconds", http.StatusBadRequest)
			return
		}
		seconds = int64(parsed)
	}
	seconds = min(max(seconds, 1), 30)
	var profile bytes.Buffer
	if err := runtimepprof.StartCPUProfile(&profile); err != nil {
		log.Printf("CPU profiling failed: %v", err)
		http.Error(w, "CPU profiling failed", http.StatusInternalServerError)
		return
	}
	select {
	case <-time.After(time.Duration(seconds) * time.Second):
	case <-r.Context().Done():
	}
	runtimepprof.StopCPUProfile()
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(profile.Bytes())
}
