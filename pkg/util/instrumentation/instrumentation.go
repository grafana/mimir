// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/instrumentation.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package instrumentation

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // Imported for side-effects
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type InstrumentationServer struct {
	port     int
	registry *prometheus.Registry
	logger   log.Logger
	srv      *http.Server
}

// NewInstrumentationServer returns a server exposing Prometheus metrics and pprof profiles.
func NewInstrumentationServer(port int, registry *prometheus.Registry, logger log.Logger) *InstrumentationServer {
	return &InstrumentationServer{
		port:     port,
		logger:   logger,
		registry: registry,
	}
}

// Start the instrumentation server.
func (s *InstrumentationServer) Start() error {
	// Setup listener first, so we can fail early if the port is in use.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}

	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	router.PathPrefix("/debug/pprof").Handler(http.DefaultServeMux)

	s.srv = &http.Server{
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 2 * time.Minute,
	}

	go func() {
		if err := s.srv.Serve(listener); err != nil {
			level.Error(s.logger).Log("msg", "metrics server terminated", "err", err)
		}
	}()

	return nil
}

// Stop closes the instrumentation server.
func (s *InstrumentationServer) Stop() {
	if s.srv != nil {
		s.srv.Close()
		s.srv = nil
	}
}
