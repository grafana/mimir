// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"context"
	"net/http"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/server"
	"github.com/grafana/dskit/services"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/mimir"
)

type TeeServer struct {
	srv        *server.Server
	srvMetrics *server.Metrics
	tee        *Tee
}

func NewTeeServer(tee *Tee) *TeeServer {
	return &TeeServer{tee: tee}
}

// Init is a simplified version of Mimir modules initServer
func (t *TeeServer) Init(cfg Config) (services.Service, error) {
	var err error

	t.srvMetrics = server.NewServerMetrics(cfg.Server)
	t.srv, err = server.NewWithMetrics(cfg.Server, t.srvMetrics)
	if err != nil {
		return nil, err
	}

	servicesToWaitFor := func() []services.Service {
		return nil // TBD
	}

	s := mimir.NewServerService(t.srv, servicesToWaitFor)

	// The tee is ready while its only service is Running.
	// The state changes to Stopping on shutdown.
	isRunning := func(context.Context) bool { return s.State() == services.Running }

	// Health check endpoint.
	t.srv.HTTP.Path("/").Methods("GET").HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Readiness check endpoint.
	t.srv.HTTP.Path("/ready").Methods("GET").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isRunning(r.Context()) {
			http.Error(w, "Application is not running", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{\"status\": \"ready\"}"))
	})
	grpc_health_v1.RegisterHealthServer(t.srv.GRPC, grpcutil.NewHealthCheckFrom(isRunning))

	return s, nil
}
