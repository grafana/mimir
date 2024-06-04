package backend

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"

	"github.com/grafana/grafana-plugin-sdk-go/backend/grpcplugin"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/genproto/pluginv2"
	"github.com/grafana/grafana-plugin-sdk-go/internal/standalone"
	"github.com/grafana/grafana-plugin-sdk-go/internal/tracerprovider"
)

const defaultServerMaxReceiveMessageSize = 1024 * 1024 * 16

// GRPCSettings settings for gRPC.
type GRPCSettings struct {
	// MaxReceiveMsgSize the max gRPC message size in bytes the plugin can receive.
	// If this is <= 0, gRPC uses the default 16MB.
	MaxReceiveMsgSize int

	// MaxSendMsgSize the max gRPC message size in bytes the plugin can send.
	// If this is <= 0, gRPC uses the default `math.MaxInt32`.
	MaxSendMsgSize int
}

// ServeOpts options for serving plugins.
type ServeOpts struct {
	// CheckHealthHandler handler for health checks.
	CheckHealthHandler CheckHealthHandler

	// CallResourceHandler handler for resource calls.
	// Optional to implement.
	CallResourceHandler CallResourceHandler

	// QueryDataHandler handler for data queries.
	// Required to implement if data source.
	QueryDataHandler QueryDataHandler

	// StreamHandler handler for streaming queries.
	// This is EXPERIMENTAL and is a subject to change till Grafana 8.
	StreamHandler StreamHandler

	// GRPCSettings settings for gPRC.
	GRPCSettings GRPCSettings
}

func asGRPCServeOpts(opts ServeOpts) grpcplugin.ServeOpts {
	pluginOpts := grpcplugin.ServeOpts{
		DiagnosticsServer: newDiagnosticsSDKAdapter(prometheus.DefaultGatherer, opts.CheckHealthHandler),
	}

	if opts.CallResourceHandler != nil {
		pluginOpts.ResourceServer = newResourceSDKAdapter(opts.CallResourceHandler)
	}

	if opts.QueryDataHandler != nil {
		pluginOpts.DataServer = newDataSDKAdapter(opts.QueryDataHandler)
	}

	if opts.StreamHandler != nil {
		pluginOpts.StreamServer = newStreamSDKAdapter(opts.StreamHandler)
	}
	return pluginOpts
}

// grpcServerOptions returns a new []grpc.ServerOption that can be passed to grpc.NewServer.
// The returned options are the default ones, and any customOpts are appended at the end.
// The default options are:
//   - default middlewares (see defaultGRPCMiddlewares)
//   - otel grpc stats handler (see otelgrpc.NewServerHandler)
func grpcServerOptions(serveOpts ServeOpts, customOpts ...grpc.ServerOption) []grpc.ServerOption {
	options := defaultGRPCMiddlewares(serveOpts)
	options = append(options, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	options = append(options, customOpts...)
	return options
}

func defaultGRPCMiddlewares(opts ServeOpts) []grpc.ServerOption {
	if opts.GRPCSettings.MaxReceiveMsgSize <= 0 {
		opts.GRPCSettings.MaxReceiveMsgSize = defaultServerMaxReceiveMessageSize
	}
	grpcMiddlewares := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(opts.GRPCSettings.MaxReceiveMsgSize),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_prometheus.StreamServerInterceptor,
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_prometheus.UnaryServerInterceptor,
		)),
	}
	if opts.GRPCSettings.MaxSendMsgSize > 0 {
		grpcMiddlewares = append([]grpc.ServerOption{grpc.MaxSendMsgSize(opts.GRPCSettings.MaxSendMsgSize)}, grpcMiddlewares...)
	}
	return grpcMiddlewares
}

// Serve starts serving the plugin over gRPC.
func Serve(opts ServeOpts) error {
	grpc_prometheus.EnableHandlingTimeHistogram()
	pluginOpts := asGRPCServeOpts(opts)
	pluginOpts.GRPCServer = func(customOptions []grpc.ServerOption) *grpc.Server {
		return grpc.NewServer(grpcServerOptions(opts, customOptions...)...)
	}
	return grpcplugin.Serve(pluginOpts)
}

// GracefulStandaloneServe starts a gRPC server that is not managed by hashicorp.
// The provided standalone.Args must have an address set, or the function returns an error.
// The function handles creating/cleaning up the standalone address file, and graceful GRPC server termination.
// The function returns after the GRPC server has been terminated.
func GracefulStandaloneServe(dsopts ServeOpts, info standalone.ServerSettings) error {
	// We must have an address if we want to run the plugin in standalone mode
	if info.Address == "" {
		return errors.New("standalone address must be specified")
	}

	if info.Dir == "" {
		return errors.New("directory must be specified")
	}

	// Write the address and PID to local files
	log.DefaultLogger.Info("Creating standalone address and pid files", "dir", info.Dir)
	if err := standalone.CreateStandaloneAddressFile(info.Address, info.Dir); err != nil {
		return fmt.Errorf("create standalone address file: %w", err)
	}
	if err := standalone.CreateStandalonePIDFile(os.Getpid(), info.Dir); err != nil {
		return fmt.Errorf("create standalone pid file: %w", err)
	}

	// sadly vs-code can not listen to shutdown events
	// https://github.com/golang/vscode-go/issues/120

	// Cleanup function that deletes standalone.txt and pid.txt, if it exists. Fails silently.
	// This is so the address file is deleted when the plugin shuts down gracefully, if possible.
	defer func() {
		log.DefaultLogger.Info("Cleaning up standalone address and pid files")
		if err := standalone.CleanupStandaloneAddressFile(info); err != nil {
			log.DefaultLogger.Error("Error while cleaning up standalone address file", "error", err)
		}
		if err := standalone.CleanupStandalonePIDFile(info); err != nil {
			log.DefaultLogger.Error("Error while cleaning up standalone pid file", "error", err)
		}
		// Kill the dummy locator so Grafana reloads the plugin
		standalone.FindAndKillCurrentPlugin(info.Dir)
	}()

	// When debugging, be sure to kill the running instances, so that we can reconnect
	standalone.FindAndKillCurrentPlugin(info.Dir)

	// Start GRPC server
	pluginOpts := asGRPCServeOpts(dsopts)
	if pluginOpts.GRPCServer == nil {
		pluginOpts.GRPCServer = func(customOptions []grpc.ServerOption) *grpc.Server {
			return grpc.NewServer(grpcServerOptions(dsopts, customOptions...)...)
		}
	}

	server := pluginOpts.GRPCServer(nil)

	var plugKeys []string
	if pluginOpts.DiagnosticsServer != nil {
		pluginv2.RegisterDiagnosticsServer(server, pluginOpts.DiagnosticsServer)
		plugKeys = append(plugKeys, "diagnostics")
	}

	if pluginOpts.ResourceServer != nil {
		pluginv2.RegisterResourceServer(server, pluginOpts.ResourceServer)
		plugKeys = append(plugKeys, "resources")
	}

	if pluginOpts.DataServer != nil {
		pluginv2.RegisterDataServer(server, pluginOpts.DataServer)
		plugKeys = append(plugKeys, "data")
	}

	if pluginOpts.StreamServer != nil {
		pluginv2.RegisterStreamServer(server, pluginOpts.StreamServer)
		plugKeys = append(plugKeys, "stream")
	}

	// Start the GRPC server and handle graceful shutdown to ensure we execute deferred functions correctly
	log.DefaultLogger.Debug("Standalone plugin server", "capabilities", plugKeys)
	listener, err := net.Listen("tcp", info.Address)
	if err != nil {
		return err
	}

	signalChan := make(chan os.Signal, 1)
	serverErrChan := make(chan error, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Unregister signal handlers before returning
	defer signal.Stop(signalChan)

	// Start GRPC server in a separate goroutine
	go func() {
		serverErrChan <- server.Serve(listener)
	}()

	// Block until signal or GRPC server termination
	select {
	case <-signalChan:
		// Signal received, stop the server
		server.Stop()
		if err := <-serverErrChan; err != nil {
			// Bubble up error
			return err
		}
	case err := <-serverErrChan:
		// Server stopped prematurely, bubble up the error
		return err
	}

	log.DefaultLogger.Debug("Plugin server exited")
	return nil
}

// Manage runs the plugin in either standalone mode, dummy locator or normal (hashicorp) mode.
func Manage(pluginID string, serveOpts ServeOpts) error {
	defer func() {
		tp, ok := otel.GetTracerProvider().(tracerprovider.TracerProvider)
		if !ok {
			return
		}

		Logger.Debug("Closing tracing")
		ctx, canc := context.WithTimeout(context.Background(), time.Second*5)
		defer canc()
		if err := tp.Shutdown(ctx); err != nil {
			Logger.Error("error while shutting down tracer", "error", err)
		}
	}()

	if s, enabled := standalone.ServerModeEnabled(pluginID); enabled {
		// Run the standalone GRPC server
		return GracefulStandaloneServe(serveOpts, s)
	}

	if s, enabled := standalone.ClientModeEnabled(pluginID); enabled {
		// Grafana is trying to run the dummy plugin locator to connect to the standalone GRPC server (separate process)
		Logger.Debug("Running dummy plugin locator", "addr", s.TargetAddress, "pid", strconv.Itoa(s.TargetPID))
		standalone.RunDummyPluginLocator(s.TargetAddress)
		return nil
	}

	// The default/normal hashicorp path.
	return Serve(serveOpts)
}

// TestStandaloneServe starts a gRPC server that is not managed by hashicorp.
// The function returns the gRPC server which should be closed by the consumer.
func TestStandaloneServe(opts ServeOpts, address string) (*grpc.Server, error) {
	pluginOpts := asGRPCServeOpts(opts)
	if pluginOpts.GRPCServer == nil {
		pluginOpts.GRPCServer = func(customOptions []grpc.ServerOption) *grpc.Server {
			return grpc.NewServer(grpcServerOptions(opts, customOptions...)...)
		}
	}

	server := pluginOpts.GRPCServer(nil)

	var plugKeys []string
	if pluginOpts.DiagnosticsServer != nil {
		pluginv2.RegisterDiagnosticsServer(server, pluginOpts.DiagnosticsServer)
		plugKeys = append(plugKeys, "diagnostics")
	}

	if pluginOpts.ResourceServer != nil {
		pluginv2.RegisterResourceServer(server, pluginOpts.ResourceServer)
		plugKeys = append(plugKeys, "resources")
	}

	if pluginOpts.DataServer != nil {
		pluginv2.RegisterDataServer(server, pluginOpts.DataServer)
		plugKeys = append(plugKeys, "data")
	}

	if pluginOpts.StreamServer != nil {
		pluginv2.RegisterStreamServer(server, pluginOpts.StreamServer)
		plugKeys = append(plugKeys, "stream")
	}

	// Start the GRPC server and handle graceful shutdown to ensure we execute deferred functions correctly
	log.DefaultLogger.Info("Standalone plugin server", "capabilities", plugKeys)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	serverErrChan := make(chan error, 1)
	// Start GRPC server in a separate goroutine
	go func() {
		serverErrChan <- server.Serve(listener)
	}()

	// Wait until signal or GRPC server termination in a separate goroutine
	go func() {
		err := <-serverErrChan
		if err != nil {
			log.DefaultLogger.Error("Server experienced an error", "error", err)
		}
	}()

	return server, nil
}
