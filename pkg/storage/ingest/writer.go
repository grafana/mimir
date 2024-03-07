// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/servicediscovery"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

type writeAgentSelector interface {
	services.Service
	PickServer(partitionID int32) (ingestpb.WriteAgentClient, error)
}

// Writer is responsible to write incoming data to the ingest storage.
type Writer struct {
	services.Service

	logger             log.Logger
	registerer         prometheus.Registerer
	compressionEnabled bool

	writeAgents writeAgentSelector

	// Metrics.
	writeLatency    prometheus.Summary
	writeBytesTotal prometheus.Counter

	// The following settings can only be overridden in tests.
	maxInflightProduceRequests int
}

func NewWriter(waConfig WriteAgentConfig, logger log.Logger, reg prometheus.Registerer) (*Writer, error) {
	waSelector, err := newWriteAgentServerSelector(waConfig, logger)
	if err != nil {
		return nil, errors.Wrap(err, "creating write agent server selector")
	}
	return newWriter(waSelector, waConfig.CompressionEnabled, logger, reg)
}

func newWriter(waSelector writeAgentSelector, compressionEnabled bool, logger log.Logger, reg prometheus.Registerer) (*Writer, error) {
	w := &Writer{
		logger:                     logger,
		compressionEnabled:         compressionEnabled,
		registerer:                 reg,
		writeAgents:                waSelector,
		maxInflightProduceRequests: 20,

		// Metrics.
		writeLatency: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Name:       "cortex_ingest_storage_writer_latency_seconds",
			Help:       "Latency to write an incoming request to the ingest storage.",
			Objectives: latencySummaryObjectives,
			MaxAge:     time.Minute,
			AgeBuckets: 10,
		}),
		writeBytesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_writer_sent_bytes_total",
			Help: "Total number of bytes sent to the ingest storage.",
		}),
	}

	w.Service = services.NewIdleService(w.starting, w.stopping)

	return w, nil
}

func (w *Writer) stopping(_ error) error {
	return nil
}

// WriteSync the input data to the ingest storage. The function blocks until the data has been successfully committed,
// or an error occurred.
func (w *Writer) WriteSync(ctx context.Context, partitionID int32, userID string, req *mimirpb.WriteRequest) error {
	startTime := time.Now()

	// Nothing to do if the input data is empty.
	if len(req.Timeseries) == 0 && len(req.Metadata) == 0 {
		return nil
	}

	// Write to backend.
	writer, err := w.getWriteAgentForPartition(partitionID)
	if err != nil {
		return err
	}

	var waReqSize int
	unencodedData, err := req.Marshal()
	if err != nil {
		return errors.Wrap(err, "sending request to write agent")
	}

	// Snappy encode it (if enabled).
	var dataToSend []byte
	if w.compressionEnabled {
		dataToSend = snappy.Encode(nil, unencodedData)
	} else {
		dataToSend = unencodedData
	}

	waReq := &ingestpb.WriteRequest{
		Piece:       &ingestpb.Piece{Data: dataToSend, SnappyEncoded: w.compressionEnabled, TenantId: userID, CreatedAtMs: startTime.UnixMilli()},
		PartitionId: partitionID,
	}
	waReqSize = waReq.Size()

	_, err = writer.Write(ctx, waReq) // response is empty
	if err != nil {
		return errors.Wrap(err, "sending request to write agent")
	}

	// Track latency and payload size only for successful requests.
	w.writeLatency.Observe(time.Since(startTime).Seconds())
	w.writeBytesTotal.Add(float64(waReqSize))

	return nil
}

func (w *Writer) getWriteAgentForPartition(partitionID int32) (ingestpb.WriteAgentClient, error) {
	writer, err := w.writeAgents.PickServer(partitionID)
	if err != nil {
		return nil, errors.Wrapf(err, "picking write agent for partition %d", partitionID)
	}
	return writer, nil
}

func (w *Writer) starting(ctx context.Context) error {
	return services.StartAndAwaitRunning(ctx, w.writeAgents)
}

type dnsBalancingStrategy interface {
	SetServers(servers ...string) error
	PickServer(key string) (net.Addr, error)
}

type writeAgentServerSelector struct {
	services.Service

	subservices *services.Manager

	logger log.Logger

	grpcConfig     grpcclient.Config
	serverSelector dnsBalancingStrategy

	writeAgentsMtx *sync.RWMutex
	writeAgents    map[string]ingestpb.WriteAgentClient
}

func newWriteAgentServerSelector(waConfig WriteAgentConfig, logger log.Logger) (writeAgentServerSelector, error) {
	w := writeAgentServerSelector{
		logger:         logger,
		grpcConfig:     waConfig.WriteAgentGRPCClientConfig,
		serverSelector: getDNSBalancingStrategy(waConfig.DNSBalancingStrategy),
		writeAgentsMtx: &sync.RWMutex{},
		writeAgents:    map[string]ingestpb.WriteAgentClient{},
	}

	sd, err := servicediscovery.NewDNS(logger, waConfig.Address, waConfig.DNSLookupPeriod, w)
	if err != nil {
		return writeAgentServerSelector{}, errors.Wrap(err, "creating service discovery")
	}
	w.subservices, err = services.NewManager(sd)
	if err != nil {
		return writeAgentServerSelector{}, errors.Wrap(err, "creating service manager")
	}
	w.Service = services.NewIdleService(w.start, w.stop)

	return w, nil
}

func (w writeAgentServerSelector) InstanceAdded(instance servicediscovery.Instance) {
	w.writeAgentsMtx.Lock()
	defer w.writeAgentsMtx.Unlock()

	_, ok := w.writeAgents[instance.Address]
	if ok {
		return
	}
	// Dial the new agent.
	opts, err := w.grpcConfig.DialOption(nil, nil)
	if err != nil {
		level.Warn(w.logger).Log("msg", "failed to create gRPC dial options", "err", err)
		return
	}
	cc, err := grpc.Dial(instance.Address, opts...)
	if err != nil {
		level.Warn(w.logger).Log("msg", "failed to create gRPC client", "err", err, "address", instance.Address)
		return
	}
	w.writeAgents[instance.Address] = ingestpb.NewWriteAgentClient(cc)
	w.updateSelectorFromAgents(instance)
	level.Info(w.logger).Log("msg", "added write agent client", "address", instance.Address)
}

func (w writeAgentServerSelector) InstanceRemoved(instance servicediscovery.Instance) {
	w.writeAgentsMtx.Lock()
	defer w.writeAgentsMtx.Unlock()

	if _, ok := w.writeAgents[instance.Address]; !ok {
		return
	}
	delete(w.writeAgents, instance.Address)
	w.updateSelectorFromAgents(instance)
	level.Info(w.logger).Log("msg", "removed write agent client", "address", instance.Address)
}

func (w writeAgentServerSelector) InstanceChanged(instance servicediscovery.Instance) {
	// If we get a notification for something we haven't seen yet, then just add it.
	w.InstanceAdded(instance)
}

// updateSelectorFromAgents assumes that w.writeAgents is already updated and that w.writeAgentsMtx is being held.
func (w writeAgentServerSelector) updateSelectorFromAgents(diff servicediscovery.Instance) {
	allAddresses := make([]string, 0, len(w.writeAgents))
	for addr := range w.writeAgents {
		allAddresses = append(allAddresses, addr)
	}
	err := w.serverSelector.SetServers(allAddresses...)
	if err != nil {
		level.Warn(w.logger).Log("msg", "couldn't update list of write agent clients; keeping same servers", "err", err, "changed_instance", diff.Address)
		return
	}
}

func (w writeAgentServerSelector) PickServer(partitionID int32) (ingestpb.WriteAgentClient, error) {
	w.writeAgentsMtx.RLock()
	defer w.writeAgentsMtx.RUnlock()
	addr, err := w.serverSelector.PickServer(strconv.Itoa(int(partitionID)))
	if err != nil {
		return nil, fmt.Errorf("picking server: %w", err)
	}
	agent := w.writeAgents[addr.String()]
	if agent == nil {
		return nil, fmt.Errorf("no write agent client for address %s, this shouldn't happen, report a bug", addr)
	}
	return agent, nil
}

func (w writeAgentServerSelector) start(ctx context.Context) error {
	return services.StartManagerAndAwaitHealthy(ctx, w.subservices)
}

func (w writeAgentServerSelector) stop(err error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), w.subservices)
}

func getDNSBalancingStrategy(name string) dnsBalancingStrategy {
	switch name {
	case DNSBalancingStrategyMod:
		return &dnsBalancingStrategyMod{}

	default:
		return &cache.MemcachedJumpHashSelector{} // it says memcached, but this is just a host jump hash selector; TODO: move in its own package
	}
}
