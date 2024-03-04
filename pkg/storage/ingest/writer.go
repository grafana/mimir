// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/servicediscovery"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

const (
	// writerRequestTimeoutOverhead is the overhead applied by the Writer to every Kafka timeout.
	// You can think about this overhead as an extra time for requests sitting in the client's buffer
	// before being sent on the wire and the actual time it takes to send it over the network and
	// start being processed by Kafka.
	writerRequestTimeoutOverhead = 2 * time.Second
)

type writeAgentSelector interface {
	services.Service
	PickServer(partitionID int32) (ingestpb.WriteAgentClient, error)
}

// Writer is responsible to write incoming data to the ingest storage.
type Writer struct {
	services.Service

	logger     log.Logger
	registerer prometheus.Registerer

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
	return newWriter(waSelector, logger, reg)
}

func newWriter(waSelector writeAgentSelector, logger log.Logger, reg prometheus.Registerer) (*Writer, error) {
	w := &Writer{
		logger:                     logger,
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
	data, err := req.Marshal()
	if err == nil {
		waReq := &ingestpb.WriteRequest{
			Piece:       &ingestpb.Piece{Data: data, TenantId: userID},
			PartitionId: partitionID,
		}
		waReqSize = waReq.Size()

		_, err = writer.Write(ctx, waReq) // response is empty
	}
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

// TODO remove
// newKafkaWriter creates a new Kafka client used to write to a specific partition.
func (w *Writer) newKafkaWriter(partitionID int32) (*kgo.Client, error) {
	//logger := log.With(w.logger, "partition", partitionID)

	// Do not export the client ID, because we use it to specify options to the backend.
	//metrics := kprom.NewMetrics("cortex_ingest_storage_writer",
	//	kprom.Registerer(prometheus.WrapRegistererWith(prometheus.Labels{"partition": strconv.Itoa(int(partitionID))}, w.registerer)),
	//	kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes))

	opts := append([]kgo.Opt{},
		//commonKafkaClientOptions(w.kafkaCfg, metrics, logger),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		//kgo.DefaultProduceTopic(w.kafkaCfg.Topic),

		// Use a static partitioner because we want to be in control of the partition.
		kgo.RecordPartitioner(newKafkaStaticPartitioner(int(partitionID))),

		// Set the upper bounds the size of a record batch.
		kgo.ProducerBatchMaxBytes(16_000_000),

		// By default, the Kafka client allows 1 Produce in-flight request per broker. Disabling write idempotency
		// (which we don't need), we can increase the max number of in-flight Produce requests per broker. A higher
		// number of in-flight requests, in addition to short buffering ("linger") in client side before firing the
		// next Produce request allows us to reduce the end-to-end latency.
		//
		// The result of the multiplication of producer linger and max in-flight requests should match the maximum
		// Produce latency expected by the Kafka backend in a steady state. For example, 50ms * 20 requests = 1s,
		// which means the Kafka client will keep issuing a Produce request every 50ms as far as the Kafka backend
		// doesn't take longer than 1s to process them (if it takes longer, the client will buffer data and stop
		// issuing new Produce requests until some previous ones complete).
		kgo.DisableIdempotentWrite(),
		kgo.ProducerLinger(50*time.Millisecond),
		kgo.MaxProduceRequestsInflightPerBroker(w.maxInflightProduceRequests),

		// Unlimited number of Produce retries but a deadline on the max time a record can take to be delivered.
		// With the default config it would retry infinitely.
		//
		// Details of the involved timeouts:
		// - RecordDeliveryTimeout: how long a Kafka client Produce() call can take for a given record. The overhead
		//   timeout is NOT applied.
		// - ProduceRequestTimeout: how long to wait for the response to the Produce request (the Kafka protocol message)
		//   after being sent on the network. The actual timeout is increased by the configured overhead.
		//
		// When a Produce request to Kafka fail, the client will retry up until the RecordDeliveryTimeout is reached.
		// Once the timeout is reached, the Produce request will fail and all other buffered requests in the client
		// (for the same partition) will fail too. See kgo.RecordDeliveryTimeout() documentation for more info.
		kgo.RecordRetries(math.MaxInt64),
		//kgo.RecordDeliveryTimeout(w.kafkaCfg.WriteTimeout),
		//kgo.ProduceRequestTimeout(w.kafkaCfg.WriteTimeout),
		kgo.RequestTimeoutOverhead(writerRequestTimeoutOverhead),
	)
	return kgo.NewClient(opts...)
}

func (w *Writer) starting(ctx context.Context) error {
	return services.StartAndAwaitRunning(ctx, w.writeAgents)
}

type kafkaStaticPartitioner struct {
	partitionID int
}

func newKafkaStaticPartitioner(partitionID int) *kafkaStaticPartitioner {
	return &kafkaStaticPartitioner{
		partitionID: partitionID,
	}
}

// ForTopic implements kgo.Partitioner.
func (p *kafkaStaticPartitioner) ForTopic(string) kgo.TopicPartitioner {
	return p
}

// RequiresConsistency implements kgo.TopicPartitioner.
func (p *kafkaStaticPartitioner) RequiresConsistency(_ *kgo.Record) bool {
	// Never let Kafka client to write the record to another partition
	// if the partition is down.
	return true
}

// Partition implements kgo.TopicPartitioner.
func (p *kafkaStaticPartitioner) Partition(_ *kgo.Record, _ int) int {
	return p.partitionID
}

type writeAgentServerSelector struct {
	services.Service

	subservices *services.Manager

	logger log.Logger

	grpcConfig     grpcclient.Config
	serverSelector *cache.MemcachedJumpHashSelector // it says memcached, but this is just a host jump hash selector; TODO: move in its own package

	writeAgentsMtx *sync.RWMutex
	writeAgents    map[string]ingestpb.WriteAgentClient
}

func newWriteAgentServerSelector(waConfig WriteAgentConfig, logger log.Logger) (writeAgentServerSelector, error) {
	w := writeAgentServerSelector{
		logger:         logger,
		grpcConfig:     waConfig.WriteAgentGRPCClientConfig,
		serverSelector: &cache.MemcachedJumpHashSelector{},
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
