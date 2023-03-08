package eventbus

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/eventlogger"
	"github.com/hashicorp/eventlogger/formatter_filters/cloudevents"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-uuid"
	"github.com/hashicorp/vault/helper/namespace"
	"github.com/hashicorp/vault/sdk/logical"
	"github.com/ryanuber/go-glob"
)

const (
	// eventTypeAll is purely internal to the event bus. We use it to send all
	// events down one big firehose, and pipelines define their own filtering
	// based on what each subscriber is interested in.
	eventTypeAll   = "*"
	defaultTimeout = 60 * time.Second
)

var (
	ErrNotStarted              = errors.New("event broker has not been started")
	cloudEventsFormatterFilter *cloudevents.FormatterFilter
	subscriptions              atomic.Int64 // keeps track of event subscription count in all event buses
)

// EventBus contains the main logic of running an event broker for Vault.
// Start() must be called before the EventBus will accept events for sending.
type EventBus struct {
	logger          hclog.Logger
	broker          *eventlogger.Broker
	started         atomic.Bool
	formatterNodeID eventlogger.NodeID
	timeout         time.Duration
}

type pluginEventBus struct {
	bus        *EventBus
	namespace  *namespace.Namespace
	pluginInfo *logical.EventPluginInfo
}

type asyncChanNode struct {
	// TODO: add bounded deque buffer of *EventReceived
	ctx    context.Context
	ch     chan *eventlogger.Event
	logger hclog.Logger

	// used to close the connection
	closeOnce  sync.Once
	cancelFunc context.CancelFunc
	pipelineID eventlogger.PipelineID
	broker     *eventlogger.Broker
}

var (
	_ eventlogger.Node    = (*asyncChanNode)(nil)
	_ logical.EventSender = (*pluginEventBus)(nil)
)

// Start starts the event bus, allowing events to be written.
// It is not possible to stop or restart the event bus.
// It is safe to call Start() multiple times.
func (bus *EventBus) Start() {
	wasStarted := bus.started.Swap(true)
	if !wasStarted {
		bus.logger.Info("Starting event system")
	}
}

// SendInternal sends an event to the event bus and routes it to all relevant subscribers.
// This function does *not* wait for all subscribers to acknowledge before returning.
// This function is meant to be used by trusted internal code, so it can specify details like the namespace
// and plugin info. Events from plugins should be routed through WithPlugin(), which will populate
// the namespace and plugin info automatically.
func (bus *EventBus) SendInternal(ctx context.Context, ns *namespace.Namespace, pluginInfo *logical.EventPluginInfo, eventType logical.EventType, data *logical.EventData) error {
	if ns == nil {
		return namespace.ErrNoNamespace
	}
	if !bus.started.Load() {
		return ErrNotStarted
	}
	eventReceived := &logical.EventReceived{
		Event:      data,
		Namespace:  ns.Path,
		EventType:  string(eventType),
		PluginInfo: pluginInfo,
	}
	bus.logger.Info("Sending event", "event", eventReceived)

	// We can't easily know when the Send is complete, so we can't call the cancel function.
	// But, it is called automatically after bus.timeout, so there won't be any leak as long as bus.timeout is not too long.
	ctx, _ = context.WithTimeout(ctx, bus.timeout)
	_, err := bus.broker.Send(ctx, eventTypeAll, eventReceived)
	if err != nil {
		// if no listeners for this event type are registered, that's okay, the event
		// will just not be sent anywhere
		if strings.Contains(strings.ToLower(err.Error()), "no graph for eventtype") {
			return nil
		}
	}
	return err
}

func (bus *EventBus) WithPlugin(ns *namespace.Namespace, eventPluginInfo *logical.EventPluginInfo) (*pluginEventBus, error) {
	if ns == nil {
		return nil, namespace.ErrNoNamespace
	}
	return &pluginEventBus{
		bus:        bus,
		namespace:  ns,
		pluginInfo: eventPluginInfo,
	}, nil
}

// Send sends an event to the event bus and routes it to all relevant subscribers.
// This function does *not* wait for all subscribers to acknowledge before returning.
func (bus *pluginEventBus) Send(ctx context.Context, eventType logical.EventType, data *logical.EventData) error {
	return bus.bus.SendInternal(ctx, bus.namespace, bus.pluginInfo, eventType, data)
}

func init() {
	// TODO: maybe this should relate to the Vault core somehow?
	sourceUrl, err := url.Parse("https://vaultproject.io/")
	if err != nil {
		panic(err)
	}
	cloudEventsFormatterFilter = &cloudevents.FormatterFilter{
		Source: sourceUrl,
		Predicate: func(_ context.Context, e interface{}) (bool, error) {
			return true, nil
		},
	}
}

func NewEventBus(logger hclog.Logger) (*EventBus, error) {
	broker := eventlogger.NewBroker()

	formatterID, err := uuid.GenerateUUID()
	if err != nil {
		return nil, err
	}
	formatterNodeID := eventlogger.NodeID(formatterID)
	err = broker.RegisterNode(formatterNodeID, cloudEventsFormatterFilter)
	if err != nil {
		return nil, err
	}

	if logger == nil {
		logger = hclog.Default().Named("events")
	}

	return &EventBus{
		logger:          logger,
		broker:          broker,
		formatterNodeID: formatterNodeID,
		timeout:         defaultTimeout,
	}, nil
}

func (bus *EventBus) Subscribe(ctx context.Context, ns *namespace.Namespace, pattern string) (<-chan *eventlogger.Event, context.CancelFunc, error) {
	// subscriptions are still stored even if the bus has not been started
	pipelineID, err := uuid.GenerateUUID()
	if err != nil {
		return nil, nil, err
	}

	filterNodeID, err := uuid.GenerateUUID()
	if err != nil {
		return nil, nil, err
	}

	filterNode := newFilterNode(ns, pattern)
	err = bus.broker.RegisterNode(eventlogger.NodeID(filterNodeID), filterNode)
	if err != nil {
		return nil, nil, err
	}

	sinkNodeID, err := uuid.GenerateUUID()
	if err != nil {
		return nil, nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	asyncNode := newAsyncNode(ctx, bus.logger)
	err = bus.broker.RegisterNode(eventlogger.NodeID(sinkNodeID), asyncNode)
	if err != nil {
		defer cancel()
		return nil, nil, err
	}

	nodes := []eventlogger.NodeID{eventlogger.NodeID(filterNodeID), bus.formatterNodeID, eventlogger.NodeID(sinkNodeID)}

	pipeline := eventlogger.Pipeline{
		PipelineID: eventlogger.PipelineID(pipelineID),
		EventType:  eventTypeAll,
		NodeIDs:    nodes,
	}
	err = bus.broker.RegisterPipeline(pipeline)
	if err != nil {
		defer cancel()
		return nil, nil, err
	}

	addSubscriptions(1)
	// add info needed to cancel the subscription
	asyncNode.pipelineID = eventlogger.PipelineID(pipelineID)
	asyncNode.cancelFunc = cancel
	return asyncNode.ch, asyncNode.Close, nil
}

// SetSendTimeout sets the timeout of sending events. If the events are not accepted by the
// underlying channel before this timeout, then the channel closed.
func (bus *EventBus) SetSendTimeout(timeout time.Duration) {
	bus.timeout = timeout
}

func newFilterNode(ns *namespace.Namespace, pattern string) *eventlogger.Filter {
	return &eventlogger.Filter{
		Predicate: func(e *eventlogger.Event) (bool, error) {
			eventRecv := e.Payload.(*logical.EventReceived)

			// Drop if event is not in our namespace.
			// TODO: add wildcard/child namespace processing here in some cases?
			if eventRecv.Namespace != ns.Path {
				return false, nil
			}

			// Filter for correct event type, including wildcards.
			if !glob.Glob(pattern, eventRecv.EventType) {
				return false, nil
			}

			return true, nil
		},
	}
}

func newAsyncNode(ctx context.Context, logger hclog.Logger) *asyncChanNode {
	return &asyncChanNode{
		ctx:    ctx,
		ch:     make(chan *eventlogger.Event),
		logger: logger,
	}
}

// Close tells the bus to stop sending us events.
func (node *asyncChanNode) Close() {
	node.closeOnce.Do(func() {
		defer node.cancelFunc()
		if node.broker != nil {
			err := node.broker.RemovePipeline(eventTypeAll, node.pipelineID)
			if err != nil {
				node.logger.Warn("Error removing pipeline for closing node", "error", err)
			}
		}
		addSubscriptions(-1)
	})
}

func (node *asyncChanNode) Process(ctx context.Context, e *eventlogger.Event) (*eventlogger.Event, error) {
	// sends to the channel async in another goroutine
	go func() {
		var timeout bool
		select {
		case node.ch <- e:
		case <-ctx.Done():
			timeout = errors.Is(ctx.Err(), context.DeadlineExceeded)
		case <-node.ctx.Done():
			timeout = errors.Is(node.ctx.Err(), context.DeadlineExceeded)
		}
		if timeout {
			node.logger.Info("Subscriber took too long to process event, closing", "ID", e.Payload.(*logical.EventReceived).Event.Id)
			node.Close()
		}
	}()
	return e, nil
}

func (node *asyncChanNode) Reopen() error {
	return nil
}

func (node *asyncChanNode) Type() eventlogger.NodeType {
	return eventlogger.NodeTypeSink
}

func addSubscriptions(delta int64) {
	metrics.SetGauge([]string{"events", "subscriptions"}, float32(subscriptions.Add(delta)))
}
