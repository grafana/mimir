package receivers

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/prometheus/alertmanager/notify"

	"github.com/grafana/alerting/receivers/schema"
)

// Base is the base implementation of a notifier. It contains the common fields across all notifier types.
type Base struct {
	Index                 int
	Name                  string
	Type                  schema.IntegrationType
	Version               schema.Version
	UID                   string
	DisableResolveMessage bool
	logger                log.Logger
}

func (n *Base) GetDisableResolveMessage() bool {
	return n.DisableResolveMessage
}

func (n *Base) GetLogger(ctx context.Context) log.Logger {
	gkey, _ := notify.GroupKey(ctx)
	return log.With(n.logger, "receiver", n.Name, "integration", fmt.Sprintf("%s[%d]", n.Type, n.Index), "version", n.Version, "aggrGroup", gkey)
}

// Metadata contains the metadata of the notifier.
type Metadata struct {
	Index                 int
	UID                   string
	Name                  string
	Type                  schema.IntegrationType
	Version               schema.Version
	DisableResolveMessage bool
}

func NewBase(cfg Metadata, logger log.Logger) *Base {
	return &Base{
		Index:                 cfg.Index,
		UID:                   cfg.UID,
		Name:                  cfg.Name,
		Type:                  cfg.Type,
		Version:               cfg.Version,
		DisableResolveMessage: cfg.DisableResolveMessage,
		logger:                logger,
	}
}
