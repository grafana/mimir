package shardlayout

import (
	"time"

	"github.com/grafana/dskit/services"
)

type Synchronizer struct {
	services.Service
	subServices        *services.Manager
	subServicesWatcher *services.FailureWatcher
}

const discoverInterval = 1 * time.Minute

func NewSynchronizer(discoverer IndexHeaderMetaDiscoverer) *Synchronizer {
	discovererSubSvc := services.NewTimerService(discoverInterval, nil, discoverer.Discover, nil)

	return &Synchronizer{}
}
