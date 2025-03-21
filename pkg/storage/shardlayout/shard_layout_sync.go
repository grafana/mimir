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

func NewSynchronizer(discoverer IndexHeaderMetaDiscoverer) (*Synchronizer, error) {
	discovererSubSvc := services.NewTimerService(discoverInterval, nil, discoverer.Discover, nil)
	subServicesWatcher := services.NewFailureWatcher()
	subServicesManager, err := services.NewManager(discovererSubSvc)
	if err != nil {
		return nil, err
	}
	subServicesWatcher.WatchManager(subServicesManager)

	return &Synchronizer{
		Service:            discovererSubSvc,
		subServices:        subServicesManager,
		subServicesWatcher: subServicesWatcher,
	}, nil
}
