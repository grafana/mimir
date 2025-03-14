package shardlayout

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/backoff"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

type IndexHeaderMetaDiscoverer interface {
	Discover(context.Context) (*IndexHeaderMetaMap, error)
}

type BucketIndexHeaderMetaDiscoverer struct {
	bucket objstore.Bucket
	// Keeps a bucket client for each tenant.
	bucketClientsMu sync.RWMutex
	bucketClients   map[string]objstore.InstrumentedBucket
	//fetcher indexheader.BlockMetadataFetcher

	// configs
	cfg               tsdb.BlocksStorageConfig
	syncBackoffConfig backoff.Config
	allowedTenants    *util.AllowedTenants
	limits            *validation.Overrides

	logger log.Logger
	reg    prometheus.Registerer
}

func NewBucketIndexHeaderMetaDiscoverer(
	bucket objstore.Bucket,
	blocksStorageConfig tsdb.BlocksStorageConfig,
	syncBackoffConfig backoff.Config,
	allowedTenants *util.AllowedTenants,
	logger log.Logger,
	reg prometheus.Registerer,
) *BucketIndexHeaderMetaDiscoverer {
	return &BucketIndexHeaderMetaDiscoverer{
		bucket:        bucket,
		bucketClients: map[string]objstore.InstrumentedBucket{},

		//fetcher:             fetcher,
		cfg:               blocksStorageConfig,
		syncBackoffConfig: syncBackoffConfig,
		allowedTenants:    allowedTenants,
		logger:            logger,
		reg:               reg,
	}
}

func (ihd *BucketIndexHeaderMetaDiscoverer) Discover(ctx context.Context) (*IndexHeaderMetaMap, error) {
	retries := backoff.New(ctx, ihd.syncBackoffConfig)

	var indexHeaderMetaMap *IndexHeaderMetaMap
	var lastErr error
	for retries.Ongoing() {
		tenantIDs, err := ihd.ListAllowedTenants(ctx)
		if err != nil {
			retries.Wait()
			continue
		}
		indexHeaderMetaMap, lastErr = ihd.discoverForTenants(ctx, tenantIDs)
		if lastErr == nil {
			return indexHeaderMetaMap, nil
		}

		retries.Wait()
	}

	if lastErr == nil {
		return indexHeaderMetaMap, retries.Err()
	}

	return indexHeaderMetaMap, lastErr
}

func (ihd *BucketIndexHeaderMetaDiscoverer) discoverForTenants(
	ctx context.Context, tenantIDs []string,
) (*IndexHeaderMetaMap, error) {
	indexHeaderMetaMap := &IndexHeaderMetaMap{}

	tenantIDs, err := ihd.ListAllowedTenants(ctx)
	if err != nil {
		return nil, err
	}

	type tenantJob struct {
		tenantID string
	}

	tenantDiscoveryWaitGroup := &sync.WaitGroup{}
	tenantJobs := make(chan tenantJob)
	errs := tsdb_errors.NewMulti()
	errsMx := sync.Mutex{}

	// Create tenant index header worker pool up to configured concurrency level
	for i := 0; i < ihd.cfg.BucketStore.TenantSyncConcurrency; i++ {
		tenantDiscoveryWaitGroup.Add(1)
		go func() {
			defer tenantDiscoveryWaitGroup.Done()

			for tenantJob := range tenantJobs {
				err := ihd.discoverForTenant(ctx, tenantJob.tenantID, indexHeaderMetaMap)
				if err != nil {
					err = errors.Wrapf(
						err, "failed to discover TSDB index header for user %s", tenantJob.tenantID,
					)
					errsMx.Lock()
					errs.Add(err)
					errsMx.Unlock()
				}
			}
		}()
	}

	// Submit tenant index header discovery jobs
	for _, tenantID := range tenantIDs {
		tenantJobs <- tenantJob{tenantID: tenantID}
	}

	close(tenantJobs)
	tenantDiscoveryWaitGroup.Wait()

	return indexHeaderMetaMap, errs.Err()

	//
	//// Lazily create a bucket store for each new user found
	//// and submit a sync tenantJob for each user.
	//for _, userID := range allowedTenantIDs {
	//	bs, err := u.getOrCreateStoreNoShardingFilter(ctx, userID)
	//	if err != nil {
	//		errsMx.Lock()
	//		errs.Add(err)
	//		errsMx.Unlock()
	//
	//		continue
	//	}
	//
	//	select {
	//	case tenantJobs <- tenantJob{tenantID: userID, store: bs}:
	//		// Nothing to do. Will loop to push more tenantJobs.
	//	case <-ctx.Done():
	//		// Wait until all workers have done, so the goroutines leak detector doesn't
	//		// report any issue. This is expected to be quick, considering the done ctx
	//		// is used by the worker callback function too.
	//		close(tenantJobs)
	//		tenantDiscoveryWaitGroup.Wait()
	//
	//		return ctx.Err()
	//	}
	//}
	//
	//// Wait until all workers completed.
	//close(tenantJobs)
	//tenantDiscoveryWaitGroup.Wait()
	//
	////u.closeBucketStoreAndDeleteLocalFilesForExcludedTenants(UserIDs)
	//
	//return errs.Err()
}

func (ihd *BucketIndexHeaderMetaDiscoverer) discoverForTenant(
	ctx context.Context, tenantID string, indexHeaderMetaMap *IndexHeaderMetaMap,
) error {
	tenantBucketClient := ihd.getOrCreateTenantBucketClient(tenantID)
	tenantLogger := util_log.WithUserID(tenantID, ihd.logger)

	indexHeaderMetaFetcherFilters := []block.MetadataFilter{
		indexheader.NewMinTimeMetaFilter(ihd.cfg.BucketStore.IgnoreBlocksWithin),
		indexheader.NewIgnoreDeletionMarkFilter(
			tenantLogger,
			tenantBucketClient,
			ihd.cfg.BucketStore.IgnoreDeletionMarksInStoreGatewayDelay,
			ihd.cfg.BucketStore.MetaSyncConcurrency),
	}

	indexHeaderMetaFetcher := indexheader.NewBucketIndexMetadataFetcher(
		tenantID,
		ihd.bucket,
		ihd.limits,
		ihd.logger,
		ihd.reg,
		indexHeaderMetaFetcherFilters,
	)

	metas, _, metaFetchErr := indexHeaderMetaFetcher.Fetch(ctx)
	// For partial view allow adding new blocks at least. TODO figure out what this means
	if metaFetchErr != nil && metas == nil {
		return metaFetchErr
	}

	errGroup, ctx := errgroup.WithContext(ctx)
	blockc := make(chan *block.Meta)

	for i := 0; i < ihd.cfg.BucketStore.BlockSyncConcurrency; i++ {
		errGroup.Go(func() error {
			for meta := range blockc {
				indexFilepath := filepath.Join(meta.ULID.String(), block.IndexFilename)
				attrs, err := tenantBucketClient.Attributes(ctx, indexFilepath)
				if err != nil {
					err = errors.Wrapf(err, "get object attributes of %s", indexFilepath)
					return err
				}
				err = indexHeaderMetaMap.Add(tenantID, meta.ULID, attrs)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	for blockID, meta := range metas {
		if indexHeaderMetaMap.Contains(tenantID, blockID) {
			continue
		}
		select {
		case <-ctx.Done():
		case blockc <- meta:
		}
	}

	close(blockc)
	err := errGroup.Wait()
	if err != nil {
		return err
	}

	if metaFetchErr != nil {
		return metaFetchErr
	}

	return nil
}

func (ihd *BucketIndexHeaderMetaDiscoverer) getTenantBucketClient(tenantID string) objstore.InstrumentedBucket {
	ihd.bucketClientsMu.RLock()
	defer ihd.bucketClientsMu.RUnlock()
	return ihd.bucketClients[tenantID]
}

func (ihd *BucketIndexHeaderMetaDiscoverer) getOrCreateTenantBucketClient(tenantID string) objstore.InstrumentedBucket {
	tenantBucketClient := ihd.getTenantBucketClient(tenantID)
	if tenantBucketClient != nil {
		return tenantBucketClient
	}

	tenantBucketClient = bucket.NewUserBucketClient(tenantID, ihd.bucket, ihd.limits)
	ihd.bucketClientsMu.Lock()
	defer ihd.bucketClientsMu.Unlock()
	ihd.bucketClients[tenantID] = tenantBucketClient

	return tenantBucketClient
}

// ListAllowedTenants in the bucket and return the list of found users,
// respecting any specifically enabled or disabled users.
func (ihd *BucketIndexHeaderMetaDiscoverer) ListAllowedTenants(ctx context.Context) ([]string, error) {
	users, err := tsdb.ListUsers(ctx, ihd.bucket)
	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(users))
	for _, user := range users {
		if ihd.allowedTenants.IsAllowed(user) {
			filtered = append(filtered, user)
		}
	}

	return filtered, nil
}

//func (ihd *BucketIndexHeaderMetaDiscoverer) blockIndexHeaderAttrs(ctx context.Context, blockID ulid.ULID) (objstore.ObjectAttributes, error) {
//	indexFilepath := filepath.Join(blockID.String(), block.IndexFilename)
//	tenantBucketClient := ihd.getOrCreateTenantBucketClient()
//	attrs, err := ihd.bucket.Attributes(ctx, indexFilepath)
//	if err != nil {
//		err = errors.Wrapf(err, "get object attributes of %s", indexFilepath)
//	}
//	return attrs, nil
//}

// IndexHeaderMetaMap is a thread-safe nested map structure.
// This type serves as a synchronized map[string]map[ulid.ULID]objstore.ObjectAttributes.
// The objstore.ObjectAttributes are used to get the index header size.
type IndexHeaderMetaMap struct {
	data sync.Map // map[string]sync.Map
}

func (t *IndexHeaderMetaMap) String() string {
	s := "IndexHeaderMetaMap: "
	t.data.Range(func(key, value interface{}) bool {
		tenantID := key.(string)
		indexHeaders := value.(*sync.Map)
		indexHeaders.Range(func(key, value interface{}) bool {
			blockID := key.(ulid.ULID)
			attrs := value.(objstore.ObjectAttributes)
			s += fmt.Sprintf("tenantID: %s, blockID: %s, attrs: %v --- ", tenantID, blockID, attrs)
			return true
		})
		return true
	})
	return s
}

func (t *IndexHeaderMetaMap) Contains(tenantID string, blockID ulid.ULID) bool {
	indexHeadersVal, ok := t.data.Load(tenantID)
	if !ok {
		return false
	}
	indexHeaders := indexHeadersVal.(*sync.Map)
	_, ok = indexHeaders.Load(blockID)
	return ok
}

func (t *IndexHeaderMetaMap) Add(
	tenantID string,
	blockID ulid.ULID,
	indexHeaderAttrs objstore.ObjectAttributes,
) error {
	indexHeadersVal, ok := t.data.Load(tenantID)
	if !ok {
		newIndexHeaders := &sync.Map{} // map[ulid.ULID]objstore.ObjectAttributes
		newIndexHeaders.Store(blockID, indexHeaderAttrs)
		t.data.Store(tenantID, newIndexHeaders)
		return nil
	}
	indexHeaders := indexHeadersVal.(*sync.Map)
	_, ok = indexHeaders.LoadOrStore(blockID, indexHeaderAttrs)
	if ok {
		// This should not ever happen.
		return fmt.Errorf("block %s already exists in the set", blockID)
	}
	return nil
}
