// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	dskit_tenant "github.com/grafana/dskit/tenant"
	"go.etcd.io/bbolt"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/util"
)

const (
	// metadataBucketName is the name of the bbolt bucket used to store persistence metadata. It must not clash with a tenant name.
	metadataBucketName = "."
	// metadataKey is the key within the metadata bucket that holds the PersistenceMetadata protobuf.
	metadataKey = "metadata"
)

type BboltConfig struct {
	Dir        string `yaml:"dir" category:"experimental"`
	ShardCount int    `yaml:"shard_count" category:"experimental"`
}

func (cfg *BboltConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Dir, prefix+".dir", "./data-compactor-scheduler", "The directory where bbolt shard database files are stored for the compactor scheduler.")
	f.IntVar(&cfg.ShardCount, prefix+".shard-count", 16, "The target number of bbolt database shards for the compactor scheduler.")
}

func (cfg *BboltConfig) Validate(prefix string) error {
	if cfg.Dir == "" {
		return errors.New(prefix + ".dir must be set")
	}
	if cfg.ShardCount < 1 {
		return errors.New(prefix + ".shard-count must be at least 1")
	}
	return nil
}

type BboltJobPersister struct {
	db     *bbolt.DB
	name   []byte
	logger log.Logger
}

func newBboltJobPersister(db *bbolt.DB, name []byte, logger log.Logger) *BboltJobPersister {
	return &BboltJobPersister{
		db:     db,
		name:   name,
		logger: logger,
	}
}

func (jp *BboltJobPersister) Drop() error {
	return jp.db.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket(jp.name)
	})
}

func (jp *BboltJobPersister) WriteJob(j TrackedJob) error {
	content, err := j.Serialize()
	if err != nil {
		return err
	}
	return jp.db.Update(func(t *bbolt.Tx) error {
		b := t.Bucket(jp.name)
		if b == nil {
			// This should never happen
			return fmt.Errorf("bucket does not exist for %s", jp.name)
		}
		return b.Put([]byte(j.ID()), content)
	})
}

func (jp *BboltJobPersister) DeleteJob(j TrackedJob) error {
	return jp.db.Update(func(t *bbolt.Tx) error {
		b := t.Bucket(jp.name)
		if b == nil {
			// This should never happen
			return fmt.Errorf("bucket does not exist for %s", jp.name)
		}
		return b.Delete([]byte(j.ID()))
	})
}

func (jp *BboltJobPersister) WriteAndDeleteJobs(writes, deletes []TrackedJob) error {
	jobBytes := make([][]byte, len(writes))
	var err error
	for i, j := range writes {
		jobBytes[i], err = j.Serialize()
		if err != nil {
			return fmt.Errorf("failed serializing a job: %w", err)
		}
	}
	return jp.db.Update(func(t *bbolt.Tx) error {
		b := t.Bucket(jp.name)
		if b == nil {
			// This should never happen
			return fmt.Errorf("bucket does not exist for %s", jp.name)
		}
		for i, jb := range jobBytes {
			if err := b.Put([]byte(writes[i].ID()), jb); err != nil {
				return err
			}
		}
		for _, j := range deletes {
			if err := b.Delete([]byte(j.ID())); err != nil {
				return err
			}
		}
		return nil
	})
}

type BboltJobPersistenceManager struct {
	dbs    []*bbolt.DB
	meta   *compactorschedulerpb.PersistenceMetadata
	logger log.Logger
}

func openBboltJobPersistenceManager(dir string, shardCount int, logger log.Logger) (*BboltJobPersistenceManager, error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create bbolt shard directory: %w", err)
	}

	// Don't know if we created the directory or not. It's generally expected to be only one level.
	if err := syncDir(filepath.Dir(dir)); err != nil {
		return nil, fmt.Errorf("failed to sync parent directory: %w", err)
	}

	dbs, meta, err := prepare(dir, shardCount, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare bbolt shards: %w", err)
	}

	return &BboltJobPersistenceManager{
		dbs:    dbs,
		meta:   meta,
		logger: logger,
	}, nil
}

func (m *BboltJobPersistenceManager) shardDB(tenant string) *bbolt.DB {
	return m.dbs[shardForTenant(tenant, len(m.dbs))]
}

func (m *BboltJobPersistenceManager) CreationTime() time.Time {
	return time.Unix(m.meta.CreationTime, 0)
}

func (m *BboltJobPersistenceManager) InitializeTenant(tenant string) (JobPersister, error) {
	return m.initializeTenantOnDB(m.shardDB(tenant), tenant)
}

func (m *BboltJobPersistenceManager) initializeTenantOnDB(db *bbolt.DB, tenant string) (JobPersister, error) {
	tenantName := []byte(tenant)
	// Create a tenant bucket for the first time
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket(tenantName)
		return err
	})
	if err != nil {
		return nil, err
	}
	return newBboltJobPersister(db, tenantName, m.logger), nil
}

func (m *BboltJobPersistenceManager) RecoverAll(allowedTenants *util.AllowList, jobTrackerFactory func(tenant string, persister JobPersister) *JobTracker) (map[string]*JobTracker, error) {
	jobTrackers := make(map[string]*JobTracker)

	var (
		numCompactionJobsRecovered int
		numPlanJobsRecovered       int
		numBucketCleanups          int
		numKeyCleanups             int
	)

	level.Info(m.logger).Log("msg", "starting job recovery")

	for _, db := range m.dbs {
		stats, err := recoverDB(db, m.logger, allowedTenants, jobTrackers, jobTrackerFactory)
		if err != nil {
			level.Error(m.logger).Log("msg", "failed job recovery for shard", "path", db.Path(), "err", err)
			return nil, fmt.Errorf("failed recovering jobs from %q: %w", db.Path(), err)
		}
		numCompactionJobsRecovered += stats.numCompactionJobs
		numPlanJobsRecovered += stats.numPlanJobs
		numBucketCleanups += stats.numBucketCleanups
		numKeyCleanups += stats.numKeyCleanups
	}

	level.Info(m.logger).Log(
		"msg", "completed job recovery",
		"num_tenants_recovered", len(jobTrackers),
		"num_compaction_jobs_recovered", numCompactionJobsRecovered,
		"num_plan_jobs_recovered", numPlanJobsRecovered,
		"num_bucket_cleanups", numBucketCleanups,
		"num_key_cleanups", numKeyCleanups,
	)

	return jobTrackers, nil
}

type recoveryStats struct {
	numCompactionJobs int
	numPlanJobs       int
	numBucketCleanups int
	numKeyCleanups    int
}

// recoverDB recovers all job trackers from a single bbolt database.
func recoverDB(db *bbolt.DB, logger log.Logger, allowedTenants *util.AllowList, jobTrackers map[string]*JobTracker, jobTrackerFactory func(tenant string, persister JobPersister) *JobTracker) (recoveryStats, error) {
	var stats recoveryStats

	// Iterate through each bucket and create the corresponding JobTracker.
	// This is Update and not View due to the need to delete buckets that are no longer needed and values that failed to deserialize.
	err := db.Update(func(tx *bbolt.Tx) error {
		// Deletions can not occur during iteration since it interacts with the bucket cursor.
		// This maps tenants to required cleanup. If the cleanup is nil then the tenant bucket itself should be deleted.
		cleanup := make(map[string]*keyCleanup)

		// No bucket modification can occur within the ForEach.
		if err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			tenant := string(name)
			if tenant == metadataBucketName {
				return nil // skip metadata bucket
			}
			if err := dskit_tenant.ValidTenantID(tenant); err != nil {
				level.Warn(logger).Log("msg", "will delete bbolt bucket with invalid tenant name", "user", tenant, "err", err)
				cleanup[tenant] = nil
				return nil
			}
			if !allowedTenants.IsAllowed(tenant) {
				level.Warn(logger).Log("msg", "will delete bbolt bucket of tenant no longer allowed", "user", tenant)
				cleanup[tenant] = nil
				return nil
			}

			compactionJobs, planJob, keyCleanup := jobsFromTenantBucket(b)
			stats.numCompactionJobs += len(compactionJobs)
			if planJob != nil {
				stats.numPlanJobs++
			}
			if keyCleanup != nil {
				cleanup[tenant] = keyCleanup
			}

			jp := newBboltJobPersister(db, []byte(tenant), logger)
			jt := jobTrackerFactory(tenant, jp)
			jt.recoverFrom(compactionJobs, planJob)
			jobTrackers[tenant] = jt
			return nil
		}); err != nil {
			return err
		}

		stats.numBucketCleanups, stats.numKeyCleanups = cleanupBuckets(tx, cleanup, logger)
		return nil
	})
	return stats, err
}

// keyCleanup is only valid within the transaction in which it was created.
type keyCleanup struct {
	bucket    *bbolt.Bucket
	keyErrors []keyError
}

// keyError is only valid within the transaction in which it was created.
type keyError struct {
	key []byte
	err error
}

func cleanupBuckets(tx *bbolt.Tx, cleanup map[string]*keyCleanup, logger log.Logger) (numBucketCleanups, numKeyCleanups int) {
	for tenant, kc := range cleanup {
		name := []byte(tenant)
		if kc == nil {
			// Delete the bucket.
			if err := tx.DeleteBucket(name); err != nil {
				level.Warn(logger).Log("msg", "failed to delete bbolt bucket", "err", err)
				continue
			}
			numBucketCleanups++
			continue
		}

		// Delete the keys that failed.
		for _, ke := range kc.keyErrors {
			failureLogger := log.With(logger, "user", tenant, "key", string(ke.key), "err", ke.err)

			if err := kc.bucket.Delete(ke.key); err != nil {
				// Only returns an error if we're in a read-only transaction, which should never be the case.
				// This isn't the actual write, but instead an addition to the write transaction which hasn't reached commit yet.
				level.Warn(failureLogger).Log("msg", "failed to delete job that failed to deserialize", "delete_err", err)
				continue
			}
			numKeyCleanups++
			level.Warn(failureLogger).Log("msg", "deleted job that failed to deserialize")
		}
	}
	return
}

func jobsFromTenantBucket(bucket *bbolt.Bucket) ([]*TrackedCompactionJob, *TrackedPlanJob, *keyCleanup) {
	compactionJobs := make([]*TrackedCompactionJob, 0, 10)
	var planJob *TrackedPlanJob // may be nil
	var keyErrs []keyError

	c := bucket.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if len(k) == reservedJobIdLen {
			sk := string(k[0])
			switch sk {
			case planJobId:
				job, err := deserializePlanJob(v)
				if err != nil {
					keyErrs = append(keyErrs, keyError{k, err})
					continue
				}
				planJob = job
			default:
				keyErrs = append(keyErrs, keyError{k, errors.New("unknown key")})
			}
		} else {
			compactionJob, err := deserializeCompactionJob(k, v)
			if err != nil {
				keyErrs = append(keyErrs, keyError{k, err})
				continue
			}
			compactionJobs = append(compactionJobs, compactionJob)
		}
	}

	var kc *keyCleanup
	if len(keyErrs) > 0 {
		kc = &keyCleanup{bucket, keyErrs}
	}

	return compactionJobs, planJob, kc
}

func (m *BboltJobPersistenceManager) Close() error {
	var firstErr error
	for _, db := range m.dbs {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// shardForTenant returns the shard index for the given tenant
func shardForTenant(tenant string, shardCount int) int {
	return int(jumpHash(xxhash.Sum64String(tenant), shardCount))
}

// jumpHash consistently chooses a hash bucket number in the range
// [0, numBuckets) for the given key. numBuckets must be >= 1.
// Copied from github.com/grafana/dskit/cache/jump_hash.go
// which sourced from github.com/dgryski/go-jump/blob/master/jump.go
func jumpHash(key uint64, numBuckets int) int32 {
	var b int64 = -1
	var j int64

	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

// shardFilePath returns the path to the bbolt database file for the given shard index.
func shardFilePath(dir string, index int) string {
	// The padding here is purely cosmetic
	return filepath.Join(dir, fmt.Sprintf("bbolt_shard_%03d", index))
}

// countShardFiles returns the number of contiguous shard files in dir [0..n).
// Returns an error if the shard files are not contiguous.
func countShardFiles(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("failed to list shard directory: %w", err)
	}
	seen := make(map[int]struct{})
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		shardStr, ok := strings.CutPrefix(e.Name(), "bbolt_shard_")
		if !ok {
			continue
		}
		idx, err := strconv.Atoi(shardStr)
		if err != nil || idx < 0 {
			continue
		}
		seen[idx] = struct{}{}
	}
	for i := range len(seen) {
		if _, ok := seen[i]; !ok {
			return 0, fmt.Errorf("shard files are not contiguous: missing shard %d", i)
		}
	}
	return len(seen), nil
}

func prepare(dir string, targetCount int, logger log.Logger) ([]*bbolt.DB, *compactorschedulerpb.PersistenceMetadata, error) {
	existingCount, err := countShardFiles(dir)
	if err != nil {
		return nil, nil, err
	}

	// Cold start - no existing shard files
	if existingCount == 0 {
		level.Info(logger).Log("msg", "no existing bbolt shard files found")
		dbs, err := appendOpenShards(nil, dir, targetCount, logger)
		if err != nil {
			return nil, nil, err
		}
		if err := syncDir(dir); err != nil {
			closeDbs(dbs, logger)
			return nil, nil, fmt.Errorf("failed to sync shard directory: %w", err)
		}

		meta := &compactorschedulerpb.PersistenceMetadata{
			ShardCount:   uint32(targetCount),
			CreationTime: time.Now().Unix(),
		}
		if err := writeShardMetadata(dbs[0], meta); err != nil {
			closeDbs(dbs, logger)
			return nil, nil, fmt.Errorf("failed to write metadata to shard 0: %w", err)
		}
		level.Info(logger).Log("msg", "initialized bbolt shard files successfully", "shard_count", targetCount)
		return dbs, meta, nil
	}

	dbs, err := appendOpenShards(nil, dir, existingCount, logger)
	if err != nil {
		return nil, nil, err
	}

	meta, err := readPersistenceMetadata(dbs[0])
	if err != nil {
		closeDbs(dbs, logger)
		return nil, nil, fmt.Errorf("failed to read shard metadata: %w", err)
	}

	// Verify that no shard files have gone missing. countShardFiles checks that files are
	// contiguous [0..n), but it does not know how many should exist.
	// On scale up (including cold start) -> metadata written after shard files written
	// On scale down -> metadata written before shard files deleted
	// In both cases having more shards than the meta has is okay, but having less is not
	metaCount := int(meta.ShardCount)
	if existingCount < metaCount {
		closeDbs(dbs, logger)
		return nil, nil, fmt.Errorf("expected at least %d shard files based on metadata but only found %d", meta.ShardCount, existingCount)
	}

	if metaCount == targetCount && existingCount == targetCount {
		level.Info(logger).Log("msg", "bbolt shard count matches target, no migration needed", "shard_count", targetCount)
		return dbs, meta, nil
	}

	level.Info(logger).Log("msg", "migrating bbolt shards", "stored_shard_count", meta.ShardCount, "target_shard_count", targetCount, "existing_count", existingCount)
	start := time.Now()
	migratedDbs, err := runMigration(dbs, dir, targetCount, meta, logger)
	if err != nil {
		// runMigration handles closing databases itself since it potentially created more
		return nil, nil, err
	}
	level.Info(logger).Log("msg", "bbolt shard migration completed", "duration_ms", time.Since(start).Milliseconds())
	return migratedDbs, meta, nil
}

// runMigration ensures there are targetCount shards and reshards tenants if necessary
func runMigration(existingDBs []*bbolt.DB, dir string, targetCount int, meta *compactorschedulerpb.PersistenceMetadata, logger log.Logger) (dbs []*bbolt.DB, err error) {
	defer func() {
		if err != nil {
			closeDbs(dbs, logger)
		}
	}()

	dbs, err = createShardsIfNeeded(existingDBs, dir, targetCount, logger)
	if err != nil {
		return dbs, err
	}

	copyTargets, source, toDelete, err := scanShardsForTenants(dbs, targetCount)
	if err != nil {
		return dbs, err
	}

	err = copyTenantsToShards(dbs, copyTargets, source)
	if err != nil {
		return dbs, err
	}

	err = deleteTenantsFromShards(dbs, toDelete)
	if err != nil {
		return dbs, err
	}

	// Write metadata before deleting extra shard files. This ensures that if a crash occurs
	// between metadata write and file deletion that the extra files can be cleaned up.
	previousCount := meta.ShardCount
	meta.ShardCount = uint32(targetCount)
	if err = writeShardMetadata(dbs[0], meta); err != nil {
		meta.ShardCount = previousCount // defensively undo modification
		return dbs, fmt.Errorf("failed to write metadata to %q: %w", dbs[0].Path(), err)
	}

	dbs, err = closeAndDeleteExtraShards(dir, dbs, targetCount, logger)
	if err != nil {
		return dbs, err
	}

	return
}

// openShard opens or creates the shard file for the given index in dir.
func openShard(dir string, index int) (*bbolt.DB, error) {
	path := shardFilePath(dir, index)
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open shard %q: %w", path, err)
	}
	return db, nil
}

// appendOpenShards opens shard files [len(dbs), end) in dir, appending them to dbs.
// On error, any newly opened databases are closed and the original slice is returned.
func appendOpenShards(dbs []*bbolt.DB, dir string, end int, logger log.Logger) ([]*bbolt.DB, error) {
	begin := len(dbs)
	for i := begin; i < end; i++ {
		db, err := openShard(dir, i)
		if err != nil {
			closeDbs(dbs[begin:], logger)
			return dbs[:begin], err
		}
		dbs = append(dbs, db)
	}
	return dbs, nil
}

func createShardsIfNeeded(existingDBs []*bbolt.DB, dir string, targetCount int, logger log.Logger) ([]*bbolt.DB, error) {
	if len(existingDBs) >= targetCount {
		return existingDBs, nil
	}

	dbs, err := appendOpenShards(existingDBs, dir, targetCount, logger)
	if err != nil {
		// not returning nil dbs to allow for closing
		return dbs, err
	}

	if err := syncDir(dir); err != nil {
		return dbs, fmt.Errorf("failed to sync shard directory: %w", err)
	}

	return dbs, nil
}

func copyTenantsToShards(dbs []*bbolt.DB, copyTargets [][]string, source map[string]int) error {
	for i, tenants := range copyTargets {
		if len(tenants) == 0 {
			continue
		}
		err := dbs[i].Update(func(tx *bbolt.Tx) error {
			for _, tenant := range tenants {
				srcIdx := source[tenant]
				kvs, err := readDataFromTenantBucket(dbs[srcIdx], tenant)
				if err != nil {
					return fmt.Errorf("failed to read tenant %q from shard %d: %w", tenant, srcIdx, err)
				}
				b, err := tx.CreateBucket([]byte(tenant))
				if err != nil {
					return fmt.Errorf("failed to create bucket for tenant %q: %w", tenant, err)
				}
				for _, kv := range kvs {
					if err := b.Put(kv.key, kv.value); err != nil {
						return fmt.Errorf("failed to write key value pair during migration for tenant %q: %w", tenant, err)
					}
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to copy tenants to %q: %w", dbs[i].Path(), err)
		}
	}
	return nil
}

func closeAndDeleteExtraShards(dir string, dbs []*bbolt.DB, targetCount int, logger log.Logger) ([]*bbolt.DB, error) {
	deleted := false
	for len(dbs) > targetCount {
		db := dbs[len(dbs)-1]
		path := db.Path()
		dbs = dbs[:len(dbs)-1] // shrink before close so when we return dbs the caller won't attempt a double close
		if err := db.Close(); err != nil {
			return dbs, fmt.Errorf("failed to close extra shard %q: %w", path, err)
		}
		if err := os.Remove(path); err != nil {
			return dbs, fmt.Errorf("failed to delete extra shard file %q: %w", path, err)
		}
		deleted = true
		level.Info(logger).Log("msg", "deleted extra shard file after migration", "path", path)
	}
	if deleted {
		if err := syncDir(dir); err != nil {
			return dbs, fmt.Errorf("failed to sync shard directory: %w", err)
		}
	}
	return dbs, nil
}

func deleteTenantsFromShards(dbs []*bbolt.DB, delete [][]string) error {
	for i, tenants := range delete {
		if len(tenants) == 0 {
			continue
		}
		err := dbs[i].Update(func(tx *bbolt.Tx) error {
			for _, tenant := range tenants {
				if err := tx.DeleteBucket([]byte(tenant)); err != nil {
					return fmt.Errorf("failed to delete tenant %q: %w", tenant, err)
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to delete tenants from %s during migration: %w", dbs[i].Path(), err)
		}
	}

	return nil
}

// scanShardsForTenants scans all shards to identify misplaced tenants.
// It returns the per-target copy list, a source shard for each misplaced tenant,
// and the per-shard list of tenant buckets to delete.
func scanShardsForTenants(dbs []*bbolt.DB, targetCount int) (copyTargets [][]string, source map[string]int, toDelete [][]string, err error) {
	tenantTarget := make(map[string]int)
	placed := make(map[string]struct{})
	source = make(map[string]int)
	toDelete = make([][]string, len(dbs))

	for i, db := range dbs {
		err = db.View(func(tx *bbolt.Tx) error {
			return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
				tenant := string(name)
				if tenant == metadataBucketName {
					return nil
				}
				target, ok := tenantTarget[tenant]
				if !ok {
					target = shardForTenant(tenant, targetCount)
					tenantTarget[tenant] = target
				}
				if i == target {
					placed[tenant] = struct{}{}
				} else {
					source[tenant] = i // could be an overwrite, but all sources should be equivalent
					toDelete[i] = append(toDelete[i], tenant)
				}
				return nil
			})
		})
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to scan %q for migration: %w", db.Path(), err)
		}
	}

	copyTargets = make([][]string, len(dbs))
	for tenant := range source {
		if _, ok := placed[tenant]; ok {
			continue
		}
		target := tenantTarget[tenant]
		copyTargets[target] = append(copyTargets[target], tenant)
	}
	return copyTargets, source, toDelete, nil
}

// readPersistenceMetadata reads PersistenceMetadata from the given database's metadata bucket.
// Returns a zero-value metadata if the bucket or key does not exist.
func readPersistenceMetadata(db *bbolt.DB) (*compactorschedulerpb.PersistenceMetadata, error) {
	meta := &compactorschedulerpb.PersistenceMetadata{}
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(metadataBucketName))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(metadataKey))
		if v == nil {
			return nil
		}
		return meta.Unmarshal(v)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read shard metadata: %w", err)
	}
	return meta, nil
}

// writeShardMetadata writes PersistenceMetadata to the metadata bucket of db.
func writeShardMetadata(db *bbolt.DB, meta *compactorschedulerpb.PersistenceMetadata) error {
	data, err := meta.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal shard metadata: %w", err)
	}
	return db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(metadataBucketName))
		if err != nil {
			return err
		}
		return b.Put([]byte(metadataKey), data)
	})
}

type kvPair struct {
	key   []byte
	value []byte
}

// readDataFromTenantBucket copies all key-value pairs from the tenant's bbolt bucket.
func readDataFromTenantBucket(db *bbolt.DB, tenant string) ([]kvPair, error) {
	var pairs []kvPair
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(tenant))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// Must copy since the values are otherwise only valid during the lifetime of the View call
			kCopy := make([]byte, len(k))
			vCopy := make([]byte, len(v))
			copy(kCopy, k)
			copy(vCopy, v)
			pairs = append(pairs, kvPair{kCopy, vCopy})
		}
		return nil
	})
	return pairs, err
}

// syncDir fsyncs a directory to ensure durable directory entries.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	merr := multierror.New()
	merr.Add(d.Sync())
	merr.Add(d.Close())
	return merr.Err()
}

// closeDbs closes all databases while logging any errors.
func closeDbs(dbs []*bbolt.DB, logger log.Logger) {
	for i, db := range dbs {
		if err := db.Close(); err != nil {
			level.Warn(logger).Log("msg", "failed to close shard db", "shard", i, "err", err)
		}
	}
}
