// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/objtools"
	"github.com/oklog/ulid"
)

type config struct {
	bucketConfig    objtools.BucketConfig
	includedTenants flagext.StringSliceCSV
	excludedTenants flagext.StringSliceCSV
	inputJsonFile   string
	dryRun          bool
}

func (c *config) registerFlags(f *flag.FlagSet) {
	c.bucketConfig.RegisterFlags(f)
	f.Var(&c.includedTenants, "included-tenants", "If not empty, only blocks for these tenants are recovered.")
	f.Var(&c.excludedTenants, "excluded-tenants", "Blocks for these tenants are not recovered.")
	f.StringVar(&c.inputJsonFile, "input-json-file", "", "If set, the blocks to undelete are read from the provided json file. If unset listings are performed to discover deleted blocks.")
	f.BoolVar(&c.dryRun, "dry-run", false, "If true, no writes/deletes are performed and are instead logged.")
}

func main() {
	cfg := config{}
	cfg.registerFlags(flag.CommandLine)

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if err := cfg.bucketConfig.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	err := run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	bucket, err := cfg.bucketConfig.ToBucket(ctx)
	if err != nil {
		return err
	}

	blocks, err := getBlocks(ctx, cfg, bucket)
	if err != nil {
		return err
	}

	undeleteBlocks(ctx, bucket, blocks, cfg.dryRun)

	return nil
}

type tenantFilter func(string) bool

func newTenantFilter(cfg config) tenantFilter {
	includedTenants := map[string]struct{}{}
	for _, u := range cfg.includedTenants {
		includedTenants[u] = struct{}{}
	}
	excludedTenants := map[string]struct{}{}
	for _, u := range cfg.excludedTenants {
		excludedTenants[u] = struct{}{}
	}

	return func(tenantID string) bool {
		if len(includedTenants) > 0 {
			if _, ok := includedTenants[tenantID]; !ok {
				return false
			}
		}
		_, ok := excludedTenants[tenantID]
		return !ok
	}
}

func getBlocks(ctx context.Context, cfg config, bucket objtools.Bucket) (map[string][]ulid.ULID, error) {
	tenantFilter := newTenantFilter(cfg)
	if cfg.inputJsonFile != "" {
		return getBlocksFromJsonFile(cfg.inputJsonFile, tenantFilter)
	}
	return getBlocksFromListing(ctx, bucket, tenantFilter)
}

// getBlocksFromFile reads a JSON tenant to blocks map from the specified file
func getBlocksFromJsonFile(filePath string, filter tenantFilter) (map[string][]ulid.ULID, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	m := make(map[string][]ulid.ULID)
	if err = json.Unmarshal(b, &m); err != nil {
		return nil, err
	}

	for tenant := range m {
		if !filter(tenant) {
			delete(m, tenant)
		}
	}

	return m, nil
}

func getBlocksFromListing(ctx context.Context, bucket objtools.Bucket, filter tenantFilter) (map[string][]ulid.ULID, error) {
	tenants, err := listTenants(ctx, bucket)
	if err != nil {
		return nil, err
	}
	m := make(map[string][]ulid.ULID, len(tenants))

	for _, tenantID := range tenants {
		if !filter(tenantID) {
			continue
		}

		blockIDs, err := listDeletedBlocksForTenant(ctx, bucket, tenantID)
		if err != nil {
			return nil, err
		}

		m[tenantID] = blockIDs
	}

	return m, nil
}

func listTenants(ctx context.Context, bkt objtools.Bucket) ([]string, error) {
	// Using versioned listing in case a tenant only has deleted objects
	result, err := bkt.List(ctx, objtools.ListOptions{
		Versioned: true,
	})
	if err != nil {
		return nil, err
	}

	return result.ToNames(), nil
}

func listDeletedBlocksForTenant(ctx context.Context, bkt objtools.Bucket, tenantID string) ([]ulid.ULID, error) {
	prefix := path.Join(tenantID, block.MarkersPathname)
	result, err := bkt.List(ctx, objtools.ListOptions{
		Prefix:    prefix,
		Versioned: true,
	})
	if err != nil {
		return nil, err
	}

	markers, err := result.ToNamesWithoutPrefix(prefix)
	if err != nil {
		return nil, err
	}
	blocks := make([]ulid.ULID, 0, len(markers))
	var last ulid.ULID
	for _, name := range markers {
		// preventing duplicates from multiple versions (used a version listing only to get deleted delete markers)
		if blockId, ok := block.IsDeletionMarkFilename(name); ok && blockId.Compare(last) != 0 {
			blocks = append(blocks, blockId)
		}
	}
	return blocks, nil
}

func undeleteBlocks(ctx context.Context, bucket objtools.Bucket, blocks map[string][]ulid.ULID, dryRun bool) {
	for tenantID, blockIDs := range blocks {
		for _, blockID := range blockIDs {
			err := undeleteBlock(ctx, bucket, tenantID, blockID, dryRun)
			if err != nil {
				slog.Error("failed to undelete block", "tenant", tenantID, "block", blockID, "err", err)
			} else if !dryRun {
				slog.Info("successfully restored block", "tenant", tenantID, "block", blockID)
			}
		}
	}
}

type version struct {
	lastModified time.Time
	info         objtools.VersionInfo
}

func undeleteBlock(ctx context.Context, bkt objtools.Bucket, tenantID string, blockID ulid.ULID, dryRun bool) error {
	/*
	 Lifecycle of a block
	 0. Nothing
	 1. Files without meta.json
	 2. Meta.json (block now complete)
	 3. Delete markers added (local delete marker, then global delete marker)
	 5. Files are deleted (meta first, then all except delete markers)
	 6. Delete the delete markers, local then global

	 To undelete a block we are going to restore objects from versions as needed following steps 1-2, then perform step 6.
	*/
	logger := slog.With("tenant", tenantID, "block", blockID)

	blockPrefix := tenantID + objtools.Delim + blockID.String()
	result, err := bkt.List(ctx, objtools.ListOptions{
		Prefix:    blockPrefix,
		Recursive: true,
		Versioned: true,
	})
	if err != nil {
		logger.Error("failed listing versions")
		return err
	}

	// First we'll sort the objects into version lists
	objects := result.Objects
	objVersions := make(map[string][]version, len(objects))
	for _, object := range objects {
		objVersions[object.Name] = append(objVersions[object.Name], version{
			object.LastModified,
			object.VersionInfo,
		})
	}

	// Read the meta (possibly from a noncurrent version)
	metaName := blockPrefix + objtools.Delim + block.MetaFilename
	m, metaVersion, err := getMeta(ctx, bkt, metaName, objVersions[metaName])
	if err != nil {
		logger.Error("failed reading the block meta file")
		return err
	}

	targets, err := getTargetsInBlock(m, objVersions, blockPrefix)
	if err != nil {
		logger.Error("failed getting target versions to restore")
		return err
	}
	// Restore the meta last if it's needed
	if metaVersion != nil {
		targets = append(targets, restorableVersion{
			objectName:  metaName,
			versionInfo: metaVersion.info,
		})
	}

	localDeleteMarkerPath := blockPrefix + objtools.Delim + block.DeletionMarkFilename
	globalDeleteMarkerPath := tenantID + objtools.Delim + block.DeletionMarkFilepath(blockID)

	if dryRun {
		logger.Info("dry run: block restorable")
		for _, target := range targets {
			logger.Info("dry run: would restore", "object", target.objectName, "version", target.versionInfo.VersionID)
		}
		logger.Info("dry run: would delete", "local", localDeleteMarkerPath, "global", globalDeleteMarkerPath)
		return nil
	}

	for _, target := range targets {
		if err := bkt.RestoreVersion(ctx, target.objectName, target.versionInfo); err != nil {
			logger.Error("failed to restore an object version", "object", target.objectName, "version", target.versionInfo.VersionID)
			return err
		} else {
			logger.Info("restored an object version", "object", target.objectName, "version", target.versionInfo.VersionID)
		}
	}
	for _, objectName := range []string{localDeleteMarkerPath, globalDeleteMarkerPath} {
		if err = bkt.Delete(ctx, objectName, objtools.DeleteOptions{}); err != nil {
			logger.Error("failed to delete a delete marker", "object", objectName)
			return err
		}
	}
	return nil
}

func getMeta(ctx context.Context, bkt objtools.Bucket, path string, versions []version) (*block.Meta, *version, error) {
	metaVersion, ok := versionToRestore(versions)
	if !ok {
		return nil, nil, fmt.Errorf("the path %s does not have a restorable meta file", path)
	}

	var metaVersionID string
	if metaVersion == nil {
		metaVersionID = ""
	} else {
		metaVersionID = metaVersion.info.VersionID
	}

	body, err := bkt.Get(ctx, path, objtools.GetOptions{
		VersionID: metaVersionID,
	})
	if err != nil {
		return nil, nil, err
	}

	m, err := block.ReadMeta(body)
	if err != nil {
		return nil, nil, err
	}

	return m, metaVersion, nil
}

func getTargetsInBlock(m *block.Meta, objVersions map[string][]version, blockPrefix string) ([]restorableVersion, error) {
	targetVersions := make([]restorableVersion, 0, len(m.Thanos.Files))

	// Verify that every expected file is present in the block and restorable
	for _, file := range m.Thanos.Files {
		if strings.HasSuffix(file.RelPath, block.MetaFilename) {
			// Skip the meta, since we're using it we know it's handled elsewhere
			continue
		}
		name := blockPrefix + objtools.Delim + file.RelPath
		versions, ok := objVersions[name]
		if !ok {
			return nil, fmt.Errorf("block %s had no versions for needed file %s", blockPrefix, file.RelPath)
		}
		restoreVersion, ok := versionToRestore(versions)
		if !ok {
			return nil, fmt.Errorf("block %s contained versions for %s, but none were restorable", blockPrefix, file.RelPath)
		}
		if restoreVersion != nil { // nil indicates the object has an existing current version
			targetVersions = append(targetVersions, restorableVersion{
				objectName:  name,
				versionInfo: restoreVersion.info,
			})
		}
	}

	return targetVersions, nil
}

type restorableVersion struct {
	objectName  string
	versionInfo objtools.VersionInfo
}

func versionToRestore(versions []version) (v *version, ok bool) {
	// Note: Doesn't depend on the ordering within the object version listing, only on the metadata of the versions
	var target *version
	var deleteMarkerTarget *version

	for _, version := range versions {
		info := version.info
		if info.IsDeleteMarker {
			if info.IsCurrent {
				deleteMarkerTarget = &version
			}
		} else if info.IsCurrent {
			return nil, true // nothing needs to be restored
		} else if target == nil || version.lastModified.After(target.lastModified) {
			target = &version
		}
	}

	// Only remove delete markers if there are at most two versions for simplicity
	if deleteMarkerTarget != nil && target != nil && len(versions) == 2 {
		return deleteMarkerTarget, true
	} else if target != nil {
		return target, true
	}
	return nil, false
}
