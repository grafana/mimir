// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/objtools"
)

type config struct {
	bucketConfig   objtools.BucketConfig
	blocksFrom     string
	inputFile      string
	includeTenants flagext.StringSliceCSV
	excludeTenants flagext.StringSliceCSV
	dryRun         bool
}

func (c *config) registerFlags(f *flag.FlagSet) {
	c.bucketConfig.RegisterFlags(f)
	f.StringVar(&c.blocksFrom, "blocks-from", "", "Accepted values are json, lines, or listing. When listing is provided --input-file is ignored and object storage listings are used to discover tenants and blocks.")
	f.StringVar(&c.inputFile, "input-file", "", "The file path to read when --blocks-from is json or lines, otherwise ignored. The default (\"\") assumes reading from standard input.")
	f.Var(&c.includeTenants, "include-tenants", "A comma separated list of what tenants to target.")
	f.Var(&c.excludeTenants, "exclude-tenants", "A comma separated list of what tenants to ignore. Has precedence over included tenants.")
	f.BoolVar(&c.dryRun, "dry-run", false, "When set the changes that would be made to object storage are only logged rather than performed.")
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
		return errors.Wrap(err, "failed to create bucket")
	}

	blocks, err := getBlocks(ctx, cfg, bucket)
	if err != nil {
		return errors.Wrap(err, "failed to get blocks")
	}

	undeleteBlocks(ctx, bucket, blocks, cfg.dryRun)
	return nil
}

type tenantFilter func(string) bool

func newTenantFilter(cfg config) tenantFilter {
	includeTenants := map[string]struct{}{}
	for _, u := range cfg.includeTenants {
		includeTenants[u] = struct{}{}
	}
	excludeTenants := map[string]struct{}{}
	for _, u := range cfg.excludeTenants {
		excludeTenants[u] = struct{}{}
	}

	return func(tenantID string) bool {
		if len(includeTenants) > 0 {
			if _, ok := includeTenants[tenantID]; !ok {
				return false
			}
		}
		_, ok := excludeTenants[tenantID]
		return !ok
	}
}

func getBlocks(ctx context.Context, cfg config, bucket objtools.Bucket) (map[string][]ulid.ULID, error) {
	tenantFilter := newTenantFilter(cfg)
	switch strings.ToLower(cfg.blocksFrom) {
	case "json":
		return getBlocksFromJSONFile(cfg.inputFile, tenantFilter)
	case "lines":
		return getBlocksFromLinesFile(cfg.inputFile, tenantFilter)
	case "listing":
		return getBlocksFromListing(ctx, bucket, tenantFilter)
	case "":
		return nil, errors.New("providing --blocks-from is required")
	default:
		return nil, errors.Errorf("unrecognized --blocks-from value: %s", cfg.blocksFrom)
	}
}

func getInputFile(filePath string) (*os.File, error) {
	if filePath == "" {
		return os.Stdin, nil
	}
	return os.Open(filePath)
}

// getBlocksFromJSONFile reads a JSON tenant to blockIDs map from the specified file
func getBlocksFromJSONFile(filePath string, filter tenantFilter) (map[string][]ulid.ULID, error) {
	f, err := getInputFile(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	m := make(map[string][]string)
	if err = json.Unmarshal(b, &m); err != nil {
		return nil, err
	}

	for tenant := range m {
		if !filter(tenant) {
			delete(m, tenant)
		}
	}

	m2 := make(map[string][]ulid.ULID)
	for k, v := range m {
		v2 := make([]ulid.ULID, 0, len(v))
		for _, s := range v {
			u, err := ulid.Parse(s)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse a string=%s as a ULID for tenant=%s", s, k)
			}
			v2 = append(v2, u)
		}
		m2[k] = v2
	}

	return m2, nil
}

// getBlocksFromLinesFile reads a file with each line having a tenant and a blockID separated by a space
func getBlocksFromLinesFile(filePath string, filter tenantFilter) (map[string][]ulid.ULID, error) {
	f, err := getInputFile(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m := make(map[string][]ulid.ULID)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		tenant, blockString, found := strings.Cut(line, objtools.Delim)
		if !found {
			return nil, errors.Errorf("no %s separating tenant and block in line formatted file: %s", objtools.Delim, line)
		}
		if !filter(tenant) {
			continue
		}
		u, err := ulid.Parse(blockString)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse a string=%s as a ULID for tenant=%s", blockString, tenant)
		}
		m[tenant] = append(m[tenant], u)
	}
	return m, nil
}

// getBlocksFromListing does a prefixed versioned listing for to find all block prefixes in each unfiltered tenant
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

		blockIDs, err := listBlocksForTenant(ctx, bucket, tenantID)
		if err != nil {
			return nil, errors.Wrapf(err, "failed while listing blocks for tenant %s", tenantID)
		}

		m[tenantID] = blockIDs
	}

	return m, nil
}

func listTenants(ctx context.Context, bkt objtools.Bucket) ([]string, error) {
	result, err := bkt.List(ctx, objtools.ListOptions{
		Versioned: true, // using versioned listing in case a tenant only has deleted objects
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed while listing tenants")
	}

	return result.ToNames(), nil
}

func listBlocksForTenant(ctx context.Context, bkt objtools.Bucket, tenantID string) ([]ulid.ULID, error) {
	result, err := bkt.List(ctx, objtools.ListOptions{
		Prefix:    tenantID,
		Versioned: true, // using versioned listing in case a block only has deleted objects
	})
	if err != nil {
		return nil, err
	}

	result.Objects = nil // avoids bucket index versions and any other object noise
	names, err := result.ToNamesWithoutPrefix(tenantID)
	if err != nil {
		return nil, errors.Wrap(err, "failed while listing for block prefixes")
	}

	blockIDs := make([]ulid.ULID, 0, len(names))

	for _, name := range names {
		blockID, err := ulid.Parse(name)
		if err != nil {
			// block directories will always be a ulid
			continue
		}
		blockIDs = append(blockIDs, blockID)
	}
	return blockIDs, nil
}

func undeleteBlocks(ctx context.Context, bucket objtools.Bucket, blocks map[string][]ulid.ULID, dryRun bool) {
	succeeded, notNeeded, failed := 0, 0, 0
	defer func() {
		slog.Info("undelete operations summary", "succeeded", succeeded, "notNeeded", notNeeded, "failed", failed, "dryRun", dryRun)
	}()

	for tenantID, blockIDs := range blocks {
		for _, blockID := range blockIDs {
			logger := slog.With("tenant", tenantID, "block", blockID)
			if err := ctx.Err(); err != nil {
				logger.Error("context error", "err", err)
				return
			}

			undeleted, err := undeleteBlock(ctx, bucket, tenantID, blockID, logger, dryRun)
			if err != nil {
				failed++
				logger.Error("failed to undelete block", "err", err)
			} else if undeleted {
				succeeded++
				if !dryRun {
					logger.Info("successfully undeleted block")
				}
			} else {
				notNeeded++
				logger.Info("block did not need to be undeleted")
			}
		}
	}
}

type version struct {
	lastModified time.Time
	info         objtools.VersionInfo
}

func undeleteBlock(ctx context.Context, bkt objtools.Bucket, tenantID string, blockID ulid.ULID, logger *slog.Logger, dryRun bool) (bool, error) {
	/*
	 Lifecycle of a block
	 0. Nothing
	 1. Files without meta.json
	 2. Meta.json (block now complete)
	 3. Delete markers added (local delete marker, then global delete marker)
	 4. Files are deleted (meta first, then all except delete markers)
	 5. Delete the delete markers, local then global

	 To undelete a block we are going to restore objects from versions as needed following steps 1-2, then perform step 5.
	*/

	blockPrefix := tenantID + objtools.Delim + blockID.String()
	result, err := bkt.List(ctx, objtools.ListOptions{
		Prefix:    blockPrefix,
		Recursive: true,
		Versioned: true,
	})
	if err != nil {
		logger.Error("failed listing versions")
		return false, err
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
		return false, err
	}

	targets, err := getTargetsInBlock(m, objVersions, blockPrefix)
	if err != nil {
		logger.Error("failed getting target versions to restore")
		return false, err
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

	// avoids making an exists call by using the version listing we already have
	var localDeleteMarkerExists bool
	if markerVersions, ok := objVersions[localDeleteMarkerPath]; ok {
		v, ok := versionToRestore(markerVersions)
		localDeleteMarkerExists = v == nil && ok // "nothing needed to restore" for the block delete marker means it exists
	}

	if !localDeleteMarkerExists && len(targets) == 0 {
		return false, nil
	}

	logger.Info("block can be undeleted")

	for _, target := range targets {
		if dryRun {
			logger.Info("dry run: would restore", "object", target.objectName, "version", target.versionInfo.VersionID)
			continue
		}
		if err := bkt.RestoreVersion(ctx, target.objectName, target.versionInfo); err != nil {
			logger.Error("failed to restore an object version", "object", target.objectName, "version", target.versionInfo.VersionID)
			return false, err
		}
		logger.Info("restored an object version", "object", target.objectName, "version", target.versionInfo.VersionID)
	}

	if localDeleteMarkerExists {
		for _, objectName := range []string{localDeleteMarkerPath, globalDeleteMarkerPath} {
			if dryRun {
				logger.Info("dry run: would delete a delete marker", "object", objectName)
				continue
			}

			if err = bkt.Delete(ctx, objectName, objtools.DeleteOptions{}); err != nil {
				logger.Error("failed to delete a delete marker", "object", objectName)
				return false, err
			}

			logger.Info("delete succeeded", "object", objectName)
		}
	}

	return true, nil
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
		return nil, nil, errors.Wrapf(err, "failed to get metadata at %s with version %s", path, metaVersionID)
	}

	m, err := block.ReadMeta(body)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to decode metadata at %s with version %s", path, metaVersionID)
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

	for _, version := range versions {
		info := version.info
		if info.IsDeleteMarker {
			continue
		} else if info.IsCurrent {
			return nil, true // nothing needs to be restored
		} else if target == nil || version.lastModified.After(target.lastModified) {
			target = &version
		}
	}

	return target, target != nil
}
