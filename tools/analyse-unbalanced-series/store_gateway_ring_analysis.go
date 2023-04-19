package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/dgryski/go-shardedkv/choosers/jump"
	"github.com/dgryski/go-shardedkv/choosers/maglev"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"golang.org/x/exp/slices"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

func analyzeStoreGatewayActualBlocksOwnership(blocks bucketindex.Blocks, ringDesc *ring.Desc, logger log.Logger) error {
	const (
		replicationFactor    = 3
		zoneAwarenessEnabled = true
	)

	var (
		ringTokens          = ringDesc.GetTokens()
		ringInstanceByToken = getRingInstanceByToken(ringDesc)

		bufDescs [ring.GetBufferSize]string
		bufHosts [ring.GetBufferSize]string
		bufZones [ring.GetBufferSize]string
	)

	result, err := getStoreGatewayActualBlocksOwnershipWithShardFunction(blocks, func(blockID ulid.ULID) ([]string, error) {
		key := mimir_tsdb.HashBlockID(blockID)
		return ringGet(key, ringDesc, ringTokens, ringInstanceByToken, ring.WriteNoExtend, replicationFactor, zoneAwarenessEnabled, bufDescs[:0], bufHosts[:0], bufZones[:0])
	})

	if err != nil {
		return err
	}

	w := newCSVWriter[instanceOwnership]()
	w.setHeader([]string{"pod", fmt.Sprintf("Blocks actual ownership zone-aware=%s RF=%d", formatEnabled(zoneAwarenessEnabled), replicationFactor)})
	w.setData(result, func(entry instanceOwnership) []string {
		// To make the percentage easy to compare with different RFs, we divide it by the RF.
		return []string{entry.id, fmt.Sprintf("%.3f", entry.percentage/float64(replicationFactor))}
	})
	if err := w.writeCSV(fmt.Sprintf("store-gateway-blocks-actual-ownership-with-zone-aware-%s-and-rf-%d.csv", formatEnabled(zoneAwarenessEnabled), replicationFactor)); err != nil {
		return err
	}

	return nil
}

func analyzeStoreGatewaySimulateBlocksOwnershipWithJumpHash(blocks bucketindex.Blocks, ringDesc *ring.Desc, replicationFactor int, logger log.Logger) error {
	const (
		// TODO make it zone-aware
		zoneAwarenessEnabled = false
	)

	// Get the list of instances.
	instanceIDs := make([]string, 0, len(ringDesc.Ingesters))
	for instanceID := range ringDesc.Ingesters {
		instanceIDs = append(instanceIDs, instanceID)
	}
	slices.Sort(instanceIDs)

	chooser := jump.New(xxhash.Sum64)
	chooser.SetBuckets(instanceIDs)

	// Compute the ownership.
	result, err := getStoreGatewayActualBlocksOwnershipWithShardFunction(blocks, func(blockID ulid.ULID) ([]string, error) {
		return chooser.ChooseReplicas(blockID.String(), replicationFactor), nil
	})

	if err != nil {
		return err
	}

	w := newCSVWriter[instanceOwnership]()
	w.setHeader([]string{"pod", fmt.Sprintf("Blocks simulated ownership with jump hash zone-aware=%s RF=%d", formatEnabled(zoneAwarenessEnabled), replicationFactor)})
	w.setData(result, func(entry instanceOwnership) []string {
		// To make the percentage easy to compare with different RFs, we divide it by the RF.
		return []string{entry.id, fmt.Sprintf("%.3f", entry.percentage/float64(replicationFactor))}
	})
	if err := w.writeCSV(fmt.Sprintf("store-gateway-blocks-simulated-ownership-with-jump-hash-and-zone-aware-%s-and-rf-%d.csv", formatEnabled(zoneAwarenessEnabled), replicationFactor)); err != nil {
		return err
	}

	return nil
}

func analyzeStoreGatewaySimulateBlocksOwnershipWithMaglev(blocks bucketindex.Blocks, ringDesc *ring.Desc, replicationFactor int, logger log.Logger) error {
	// TODO currently support only RF=1
	if replicationFactor != 1 {
		return errors.New("unsupported replication factor")
	}

	const (
		// TODO make it zone-aware
		zoneAwarenessEnabled = false
	)

	// Get the list of instances.
	instanceIDs := make([]string, 0, len(ringDesc.Ingesters))
	for instanceID := range ringDesc.Ingesters {
		instanceIDs = append(instanceIDs, instanceID)
	}
	slices.Sort(instanceIDs)

	chooser := maglev.New()
	chooser.SetBuckets(instanceIDs)

	// Compute the ownership.
	result, err := getStoreGatewayActualBlocksOwnershipWithShardFunction(blocks, func(blockID ulid.ULID) ([]string, error) {
		key := blockID.String()

		return []string{chooser.Choose(key)}, nil
	})

	if err != nil {
		return err
	}

	w := newCSVWriter[instanceOwnership]()
	w.setHeader([]string{"pod", fmt.Sprintf("Blocks simulated ownership with maglev zone-aware=%s RF=%d", formatEnabled(zoneAwarenessEnabled), replicationFactor)})
	w.setData(result, func(entry instanceOwnership) []string {
		// To make the percentage easy to compare with different RFs, we divide it by the RF.
		return []string{entry.id, fmt.Sprintf("%.3f", entry.percentage/float64(replicationFactor))}
	})
	if err := w.writeCSV(fmt.Sprintf("store-gateway-blocks-simulated-ownership-with-maglev-and-zone-aware-%s-and-rf-%d.csv", formatEnabled(zoneAwarenessEnabled), replicationFactor)); err != nil {
		return err
	}

	return nil
}

func getStoreGatewayActualBlocksOwnershipWithShardFunction(blocks bucketindex.Blocks, shardFunc func(blockID ulid.ULID) ([]string, error)) ([]instanceOwnership, error) {
	ownedBlocks := map[string]int{}

	for _, block := range blocks {
		instanceIDs, err := shardFunc(block.ID)
		if err != nil {
			return nil, err
		}

		for _, instanceID := range instanceIDs {
			ownedBlocks[instanceID]++
		}
	}

	// Compute the per-instance % of owned blocks.
	var result []instanceOwnership

	for id, numBlocks := range ownedBlocks {
		result = append(result, instanceOwnership{
			id:         id,
			percentage: (float64(numBlocks) / float64(len(blocks))) * 100,
		})
	}

	slices.SortFunc(result, func(a, b instanceOwnership) bool {
		return a.id < b.id
	})

	return result, nil
}

func readBucketIndex(bucketIndexFilepath string, logger log.Logger) (*bucketindex.Index, error) {
	content, err := os.ReadFile(bucketIndexFilepath)
	if err != nil {
		return nil, err
	}

	// Read all the content.
	gzipReader, err := gzip.NewReader(strings.NewReader(string(content)))
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithLogOnErr(logger, gzipReader, "close bucket index gzip reader")

	// Deserialize it.
	index := &bucketindex.Index{}
	d := json.NewDecoder(gzipReader)
	if err := d.Decode(index); err != nil {
		return nil, err
	}

	return index, nil
}
