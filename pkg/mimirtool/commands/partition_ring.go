// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/ingester"
)

// PartitionRingCommand is the kingpin command for partition ring operations.
type PartitionRingCommand struct{}

// AddPartitionCommand handles the add-partition subcommand.
type AddPartitionCommand struct {
	memberlistJoin         []string
	memberlistClusterLabel string
	memberlistBindPort     int
	partitionIDs           string
	partitionState         string
	verbose                bool
	logger                 log.Logger
	stdin                  io.Reader // For testing; defaults to os.Stdin if nil.
}

// RemovePartitionCommand handles the remove-partition subcommand.
type RemovePartitionCommand struct {
	memberlistJoin         []string
	memberlistClusterLabel string
	memberlistBindPort     int
	partitionIDs           string
	verbose                bool
	logger                 log.Logger
	stdin                  io.Reader // For testing; defaults to os.Stdin if nil.
}

// Register is used to register the command to a parent command.
func (c *PartitionRingCommand) Register(app *kingpin.Application, _ EnvVarNames, logConfig *LoggerConfig) {
	partitionRingCmd := app.Command("partition-ring", "Commands for managing the ingest storage partition ring.")

	// Register add-partition subcommand.
	addCmd := &AddPartitionCommand{}
	addPartitionCmd := partitionRingCmd.Command("add-partition", "Forcefully add a partition to the ingest storage partition ring.").
		Action(func(_ *kingpin.ParseContext) error {
			if addCmd.verbose {
				addCmd.logger = logConfig.Logger()
			} else {
				addCmd.logger = log.NewNopLogger()
			}
			return addCmd.run()
		})

	addPartitionCmd.Flag("partition.state", "The state of the partition to add. Must be one of: pending, active, inactive.").
		Required().
		StringVar(&addCmd.partitionState)

	// Register remove-partition subcommand.
	removeCmd := &RemovePartitionCommand{}
	removePartitionCmd := partitionRingCmd.Command("remove-partition", "Forcefully remove a partition from the ingest storage partition ring.").
		Action(func(_ *kingpin.ParseContext) error {
			if removeCmd.verbose {
				removeCmd.logger = logConfig.Logger()
			} else {
				removeCmd.logger = log.NewNopLogger()
			}
			return removeCmd.run()
		})

	// Register common flags on all subcommands.
	for _, cfg := range []struct {
		cmd                    *kingpin.CmdClause
		partitionIDs           *string
		memberlistJoin         *[]string
		memberlistClusterLabel *string
		memberlistBindPort     *int
		verbose                *bool
	}{
		{addPartitionCmd, &addCmd.partitionIDs, &addCmd.memberlistJoin, &addCmd.memberlistClusterLabel, &addCmd.memberlistBindPort, &addCmd.verbose},
		{removePartitionCmd, &removeCmd.partitionIDs, &removeCmd.memberlistJoin, &removeCmd.memberlistClusterLabel, &removeCmd.memberlistBindPort, &removeCmd.verbose},
	} {
		cfg.cmd.Flag("partition.id", "Comma-separated list of partition IDs (must be >= 0).").
			Required().
			StringVar(cfg.partitionIDs)

		cfg.cmd.Flag("memberlist.join", "Address of a memberlist node to join. Can be specified multiple times.").
			Required().
			StringsVar(cfg.memberlistJoin)

		cfg.cmd.Flag("memberlist.cluster-label", "The cluster label to use when joining the memberlist cluster.").
			Default("").
			StringVar(cfg.memberlistClusterLabel)

		cfg.cmd.Flag("memberlist.bind-port", "Port to listen on for memberlist gossip messages.").
			Default("7946").
			IntVar(cfg.memberlistBindPort)

		cfg.cmd.Flag("verbose", "Enable verbose logging.").
			Default("false").
			BoolVar(cfg.verbose)
	}
}

func (c *AddPartitionCommand) run() error {
	// Parse partition IDs.
	partitionIDs, err := parsePartitionIDs(c.partitionIDs)
	if err != nil {
		return err
	}

	// Parse and validate partition state.
	state, err := parsePartitionState(c.partitionState)
	if err != nil {
		return err
	}

	// Ask for confirmation.
	message := fmt.Sprintf(`WARNING: This is a dangerous operation NOT intended for production systems.
Adding partitions directly to the ring bypasses normal ingester lifecycle.
About to add partition(s) %v with state '%s' to the ring.`, partitionIDs, state.CleanName())
	if err := askForConfirmation(message, c.getStdin()); err != nil {
		return err
	}

	// Use a timeout to avoid hanging indefinitely if memberlist can't join.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Initialize memberlist KV.
	fmt.Fprintln(os.Stderr, "Joining memberlist cluster...")
	kvClient, cleanup, err := initMemberlistKV(ctx, c.memberlistJoin, c.memberlistClusterLabel, c.memberlistBindPort, c.logger)
	if err != nil {
		return errors.Wrap(err, "failed to initialize memberlist KV")
	}
	defer cleanup()
	fmt.Fprintln(os.Stderr, "Successfully joined memberlist cluster.")

	// Perform the CAS operation to add the partitions.
	if err := addPartitions(ctx, kvClient, partitionIDs, state); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Successfully added partition(s) %v with state '%s' to the ring.\n", partitionIDs, state.CleanName())
	return nil
}

func (c *RemovePartitionCommand) run() error {
	// Parse partition IDs.
	partitionIDs, err := parsePartitionIDs(c.partitionIDs)
	if err != nil {
		return err
	}

	// Ask for confirmation.
	message := fmt.Sprintf(`WARNING: This is a dangerous operation NOT intended for production systems.
Removing partitions directly from the ring bypasses normal ingester lifecycle.
About to remove partition(s) %v from the ring.`, partitionIDs)
	if err := askForConfirmation(message, c.getStdin()); err != nil {
		return err
	}

	// Use a timeout to avoid hanging indefinitely if memberlist can't join.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Initialize memberlist KV.
	fmt.Fprintln(os.Stderr, "Joining memberlist cluster...")
	kvClient, cleanup, err := initMemberlistKV(ctx, c.memberlistJoin, c.memberlistClusterLabel, c.memberlistBindPort, c.logger)
	if err != nil {
		return errors.Wrap(err, "failed to initialize memberlist KV")
	}
	defer cleanup()
	fmt.Fprintln(os.Stderr, "Successfully joined memberlist cluster.")

	// Perform the CAS operation to remove the partitions.
	if err := removePartitions(ctx, kvClient, partitionIDs); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Successfully removed partition(s) %v from the ring.\n", partitionIDs)
	return nil
}

func parsePartitionState(stateStr string) (ring.PartitionState, error) {
	switch strings.ToLower(stateStr) {
	case "pending":
		return ring.PartitionPending, nil
	case "active":
		return ring.PartitionActive, nil
	case "inactive":
		return ring.PartitionInactive, nil
	default:
		return ring.PartitionUnknown, fmt.Errorf("invalid partition state %q: must be one of pending, active, inactive", stateStr)
	}
}

// parsePartitionIDs parses a comma-separated list of partition IDs.
func parsePartitionIDs(input string) ([]int32, error) {
	var ids []int32
	for _, s := range strings.Split(input, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		id, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid partition ID %q: %w", s, err)
		}
		if id < 0 {
			return nil, fmt.Errorf("partition ID must be >= 0, got %d", id)
		}
		ids = append(ids, int32(id))
	}
	if len(ids) == 0 {
		return nil, errors.New("at least one partition ID is required")
	}
	return ids, nil
}

func (c *AddPartitionCommand) getStdin() io.Reader {
	if c.stdin != nil {
		return c.stdin
	}
	return os.Stdin
}

func (c *RemovePartitionCommand) getStdin() io.Reader {
	if c.stdin != nil {
		return c.stdin
	}
	return os.Stdin
}

// askForConfirmation prints the given message to stderr and asks the user to type 'yes' to confirm.
// The message can be multiline and should not include a trailing newline.
func askForConfirmation(message string, stdin io.Reader) error {
	fmt.Fprintln(os.Stderr, message)
	fmt.Fprint(os.Stderr, "Type 'yes' to confirm: ")

	reader := bufio.NewReader(stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		return errors.Wrap(err, "failed to read user input")
	}

	// Add an extra newline to make the output look nicer.
	fmt.Fprintln(os.Stderr)

	input = strings.TrimSpace(input)
	if input != "yes" {
		return errors.New("operation cancelled by user")
	}

	return nil
}

func initMemberlistKV(ctx context.Context, joinAddrs []string, clusterLabel string, bindPort int, logger log.Logger) (_ kv.Client, _ func(), returnErr error) {
	// Create memberlist config with defaults.
	cfg := memberlist.KVConfig{}
	flagext.DefaultValues(&cfg)

	// Override with user-provided values.
	cfg.TCPTransport.BindPort = bindPort
	cfg.JoinMembers = joinAddrs
	cfg.ClusterLabel = clusterLabel
	cfg.AbortIfJoinFails = true
	cfg.AbortIfFastJoinFails = true

	// Register codecs for the data stored in memberlist.
	cfg.Codecs = append(cfg.Codecs, ring.GetCodec())
	cfg.Codecs = append(cfg.Codecs, ring.GetPartitionRingCodec())

	// Create DNS provider for memberlist.
	dnsProvider := dns.NewProvider(logger, nil, dns.GolangResolverType)

	// Create and start the memberlist KV service.
	memberlistKV := memberlist.NewKVInitService(&cfg, logger, dnsProvider, nil)
	if err := services.StartAndAwaitRunning(ctx, memberlistKV); err != nil {
		return nil, nil, errors.Wrap(err, "failed to start memberlist KV service")
	}

	cleanup := func() {
		if err := services.StopAndAwaitTerminated(context.Background(), memberlistKV); err != nil {
			level.Warn(logger).Log("msg", "failed to stop memberlist KV service", "err", err)
		}
	}

	// Stop the memberlist KV service if we return with an error.
	defer func() {
		if returnErr != nil {
			cleanup()
		}
	}()

	// Get the memberlist KV.
	mlKV, err := memberlistKV.GetMemberlistKV()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get memberlist KV")
	}

	// Wait for memberlist KV to be running.
	if err := mlKV.AwaitRunning(ctx); err != nil {
		return nil, nil, errors.Wrap(err, "failed to wait for memberlist KV to be running")
	}

	// Create the KV client.
	kvClient, err := memberlist.NewClient(mlKV, ring.GetPartitionRingCodec())
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create memberlist client")
	}

	return kvClient, cleanup, nil
}

func addPartitions(ctx context.Context, kvClient kv.Client, partitionIDs []int32, state ring.PartitionState) error {
	return kvClient.CAS(ctx, ingester.PartitionRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		ringDesc := ring.GetOrCreatePartitionRingDesc(in)

		// First pass: validate ALL partitions before making any changes.
		for _, partitionID := range partitionIDs {
			if ringDesc.HasPartition(partitionID) {
				return nil, false, fmt.Errorf("partition %d already exists in the ring", partitionID)
			}
		}

		// Second pass: add all partitions.
		now := time.Now()
		for _, partitionID := range partitionIDs {
			ringDesc.AddPartition(partitionID, state, now)
		}

		return ringDesc, true, nil
	})
}

func removePartitions(ctx context.Context, kvClient kv.Client, partitionIDs []int32) error {
	return kvClient.CAS(ctx, ingester.PartitionRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		ringDesc := ring.GetOrCreatePartitionRingDesc(in)

		// First pass: validate ALL partitions before making any changes.
		for _, partitionID := range partitionIDs {
			if !ringDesc.HasPartition(partitionID) {
				return nil, false, fmt.Errorf("partition %d does not exist in the ring", partitionID)
			}
			if count := ringDesc.PartitionOwnersCount(partitionID); count > 0 {
				return nil, false, fmt.Errorf("partition %d has %d owner(s), cannot remove", partitionID, count)
			}
		}

		// Second pass: remove all partitions.
		for _, partitionID := range partitionIDs {
			ringDesc.RemovePartition(partitionID)
		}

		return ringDesc, true, nil
	})
}
