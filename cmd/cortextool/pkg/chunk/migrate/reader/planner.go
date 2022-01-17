package reader

import (
	"fmt"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/cmd/cortextool/pkg/chunk"
)

// PlannerConfig is used to configure the Planner
type PlannerConfig struct {
	FirstShard int
	LastShard  int
	UserIDList string
	Tables     string
}

// Notes on Planned Shards
// #######################
// When doing migrations each database is discreetly partitioned into 240 shards
// based on aspects of the databases underlying implementation. 240 was chosen due
// to the bigtable implementation sharding on the first two character of the hex encoded
// metric fingerprint. Cassandra is encoded into 240 discreet shards using the Murmur3
// partition tokens.
//
// Shards are an integer between 0 and 240 that map onto 2 hex characters.
// For Example:
// 			Shard | Prefix
//			    1 | 10
//			    2 | 11
//			  ... | ...
//			   16 |
//			  240 | ff
//
// Technically there are 256 combinations of 2 hex character (16^2). However,
// fingerprints will not lead with a 0 character so 00->0f excluded, leading to
// 240

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *PlannerConfig) Register(cmd *kingpin.CmdClause) {
	cmd.Flag("plan.firstShard", "first shard in range of shards to be migrated (1-240)").Default("1").IntVar(&cfg.FirstShard)
	cmd.Flag("plan.lastShard", "last shard in range of shards to be migrated (1-240)").Default("240").IntVar(&cfg.LastShard)
	cmd.Flag("plan.users", "comma separated list of user ids, if empty all users will be queried").StringVar(&cfg.UserIDList)
	cmd.Flag("plan.tables", "comma separated list of tables to migrate").StringVar(&cfg.Tables)
}

// Planner plans the queries required for the migrations
type Planner struct {
	firstShard int
	lastShard  int
	tables     []string
	users      []string
}

// NewPlanner returns a new planner struct
func NewPlanner(cfg PlannerConfig) (*Planner, error) {
	if cfg.FirstShard < 1 || cfg.FirstShard > 240 {
		return &Planner{}, fmt.Errorf("plan.firstShard set to %v, must be in range 1-240", cfg.FirstShard)
	}
	if cfg.LastShard < 1 || cfg.LastShard > 240 {
		return &Planner{}, fmt.Errorf("plan.lastShard set to %v, must be in range 1-240", cfg.LastShard)
	}
	if cfg.FirstShard > cfg.LastShard {
		return &Planner{}, fmt.Errorf("plan.lastShard (%v) is set to less than plan.from (%v)", cfg.LastShard, cfg.FirstShard)
	}

	userList := strings.Split(cfg.UserIDList, ",")
	tableList := strings.Split(cfg.Tables, ",")
	return &Planner{
		firstShard: cfg.FirstShard,
		lastShard:  cfg.LastShard,
		users:      userList,
		tables:     tableList,
	}, nil
}

// Plan updates a Streamer with the correct queries for the planned migration
func (p Planner) Plan() []chunk.ScanRequest {
	reqs := []chunk.ScanRequest{}
	for _, table := range p.tables {
		for _, user := range p.users {
			for shard := p.firstShard; shard <= p.lastShard; shard++ {
				reqs = append(reqs, chunk.ScanRequest{
					Table:  table,
					User:   user,
					Prefix: fmt.Sprintf("%02x", shard+15),
				})
			}
		}
	}
	return reqs
}
