// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"github.com/alecthomas/kingpin/v2"

	blockscopy "github.com/grafana/mimir/pkg/mimirtool/commands/blocks/copy"
	blockslist "github.com/grafana/mimir/pkg/mimirtool/commands/blocks/list"
	blocksmark "github.com/grafana/mimir/pkg/mimirtool/commands/blocks/mark"
	blockssplit "github.com/grafana/mimir/pkg/mimirtool/commands/blocks/split"
	blocksundelete "github.com/grafana/mimir/pkg/mimirtool/commands/blocks/undelete"
)

type BlocksCommand struct {
	mark     blocksmark.Command
	list     blockslist.Command
	copy     blockscopy.Command
	split    blockssplit.Command
	undelete blocksundelete.Command
}

func (c *BlocksCommand) Register(app *kingpin.Application, _ EnvVarNames, logConfig *LoggerConfig) {
	blocksCmd := app.Command("blocks", "Manage TSDB blocks in object storage.")

	c.mark.Register(blocksCmd, logConfig.Logger)
	c.list.Register(blocksCmd, logConfig.Logger)
	c.copy.Register(blocksCmd, logConfig.Logger)
	c.split.Register(blocksCmd, logConfig.Logger)
	c.undelete.Register(blocksCmd, logConfig.Logger)
}
