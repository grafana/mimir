package automemlimit

import (
	"log/slog"

	"github.com/KimMachineGun/automemlimit/memlimit"
)

func init() {
	memlimit.SetGoMemLimitWithOpts(
		memlimit.WithLogger(slog.Default()),
	)
}
