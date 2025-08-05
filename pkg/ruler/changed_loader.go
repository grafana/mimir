package ruler

import (
	"crypto/sha256"
	"fmt"
	"os"

	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql/parser"
)

// ChangedFileLoader is a stateful GroupLoader that only returns groups that changed since the last call.
type ChangedFileLoader struct {
	checksums map[string][32]byte
}

func (c ChangedFileLoader) Load(identifier string, ignoreUnknownFields bool) (*rulefmt.RuleGroups, []error) {
	// inlined rulefmt.ParseFile with modifications.
	// We can't just wrap a regular FileLoader, because the FileLoader already does a layer of parsing that might be wasted effort.
	// Instead we want to just skip if the file hasn't changed.
	b, err := os.ReadFile(identifier)
	if err != nil {
		return nil, []error{fmt.Errorf("%s: %w", identifier, err)}
	}
	checksum := sha256.Sum256(b)
	if latestChecksum, ok := c.checksums[identifier]; ok && checksum == latestChecksum {
		return nil, nil
	}

	rgs, errs := rulefmt.Parse(b, rulefmt.WithIgnoreUnknownFields(ignoreUnknownFields))
	for i := range errs {
		errs[i] = fmt.Errorf("%s: %w", identifier, errs[i])
	}
	return rgs, errs
}

func (c ChangedFileLoader) Parse(query string) (parser.Expr, error) {
	return parser.ParseExpr(query)
}
