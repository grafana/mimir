package memberlist

import (
	"context"
)

type DNSProvider interface {
	// Resolve stores a list of provided addresses or their DNS records if requested.
	// Addresses prefixed with `dns+` or `dnssrv+` will be resolved through respective DNS lookup (A/AAAA or SRV).
	// For non-SRV records, it will return an error if a port is not supplied.
	Resolve(ctx context.Context, addrs []string) error

	// Addresses returns the latest addresses present in the DNSProvider.
	Addresses() []string
}
