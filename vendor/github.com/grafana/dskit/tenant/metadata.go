package tenant

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

const (
	// metadataSeparator separates the tenant ID from the metadata. The format is
	// "tenantID:key=value" (e.g. "123456:product=k6"). The colon is not a valid
	// character in tenant IDs, making it safe to use as a separator.
	metadataSeparator = ':'
	// metadataKVSeparator separates individual key value pairs of metadata. The
	// format is "key=value" (e.g., "source=test"). The equal sign is not a valid
	// character in tenant IDs, making it safe to use as a separator.
	metadataKVSeparator = '='
)

var validMetadataChars [256]bool

func init() {
	validMetadataChars[metadataSeparator] = true
	validMetadataChars[metadataKVSeparator] = true

	for c := 'a'; c <= 'z'; c++ {
		validMetadataChars[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		validTenantIdChars[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		validMetadataChars[c] = true
	}
	for _, c := range "-_" {
		validMetadataChars[c] = true
	}
}

type errMetadataUnsupportedCharacter struct {
	pos      int
	metadata string
}

func (e *errMetadataUnsupportedCharacter) Error() string {
	return fmt.Sprintf(
		"metadata '%s' contains unsupported character '%c'",
		e.metadata,
		e.metadata[e.pos],
	)
}

type Metadata struct {
	data map[string]string
}

func NewMetadata() Metadata {
	return Metadata{}
}

// ParseMetadata from string without validating input. ValidMetadata must be used
// to ensure input contains only allowed characters. The format for metadata is a
// colon-delimited list of key value pairs.
// Examples:
// - key=value
// - key=value:foo=bar
func ParseMetadata(input string) (Metadata, error) {
	if input == "" {
		return Metadata{}, nil
	}
	var (
		currentPair  string
		hasMorePairs bool
		remaining    = input
		metadata     = NewMetadata()
	)
	for {
		currentPair, remaining, hasMorePairs = stringsCut(remaining, metadataSeparator)
		key, val, ok := stringsCut(currentPair, metadataKVSeparator)
		if !ok {
			return Metadata{}, fmt.Errorf("invalid key value pair %s", currentPair)
		}
		metadata.Set(key, val)
		if !hasMorePairs {
			break
		}
	}
	return metadata, nil
}

// ValidMetadata returns an error if the metadata is invalid, nil otherwise.
// Metadata must contain only lowercase letters, digits, '-', and '='.
func ValidMetadata(s string) error {
	for i := 0; i < len(s); i++ {
		if !validMetadataChars[s[i]] {
			return &errMetadataUnsupportedCharacter{metadata: s, pos: i}
		}
	}
	if len(s) > MaxMetadataLength {
		return fmt.Errorf("metadata too long: %d", len(s))
	}
	return nil
}

// Set a key value pair.
func (m *Metadata) Set(key string, val string) {
	if m.data == nil {
		m.data = make(map[string]string)
	}
	m.data[key] = val
}

// Has checks whether a specific metadata key is present.
func (m Metadata) Has(key string) bool {
	if m.data == nil {
		return false
	}
	_, ok := m.data[key]
	return ok
}

// Get the value set for key.
func (m Metadata) Get(key string) (string, bool) {
	if m.data == nil {
		return "", false
	}
	val, ok := m.data[key]
	return val, ok
}

// WithTenant encodes the metadata as a tenant-prefixed string.
// The format is "tenantID:key1=val1:key2=val2" with keys sorted alphabetically.
func (m Metadata) WithTenant(tenantID string) string {
	var sb strings.Builder
	sb.WriteString(tenantID)
	for _, k := range slices.Sorted(maps.Keys(m.data)) {
		sb.WriteRune(metadataSeparator)
		sb.WriteString(k)
		sb.WriteRune(metadataKVSeparator)
		sb.WriteString(m.data[k])
	}
	return sb.String()
}
