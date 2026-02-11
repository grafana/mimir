package tenant

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/grafana/dskit/user"
)

const (
	// MaxTenantIDLength is the max length of single tenant ID in bytes
	MaxTenantIDLength = 150
	MaxMetadataLength = 64

	tenantIDsSeparator = '|'

	// metadataSeparator separates the tenant ID from the metadata.
	// The format is "tenantID:key=value" (e.g., "123456:product=k6").
	// The colon is not a valid character in tenant IDs, making it safe to use as a separator.
	metadataSeparator = ':'
)

var (
	// validTenantIdChars is a lookup table for valid tenant ID characters.
	validTenantIdChars [256]bool
	validMetadataChars [256]bool

	errTenantIDTooLong = fmt.Errorf("tenant ID is too long: max %d characters", MaxTenantIDLength)
	errUnsafeTenantID  = errors.New("tenant ID is '.' or '..'")
)

func init() {
	// letters
	for c := 'a'; c <= 'z'; c++ {
		validTenantIdChars[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		validTenantIdChars[c] = true
	}
	// digits
	for c := '0'; c <= '9'; c++ {
		validTenantIdChars[c] = true
	}
	// special characters: ! - _ . * ' ( )
	for _, c := range "!-_.*'()" {
		validTenantIdChars[c] = true
	}

	// Metadata:
	for _, c := range "-=" {
		validMetadataChars[c] = true
	}
	for c := 'a'; c <= 'z'; c++ {
		validMetadataChars[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		validMetadataChars[c] = true
	}
}

type Metadata struct {
	Key   string
	Value string
}

type errTenantIDUnsupportedCharacter struct {
	pos      int
	tenantID string
}

func (e *errTenantIDUnsupportedCharacter) Error() string {
	return fmt.Sprintf(
		"tenant ID '%s' contains unsupported character '%c'",
		e.tenantID,
		e.tenantID[e.pos],
	)
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

// NormalizeTenantIDs creates a normalized form by sorting and de-duplicating the list of tenantIDs
func NormalizeTenantIDs(tenantIDs []string) []string {
	sort.Strings(tenantIDs)

	count := len(tenantIDs)
	if count <= 1 {
		return tenantIDs
	}

	posOut := 1
	for posIn := 1; posIn < count; posIn++ {
		if tenantIDs[posIn] != tenantIDs[posIn-1] {
			tenantIDs[posOut] = tenantIDs[posIn]
			posOut++
		}
	}

	return tenantIDs[0:posOut]
}

// ValidTenantID returns an error if the tenant ID is invalid, nil otherwise.
func ValidTenantID(s string) error {
	for i := 0; i < len(s); i++ {
		if !validTenantIdChars[s[i]] {
			return &errTenantIDUnsupportedCharacter{tenantID: s, pos: i}
		}
	}
	if len(s) > MaxTenantIDLength {
		return errTenantIDTooLong
	}
	if s == "." || s == ".." {
		return errUnsafeTenantID
	}
	return nil
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

// JoinTenantIDs returns all tenant IDs concatenated with the separator character `|`
func JoinTenantIDs(tenantIDs []string) string {
	return strings.Join(tenantIDs, string(tenantIDsSeparator))
}

// ExtractTenantIDFromHTTPRequest extracts a single tenant ID directly from a HTTP request.
func ExtractTenantIDFromHTTPRequest(req *http.Request) (string, context.Context, error) {
	//lint:ignore faillint wrapper around upstream method
	_, ctx, err := user.ExtractOrgIDFromHTTPRequest(req)
	if err != nil {
		return "", nil, err
	}

	tenantID, err := TenantID(ctx)
	if err != nil {
		return "", nil, err
	}

	return tenantID, ctx, nil
}

// TenantIDsFromOrgID extracts different tenants from an orgID string value
//
// ignore stutter warning
//
//nolint:revive
func TenantIDsFromOrgID(orgID string) ([]string, error) {
	return TenantIDs(user.InjectOrgID(context.TODO(), orgID))
}

func trimMetadata(orgID string) string {
	idx := strings.IndexByte(orgID, metadataSeparator)
	if idx == -1 {
		return orgID
	}
	return orgID[:idx]
}

// splitTenantAndMetadata splits an orgID into tenant ID and metadata.
// If the orgID contains no metadata separator, the metadata string will be empty.
// The format is "tenantID:key=value" (e.g., "123456:product=k6").
func splitTenantAndMetadata(orgID string) (tenantID, metadata string) {
	idx := strings.IndexByte(orgID, metadataSeparator)
	if idx == -1 {
		return orgID, ""
	}
	return orgID[:idx], orgID[idx+1:]
}

// stringsCut is like strings.Cut but uses strings.IndexByte instead.
func stringsCut(s string, sep byte) (string, string, bool) {
	idx := strings.IndexByte(s, sep)
	if idx == -1 {
		return s, "", false
	}
	return s[:idx], s[idx+1:], true
}
