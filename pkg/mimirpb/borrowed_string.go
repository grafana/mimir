// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/common/model"
	"golang.org/x/crypto/blake2b"
)

// BorrowedString is a read-only view of string storage owned by a request.
// It must not outlive that request or be used as a persistent map key.
// Clone returns an ordinary string that is safe to retain.
// The zero value is empty. BorrowedString deliberately has no String method.
// Its private representation is used only by audited label conversions in this package.
type BorrowedString struct {
	value string
}

// BorrowString wraps s without copying. The caller must keep its backing storage
// valid and unchanged while the returned value is in use.
func BorrowString(s string) BorrowedString { return BorrowedString{value: s} }

// Clone returns a string with independent storage, safe to retain after release.
func (s BorrowedString) Clone() string { return strings.Clone(s.value) }

// Len returns the length in bytes.
func (s BorrowedString) Len() int { return len(s.value) }

// IsEmpty reports whether s is empty.
func (s BorrowedString) IsEmpty() bool { return s.value == "" }

// Equal reports whether s and other have equal contents.
func (s BorrowedString) Equal(other BorrowedString) bool { return s.value == other.value }

// EqualString reports whether s and other have equal contents.
func (s BorrowedString) EqualString(other string) bool { return s.value == other }

// Compare compares s and other lexicographically.
func (s BorrowedString) Compare(other BorrowedString) int {
	return strings.Compare(s.value, other.value)
}

// CompareString compares s and other lexicographically.
func (s BorrowedString) CompareString(other string) int { return strings.Compare(s.value, other) }

// Slice returns a borrowed byte substring with the same lifetime as s.
// It panics for invalid indexes, like a string slice expression.
func (s BorrowedString) Slice(start, end int) BorrowedString { return BorrowString(s.value[start:end]) }

// AppendTo copies s into dst and returns the extended slice.
func (s BorrowedString) AppendTo(dst []byte) []byte { return append(dst, s.value...) }

// HasPrefix reports whether s starts with prefix.
func (s BorrowedString) HasPrefix(prefix string) bool { return strings.HasPrefix(s.value, prefix) }

// HasSuffix reports whether s ends with suffix.
func (s BorrowedString) HasSuffix(suffix string) bool { return strings.HasSuffix(s.value, suffix) }

// RuneCount returns the number of UTF-8 encoded runes.
func (s BorrowedString) RuneCount() int { return utf8.RuneCountInString(s.value) }

// ValidUTF8 reports whether s contains valid UTF-8.
func (s BorrowedString) ValidUTF8() bool { return utf8.ValidString(s.value) }

// IsValidLabelName validates s using the selected Prometheus validation scheme.
func (s BorrowedString) IsValidLabelName(scheme model.ValidationScheme) bool {
	return scheme.IsValidLabelName(s.value)
}

// IsValidMetricName validates s using the selected Prometheus validation scheme.
func (s BorrowedString) IsValidMetricName(scheme model.ValidationScheme) bool {
	return scheme.IsValidMetricName(s.value)
}

// InStringSet reports whether s is present without allocating a persistent key.
func (s BorrowedString) InStringSet(set map[string]struct{}) bool {
	_, ok := set[s.value]
	return ok
}

// IsValidLabelValue reports whether s is a valid Prometheus label value.
func (s BorrowedString) IsValidLabelValue() bool { return model.LabelValue(s.value).IsValid() }

// Hash returns the xxhash64 of s without allocating.
func (s BorrowedString) Hash() uint64 { return xxhash.Sum64String(s.value) }

// MarshalJSON writes s as a JSON string into independently owned bytes.
func (s BorrowedString) MarshalJSON() ([]byte, error) { return json.Marshal(s.value) }

// UnmarshalJSON decodes an independently owned JSON string.
func (s *BorrowedString) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &s.value)
}

// UnsafeHashLabelValueInto replaces the beginning of dst with the hexadecimal
// BLAKE2b-256 hash of src, enclosed in "(hash:" and ")". The returned view contains
// only the replacement. dst must reference writable, exclusively request-owned
// storage, not an immutable Go string. src may overlap dst: it is hashed first.
// Neither the source nor destination storage may be accessed concurrently.
func UnsafeHashLabelValueInto(dst, src BorrowedString) BorrowedString {
	const size = len("(hash:") + blake2b.Size256*2 + len(")")
	if len(dst.value) < size {
		panic("hash destination is shorter than the label hash")
	}
	hash := blake2b.Sum256(unsafe.Slice(unsafe.StringData(src.value), len(src.value))) //nolint:gosec // Read-only request view.
	buf := unsafe.Slice(unsafe.StringData(dst.value), size)                            //nolint:gosec // Explicit writable request-storage contract.
	n := copy(buf, "(hash:")
	n += hex.Encode(buf[n:], hash[:])
	buf[n] = ')'
	return dst.Slice(0, size)
}
