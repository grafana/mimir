// SPDX-License-Identifier: AGPL-3.0-only

package httpgrpcpb

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/stretchr/testify/require"
)

// This file guards httpgrpcpb.proto against silently drifting from the vendored
// dskit httpgrpc.proto it shadows.
//
// httpgrpcpb.proto re-declares dskit's HTTPRequest/HTTPResponse/Header messages using
// wiresmith instead of gogoproto, because wiresmith-generated code cannot embed message
// types from another protobuf runtime (see convert.go and httpgrpcpb.proto's own doc
// comment). Deleting the shadow in favor of embedding was investigated and found
// infeasible, because HTTPRequest is a oneof variant of scheduler.proto and wiresmith
// rejects customtype on oneof variants.
//
// The two files are compiled independently by two different protobuf runtimes, so
// nothing else notices if a future `go mod vendor` bump changes dskit's httpgrpc.proto
// (adds/removes/renames/retypes a field or message) without a matching update here.
// TestHTTPGRPCPBMatchesVendoredDskitSchema catches that via a normalized text diff of
// the two .proto files. TestHTTPGRPCPBWireRoundTrip additionally proves that, as they
// stand today, the two runtimes actually produce and consume identical bytes on the
// wire in both directions.

// trackedProtoMessages lists the message names httpgrpcpb.proto's doc comment
// identifies as "wire-identical copies" of the corresponding dskit messages. Messages
// outside this list, in either file, are not compared: httpgrpcpb.proto only ever
// needs to shadow the messages actually referenced from wiresmith-migrated protos.
var trackedProtoMessages = []string{"HTTPRequest", "HTTPResponse", "Header"}

const (
	vendoredDskitProtoPath = "../../../vendor/github.com/grafana/dskit/httpgrpc/httpgrpc.proto"
	localShadowProtoPath   = "httpgrpcpb.proto"
)

const regenInstructions = "re-sync pkg/util/httpgrpcpb/httpgrpcpb.proto's message definitions from " +
	"vendor/github.com/grafana/dskit/httpgrpc/httpgrpc.proto, regenerate with `./tools/wiresmith-regen.sh cqa3` " +
	"(or `make protos`), and reconcile convert.go and its call sites with any shape change."

// TestHTTPGRPCPBMatchesVendoredDskitSchema fails if the vendored dskit httpgrpc.proto
// declares a tracked message with different fields (name, number, type, or
// repeated-ness) than the local httpgrpcpb.proto shadow.
func TestHTTPGRPCPBMatchesVendoredDskitSchema(t *testing.T) {
	dskitSrc, err := os.ReadFile(vendoredDskitProtoPath)
	require.NoErrorf(t, err, "read vendored dskit proto at %s (relative to pkg/util/httpgrpcpb/)", vendoredDskitProtoPath)

	shadowSrc, err := os.ReadFile(localShadowProtoPath)
	require.NoErrorf(t, err, "read local shadow proto at %s", localShadowProtoPath)

	dskitSchema, err := parseProtoSchema(string(dskitSrc))
	require.NoErrorf(t, err, "parse %s", vendoredDskitProtoPath)

	shadowSchema, err := parseProtoSchema(string(shadowSrc))
	require.NoErrorf(t, err, "parse %s", localShadowProtoPath)

	for _, name := range trackedProtoMessages {
		dskitFields, ok := dskitSchema.messages[name]
		require.Truef(t, ok,
			"vendor/github.com/grafana/dskit/httpgrpc/httpgrpc.proto no longer declares message %q. "+
				"This is a breaking upstream rename/removal, not routine drift -- investigate the dskit "+
				"change first, then %s", name, regenInstructions)

		shadowFields, ok := shadowSchema.messages[name]
		require.Truef(t, ok,
			"pkg/util/httpgrpcpb/httpgrpcpb.proto no longer declares message %q, but the vendored "+
				"dskit httpgrpc.proto still does. %s", name, regenInstructions)

		if diff := cmp.Diff(dskitFields, shadowFields); diff != "" {
			t.Errorf(
				"message %s in pkg/util/httpgrpcpb/httpgrpcpb.proto has drifted from the vendored "+
					"vendor/github.com/grafana/dskit/httpgrpc/httpgrpc.proto "+
					"(-vendored dskit fields by number, +local shadow fields by number):\n%s\n%s",
				name, diff, regenInstructions)
		}
	}
}

// TestHTTPGRPCPBWireRoundTrip proves that, as they stand today, marshaling with one
// protobuf runtime and unmarshaling with the other reproduces the original message for
// every field of HTTPRequest and HTTPResponse (including nested Header slices and raw
// body bytes), in both directions. This does not by itself catch every possible drift
// (e.g. a newly added upstream field, with no counterpart to diff against, marshals and
// unmarshals silently as an unrecognized/dropped field on either side) -- that's what
// TestHTTPGRPCPBMatchesVendoredDskitSchema is for. This test instead guards against the
// two schemas agreeing on paper while actually encoding/decoding differently.
func TestHTTPGRPCPBWireRoundTrip(t *testing.T) {
	t.Run("HTTPRequest gogo-marshal wiresmith-unmarshal", func(t *testing.T) {
		b, err := sampleDskitRequest().Marshal()
		require.NoError(t, err)

		got := &HTTPRequest{}
		require.NoError(t, got.Unmarshal(b))

		if diff := cmp.Diff(sampleWiresmithRequest(), got); diff != "" {
			t.Errorf("HTTPRequest marshaled by the vendored dskit gogo runtime does not unmarshal "+
				"identically via the wiresmith pkg/util/httpgrpcpb shadow (-want, +got):\n%s\n%s", diff, regenInstructions)
		}
	})

	t.Run("HTTPRequest wiresmith-marshal gogo-unmarshal", func(t *testing.T) {
		b, err := sampleWiresmithRequest().Marshal()
		require.NoError(t, err)

		got := &httpgrpc.HTTPRequest{}
		require.NoError(t, got.Unmarshal(b))

		if diff := cmp.Diff(sampleDskitRequest(), got); diff != "" {
			t.Errorf("HTTPRequest marshaled by the wiresmith pkg/util/httpgrpcpb shadow does not unmarshal "+
				"identically via the vendored dskit gogo runtime (-want, +got):\n%s\n%s", diff, regenInstructions)
		}
	})

	t.Run("HTTPResponse gogo-marshal wiresmith-unmarshal", func(t *testing.T) {
		b, err := sampleDskitResponse().Marshal()
		require.NoError(t, err)

		got := &HTTPResponse{}
		require.NoError(t, got.Unmarshal(b))

		if diff := cmp.Diff(sampleWiresmithResponse(), got); diff != "" {
			t.Errorf("HTTPResponse marshaled by the vendored dskit gogo runtime does not unmarshal "+
				"identically via the wiresmith pkg/util/httpgrpcpb shadow (-want, +got):\n%s\n%s", diff, regenInstructions)
		}
	})

	t.Run("HTTPResponse wiresmith-marshal gogo-unmarshal", func(t *testing.T) {
		b, err := sampleWiresmithResponse().Marshal()
		require.NoError(t, err)

		got := &httpgrpc.HTTPResponse{}
		require.NoError(t, got.Unmarshal(b))

		if diff := cmp.Diff(sampleDskitResponse(), got); diff != "" {
			t.Errorf("HTTPResponse marshaled by the wiresmith pkg/util/httpgrpcpb shadow does not unmarshal "+
				"identically via the vendored dskit gogo runtime (-want, +got):\n%s\n%s", diff, regenInstructions)
		}
	})
}

// sampleDskitRequest and sampleWiresmithRequest (and the HTTPResponse pair below) build
// field-for-field equivalent values in both runtimes' Go types, with every field set
// including a multi-entry Headers slice (itself multi-value) and non-empty Body bytes.

func sampleDskitRequest() *httpgrpc.HTTPRequest {
	return &httpgrpc.HTTPRequest{
		Method: "POST",
		Url:    "/api/v1/push?tenant=fake",
		Headers: []*httpgrpc.Header{
			{Key: "Content-Type", Values: []string{"application/x-protobuf"}},
			{Key: "X-Scope-OrgID", Values: []string{"tenant-a", "tenant-b"}},
		},
		Body: []byte{0x00, 0x01, 0xff, 'h', 'i'},
	}
}

func sampleWiresmithRequest() *HTTPRequest {
	return &HTTPRequest{
		Method: "POST",
		Url:    "/api/v1/push?tenant=fake",
		Headers: []*Header{
			{Key: "Content-Type", Values: []string{"application/x-protobuf"}},
			{Key: "X-Scope-OrgID", Values: []string{"tenant-a", "tenant-b"}},
		},
		Body: []byte{0x00, 0x01, 0xff, 'h', 'i'},
	}
}

func sampleDskitResponse() *httpgrpc.HTTPResponse {
	return &httpgrpc.HTTPResponse{
		Code: 503,
		Headers: []*httpgrpc.Header{
			{Key: "Retry-After", Values: []string{"5"}},
		},
		Body: []byte(`{"status":"error","error":"too many requests"}`),
	}
}

func sampleWiresmithResponse() *HTTPResponse {
	return &HTTPResponse{
		Code: 503,
		Headers: []*Header{
			{Key: "Retry-After", Values: []string{"5"}},
		},
		Body: []byte(`{"status":"error","error":"too many requests"}`),
	}
}

// protoField is one message field's shape, as read from a .proto source file: enough
// to detect wire-relevant drift (field number/type/repeated-ness) as well as
// source-level drift (a rename) that doesn't affect the wire but does mean the shadow
// no longer faithfully mirrors dskit's definition.
type protoField struct {
	Type     string
	Name     string
	Repeated bool
}

// protoSchema is the result of parsing a .proto file down to its message fields,
// keyed by message name and then by field number (so declaration order doesn't
// matter for comparison).
type protoSchema struct {
	messages map[string]map[int]protoField
}

const (
	protoParseStateTop = iota
	protoParseStateMessage
	protoParseStateOther
)

var (
	// syntax/package/import/option lines legitimately differ between the gogoproto
	// original and the wiresmith shadow (different package names, different
	// annotation frameworks) and carry no wire-shape information.
	protoPreambleRe = regexp.MustCompile(`^(syntax\s*=|package\s|import\s|option\s)`)

	protoMessageOpenRe = regexp.MustCompile(`^message\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{$`)
	protoServiceOpenRe = regexp.MustCompile(`^service\s+[A-Za-z_][A-Za-z0-9_]*\s*\{`)

	// Matches a proto3 scalar/message field declaration, e.g.:
	//   string method = 1;
	//   repeated Header headers = 3 [(wiresmith.options.pointer) = true];
	// The trailing bracketed field options (wiresmith- or gogo-specific annotations)
	// are intentionally not captured: they control per-runtime codegen, not wire shape.
	protoFieldRe = regexp.MustCompile(`^(repeated\s+)?([A-Za-z_][A-Za-z0-9_.]*)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)\s*(\[[^\]]*\])?;$`)
)

// parseProtoSchema extracts message field shapes from a flat proto3 file containing
// only top-level messages and (optionally) a service block -- which is all that either
// httpgrpc.proto or httpgrpcpb.proto contain today. It deliberately fails closed: any
// construct it doesn't recognize (nested messages, oneofs, maps, enums, reserved
// statements, multiple services, ...) is a parse error rather than a silent skip, since
// silently skipping is exactly the failure mode this guard exists to prevent.
func parseProtoSchema(src string) (*protoSchema, error) {
	schema := &protoSchema{messages: map[string]map[int]protoField{}}

	state := protoParseStateTop
	otherDepth := 0
	currentMessage := ""

	for i, raw := range strings.Split(src, "\n") {
		line := raw
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		switch state {
		case protoParseStateTop:
			switch {
			case protoPreambleRe.MatchString(trimmed):
				// Not wire shape; ignore.
			case protoMessageOpenRe.MatchString(trimmed):
				name := protoMessageOpenRe.FindStringSubmatch(trimmed)[1]
				if _, exists := schema.messages[name]; exists {
					return nil, fmt.Errorf("line %d: duplicate message %q", i+1, name)
				}
				schema.messages[name] = map[int]protoField{}
				currentMessage = name
				state = protoParseStateMessage
			case protoServiceOpenRe.MatchString(trimmed):
				otherDepth = strings.Count(trimmed, "{") - strings.Count(trimmed, "}")
				if otherDepth > 0 {
					state = protoParseStateOther
				}
			default:
				return nil, fmt.Errorf(
					"line %d: unrecognized top-level proto construct %q -- the httpgrpc drift-guard "+
						"parser (pkg/util/httpgrpcpb/httpgrpcpb_drift_test.go) only understands flat "+
						"proto3 messages and a single service block; it needs updating to handle this "+
						"construct, and its author needs to confirm the construct doesn't change wire "+
						"shape, before this guard can be trusted again", i+1, trimmed)
			}
		case protoParseStateMessage:
			if trimmed == "}" {
				currentMessage = ""
				state = protoParseStateTop
				continue
			}
			m := protoFieldRe.FindStringSubmatch(trimmed)
			if m == nil {
				return nil, fmt.Errorf(
					"line %d: unrecognized construct inside message %s: %q -- the httpgrpc drift-guard "+
						"parser does not understand oneofs, maps, enums, nested messages, or reserved "+
						"statements; it needs updating to handle this construct, and its author needs to "+
						"confirm the construct doesn't change wire shape, before this guard can be "+
						"trusted again", i+1, currentMessage, trimmed)
			}
			num, err := strconv.Atoi(m[4])
			if err != nil {
				return nil, fmt.Errorf("line %d: invalid field number %q: %w", i+1, m[4], err)
			}
			if _, dup := schema.messages[currentMessage][num]; dup {
				return nil, fmt.Errorf("line %d: duplicate field number %d in message %s", i+1, num, currentMessage)
			}
			schema.messages[currentMessage][num] = protoField{
				Type:     m[2],
				Name:     m[3],
				Repeated: m[1] != "",
			}
		case protoParseStateOther:
			otherDepth += strings.Count(trimmed, "{") - strings.Count(trimmed, "}")
			if otherDepth <= 0 {
				state = protoParseStateTop
			}
		}
	}

	if state != protoParseStateTop {
		return nil, fmt.Errorf("unterminated block at end of file (parser state %d)", state)
	}
	return schema, nil
}
